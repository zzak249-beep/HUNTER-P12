"""P12 Hunter SCANNER — SOLO SEÑALES por Telegram (no envía órdenes, no necesita API keys de BingX).

Escanea todas las perpetuas 24/7 de BingX con el motor P12 (engine.py), te avisa cuando hay entrada
y tú operas a mano. Cada señal se sigue "en papel" (SL / parcial / TP / cierre 15:55) para medir la
expectativa REAL en R: /stats te dice si hay ventaja o no, con estadística y no con intuición.

Velocidad: peticiones en paralelo con límite global de ritmo, y sondeo por niveles:
  · "calientes" (con sesgo del día, dentro de la ventana de entrada, o con señal en seguimiento): cada vela 5m
  · resto: cada 15 min, y todas cada 5 min entre 09:00 y 10:00 NY (cuando se decide el sesgo del día)
"""
import logging
import math
import os
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import ccxt

from config import Config
from engine import BAR_MS, NY, Engine, in_rng, ny_mins
from exchange import fetch_candles, public_exchange
from main import select_symbols
from notifier import Notifier
from state import Store
from trader import fp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("scanner")

VERSION = "P12 SCANNER v1.0 (solo señales · motor v4.6)"
MIN_BARS = 3 * 288
MIN_COVERAGE = 0.97


def _f(k, d):
    return float(os.getenv(k, d))


# ───────────────────────── red: límite global + ccxt por hilo ─────────────────────────
class Limiter:
    def __init__(self, per_sec):
        self.gap, self.nxt, self.lock = 1.0 / per_sec, 0.0, threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.nxt)
            self.nxt = t + self.gap
        if t > now:
            time.sleep(t - now)


class Feed:
    """Expone fetch_ohlcv (lo único que usa fetch_candles) con un ccxt por hilo y un limitador compartido."""

    def __init__(self, base, lim):
        self.base, self.lim, self.loc = base, lim, threading.local()

    def fetch_ohlcv(self, *a, **k):
        ex = getattr(self.loc, "ex", None)
        if ex is None:
            ex = ccxt.bingx({"enableRateLimit": False, "options": {"defaultType": "swap"}})
            ex.set_markets(self.base.markets)
            self.loc.ex = ex
        self.lim.wait()
        return ex.fetch_ohlcv(*a, **k)


# ───────────────────────── scanner ─────────────────────────
class Scanner:
    def __init__(self, cfg, notify, store, base):
        self.cfg, self.n, self.store, self.base = cfg, notify, store, base
        self.feed = Feed(base, Limiter(_f("SCAN_RATE", "8")))
        self.pool = ThreadPoolExecutor(max_workers=int(_f("SCAN_WORKERS", "12")))
        self.max_day = int(_f("MAX_SIGNALS_DAY", "6"))
        self.equity = _f("ACCOUNT_EQUITY", str(cfg.equity_override or cfg.paper_equity))
        self.local = ZoneInfo(cfg.tz_local)
        self.U, self.skipped, self.pending = {}, {}, []
        d = store.d
        for k, v in (("sent", {}), ("tracks", {}), ("sig_log", []), ("day_sides", {}), ("paused", False)):
            d.setdefault(k, v)
        self.summary_day = None
        self.cap_warned = None
        self.last_refresh = self.last_progress = time.time()
        self.last_warn = 0.0

    # ── red ──
    def _fetch(self, sym, since):
        return fetch_candles(self.feed, sym, since)

    def _warm_fetch(self, sym, now, days):
        return [b for b in self._fetch(sym, now - days * 86_400_000) if b.t + BAR_MS <= now]

    def _build(self, sym, bars):
        d = self.store.d
        if len(bars) < MIN_BARS and sym not in d["tracks"]:
            self.skipped[sym] = "poco historial"
            return
        expected = (bars[-1].t - bars[0].t) // BAR_MS + 1
        if len(bars) / expected < MIN_COVERAGE and sym not in d["tracks"]:
            self.skipped[sym] = "no opera 24/7 (huecos en velas)"
            return
        u = SimpleNamespace(sym=sym, eng=Engine(self.cfg), last_t=bars[-1].t, next_poll=0)
        for b in bars:
            self._process(u, b, live=False)
        u.eng.traded = u.eng.traded or f"{sym}|{u.eng.day_id}" in d["sent"] or sym in d["tracks"]
        self.U[sym] = u

    def setup(self):
        now = int(time.time() * 1000)
        syms = select_symbols(self.cfg, self.base)
        for s in self.store.d["tracks"]:
            if s not in syms:
                syms.insert(0, s)
        syms = [s for s in syms if s in self.base.markets]
        warm = self.cfg.warmup_days if len(syms) <= 10 else min(self.cfg.warmup_days, 10)
        self.warm = warm
        self.n.send(f"⏳ Calentando {len(syms)} símbolos con {warm} días de velas…")
        t0 = time.time()
        futs = {self.pool.submit(self._warm_fetch, s, now, warm): s for s in syms}
        for i, f in enumerate(as_completed(futs), 1):
            s = futs[f]
            try:
                self._build(s, f.result())
            except Exception as e:
                self.skipped[s] = f"error: {str(e)[:60]}"
                log.warning("warm %s: %s", s, e)
            if i % 25 == 0:
                log.info("calentados %d/%d", i, len(syms))
        if not self.U:
            raise SystemExit("Ningún símbolo válido")
        self.n.send(f"✅ Scanner listo: {len(self.U)} monedas vigiladas · {len(self.skipped)} descartadas "
                    f"(no 24/7, poco historial o error) · {time.time() - t0:.0f}s")

    # ── procesado de velas ──
    def _process(self, u, b, live, fresh=False):
        sig, _ = u.eng.on_bar(b)
        self._track(u, b)
        if sig and live and fresh:
            self._signal(u, b, sig)

    def _loc(self, hhmm):
        d = datetime.now(NY)
        t = d.replace(hour=hhmm // 100, minute=hhmm % 100, second=0, microsecond=0)
        return t.astimezone(self.local).strftime("%H:%M")

    def _signal(self, u, b, s):
        d, c, e = self.store.d, self.cfg, u.eng
        key = f"{u.sym}|{s['day_id']}"
        if key in d["sent"] or u.sym in d["tracks"]:
            e.mark_traded()
            return
        if d["paused"]:
            return
        sides = d["day_sides"].setdefault(s["day_id"], [])
        if len(sides) >= self.max_day:
            if self.cap_warned != s["day_id"]:
                self.cap_warned = s["day_id"]
                self.n.send(f"ℹ️ Límite de {self.max_day} señales/día alcanzado: no aviso más hoy (MAX_SIGNALS_DAY).")
            return
        e.mark_traded()
        sd, entry, sl, tp, rU = s["side"], s["entry_adj"], s["stop"], s["tp"], s["r_unit"]
        cost_r = c.cost_rt / 100 * entry / rU
        sides.append(sd)
        d["sent"][key] = s["day_id"]
        d["tracks"][u.sym] = dict(side=sd, entry=entry, sl=sl, tp=tp, rU=rU, t=b.t, day=s["day_id"],
                                  partial=False, gain=0.0, cost_r=cost_r)
        self.store.save()

        name = u.sym.split("/")[0]
        q = self.equity * c.risk_pct / 100 / rU * s["sz_mult"]
        notional, margin = q * entry, q * entry / c.leverage
        same = sum(1 for x in sides if x == sd)
        delay = int(time.time() - (b.t + BAR_MS) / 1000)
        ppx = entry + sd * c.partial_r * rU
        txt = [f"{'🟢 <b>COMPRA (LONG)</b>' if sd == 1 else '🔴 <b>VENTA (SHORT)</b>'} · <b>{name}</b>  (BingX {name}-USDT perp)",
               f"Entrada ≈ <b>{fp(entry)}</b>  (a mercado; referencia = cierre de la vela 5m, hace {delay}s)",
               f"SL <b>{fp(sl)}</b>  ({rU / entry * 100:.2f}% = 1R)",
               f"TP <b>{fp(tp)}</b>  (+{abs(tp - entry) / rU:.1f}R)"
               + (f"\nParcial: cierra {c.partial_pct:.0f}% en {fp(ppx)} (+{c.partial_r:g}R)" if c.partial_r > 0 else ""),
               f"Coste ≈ {cost_r:.2f}R · apertura {'coincide' if e.coinc else f'NO coincide → tamaño ×{s['sz_mult']:g}'}",
               f"Tamaño sugerido: arriesga {c.risk_pct * s['sz_mult']:.2f}% de ${self.equity:,.0f} ≈ ${self.equity * c.risk_pct / 100 * s['sz_mult']:.2f} "
               f"→ {q:.6g} {name} (nocional ${notional:,.0f}, margen a {c.leverage}x ≈ ${margin:,.0f})"
               + ("\n⚠️ El margen supera tu saldo: baja el tamaño." if margin > self.equity else ""),
               f"Lectura: {e.scen_text()} · {e.read_text()} · zona {fp(s['zEdge'])}–{fp(s['zDeep'])}",
               f"Cierra a más tardar a las {self._loc(c.force_exit)} (hora {c.tz_local})",
               f"Señales hoy: {len(sides)} ({same} en esta dirección). Las altcoins se mueven con BTC: "
               f"riesgo total recomendado ≤ {c.max_total_risk}%.",
               "<i>No entres si el precio ya perdió el SL o llegó al TP.</i>"]
        self.n.send("\n".join(txt))

    # ── seguimiento en papel ──
    def _track(self, u, b):
        d, c = self.store.d, self.cfg
        t = d["tracks"].get(u.sym)
        if not t or b.t <= t["t"]:
            return
        sd, sl, tp, rU, entry = t["side"], t["sl"], t["tp"], t["rU"], t["entry"]
        name = u.sym.split("/")[0]
        px = why = None
        if in_rng(ny_mins(b.t), u.eng.exitM, u.eng.p12S):
            px, why = b.o, "Cierre horario"
        elif (b.l <= sl) if sd == 1 else (b.h >= sl):          # SL primero: criterio conservador
            px, why = sl - sd * sl * c.stop_slip / 100, "SL"
        elif (b.h >= tp) if sd == 1 else (b.l <= tp):
            px, why = tp, "TP"
        elif c.partial_r > 0 and not t["partial"]:
            lvl = entry + sd * c.partial_r * rU
            if (b.h >= lvl) if sd == 1 else (b.l <= lvl):
                t["partial"], t["gain"] = True, c.partial_pct / 100 * c.partial_r
                self.store.save()
                self.n.send(f"✅ <b>{name}</b> alcanzó +{c.partial_r:g}R: toma parcial ({c.partial_pct:.0f}%).")
        if px is None:
            return
        left = 1 - (c.partial_pct / 100 if t["partial"] else 0)
        r = t["gain"] + left * sd * (px - entry) / rU - t["cost_r"]
        fh = c.fund_h * 3_600_000
        nf = int((b.t + BAR_MS) // fh - (t["t"] + BAR_MS) // fh)
        r -= nf * c.fund_pct / 100 * entry / rU
        d["sig_log"].append([t["day"], u.sym, sd, round(r, 3), why])
        d["sig_log"] = d["sig_log"][-3000:]
        del d["tracks"][u.sym]
        self.store.save()
        self.n.send(f"{'✓' if r > 0 else '✗'} <b>{name}</b> {why} · {r:+.2f}R (seguimiento en papel, netos de coste/funding)")

    # ── estadística ──
    def stats_text(self, arg=""):
        lg = self.store.d["sig_log"]
        rs = [x[3] for x in lg]
        n = len(rs)
        if not n:
            return "Aún no hay señales cerradas en seguimiento."
        w = sum(x for x in rs if x > 0)
        l = sum(-x for x in rs if x <= 0)
        eq = pk = dd = 0.0
        for x in rs:
            eq += x
            pk, dd = max(pk, eq), max(dd, pk - eq)
        m = sum(rs) / n
        sdv = statistics.stdev(rs) if n > 2 else 0
        t = m / (sdv / math.sqrt(n)) if sdv > 0 else 0.0
        need = f"≈ {math.ceil((3 * sdv / m) ** 2)} señales" if m > 0 and sdv > 0 else "con E ≤ 0 no hay muestra que valga"
        verdict = ("⚠️ muestra pequeña (<30): no concluyas nada" if n < 30 else
                   "✅ ventaja estadística plausible" if t >= 3 and m > 0 else
                   "❌ sin ventaja demostrada" if t < 2 else "🟡 indicio débil")
        last = " ".join(f"{x[1].split('/')[0]}{x[3]:+.1f}" for x in lg[-10:])
        return (f"<b>Seguimiento en papel</b> · {n} señales · acierto {100 * sum(x > 0 for x in rs) / n:.0f}%\n"
                f"E {m:+.3f}R · PF {(w / l if l else float('inf')):.2f} · total {sum(rs):+.1f}R · DD {dd:.1f}R · t {t:+.2f}\n"
                f"Para t≥3 con este perfil: {need}\n{verdict}\nÚltimas: {last}")

    # ── bucle ──
    def hot(self, u, now):
        e = u.eng
        if u.sym in self.store.d["tracks"]:
            return True
        return (e.open_done and e.bias != 0 and e.sz_mult > 0 and not e.traded and not e.dead and not e.eod_done
                and in_rng(ny_mins(now), e.openM, e.entE + 5))

    def next_poll(self, u, now):
        due = u.last_t + 2 * BAR_MS + 3000                      # cierre de la siguiente vela + 3s
        if self.hot(u, now) or 540 <= ny_mins(now) < 600:
            return due if due > now else now + 5000
        return now + 900_000

    def tick(self):
        now = int(time.time() * 1000)
        due = [u for u in self.U.values() if u.next_poll <= now]
        due.sort(key=lambda u: 0 if self.hot(u, now) else 1)
        futs = {self.pool.submit(self._fetch, u.sym, u.last_t + 1): u for u in due}
        for f in as_completed(futs):
            u = futs[f]
            nowm = int(time.time() * 1000)
            try:
                rows = f.result()
            except Exception as e:
                log.warning("fetch %s: %s", u.sym, e)
                u.next_poll = nowm + 30_000
                continue
            for b in rows:
                if b.t > u.last_t and b.t + BAR_MS <= nowm - 1000:
                    self._process(u, b, True, nowm - (b.t + BAR_MS) < 240_000)
                    u.last_t = b.t
                    self.last_progress = time.time()
            u.next_poll = self.next_poll(u, nowm)
        for _ in range(2):                                      # altas nuevas, pocas por vuelta
            if self.pending:
                s = self.pending.pop(0)
                try:
                    self._build(s, self._warm_fetch(s, now, self.warm))
                except Exception as e:
                    self.skipped[s] = f"error: {str(e)[:60]}"
        if time.time() - self.last_refresh > 86_400:
            self.last_refresh = time.time()
            try:
                self.pending = [s for s in select_symbols(self.cfg, self.base) if s not in self.U and s not in self.skipped]
            except Exception as e:
                log.warning("refresh: %s", e)
        if time.time() - self.last_progress > 900 and time.time() - self.last_warn > 3600:
            self.last_warn = time.time()
            self.n.send("⚠️ Hace más de 15 min que no llegan velas nuevas de BingX. Revisa los logs de Railway.")
        self.summary(now)

    def summary(self, now):
        d = datetime.fromtimestamp(now / 1000, NY)
        if not (580 <= d.hour * 60 + d.minute < 720) or self.summary_day == d.date():
            return
        self.summary_day = d.date()
        st = self.store.d                                       # limpia memoria antigua (>7 días)
        cut = sorted(st["day_sides"])[-7:][0] if st["day_sides"] else ""
        st["day_sides"] = {k: v for k, v in st["day_sides"].items() if k >= cut}
        st["sent"] = {k: v for k, v in st["sent"].items() if v >= cut}
        self.store.save()
        if self.cfg.skip_weekends and d.isoweekday() >= 6:
            self.n.send("📊 <b>P12</b> Fin de semana: no se opera.")
            return
        rd = [u for u in self.U.values() if u.eng.open_done]
        cands = [u for u in rd if u.eng.bias != 0 and u.eng.sz_mult > 0 and not u.eng.dead]
        cands.sort(key=lambda u: abs(u.eng.zDeep - u.eng.stopPx) / u.eng.zDeep)
        rows = [f"{'▲' if u.eng.bias == 1 else '▼'} {u.sym.split('/')[0]}  zona {fp(u.eng.zEdge)}–{fp(u.eng.zDeep)}  SL {fp(u.eng.stopPx)}"
                for u in cands[:25]]
        self.n.send(f"📊 <b>P12 · resumen 09:40 NY</b>\n{len(rd)}/{len(self.U)} monedas leídas · "
                    f"<b>{len(cands)}</b> con operación posible hasta 12:00 NY (te aviso cuando entren en zona)\n"
                    + "\n".join(rows) + ("\n…" if len(cands) > 25 else ""))

    # ── comandos ──
    def g_status(self, arg=""):
        d = self.store.d
        tr = [f"{'▲' if t['side'] == 1 else '▼'} {s.split('/')[0]} @ {fp(t['entry'])} SL {fp(t['sl'])} TP {fp(t['tp'])}"
              for s, t in d["tracks"].items()]
        return (f"<b>{VERSION}</b>\n{len(self.U)} monedas · {len(self.skipped)} descartadas · pausa: {'SÍ' if d['paused'] else 'no'}\n"
                f"Señales en seguimiento ({len(tr)}):\n" + ("\n".join(tr) if tr else "ninguna"))

    def g_pause(self, arg=""):
        self.store.d["paused"] = True
        self.store.save()
        return "⏸ Sin nuevas señales (las que siguen abiertas se siguen vigilando)."

    def g_resume(self, arg=""):
        self.store.d["paused"] = False
        self.store.save()
        return "▶️ Señales reanudadas."


def main():
    cfg = Config()
    cfg.symbols = cfg.symbols or "ALL"                          # el scanner vigila todo por defecto
    log.info("=== %s · SYMBOLS=%s ===", VERSION, cfg.symbols)
    notify = Notifier(cfg.tg_token, cfg.tg_chat)
    err = notify.check()
    if err:
        log.error("TELEGRAM NO FUNCIONA: %s", err)
    path = os.getenv("SCANNER_STATE", "/data/scanner_state.json" if os.path.isdir("/data") else "scanner_state.json")
    sc = Scanner(cfg, notify, Store(path), public_exchange(cfg))
    try:
        sc.setup()
    except BaseException as e:
        log.exception("fallo al arrancar")
        notify.send(f"🚨 El scanner no pudo arrancar: {type(e).__name__}: {str(e)[:300]}")
        raise
    notify.start_commands({
        "/status": sc.g_status, "/stats": sc.stats_text, "/pause": sc.g_pause, "/resume": sc.g_resume,
        "/ping": lambda a="": "pong ✅ el scanner está vivo",
        "/symbols": lambda a="": f"{len(sc.U)} activas: " + ", ".join(s.split('/')[0] for s in list(sc.U)[:80]),
        "/help": lambda a="": "/ping · /status · /stats · /symbols · /pause · /resume"})
    notify.send(f"🤖 <b>{VERSION}</b> iniciado · {len(sc.U)} monedas · solo señales (no opera)\n/help para comandos")
    last_err = 0
    while True:
        try:
            sc.tick()
        except Exception as e:
            log.exception("loop")
            if time.time() - last_err > 900:
                notify.send(f"⚠️ Error en el bucle: {e}")
                last_err = time.time()
        time.sleep(2)


if __name__ == "__main__":
    main()
