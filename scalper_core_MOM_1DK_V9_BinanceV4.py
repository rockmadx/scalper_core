#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
alpha_core_v8_plugins.py
========================
"Bilimsel + Yapısal + Olasılık-temelli" Scalper çekirdeği (USDT-M Perp)
→ Plugin tabanlı strateji setleri + kademeli giriş (3-6-10) + breadth kapısı

Boru hattı (özet):
- Evren tarama (BookTicker akışı) + Top-K Ön Eleme (r1m/uptick/spread)
- Çok-zamanlı bağlam: EMA/ADX/Donchian, LinReg kanal, (Anchored) VWAP
- Strateji setleri (plugin): TREND / BO / MR / CORRCH / (opsiyonel Forecast)
- Her set: p_win + TP/SL önerisi + kalite → EV_R ile tek skora indirgeme
- Bandit/Router: set ağırlıkları (basit: rule-based) [ileri faz: LinUCB/TS]
- Kademeli giriş: 3-6-10 (risk birimi 10’a bölünür; 3/3/4 dağıtım)
- Yönetim: BE/win-lock trail, stall, zaman stop, breadth flatten (oran+büyüklük)
- Telemetri: Heartbeat ve Supabase kapanış insert kuyruğu (opsiyonel)

Bağımlılıklar:
- numpy (opsiyonel, hız için). Yoksa saf Python çalışır.
- pandas (opsiyonel, bazı hesapları kolaylaştırır).
- statsmodels (opsiyonel, CORRCH için OLS; yoksa manuel OLS)
- binance_io_V2.py: BookTicker, get_futures_klines, parse_klines, consume_bookticker

© size özel tasarım
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple, Set, Any
from entry_gates import DynamicGates, RejectionStats, CandidateMetrics, COOLDOWN_TOUCH_SEC



# ---------- Opsiyonel bilim kütüphaneleri
try:
    import numpy as _np
except Exception:
    _np = None

try:
    import pandas as _pd
except Exception:
    _pd = None

try:
    import statsmodels.api as _sm  # OLS (opsiyonel)
except Exception:
    _sm = None

# ---------- Binance I/O (harici modül)
from binance_io_V5 import (
    BookTicker,              # dataclass: symbol, bid_price, ask_price, ts
    get_futures_klines,      # (symbol, interval, limit) -> raw
    parse_klines,            # raw -> List[dict{o,h,l,c,volume,quote,ts}]
    consume_bookticker,      # akış (coalesced/batched)
)

# ---------- Supabase logger (opsiyonel)
SUPABASE_LOG_ON = True
PROJECT_ID = "scalper_core_MOM_1DK_V9_BinanceV4"
try:
    from trade_logger import log_closed_trade, safe_log_entry_reject  # <— EKLE
except Exception as _e:
    log_closed_trade = None
    safe_log_entry_reject = None
    logging.warning("trade_logger import edilemedi (%s). Supabase insert devre dışı.", _e)

# --- optional trace helper (debug için) ---
TRACE_SYMBOLS = set()  # ör: {"HEMIUSDT"} veya {"*"} hepsi için
def _trace(sym: str, msg: str):
    if not TRACE_SYMBOLS: 
        return
    if ("*" in TRACE_SYMBOLS) or (sym in TRACE_SYMBOLS):
        logging.info(f"[TRACE {sym}] {msg}")


# ====================== Parametreler ======================
ONE_TRADE_PER_SYMBOL = True
# Zaman
WINDOW_SEC = 60.0
FRESH_TICK_SEC = 12

# Evren/Ön eleme
PREFILTER_TOPK = 20
R1M_MIN_PREFILTER = 0.30   # %0.30
UPTICK_MIN_PREFILTER = 0.52
MIN_QUOTE_VOL_5M = 100_000.0   # 5m ort quote volume filtresi
MAX_SPREAD_FRAC  = 0.0015      # (ask-bid)/mid ≤ %0.15
MIN_PRICE        = 0.0

# Breadth (oran + büyüklük, yumuşatılmış)
BREADTH_RATIO_GATE_ON   = True
BREADTH_ADV_RATIO_MIN   = 0.60  # giriş için min efektif oran (adv/ema karışık)
BREADTH_ADV_RATIO_EXIT  = 0.45  # flatten eşik (oran)
BREADTH_MAX_NEG_RATIO_VETO = 0.40
BREADTH_SLOPE_LOOKBACK  = 5
BREADTH_LOG_SEC         = 10.0
MIN_BREADTH_N           = 200  # minimum örneklem (N)

# Kalıcılık & EMA (breadth)
BREADTH_EMA_HALF_LIFE_SEC = 20.0
BREADTH_LOSS_MIN_SEC      = 12.0
BREADTH_LOSS_HARD_SEC     = 25.0
BREADTH_STRONG_NEG_PAD    = 0.05
PNL_CUSHION_R_GRACE       = 0.50
ADV_TOL_IF_CUSHION        = 0.03
NEG_TOL_IF_CUSHION        = 0.03

# Büyüklük tabanlı flatten (yalın kural)
# "AND" bağla: hem düşenlerin oranı hem de ortalama düşüş seviyesi
BREADTH_EARLY_EXIT_NEG_RATIO = 0.55    # DOWN/N ≥ 0.55
BREADTH_EARLY_EXIT_AVG_DN    = -0.50   # avg_dn ≤ -0.50% (ortalama düşüş büyüklüğü)

# 1m erken ralli filtresi (momentum)
ATR_Z_MIN = 0.60
VOL_RATIO_MIN = 1.15

# MTF göstergeler
ADX_N = 14
DONCHIAN_FAST = 20
DONCHIAN_SLOW = 55
EMA_FAST = 20
EMA_MID  = 50
EMA_SLOW = 200
REG_WIN_15M = 120   # 120x15m ≈ 30 saat
REG_SIGMA_K = 1.6

# Risk/Emir/Slot
START_EQUITY = 10_000.0
RISK_PER_TRADE_PCT = 1.0
SLIPPAGE_PCT = 0.01
TAKER_FEE_PCT = 0.05
GLOBAL_ENTRY_GAP_SEC = 60.0
TRACK_TIMEOUT_SEC = 60*60*24

# Hedef/Stop (varsayılan, setler override edebilir)
TP_MIN_PCT = 1.5
TP_MAX_PCT = 2.8
TP_ATR_MULT = 2.0
SL_ATR_MULT = 2.0
SL_MIN_PCT = 2.5
SL_MAX_PCT = 4.2

# Yönetim
LOG_EVERY_SEC = 5.0
WINLOCK_TP_FRACTION = 0.8
WINLOCK_SL_TP_FRAC = 0.25
BE_R_HOT = 0.25
BE_R_NORMAL = 0.30
BE_R_COLD = 0.40

PREFILTER_LOG_PER_SYMBOL = False

# Stage ve slotlar
STAGE_LIMITS = {1: 3, 2: 6, 3: 10}
STAGE_POS_EPS_R = 0.05

# Benzerlik kısıtı (BTC beta cluster)
CLUSTER_SIM_THRESHOLD = 0.90
CLUSTER_MAX_SLOTS = 3

# Bandit (basit epsilon-greedy; ileri faz: contextual bandit)
EPSILON = 0.10
POLICY_SETS = {
    # kind= etiketi yalnız yönetsel; asıl TP/SL ve p_win strateji içinde şekillenir
    "BREAKOUT": dict(kind="BO", burst_min_pct=0.50, uptick_floor=0.51, tp_atr_mult=2.2, sl_atr_mult=1.8),
    "PULLBACK": dict(kind="PB", burst_min_pct=0.35, uptick_floor=0.55, tp_atr_mult=2.0, sl_atr_mult=2.0),
    "MEANREV" : dict(kind="MR", burst_min_pct=0.20, uptick_floor=0.48, tp_atr_mult=1.8, sl_atr_mult=2.2),
}
POLICY_KEYS = tuple(POLICY_SETS.keys())

# ==== Adaptif mom/hacim eşikleri (yüzdelik + rejim tabanı) ====
USE_MOMVOL_PCTL = True
MOMVOL_PCTL = 0.70  # 70. yüzdelik

# Rejim tabanları (yüzdelik düşük kalırsa en az bunlar uygulanır)
Z_MIN_BY_REG    = dict(HOT=0.80, NORMAL=0.55, COLD=0.35)   # z_tr tabanı
VOLR_MIN_BY_REG = dict(HOT=1.25, NORMAL=1.10, COLD=0.95)   # volR tabanı



# Logistic p ağırlıkları (başlangıç heuristiği, aggregator kullanabilir)
LOGIT_W = dict(
    ztr = 1.00,
    vol = 0.80,
    adx = 0.40,
    conf= 0.60,
    alpha=0.35,
    upt= 0.30,
    pos = 0.20,  # reg kanal konumu (işaretli)
    trend=0.40,
    bias = -0.30,  # intercept
)

# Gün içi risk kontrol
DAILY_LOCK_IN_1 = 3.0
DAILY_LOCK_IN_2 = 5.0
DD_STEP_1 = -2.0
DD_STEP_2 = -3.0

# Kademeli giriş (3-6-10)
# Ana risk birimi 10’a bölünür; 3/3/4 dağıtım (notional ya da adet ile)
KADEMELI_ENABLED = True
KADEMELI_SIZES = (3, 3, 4)    # toplam 10 birim
KADEMELI_2_TRIGGER_ATR = 0.50 # K2: lehe ≥ 0.5*ATR gidiş veya alternatif teyit
KADEMELI_3_NEAR_T1_FRAC = 0.50 # K3: T1’e %50 yaklaşma veya CH_BREAK teyidi

# --- Pasif evren taraması (dokunulmayan ama taze olanlar da aday olsun)
PASSIVE_SCAN_ON = True
PASSIVE_MAX_AGE_SEC = 12.0   # son fiyatın en fazla bu kadar “taze” olmasını iste
PASSIVE_SCAN_TOPK = 40       # pasif taramada en çok bu kadar sembol üst sıralara alınır

# ====================== Veri sınıfları ======================

def now_ts() -> float:
    return time.time()

def pct(x: float) -> str:
    return f"{x:+.2f}%"

def safe_log(x: float) -> float:
    if x <= 1e-12:
        return -27.0
    try:
        return math.log(x)
    except Exception:
        return -27.0

@dataclass
class PricePoint:
    ts: float
    price: float

@dataclass
class Trade:
    trade_id: int
    symbol: str
    entry_ts: float
    entry_price_ref: float
    qty: float
    tp_price_ref: float
    sl_price_ref: float
    risk_usdt: float
    notional_entry: float
    side: str = "LONG"
    # canlı
    last_price: float = 0.0
    mtm_usdt: float = 0.0
    mtm_R: float = 0.0
    # istatistik
    max_up_pct: float = 0.0
    max_dn_pct: float = 0.0
    last_log_ts: float = field(default_factory=lambda: 0.0)
    # metrikler
    open_reason: str = ""
    score: float = 0.0
    r1m: float = 0.0
    atr5m: float = 0.0
    z1m: float = 0.0
    vshock: float = 0.0
    upt: float = 0.0
    trend: float = 0.0
    volr: float = 1.0
    policy_name: str = "BREAKOUT"
    vol_regime: str = "NORMAL"
    breadth_state: str = "NEU"
    # bayraklar
    be_applied: bool = False
    winlock_applied: bool = False
    # kademeli
    ladder_k: int = 1    # 1,2,3 hangi kademedeyiz
    base_qty_10: float = 0.0  # 10 birim karşılığı baz adet (risk bölüştürmede kullanılabilir)
    t1_price_hint: float = 0.0

    def update_excursions(self, last_price: float) -> None:
        if self.entry_price_ref <= 0:
            return
        chg = (last_price / self.entry_price_ref - 1.0) * 100.0
        if chg > self.max_up_pct:
            self.max_up_pct = chg
        if chg < self.max_dn_pct:
            self.max_dn_pct = chg

# ====================== Bar cache & indikatörler ======================

# ====================== Bar cache & indikatörler ======================
_BAR_TTL = {"1m": 12.0, "5m": 45.0, "15m": 90.0}
_bar_cache: Dict[Tuple[str, str], Tuple[float, List[dict]]] = {}

def get_bars(symbol: str, interval: str, limit: int) -> List[dict]:
    """
    Interval'e göre TTL’li yerel cache; sonra binance_io_V2/V4 get_futures_klines çağrısı.
    🔧 Önemli: Eğer fetch boş dönerse, önceki (varsa) cache'i KORUR ve boşu cache'lemez.
    """
    key = (symbol, interval)
    now = now_ts()
    ts, bars = _bar_cache.get(key, (0.0, []))
    if (now - ts) < _BAR_TTL.get(interval, 30.0) and bars:
        return bars

    payload = get_futures_klines(symbol, interval, limit)
    parsed = parse_klines(payload) or []

    # boş geldiyse cache'i bozma; önceki barları döndür
    if parsed:
        _bar_cache[key] = (now, parsed)
        return parsed
    else:
        # logging.debug("get_bars EMPTY %s %s L=%d (keep old %d)", symbol, interval, limit, len(bars))
        return bars

def _percentile(xs: List[float], p: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    if p <= 0:  return xs[0]
    if p >= 1:  return xs[-1]
    k = (len(xs) - 1) * p
    f = math.floor(k); c = math.ceil(k)
    if f == c: return xs[int(k)]
    return xs[f] * (c - k) + xs[c] * (k - f)

def _true_range(h: float, l: float, pc: float) -> float:
    return max(h - l, abs(h - pc), abs(l - pc))

def atr_from_bars(bars: List[dict], n: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    trs = []
    pc = bars[0]["c"]
    for i in range(1, len(bars)):
        b = bars[i]
        tr = _true_range(b["h"], b["l"], pc)
        trs.append(tr)
        pc = b["c"]
    if not trs:
        return 0.0
    if n <= 0:
        n = len(trs)
    use = trs[-n:]
    return sum(use) / len(use)

def atr_pct_last(bars: List[dict], n: int = 14) -> float:
    atr = atr_from_bars(bars, n)
    last_close = bars[-1]["c"] if bars else 0.0
    return (atr / (last_close or 1e-12)) * 100.0

def ema(xs: List[float], n: int) -> List[float]:
    if not xs:
        return []
    k = 2 / (n + 1)
    out = [xs[0]]
    for i in range(1, len(xs)):
        out.append(out[-1] + k * (xs[i] - out[-1]))
    return out

def adx_from_bars(bars: List[dict], n: int = 14) -> float:
    if len(bars) < n + 2:
        return 0.0
    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(bars)):
        up = bars[i]["h"] - bars[i - 1]["h"]
        dn = bars[i - 1]["l"] - bars[i]["l"]
        plus_dm.append(max(up, 0.0) if up > dn else 0.0)
        minus_dm.append(max(dn, 0.0) if dn > up else 0.0)
        trs.append(_true_range(bars[i]["h"], bars[i]["l"], bars[i - 1]["c"]))

    def rma(arr: List[float], n: int) -> List[float]:
        if not arr or n <= 0 or len(arr) < n:
            return []
        out = [sum(arr[:n]) / n]
        for i in range(n, len(arr)):
            out.append((out[-1] * (n - 1) + arr[i]) / n)
        return out

    tr_n = rma(trs, n)
    if not tr_n:
        return 0.0
    pdi = []
    mdi = []
    for i in range(len(tr_n)):
        idx = i + (len(trs) - len(tr_n))
        pdm = sum(plus_dm[idx - n + 1 : idx + 1]) if idx - n + 1 >= 0 else 0.0
        mdm = sum(minus_dm[idx - n + 1 : idx + 1]) if idx - n + 1 >= 0 else 0.0
        trv = sum(trs[idx - n + 1 : idx + 1]) if idx - n + 1 >= 0 else 1e-12
        pdi.append(100.0 * (pdm / trv))
        mdi.append(100.0 * (mdm / trv))
    dx = [(abs(p - m) / (p + m + 1e-12)) * 100.0 for p, m in zip(pdi, mdi)]
    if len(dx) < n:
        return 0.0
    adx = sum(dx[-n:]) / n
    return adx

def donchian(bars: List[dict], n: int = 20) -> Tuple[float, float]:
    highs = [b["h"] for b in bars[-n:]] if len(bars) >= n else [b["h"] for b in bars]
    lows  = [b["l"] for b in bars[-n:]] if len(bars) >= n else [b["l"] for b in bars]
    return (max(highs) if highs else 0.0, min(lows) if lows else 0.0)

def linreg_channel(closes: List[float], k_sigma: float = 1.6) -> Tuple[float, float, float, float, float]:
    """Döndür: (mid_last, slope, sigma, up_last, dn_last)"""
    n = len(closes)
    if n < 10:
        last = closes[-1] if closes else 0.0
        return last, 0.0, 0.0, last, last
    xs = list(range(n))
    if _np is not None:
        x = _np.array(xs, dtype=float); y = _np.array(closes, dtype=float)
        x = (x - x.mean())
        b = (x @ y) / ((x * x).sum() or 1e-12)
        a = y.mean()
        fit = a + b * x
        resid = y - fit
        sigma = float((resid**2).mean() ** 0.5)
        mid_last = float(fit[-1]); up_last = mid_last + k_sigma * sigma; dn_last = mid_last - k_sigma * sigma
        return mid_last, float(b), float(sigma), float(up_last), float(dn_last)
    # saf Python
    xm = sum(xs)/n; ym = sum(closes)/n
    num = sum((xi - xm) * (yi - ym) for xi, yi in zip(xs, closes))
    den = sum((xi - xm) ** 2 for xi in xs) or 1e-12
    b = num / den; a = ym - b * xm
    fit = [a + b * xi for xi in xs]
    resid = [yi - fi for yi, fi in zip(closes, fit)]
    sigma = (sum(r * r for r in resid) / n) ** 0.5
    mid_last = fit[-1]; up_last = mid_last + k_sigma * sigma; dn_last = mid_last - k_sigma * sigma
    return mid_last, b, sigma, up_last, dn_last

def anchored_vwap(bars: List[dict], anchor_idx: int) -> float:
    """Basit AVWAP: Σ(P*V)/ΣV, p=(H+L+C)/3"""
    if not bars:
        return 0.0
    anchor_idx = max(0, min(anchor_idx, len(bars) - 1))
    pv = 0.0; vv = 0.0
    for b in bars[anchor_idx:]:
        p = (b["h"] + b["l"] + b["c"]) / 3.0
        v = b.get("volume", 0.0)
        pv += p * v; vv += v
    return (pv / (vv or 1e-12))

def find_swing_anchor(bars: List[dict], atr_pct_thresh: float = 1.0) -> int:
    """Son belirgin swing'i yaklaşıkla: ATR% eşiğine göre dönüş aranır; index döner."""
    if len(bars) < 8:
        return max(0, len(bars) - 1)
    atrp = atr_pct_last(bars, 14)
    thr = max(atr_pct_thresh, atrp)
    closes = [b["c"] for b in bars]
    idx = len(bars) - 1
    lo_i = hi_i = idx
    for i in range(idx - 2, max(0, idx - 30), -1):
        change = (closes[idx] / closes[i] - 1.0) * 100.0
        if change <= -thr:
            lo_i = i; break
        if change >= +thr:
            hi_i = i; break
    if abs(closes[idx] - bars[lo_i]["l"]) < abs(closes[idx] - bars[hi_i]["h"]):
        return lo_i
    return hi_i

def confluence_score(price: float, atr_abs: float, levels: List[float]) -> float:
    """Seviyelere yakınlık skorlaması: mesafe/ATR ile ceza; yakın olanlar +."""
    if atr_abs <= 0:
        return 0.0
    score = 0.0
    for lv in levels:
        if lv <= 0:
            continue
        d_atr = abs(price - lv) / (atr_abs or 1e-12)
        if d_atr < 0.2:
            score += 1.0
        elif d_atr < 0.5:
            score += 0.6
        elif d_atr < 1.0:
            score += 0.2
    if len([1 for lv in levels if lv > 0 and abs(price - lv) / (atr_abs or 1e-12) < 0.35]) >= 3:
        score *= 1.15
    return score

def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    else:
        z = math.exp(x)
        return z / (1.0 + z)

def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    if n == 0:
        return 0.0
    a = a[-n:]; b = b[-n:]
    if _np is not None:
        try:
            va = _np.array(a)
            vb = _np.array(b)
            denom = (float(_np.linalg.norm(va)) * float(_np.linalg.norm(vb))) or 1e-12
            return float(_np.dot(va, vb) / denom)
        except Exception:
            pass
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1e-12
    nb = math.sqrt(sum(y * y for y in b)) or 1e-12
    return dot / (na * nb)

def pct_change(seq: List[float]) -> List[float]:
    out: List[float] = []
    for i in range(1, len(seq)):
        p0 = seq[i-1]; p1 = seq[i]
        if p0 <= 0:
            out.append(0.0)
        else:
            out.append(p1 / p0 - 1.0)
    return out

# ====================== Breadth (1dk) ======================

@dataclass
class BreadthSnap:
    ts: float
    N: int
    UP: int
    DOWN: int
    avg_up: float
    avg_dn: float
    sum_up: float
    sum_dn: float
    adv_ratio: float
    neg_ratio: float

class MarketBreadth1m:
    """
    1dk pencere; hem "oran" (adv/neg ratio) hem de "büyüklük" (avg_up/avg_dn) izler.
    """
    def __init__(self, window_sec: float = 60.0, fresh_sec: float = 2.5, min_n: int = 200):
        self.window_sec = window_sec
        self.fresh_sec = fresh_sec
        self.min_n = min_n
        self._last_eval = {
            "ts": 0.0, "N": 0, "UP": 0, "DOWN": 0,
            "avg_up": 0.0, "avg_dn": 0.0, "sum_up": 0.0, "sum_dn": 0.0
        }
        self._history: Deque[BreadthSnap] = deque(maxlen=20)

    def evaluate(self, t_now: float, history: Dict[str, "TickBuf"]) -> dict:
        ups: List[float] = []
        dns: List[float] = []
        N = 0
        for sym, buf in history.items():
            dq = buf.prices
            # Taze mi?
            if (not dq) or ((t_now - dq[-1].ts) > self.fresh_sec):
                continue
            # 60 saniye önceki fiyata erişmeyi DENE (kapsam kontrolünü buraya bırak)
            past = _price_at_or_before_dq(dq, t_now - self.window_sec)
            if not past or past.price <= 0:
                continue
            last = dq[-1].price
            r1m = (last / past.price - 1.0) * 100.0
            N += 1
            if r1m > 0:
                ups.append(r1m)
            elif r1m < 0:
                dns.append(r1m)

        def mean(x: List[float]) -> float:
            return (sum(x) / len(x)) if x else 0.0

        ev = dict(
            ts=t_now, N=N,
            UP=len(ups), DOWN=len(dns),
            avg_up=mean(ups), avg_dn=mean(dns),
            sum_up=sum(ups), sum_dn=sum(dns),
        )
        ev["adv_ratio"] = (ev["UP"] / ev["N"]) if ev["N"] else 0.0
        ev["neg_ratio"] = (ev["DOWN"] / ev["N"]) if ev["N"] else 0.0
        self._last_eval = ev
        self._history.append(BreadthSnap(
            t_now, ev["N"], ev["UP"], ev["DOWN"], ev["avg_up"], ev["avg_dn"],
            ev["sum_up"], ev["sum_dn"], ev["adv_ratio"], ev["neg_ratio"]
        ))
        return ev

    def get_last_eval(self) -> dict:
        return self._last_eval

    def slope_of_adv(self, lookback: int = BREADTH_SLOPE_LOOKBACK) -> float:
        if len(self._history) < max(3, lookback):
            return 0.0
        seq = list(self._history)[-lookback:]
        xs = [s.ts for s in seq]; ys = [s.adv_ratio for s in seq]
        x0 = xs[0]; xs = [x - x0 for x in xs]
        n = len(xs); mx = sum(xs) / n; my = sum(ys) / n
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        var = sum((x - mx) ** 2 for x in xs) or 1e-12
        return cov / var

# ====================== Akış buffer ======================

@dataclass
class TickBuf:
    prices: Deque[PricePoint] = field(default_factory=lambda: deque(maxlen=240))  # ~4dk
    uptick_buf: Deque[float] = field(default_factory=lambda: deque(maxlen=120))   # ~2dk
    last_bid: float = 0.0
    last_ask: float = 0.0

# Deque içinde 60s önceyi ararken soldan değil sağdan tarayalım (kök: O(n) → O(küçük))
def _price_at_or_before_dq(dq: Deque[PricePoint], target_ts: float) -> Optional[PricePoint]:
    for pp in reversed(dq):
        if pp.ts <= target_ts:
            return pp
    return None


# ====================== Bandit ======================

class PolicyStats:
    def __init__(self):
        self.perf: Dict[str, Deque[float]] = {name: deque(maxlen=200) for name in POLICY_KEYS}
    def update(self, policy_name: str, realized_R: float):
        self.perf.setdefault(policy_name, deque(maxlen=200)).append(realized_R)
    def best_policy(self, epsilon: float = EPSILON) -> str:
        if random.random() < epsilon:
            return random.choice(POLICY_KEYS)
        best = None
        best_mu = -1e9
        for name, dq in self.perf.items():
            mu = (sum(dq) / len(dq)) if dq else 0.0
            if mu > best_mu:
                best_mu = mu
                best = name
        return best or "BREAKOUT"

# ====================== Strateji Plugin Arabirimi ======================

@dataclass
class StrategyContext:
    """
    Stratejilere sağlanan minimal bağlam.
    """
    symbol: str
    t_now: float
    price: float
    bars1: List[dict]
    bars5: List[dict]
    bars15: List[dict]
    r1m: float
    uptick_ratio: float
    atr5p: float
    adx5: float
    ema20: float
    ema50: float
    ema200: float
    dcU_fast: float
    dcL_fast: float
    dcU_slow: float
    dcL_slow: float
    reg_mid15: float
    reg_up15: float
    reg_dn15: float
    reg_sigma15: float
    reg_slope15: float
    avwap5: float
    z_tr_1m: float
    vol_ratio_1m: float
    # BTC/ETH referans r1m (alpha-excess, corr-ch için)
    r1m_btc: float
    r1m_eth: float
    # history & buffers (isteğe bağlı)
    history_upticks: Deque[float]

class StrategySet:
    name: str = "BASE"
    def features(self, ctx: StrategyContext) -> Dict[str, Any]:
        return {}
    def propose(self, ctx: StrategyContext) -> Optional[Dict[str, Any]]:
        """
        return dict:
          {
            "p_win": float(0..1), "tp_pct": float, "sl_pct": float,
            "trigger": str, "quality": float(0..1), "policy_name": str,
            "notes": str, "t1_hint": float (ops), "kind":"BO|PB|MR" ...
          }
        None dönerse: sinyal yok.
        """
        raise NotImplementedError

# ========== Yardımcı: güvenli OLS (statsmodels varsa onu, yoksa manuel) ==========

def _safe_ols_beta_alpha(y: List[float], x: List[float]) -> Tuple[float, float]:
    """
    y ~ alpha + beta*x ; returns (alpha, beta)
    """
    n = min(len(y), len(x))
    if n < 10:
        return 0.0, 0.0
    y = y[-n:]; x = x[-n:]
    if _sm is not None:
        try:
            X = [[1.0, xi] for xi in x]
            model = _sm.OLS(y, X).fit()
            a = float(model.params[0]); b = float(model.params[1])
            return a, b
        except Exception:
            pass
    # manuel OLS
    xm = sum(x)/n; ym = sum(y)/n
    num = sum((xi - xm) * (yi - ym) for xi, yi in zip(x, y))
    den = sum((xi - xm) ** 2 for xi in x) or 1e-12
    b = num / den
    a = ym - b * xm
    return a, b

def _zscore(seq: List[float]) -> float:
    if not seq:
        return 0.0
    m = sum(seq)/len(seq)
    v = sum((x-m)*(x-m) for x in seq)/len(seq)
    s = v**0.5
    if s <= 1e-12:
        return 0.0
    return (seq[-1] - m)/s

# ========== TREND seti ==========

class TrendSet(StrategySet):
    name = "TREND"
    def propose(self, ctx: StrategyContext) -> Optional[Dict[str, Any]]:
        ema_align = 1 if (ctx.ema20 > ctx.ema50 > ctx.ema200) else (-1 if (ctx.ema20 < ctx.ema50 < ctx.ema200) else 0)
        if ema_align <= 0:
            return None
        adx_ok = ctx.adx5 >= 18.0
        mom_ok = (ctx.z_tr_1m >= ATR_Z_MIN) and (ctx.vol_ratio_1m >= VOL_RATIO_MIN)
        if not (adx_ok and mom_ok):
            return None

        # ATR tabanlı TP/SL (trend seti biraz daha agresif TP)
        atr5 = (ctx.atr5p/100.0) * (ctx.price or 1.0)
        tp_pct = max(TP_MIN_PCT, min(TP_MAX_PCT, 2.2 * ctx.atr5p))
        sl_pct = max(SL_MIN_PCT, min(SL_MAX_PCT, 1.8 * ctx.atr5p))
        # p_win heuristik (logit benzeri)
        z = (
            LOGIT_W["ztr"] * ctx.z_tr_1m +
            LOGIT_W["vol"] * safe_log(max(1e-9, ctx.vol_ratio_1m)) +
            LOGIT_W["adx"] * (ctx.adx5 / 30.0) +
            LOGIT_W["trend"] * 1.0 +  # align +
            LOGIT_W["bias"]
        )
        p_win = max(0.05, min(0.95, sigmoid(z)))
        return dict(
            p_win=p_win, tp_pct=tp_pct, sl_pct=sl_pct,
            trigger="TREND_MOM_OK", quality=0.65, policy_name="BREAKOUT",
            notes=f"ema_align=+ adx={ctx.adx5:.1f} ztr={ctx.z_tr_1m:.2f} volR={ctx.vol_ratio_1m:.2f}"
        )

# ========== BO (Donchian Breakout) seti ==========

class BreakoutSet(StrategySet):
    name = "BO"
    def propose(self, ctx: StrategyContext) -> Optional[Dict[str, Any]]:
        # üst bant kırılımına yakınlık
        top = max(ctx.dcU_fast, ctx.dcU_slow)
        if top <= 0:
            return None
        dist = (top / (ctx.price or 1e-12) - 1.0) * 100.0
        # çok uzaksa pas, çok yakınsa aday
        if dist > 0.35:
            return None
        # momentum teyidi:
        if (ctx.z_tr_1m < ATR_Z_MIN) or (ctx.vol_ratio_1m < VOL_RATIO_MIN):
            return None
        tp_pct = max(TP_MIN_PCT, min(TP_MAX_PCT, 2.2 * ctx.atr5p))
        sl_pct = max(SL_MIN_PCT, min(SL_MAX_PCT, 1.8 * ctx.atr5p))
        p_win = 0.58 + 0.08 * min(1.0, max(0.0, (ATR_Z_MIN + ctx.vol_ratio_1m - VOL_RATIO_MIN)))
        p_win = max(0.55, min(0.88, p_win))
        return dict(
            p_win=p_win, tp_pct=tp_pct, sl_pct=sl_pct,
            trigger="DONCHIAN_NEAR_BREAK", quality=0.60, policy_name="BREAKOUT",
            notes=f"topDist={dist:.2f}% ztr={ctx.z_tr_1m:.2f} volR={ctx.vol_ratio_1m:.2f}"
        )

# ========== MR (Mean-Reversion) seti ==========

class MRSet(StrategySet):
    name = "MR"
    def propose(self, ctx: StrategyContext) -> Optional[Dict[str, Any]]:
        # Donchian alt bant + uptick toparlanma
        if ctx.dcL_fast <= 0:
            return None
        near_low = ((ctx.price / ctx.dcL_fast) - 1.0) * 100.0
        if near_low < -0.4:  # çok aşağıda (sarkıtmaya açık), pas
            return None
        if near_low > 0.6:
            return None
        # hafif toparlanma:
        if ctx.uptick_ratio < 0.53:
            return None
        tp_pct = max(TP_MIN_PCT, min(TP_MAX_PCT, 1.8 * ctx.atr5p))
        sl_pct = max(SL_MIN_PCT, min(SL_MAX_PCT, 2.2 * ctx.atr5p))
        p_win = 0.52 + 0.06 * max(0.0, (ctx.uptick_ratio - 0.5) * 2.0)
        p_win = max(0.52, min(0.70, p_win))
        return dict(
            p_win=p_win, tp_pct=tp_pct, sl_pct=sl_pct,
            trigger="DONCHIAN_PULLBACK_UPTICK", quality=0.50, policy_name="PULLBACK",
            notes=f"nearLow={near_low:.2f}% upt={ctx.uptick_ratio:.2f}"
        )

# ========== CORRCH (Korelasyon Kanal) seti ==========

class CorrChSet(StrategySet):
    name = "CORRCH"
    def propose(self, ctx: StrategyContext) -> Optional[Dict[str, Any]]:
        # 5m kapanışlarından pct-return ile OLS: coin ~ a + b*BTC
        c5 = [b["c"] for b in ctx.bars5] if ctx.bars5 else []
        # 🔧 BTC 5m’i sabit 60 çek (limit hizalama)
        b5 = [b["c"] for b in get_bars("BTCUSDT", "5m", 60)]
        if len(c5) < 30 or len(b5) < 30:
            return None

        r_c = pct_change(c5)
        r_b = pct_change(b5)
        n = min(len(r_c), len(r_b))
        if n < 25:
            return None

        y = r_c[-n:]; x = r_b[-n:]
        a, beta = _safe_ols_beta_alpha(y, x)

        # residual & kanal
        resid = [ (yi - (a + beta*xi)) for yi, xi in zip(y, x) ]
        if not resid:
            return None
        m = sum(resid)/len(resid)
        s = (sum((e-m)*(e-m) for e in resid)/len(resid))**0.5
        if s <= 0:
            return None

        up = m + 1.6*s
        dn = m - 1.6*s
        last_res = resid[-1]
        resZ = (last_res - m) / (s or 1e-12)

        trig = "NONE"
        if last_res <= dn * 0.98:
            trig = "CH_REBOUND"
        elif last_res >= up * 0.98:
            trig = "CH_BREAK"
        if trig == "NONE":
            return None

        # TP/SL ATR oran
        tp_mult = 2.0 if trig == "CH_REBOUND" else 2.2
        sl_mult = 2.2 if trig == "CH_REBOUND" else 1.8
        tp_pct = max(TP_MIN_PCT, min(TP_MAX_PCT, tp_mult * ctx.atr5p))
        sl_pct = max(SL_MIN_PCT, min(SL_MAX_PCT, sl_mult * ctx.atr5p))

        base = 0.55 if trig == "CH_REBOUND" else 0.57
        p_win = max(0.52, min(0.90, base + 0.05 * min(3.0, abs(resZ))))

        return dict(
            p_win=p_win, tp_pct=tp_pct, sl_pct=sl_pct,
            trigger=trig, quality=0.62,
            policy_name="PULLBACK" if trig=="CH_REBOUND" else "BREAKOUT",
            notes=f"corr={beta:.2f} resZ={resZ:.2f} trig={trig}",
            t1_hint=ctx.reg_up15 if trig=="CH_BREAK" else ctx.reg_mid15
        )

# ========== (Opsiyonel) Forecast seti iskeleti (ileride doldurulabilir) ==========

class ForecastSet(StrategySet):
    name = "FCST"
    def propose(self, ctx: StrategyContext) -> Optional[Dict[str, Any]]:
        # Placeholder: hafif bir katkı; gerçek forecast ikinci fazda
        return None

# ====================== Router (Aggregator) ======================

class StrategyRouter:
    """
    Birden fazla seti çalıştırır; önerileri EV_R skoruna çevirip en iyiyi döndürür.
    """
    def __init__(self, sets: List[StrategySet]):
        self.sets = sets

    def ev_r(self, p_win: float, rr1: float, rr2: float, fee_slip_R: float = 0.03) -> float:
        return p_win * (0.7 * rr1 + 0.3 * rr2) - (1.0 - p_win) - fee_slip_R

    def best_for_symbol(self, ctx: StrategyContext) -> Optional[Dict[str, Any]]:
        proposals: List[Dict[str, Any]] = []
        for s in self.sets:
            try:
                pr = s.propose(ctx)
                if pr:
                    proposals.append(pr)
            except Exception as e:
                logging.exception("Strategy error in %s: %s", s.name, e)
        if not proposals:
            return None

        # rr1/rr2 kabaca T1/T2 dağılımına göre approx: T1=tp; T2=1.6*tp (trend/kırılımda)
        # Daha iyi: setler t1_hint verebilir, ama yoksa rr kabaca tp/sl’dan
        best = None
        best_score = -1e9
        for pr in proposals:
            tp = pr["tp_pct"]; sl = pr["sl_pct"]; p = pr["p_win"]
            if sl <= 0 or tp <= 0:
                # yüzdelik pozitif olmalı (stop negatif yüzde olarak verilmiyor)
                rr1 = (tp) / (sl + 1e-12)
                rr2 = 1.6 * rr1
            else:
                rr1 = (tp) / (sl + 1e-12)
                rr2 = 1.6 * rr1
            ev = self.ev_r(p, rr1, rr2)
            sc = ev * max(0.4, pr.get("quality", 0.6))
            if sc > best_score:
                best_score = sc
                best = pr.copy()
                best["EV_R"] = ev
                best["score"] = sc
                best["rr1"] = rr1
                best["rr2"] = rr2
        return best

# ====================== Çekirdek Motor ======================

class AlphaCore:
    def __init__(self) -> None:
        # akış
        self._kline_sem = asyncio.Semaphore(4)  # aynı anda en çok 4 ağır KLINE
        self.history: Dict[str, TickBuf] = defaultdict(TickBuf)
        self.last_quote: Dict[str, float] = {}
        self._touched_syms: Set[str] = set()

        # hesap & telemetri
        self.equity: float = START_EQUITY
        self.trades = 0
        self.tp_hits = 0
        self.sl_hits = 0
        self.pnl_pct_sum = 0.0

        # işlemler
        self._next_trade_id = 1
        self.active: Dict[int, Trade] = {}
        self.active_by_symbol: Dict[str, Set[int]] = defaultdict(set)
        self.open_queue: Deque[int] = deque()

        # throttle & stage
        self.last_entry_open_ts: float = 0.0
        self.stage: int = 1

        # breadth
        self.breadth_guard = MarketBreadth1m(
            window_sec=WINDOW_SEC, fresh_sec=FRESH_TICK_SEC, min_n=MIN_BREADTH_N
        )
        self._last_breadth_log_ts = 0.0
        self._breadth_ok = False

        # breadth EMA + kalıcılık
        self._adv_ema: Optional[float] = None
        self._adv_ema_ts: float = 0.0
        self._breadth_loss_t0: Optional[float] = None

        # bandit istatistikleri (rejime göre anahtar)
        self.stats_by_regime: Dict[str, PolicyStats] = defaultdict(PolicyStats)

        # günlük R takibi (lock-in & DD)
        self.cum_R_today = 0.0
        self.peak_R_today = 0.0
        self.last_day_idx = int(time.time() // 86400)

        # supabase log kuyruğu
        self._log_queue: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=1000)

        # router: strateji setleri
        self.router = StrategyRouter([
            TrendSet(),
            BreakoutSet(),
            MRSet(),
            CorrChSet(),
            # ForecastSet(),  # ikinci faz
        ])

        # === ENTRY GATES: dinamik kapılar + ret telemetrisi ===  👇 EKLE
        self.gates = DynamicGates()
        self.rej   = RejectionStats(log_name="ENTRY")
        self._last_gate_try: Dict[str, float] = {}   # sembol bazlı kısa cooldown

        # log kuyrukları
        
        self._rejlog_queue: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=5000)  # ↑ kapasite

        # evaluator throttle
        self._last_eval_ts: float = 0.0

        # passive scan throttle + cache
        self._last_passive_scan_ts: float = 0.0
        self._last_passive_out: List[str] = []

        # BTC benzerlik TTL cache
        self._sim_cache: Dict[str, Tuple[float, float]] = {}  # sym -> (sim, ts)

        # RejectionStats.record()’ı Supabase’e akıtan sarmalayıcı
        _orig_record = self.rej.record
        def _record_and_sink(tag, cand, **kw):
            # ÖNEMLİ: _orig_record’a kwargs YOLLAMA (TypeError kökü buydu)
            try:
                self._emit_reject_to_supabase(tag, cand, **kw)
            except Exception as e:
                logging.debug("reject sink err: %s", e)
            return _orig_record(tag, cand)
        self.rej.record = _record_and_sink


    def _btc_similarity_cached(self, sym: str, bars_c_5m: Optional[List[dict]] = None, ttl: float = 30.0) -> float:
        now = now_ts()
        hit = self._sim_cache.get(sym)
        if hit and (now - hit[1]) < ttl:
            return hit[0]

        bars_c = bars_c_5m or get_bars(sym, "5m", 40)
        bars_b = get_bars("BTCUSDT", "5m", 40)
        if len(bars_c) < 10 or len(bars_b) < 10:
            sim = 0.0
        else:
            rc = [bars_c[i]["c"] / bars_c[i - 1]["c"] - 1.0 for i in range(1, len(bars_c))]
            rb = [bars_b[i]["c"] / bars_b[i - 1]["c"] - 1.0 for i in range(1, len(bars_b))]
            sim = _cosine_similarity(rc, rb)
        self._sim_cache[sym] = (sim, now)
        return sim

    def _emit_reject_to_supabase(self, tag: str, cand: "CandidateMetrics", **kw) -> None:
        if (not SUPABASE_LOG_ON) or (safe_log_entry_reject is None):
            return
        item = dict(
            project_id=PROJECT_ID,
            symbol=getattr(cand, "symbol", "_NA_"),
            reason=str(tag),
            # aday metrikleri:
            score=float(getattr(cand, "score", 0.0)),
            spread_bps=float(getattr(cand, "spread_bps", 0.0)),
            atr_pct=float(getattr(cand, "atr_pct", 0.0) * 100.0),  # cand.atr_pct fraksiyon; %’ye çeviriyorum
            r_ratio=float(getattr(cand, "r_ratio", 0.0)),
            liq_rank=int(getattr(cand, "liq_rank", 999)),
            rank=int(getattr(cand, "rank", 999)),
        )
        # ekstraları (breadth_state, vol_regime, eşikler vs.) da geçir
        # trade_logger.log_entry_reject’ın kabul ettiği sahalar:
        _KNOWN = {
            "project_id","symbol","reason","side","score","spread_bps","atr_pct","r_ratio",
            "liq_rank","rank","breadth_adv","breadth_state","relax_active","open_cnt","touched",
            "vol_regime","ev_r","tp_pct","sl_pct","sim_btc","cluster_bucket","price","bid","ask","dbg","extra"
        }

        # item (çekirdek alanlar) hazırlandıktan SONRA:
        extra = {}
        for k, v in (kw or {}).items():
            if k in _KNOWN:
                item[k] = v
            else:
                extra[k] = v
        if extra:
            # varsa mevcut extra ile birleştir
            base_extra = item.get("extra", {}) or {}
            base_extra.update(extra)
            item["extra"] = base_extra

        try:
            self._rejlog_queue.put_nowait(item)
        except asyncio.QueueFull:
            logging.error("Supabase reject log kuyruğu dolu; kayıt atlandı.")


    async def _rej_log_worker(self) -> None:
        """Reject insert için arka plan işçisi."""
        if (not SUPABASE_LOG_ON) or (safe_log_entry_reject is None):
            return
        while True:
            item = await self._rejlog_queue.get()
            try:
                await asyncio.to_thread(safe_log_entry_reject, **item)
            except Exception as e:
                logging.error("Supabase reject insert HATASI: %s", e)

    # -------- yardımcılar
    def _passive_candidate_scan(self, t_now: float) -> List[str]:
        if not PASSIVE_SCAN_ON:
            return []
        # kök: her tick tarama → throttle
        if (t_now - self._last_passive_scan_ts) < 0.8:
            return self._last_passive_out

        cands: List[Tuple[str, float]] = []
        for sym, buf in self.history.items():
            if not buf.prices:
                continue
            age = t_now - buf.prices[-1].ts
            if age > PASSIVE_MAX_AGE_SEC:
                continue
            if buf.last_bid and buf.last_ask:
                mid = (buf.last_bid + buf.last_ask) * 0.5
                if mid <= 0:
                    continue
                spr = (buf.last_ask - buf.last_bid) / mid
                if spr <= 0.0 or spr > MAX_SPREAD_FRAC:
                    continue
            r1m = self._r1m_from_history(sym, t_now)
            if r1m < R1M_MIN_PREFILTER:
                continue
            upt = self._uptick_ratio_from_prices(buf.uptick_buf)
            if upt < UPTICK_MIN_PREFILTER:
                continue
            cands.append((sym, r1m))

        cands.sort(key=lambda x: x[1], reverse=True)
        out = [s for s, _ in cands[: min(PASSIVE_SCAN_TOPK, len(cands))]]
        self._last_passive_out = out
        self._last_passive_scan_ts = t_now
        return out

    def _allowed_slots(self) -> int:
        return STAGE_LIMITS.get(self.stage, 3)

    def _breakeven_uplift_pct(self) -> float:
        slip = SLIPPAGE_PCT / 100.0
        fee = TAKER_FEE_PCT / 100.0
        return ((1 + slip) * (1 + fee)) / ((1 - slip) * (1 - fee)) - 1.0

    def _r1m_from_history(self, sym: str, t_now: float) -> float:
        dq = self.history[sym].prices
        if (not dq) or ((t_now - dq[-1].ts) > FRESH_TICK_SEC) or ((t_now - dq[0].ts) < WINDOW_SEC):
            return 0.0
        past = _price_at_or_before_dq(dq, t_now - WINDOW_SEC)
        if not past or past.price <= 0:
            return 0.0
        last = dq[-1].price
        return (last / past.price - 1.0) * 100.0

    # -------- Rejim (BTC ATR persentil)
    def _btc_vol_regime(self) -> Tuple[str, float, float, float]:
        bars5 = get_bars("BTCUSDT", "5m", 60)  # 🔧 40 yerine 60
        if not bars5:
            return "NORMAL", 0.0, 0.0, 0.0
        trs = []
        pc = bars5[0]["c"]
        for i in range(1, len(bars5)):
            b = bars5[i]
            trs.append(_true_range(b["h"], b["l"], pc) / (b["c"] or 1e-12) * 100.0)
            pc = b["c"]
        cur = atr_pct_last(bars5, 14)
        if not trs:
            return "NORMAL", cur, 0.0, 0.0
        xs = sorted(trs[-24:])

        def perc(ps: float) -> float:
            if not xs:
                return 0.0
            k = (ps / 100.0) * (len(xs) - 1)
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return xs[int(k)]
            return xs[f] * (c - k) + xs[c] * (k - f)

        p40 = perc(40.0)
        p70 = perc(70.0)
        if cur >= p70:
            reg = "HOT"
        elif cur <= p40:
            reg = "COLD"
        else:
            reg = "NORMAL"
        return reg, cur, p40, p70

    # -------- Breadth snapshot + EMA + loglama
    def _breadth_snapshot(self, t_now: float) -> dict:
        ev = self.breadth_guard.get_last_eval() or {}
        if (t_now - ev.get("ts", 0.0)) > 2.0:
            ev = self.breadth_guard.evaluate(t_now, self.history)

        adv_now = ev.get("adv_ratio", 0.0)
        n_now = ev.get("N", 0)

        # EMA başlatma
        if self._adv_ema is None:
            self._adv_ema = adv_now
            self._adv_ema_ts = t_now
        else:
            # N çok düştüyse (ör. I/O tıkandıysa) EMA'yı DONDUR
            if n_now >= (MIN_BREADTH_N // 2):
                dt = max(0.0, t_now - self._adv_ema_ts)
                alpha = (
                    1.0 - math.exp(-dt * math.log(2.0) / BREADTH_EMA_HALF_LIFE_SEC)
                    if BREADTH_EMA_HALF_LIFE_SEC > 0 else 1.0
                )
                self._adv_ema = (1 - alpha) * self._adv_ema + alpha * adv_now
                self._adv_ema_ts = t_now
            # else: EMA sabit kalır (freeze)

        ev["adv_ema"] = float(self._adv_ema)

        # periyodik log (LAG etiketiyle)
        if (t_now - self._last_breadth_log_ts) >= BREADTH_LOG_SEC:
            self._last_breadth_log_ts = t_now
            tag = "BREADTH" if n_now >= (MIN_BREADTH_N // 2) else "BREADTH(LAG)"
            logging.info(
                "%s | N=%d UP=%d DOWN=%d | adv=%.2f ema=%.2f neg=%.2f | avg_dn=%.2f sum_dn=%.2f",
                tag, ev.get("N", 0), ev.get("UP", 0), ev.get("DOWN", 0),
                ev.get("adv_ratio", 0.0), ev.get("adv_ema", 0.0), ev.get("neg_ratio", 0.0),
                ev.get("avg_dn", 0.0), ev.get("sum_dn", 0.0),
            )
        return ev

    def _portfolio_cushion_R(self) -> float:
        return sum(tr.mtm_R for tr in self.active.values())

    def _breadth_gating_ok(self, ev: dict) -> Tuple[bool, str]:
        if not BREADTH_RATIO_GATE_ON:
            return True, "NEU"
        N = ev.get("N", 0)
        adv = ev.get("adv_ratio", 0.0)
        adv_ema = ev.get("adv_ema", adv)
        neg = ev.get("neg_ratio", 0.0)
        slope = self.breadth_guard.slope_of_adv()
        cushion_R = self._portfolio_cushion_R()

        adv_min = BREADTH_ADV_RATIO_MIN - (ADV_TOL_IF_CUSHION if cushion_R >= PNL_CUSHION_R_GRACE else 0.0)
        adv_exit = BREADTH_ADV_RATIO_EXIT - (ADV_TOL_IF_CUSHION if cushion_R >= PNL_CUSHION_R_GRACE else 0.0)
        neg_veto = BREADTH_MAX_NEG_RATIO_VETO + (NEG_TOL_IF_CUSHION if cushion_R >= PNL_CUSHION_R_GRACE else 0.0)

        if N < MIN_BREADTH_N:
            self._breadth_ok = False
            self._breadth_loss_t0 = None
            return False, "NEU"

        t_now = now_ts()
        # ⬇⬇⬇ BURASI DEĞİŞTİ ⬇⬇⬇
        if neg >= (neg_veto + BREADTH_STRONG_NEG_PAD):
            self._breadth_ok = False
            if self._breadth_loss_t0 is None:
                self._breadth_loss_t0 = t_now   # 0.0 yerine t_now
            return False, "OFF"
        # ⬆⬆⬆ BURASI DEĞİŞTİ ⬆⬆⬆

        adv_eff = 0.5 * adv + 0.5 * adv_ema
        is_loss = (adv_eff < adv_exit)
        if is_loss or (neg >= neg_veto):
            if self._breadth_loss_t0 is None:
                self._breadth_loss_t0 = t_now
        else:
            self._breadth_loss_t0 = None

        if self._breadth_ok:
            need_persist = (self._breadth_loss_t0 is not None) and ((t_now - self._breadth_loss_t0) >= BREADTH_LOSS_MIN_SEC)
            self._breadth_ok = not need_persist
        else:
            thr = adv_min - (0.02 if slope > 0 else 0.0)
            self._breadth_ok = (adv_eff >= thr)
        state = "ON" if self._breadth_ok else ("OFF" if adv_eff < adv_exit else "NEU")
        return self._breadth_ok, state

    # -------- Uptick oranı (akış bufferından)
    def _uptick_ratio_from_prices(self, prices: Deque[float]) -> float:
        ups = downs = 0
        prev = None
        for p in prices:
            if prev is not None:
                if p > prev: ups += 1
                elif p < prev: downs += 1
            prev = p
        tot = ups + downs
        return (ups / tot) if tot > 0 else 0.5

    # -------- Tick akışı
    async def on_tick(self, tick: BookTicker) -> None:
        t_now = tick.ts
        sym = tick.symbol
        bid = tick.bid_price
        ask = tick.ask_price
        last = (bid + ask) * 0.5
        if last <= MIN_PRICE:
            return

        # akış buffer
        buf = self.history[sym]
        buf.prices.append(PricePoint(t_now, last))
        buf.uptick_buf.append(last)
        buf.last_bid = bid
        buf.last_ask = ask
        self.last_quote[sym] = last
        self._touched_syms.add(sym)

        # aktif işlemler için MTM & yönetim
        if self.active_by_symbol.get(sym):
            self._update_trades_on_tick(sym, last, t_now)

        # Breadth snapshot + flatten kontrol
        ev = self._breadth_snapshot(t_now)
        self._flatten_on_breadth_loss(t_now, ev)

        # Giriş denemesi
        if self._entry_gating_ok(t_now):
            await self._evaluate_and_maybe_open(t_now, list(self._touched_syms))

        # Zaman stop
        for tid, tr in list(self.active.items()):
            if (t_now - tr.entry_ts) > TRACK_TIMEOUT_SEC:
                last_p = self.last_quote.get(tr.symbol, tr.entry_price_ref)
                self._close_trade(tid, "TIMEOUT", last_p, t_now)

        # Stage güncelle
        self._update_stage_by_open_queue()
        self._touched_syms.clear()

    def _entry_gating_ok(self, t_now: float) -> bool:
        # Snapshot’ta EMA freeze zaten güncel; ekstra evaluate yapma
        ev = self.breadth_guard.get_last_eval() or {}
        ok, _ = self._breadth_gating_ok(ev)
        if not ok:
            return False
        if (t_now - self.last_entry_open_ts) < GLOBAL_ENTRY_GAP_SEC:
            return False
        if len(self.active) >= self._allowed_slots():
            return False
        return True


    async def _btc_vol_regime_async(self) -> Tuple[str, float, float, float]:
        bars5 = await self._get_bars_async("BTCUSDT", "5m", 60)
        if not bars5:
            return "NORMAL", 0.0, 0.0, 0.0
        trs = []
        pc = bars5[0]["c"]
        for i in range(1, len(bars5)):
            b = bars5[i]
            trs.append(_true_range(b["h"], b["l"], pc) / (b["c"] or 1e-12) * 100.0)
            pc = b["c"]
        cur = atr_pct_last(bars5, 14)
        if not trs:
            return "NORMAL", cur, 0.0, 0.0
        xs = sorted(trs[-24:])

        def perc(ps: float) -> float:
            if not xs:
                return 0.0
            k = (ps / 100.0) * (len(xs) - 1)
            f = math.floor(k); c = math.ceil(k)
            if f == c:
                return xs[int(k)]
            return xs[f] * (c - k) + xs[c] * (k - f)

        p40 = perc(40.0); p70 = perc(70.0)
        if cur >= p70:
            reg = "HOT"
        elif cur <= p40:
            reg = "COLD"
        else:
            reg = "NORMAL"
        return reg, cur, p40, p70
    # Eski:
    # async def _get_bars_async(self, symbol: str, interval: str, limit: int) -> List[dict]:
    #     async with self._kline_sem:
    #         return await asyncio.to_thread(get_bars, symbol, interval, limit)

    # Yeni:
    async def _get_bars_async(self, symbol: str, interval: str, limit: int) -> List[dict]:
        async with self._kline_sem:
            # get_bars -> binance_io_V5 içinde create_task kullanabiliyor.
            # Bu yüzden loop'un çalıştığı ana thread'de çağırmalıyız.
            data = get_bars(symbol, interval, limit)
            # event loop’a nefes:
            await asyncio.sleep(0)
            return data


    
    # -------- Aday üretimi ve skor (PLUGIN ROUTER ile)
    async def _evaluate_and_maybe_open(self, t_now: float, touched: List[str]) -> None:
        # KÖK: aynı saniye içinde ağır evaluate turu çalışmasın
        if (t_now - self._last_eval_ts) < 0.9:
            return
        self._last_eval_ts = t_now

        try:
            # 0) Global giriş kapısı
            ok_gate_global = self._entry_gating_ok(t_now)
            if not ok_gate_global:
                return

            # 1) Rejim + breadth durumu
            vol_reg, _, _, _ = await self._btc_vol_regime_async()
            ev = self.breadth_guard.get_last_eval() or {}
            ok, breadth_state = self._breadth_gating_ok(ev)
            if not ok:
                return

            # 2) Pasif tarama ile genişlet (throttle’lı)
            passive = self._passive_candidate_scan(t_now)
            if passive:
                touched = list({*touched, *passive})

            # 3) Gate’i bilgilendir
            self.gates.begin_cycle(
                breadth_adv=ev.get("adv_ratio", 0.0),
                touched=len(touched),
                open_cnt=len(self.active),
            )

            # 4) BTC/ETH preload
            await asyncio.gather(
                self._get_bars_async("BTCUSDT", "1m", 50),
                self._get_bars_async("BTCUSDT", "5m", 60),
                self._get_bars_async("BTCUSDT", "15m", REG_WIN_15M + 10),
                self._get_bars_async("ETHUSDT", "1m", 50),
                self._get_bars_async("ETHUSDT", "5m", 60),
                self._get_bars_async("ETHUSDT", "15m", REG_WIN_15M + 10),
            )

            # 5) Top-K prefilter (flood kök: sembol başına log yerine toplu özet)
            from collections import Counter
            pref = Counter()
            light: List[Tuple[str, float]] = []

            def _mk_dummy_cand(sym: str, rank: int = 999) -> CandidateMetrics:
                return CandidateMetrics(
                    side="long", score=0.0, spread_bps=0.0,
                    atr_pct=0.0, r_ratio=0.0, liq_rank=999, rank=rank, symbol=sym
                )

            for sym in touched:
                buf = self.history[sym]
                # stale / yaşlı
                if (not buf.prices) or ((t_now - buf.prices[-1].ts) > FRESH_TICK_SEC):
                    pref["stale"] += 1
                    if PREFILTER_LOG_PER_SYMBOL:
                        self.rej.record("pref_stale", _mk_dummy_cand(sym),
                            breadth_adv=ev.get("adv_ratio", 0.0),
                            breadth_state=breadth_state,
                            open_cnt=len(self.active), touched=len(touched))
                    continue

                # spread
                if buf.last_bid and buf.last_ask:
                    mid = (buf.last_bid + buf.last_ask) * 0.5
                    spr = (buf.last_ask - buf.last_bid) / (mid or 1e-12)
                    if spr <= 0.0 or spr > MAX_SPREAD_FRAC:
                        pref["spread"] += 1
                        if PREFILTER_LOG_PER_SYMBOL:
                            self.rej.record("pref_spread", _mk_dummy_cand(sym),
                                spread_bps=spr * 1e4,
                                max_spread=MAX_SPREAD_FRAC * 1e4,
                                breadth_adv=ev.get("adv_ratio", 0.0),
                                breadth_state=breadth_state,
                                open_cnt=len(self.active), touched=len(touched))
                        continue

                # r1m + uptick
                r1m = self._r1m_from_history(sym, t_now)
                upt = self._uptick_ratio_from_prices(buf.uptick_buf)
                if r1m < R1M_MIN_PREFILTER or upt < UPTICK_MIN_PREFILTER:
                    pref["r1m_or_upt"] += 1
                    if PREFILTER_LOG_PER_SYMBOL:
                        self.rej.record("pref_r1m_or_upt", _mk_dummy_cand(sym),
                            r1m=r1m, upt=upt,
                            r1m_min=R1M_MIN_PREFILTER, upt_min=UPTICK_MIN_PREFILTER,
                            breadth_adv=ev.get("adv_ratio", 0.0),
                            breadth_state=breadth_state,
                            open_cnt=len(self.active), touched=len(touched))
                    continue

                light.append((sym, r1m))

            if not light:
                # TEK toplu özet satırı
                self.rej.record("prefilter_summary", _mk_dummy_cand("_ALL_"),
                                touched=len(touched),
                                stale=int(pref["stale"]),
                                spread=int(pref["spread"]),
                                r1m_or_upt=int(pref["r1m_or_upt"]),
                                breadth_adv=ev.get("adv_ratio", 0.0),
                                breadth_state=breadth_state)
                self.rej.flush(1.0)
                return

            light.sort(key=lambda x: x[1], reverse=True)
            touched = [sym for sym, _ in light[:PREFILTER_TOPK]]
            await asyncio.sleep(0)

            # 6) Ağır bağlam + strateji + GATE + açılış
            # >>>> YENİ: Pre-pass — z_tr ve volR örneklerini topla
            probes: List[Dict[str, Any]] = []   # her sembol için hızlı metrikler
            z_samples: List[float] = []
            volr_samples: List[float] = []

            for rank, sym in enumerate(touched):
                # bars5 ve likidite
                bars5 = await self._get_bars_async(sym, "5m", 60)
                if not bars5:
                    self.rej.record("bars5_empty",
                        CandidateMetrics("long", 0.0, 0.0, 0.0, 0.0, 999, rank, sym),
                        breadth_adv=ev.get("adv_ratio", 0.0),
                        breadth_state=breadth_state, vol_regime=vol_reg)
                    continue

                avg5q = sum(b.get("quote", 0.0) for b in bars5[-24:]) / max(1, len(bars5[-24:]))
                if avg5q < MIN_QUOTE_VOL_5M:
                    self.rej.record("liq_low",
                        CandidateMetrics("long", 0.0, 0.0, 0.0, 0.0, 999, rank, sym),
                        breadth_adv=ev.get("adv_ratio", 0.0),
                        breadth_state=breadth_state, vol_regime=vol_reg,
                        avg5q=avg5q, min_q=MIN_QUOTE_VOL_5M)
                    continue

                # bars1 (z_tr ve volR için)
                bars1 = await self._get_bars_async(sym, "1m", 50)
                if (not bars1) or (len(bars1) < 3):
                    self.rej.record("bars1_short",
                        CandidateMetrics("long", 0.0, 0.0, 0.0, 0.0, 999, rank, sym),
                        breadth_adv=ev.get("adv_ratio", 0.0),
                        breadth_state=breadth_state, vol_regime=vol_reg)
                    continue

                prev_close_1m = bars1[-3]["c"]
                b1 = bars1[-2]
                last1m_tr_pct = (_true_range(b1["h"], b1["l"], prev_close_1m) / (b1["c"] or 1e-12)) * 100.0

                # z_tr (son 24*5m TR%'ye göre)
                trpcts = [(_true_range(bars5[i]["h"], bars5[i]["l"], bars5[i-1]["c"]) / (bars5[i]["c"] or 1e-12)) * 100.0
                          for i in range(1, len(bars5))]
                tail = trpcts[-24:] if len(trpcts) >= 24 else trpcts
                if tail:
                    mu = sum(tail) / len(tail)
                    var = sum((x - mu)*(x - mu) for x in tail) / len(tail)
                    sigma = (var ** 0.5) or 1e-9
                    z_tr = (last1m_tr_pct - mu) / sigma
                else:
                    z_tr = 0.0

                # volR (1m quote / 5m ort quote)
                last1m_vol_ratio = b1.get("quote", 0.0) / (avg5q or 1e-12)

                probes.append(dict(sym=sym, rank=rank, bars1=bars1, bars5=bars5,
                                   z_tr=z_tr, volR=last1m_vol_ratio, avg5q=avg5q, b1=b1))
                z_samples.append(z_tr)
                volr_samples.append(last1m_vol_ratio)

            if not probes:
                self.rej.flush(1.0)
                return

            # --- Dinamik eşikler (yüzdelik + rejim tabanı)
            if USE_MOMVOL_PCTL and len(z_samples) >= 5 and len(volr_samples) >= 5:
                z_thr    = _percentile(z_samples,  MOMVOL_PCTL)   # örn 0.70
                volr_thr = _percentile(volr_samples, MOMVOL_PCTL)
            else:
                z_thr, volr_thr = ATR_Z_MIN, VOL_RATIO_MIN        # fallback

            z_thr    = max(z_thr,    Z_MIN_BY_REG.get(vol_reg, 0.55))
            volr_thr = max(volr_thr, VOLR_MIN_BY_REG.get(vol_reg, 1.10))

            # >>>> İkinci pass: mom_vol filtresi + kalan ağır akış
            for p in probes:
                sym   = p["sym"];  rank = p["rank"]
                bars1 = p["bars1"]; b1 = p["b1"]
                bars5 = p["bars5"]; avg5q = p["avg5q"]
                z_tr  = p["z_tr"];  last1m_vol_ratio = p["volR"]

                # Adaptif mom/hacim filtresi
                if (z_tr < z_thr) or (last1m_vol_ratio < volr_thr):
                    self.rej.record("mom_vol",
                        CandidateMetrics("long", z_tr, 0.0, 0.0, 0.0, 999, rank, sym),
                        breadth_adv=ev.get("adv_ratio", 0.0),
                        breadth_state=breadth_state, vol_regime=vol_reg,
                        z_tr=z_tr, volR=last1m_vol_ratio,
                        z_min=z_thr, volR_min=volr_thr)
                    continue

                # 15m bağlam
                bars15 = await self._get_bars_async(sym, "15m", REG_WIN_15M + 10)
                closes15 = [b["c"] for b in bars15] if bars15 else []
                if closes15:
                    mid15, slope15, sigma15, up15, dn15 = linreg_channel(closes15, REG_SIGMA_K)
                else:
                    mid15 = slope15 = sigma15 = up15 = dn15 = 0.0

                # 5m tabanlı göstergeler
                adx5 = adx_from_bars(bars5, ADX_N)
                dcU_fast, dcL_fast = donchian(bars5, DONCHIAN_FAST)
                dcU_slow, dcL_slow = donchian(bars5, DONCHIAN_SLOW)
                ema20v = (ema([b["c"] for b in bars5], EMA_FAST) or [0])[-1]
                ema50v = (ema([b["c"] for b in bars5], EMA_MID) or [0])[-1]
                ema200v = (ema([b["c"] for b in bars5], EMA_SLOW) or [0])[-1]

                anch_idx = find_swing_anchor(bars5, atr_pct_thresh=1.0)
                avw = anchored_vwap(bars5, anch_idx)
                price = self.last_quote.get(sym, bars5[-1]["c"])
                atr5p = atr_pct_last(bars5, 14)

                # BTC/ETH r1m referansları
                def r1m_of(s: str) -> float:
                    dq = self.history[s].prices
                    if (not dq) or ((t_now - dq[-1].ts) > FRESH_TICK_SEC) or ((t_now - dq[0].ts) < WINDOW_SEC):
                        return 0.0
                    past = _price_at_or_before_dq(dq, t_now - WINDOW_SEC)
                    if (not past) or (past.price <= 0):
                        return 0.0
                    lastp = dq[-1].price
                    return (lastp / past.price - 1.0) * 100.0

                r1m_coin = self._r1m_from_history(sym, t_now)
                r1m_btc  = r1m_of("BTCUSDT")
                r1m_eth  = r1m_of("ETHUSDT")

                # uptick (buffer’dan)
                ups = downs = 0; prev = None
                for px in self.history[sym].uptick_buf:
                    if prev is not None:
                        if px > prev: ups += 1
                        elif px < prev: downs += 1
                    prev = px
                tot = ups + downs
                upt = (ups / tot) if tot > 0 else 0.5

                # StrategyContext
                ctx = StrategyContext(
                    symbol=sym, t_now=t_now, price=price,
                    bars1=bars1, bars5=bars5, bars15=bars15,
                    r1m=r1m_coin, uptick_ratio=upt, atr5p=atr5p, adx5=adx5,
                    ema20=ema20v, ema50=ema50v, ema200=ema200v,
                    dcU_fast=dcU_fast, dcL_fast=dcL_fast,
                    dcU_slow=dcU_slow, dcL_slow=dcL_slow,
                    reg_mid15=mid15, reg_up15=up15, reg_dn15=dn15,
                    reg_sigma15=sigma15, reg_slope15=slope15,
                    avwap5=avw, z_tr_1m=z_tr, vol_ratio_1m=last1m_vol_ratio,
                    r1m_btc=r1m_btc, r1m_eth=r1m_eth,
                    history_upticks=self.history[sym].uptick_buf,
                )

                # Router: en iyi öneri
                best = self.router.best_for_symbol(ctx)
                if not best:
                    self.rej.record("no_proposal",
                        CandidateMetrics("long", 0.0, 0.0, 0.0, 0.0, 999, rank, sym),
                        breadth_adv=ev.get("adv_ratio", 0.0),
                        breadth_state=breadth_state, vol_regime=vol_reg)
                    continue

                # GATE METRİKLERİ
                buf = self.history[sym]
                spread_bps = 0.0
                if buf.last_bid and buf.last_ask:
                    mid = (buf.last_bid + buf.last_ask) * 0.5
                    if mid > 0:
                        spread_bps = ((buf.last_ask - buf.last_bid) / mid) * 1e4

                rr1 = best.get("rr1")
                if rr1 is None:
                    slp = max(1e-12, best.get("sl_pct", 0.0))
                    rr1 = (best.get("tp_pct", 0.0) / slp) if slp > 0 else 0.0
                r_comp = 0.7 * rr1 + 0.3 * (1.6 * rr1)

                liq_rank = 1 if avg5q >= 2_000_000 else 999
                cand = CandidateMetrics(
                    side="long",
                    score=float(best.get("score", 0.0)),
                    spread_bps=float(spread_bps),
                    atr_pct=float(atr5p / 100.0),
                    r_ratio=float(r_comp),
                    liq_rank=int(liq_rank),
                    rank=rank,
                    symbol=sym,
                )

                # eşiksel retler (EV/uplift/cluster)
                if best["EV_R"] < 0.40:
                    self.rej.record("ev_r", cand, ev_r=best["EV_R"], breadth_state=breadth_state, vol_regime=vol_reg)
                    continue
                uplift = self._breakeven_uplift_pct() * 100.0
                if best["tp_pct"] < (uplift + 0.02):
                    self.rej.record("uplift", cand, tp_pct=best["tp_pct"], uplift=uplift,
                                    breadth_state=breadth_state, vol_regime=vol_reg)
                    continue

                sim_btc = self._btc_similarity_cached(sym, bars5)
                cluster_count = self._current_cluster_count()
                if sim_btc >= CLUSTER_SIM_THRESHOLD and cluster_count.get("BTCBETA", 0) >= CLUSTER_MAX_SLOTS:
                    self.rej.record("cluster", cand, sim_btc=sim_btc,
                                    cluster_used=cluster_count.get("BTCBETA", 0),
                                    breadth_state=breadth_state, vol_regime=vol_reg)
                    continue

                # sembol kısa cooldown
                last_try = self._last_gate_try.get(sym, 0.0)
                if (t_now - last_try) < COOLDOWN_TOUCH_SEC:
                    self.rej.record("cooldown", cand, cooldown_sec=COOLDOWN_TOUCH_SEC, last_try=last_try,
                                    breadth_state=breadth_state, vol_regime=vol_reg)
                    continue

                # === DİNAMİK GATE ===
                ok_gate, rsn = self.gates.check_candidate(cand, self.rej)
                if not ok_gate:
                    self._last_gate_try[sym] = t_now
                    _trace(sym, f"gate reject: {rsn}")
                    continue

                # --- Aç
                breadth_boost = 1.10 if breadth_state == "ON" else (0.95 if breadth_state == "OFF" else 1.0)
                best["score"] *= breadth_boost
                entry_price_ref = price
                if entry_price_ref <= 0:
                    self.rej.record("bad_price", cand, price=entry_price_ref)
                    continue

                tid = self._open_trade_kademeli(
                    sym, entry_price_ref, now_ts(),
                    best["tp_pct"], -abs(best["sl_pct"]),
                    reason=(f"[{best.get('policy_name','NA')}] [VOL {vol_reg}] [BREADTH {breadth_state}] "
                            f"[SCORE {best.get('score',0):.2f}] | {best.get('notes','')}"),
                    metrics=dict(
                        score=best.get("score",0.0), r1m=r1m_coin, atr5m=atr5p, z1m=z_tr,
                        vshock=(b1.get("quote", 0.0) / (avg5q or 1e-12)), upt=upt,
                        trend=(1.0 if (ema20v and ema50v and ema200v and ema20v > ema50v > ema200v)
                            else (-1.0 if (ema20v and ema50v and ema200v and ema20v < ema50v < ema200v) else 0.0)),
                        volr=(b1.get("quote", 0.0) / (avg5q or 1e-12)),
                        policy_name=best.get("policy_name","NA"),
                        vol_regime=vol_reg, breadth_state=breadth_state,
                        t1_hint=best.get("t1_hint",0.0)
                    ),
                    sim_btc=sim_btc
                )
                if tid is None:
                    self.rej.record("open_block", cand,
                                    reason_detail="slot_or_qty", breadth_state=breadth_state, vol_regime=vol_reg)
                else:
                    self._last_gate_try[sym] = t_now
                    _trace(sym, f"OPEN EV_R={best['EV_R']:.2f} score={best['score']:.2f} {best.get('notes','')}")

            # 7) Tur sonu
            self.rej.flush(5.0)
            if len(self.open_queue) > 0:
                self.last_entry_open_ts = now_ts()

        except Exception as e:
            logging.exception("EVAL_CRASH: %s", e)

    # -------- BTC benzerliği (aktif/şarj azaltılmış)
    def _btc_similarity(self, sym: str) -> float:
        bars_c = get_bars(sym, "5m", 40)
        bars_b = get_bars("BTCUSDT", "5m", 40)
        if len(bars_c) < 10 or len(bars_b) < 10:
            return 0.0
        rc = [bars_c[i]["c"] / bars_c[i - 1]["c"] - 1.0 for i in range(1, len(bars_c))]
        rb = [bars_b[i]["c"] / bars_b[i - 1]["c"] - 1.0 for i in range(1, len(bars_b))]
        return _cosine_similarity(rc, rb)

    # -------- Kademeli açılış
    def _risk_sized_qty_base10(self, entry_price: float, sl_pct: float) -> float:
        """
        10 birim toplam risk için baz adet. K1/K2/K3 eklemeleri KADEMELI_SIZES ile ölçeklenir.
        """
        risk_pct = RISK_PER_TRADE_PCT
        day_idx = int(time.time() // 86400)
        if day_idx != self.last_day_idx:
            self.last_day_idx = day_idx
            self.cum_R_today = 0.0
            self.peak_R_today = 0.0
        if self.cum_R_today >= DAILY_LOCK_IN_2:
            risk_pct = max(0.5, risk_pct * 0.50)
        elif self.cum_R_today >= DAILY_LOCK_IN_1:
            risk_pct = max(0.75, risk_pct * 0.75)
        risk_usdt_total = self.equity * (risk_pct / 100.0)
        # 10 birim → her birim risk_usdt_total/10
        per_unit_usdt = risk_usdt_total / 10.0
        sl_move = abs(sl_pct) / 100.0 * entry_price
        if sl_move <= 0:
            return 0.0
        # 1 birim için adet:
        qty_unit = per_unit_usdt / sl_move
        return max(qty_unit * 10.0, 0.0)  # toplam 10 birimin adedi

    def _open_trade_kademeli(self, sym: str, entry_price_ref: float, t_now: float,
                             tp_pct: float, sl_pct: float, *, reason: str,
                             metrics: Dict[str, Any], sim_btc: float) -> Optional[int]:
        """
        İlk kademeyi (K1) açar. K2/K3 ileride yönetim fonksiyonlarında tetiklenir.
        """
        if ONE_TRADE_PER_SYMBOL and self.active_by_symbol.get(sym):
            return None
        if entry_price_ref <= 0:
            return None
        if len(self.active) >= self._allowed_slots():
            return None

        base_qty_10 = self._risk_sized_qty_base10(entry_price_ref, sl_pct)  # 10 birim
        if base_qty_10 <= 0:
            return None

        # K1 açılacak adet:
        k1_share = KADEMELI_SIZES[0]  # 3
        qty = base_qty_10 * (k1_share / 10.0)

        tp_ref = entry_price_ref * (1.0 + tp_pct / 100.0)
        sl_ref = entry_price_ref * (1.0 + sl_pct / 100.0)
        risk_usdt = self.equity * (RISK_PER_TRADE_PCT / 100.0)
        notional_entry = qty * entry_price_ref

        tid = self._next_trade_id
        self._next_trade_id += 1
        tr = Trade(
            trade_id=tid, symbol=sym, entry_ts=t_now, entry_price_ref=entry_price_ref, qty=qty,
            tp_price_ref=tp_ref, sl_price_ref=sl_ref, risk_usdt=risk_usdt, notional_entry=notional_entry,
            last_log_ts=t_now, open_reason=reason,
            score=metrics.get("score",0.0), r1m=metrics.get("r1m",0.0), atr5m=metrics.get("atr5m",0.0), z1m=metrics.get("z1m",0.0),
            vshock=metrics.get("vshock",0.0), upt=metrics.get("upt",0.0), trend=metrics.get("trend",0.0), volr=metrics.get("volr",1.0),
            policy_name=metrics.get("policy_name","NA"), vol_regime=metrics.get("vol_regime","NORMAL"), breadth_state=metrics.get("breadth_state","NEU"),
            ladder_k=1, base_qty_10=base_qty_10, t1_price_hint=metrics.get("t1_hint",0.0)
        )
        self.active[tid] = tr
        self.active_by_symbol[sym].add(tid)
        self.open_queue.append(tid)

        logging.info(
            "OPEN[K1] #%d %s | qty=%.6f @%.6f -> TP %.6f (%s) / SL %.6f (%s) | eq=%.2f | risk=%.2f | %s",
            tid, sym, qty, entry_price_ref, tp_ref, pct(tp_pct), sl_ref, pct(sl_pct), self.equity, risk_usdt, reason
        )
        return tid

    # -------- MTM hesap
    def _compute_net_mtm(self, tr: Trade, last_price: float) -> Tuple[float, float]:
        slip = SLIPPAGE_PCT / 100.0
        fee = TAKER_FEE_PCT / 100.0
        entry_fill = tr.entry_price_ref * (1.0 + slip)
        exit_fill = last_price * (1.0 - slip)
        notional_entry = tr.qty * entry_fill
        notional_exit = tr.qty * exit_fill
        gross = notional_exit - notional_entry
        fees = fee * (notional_entry + notional_exit)
        net = gross - fees
        R = (net / tr.risk_usdt) if tr.risk_usdt > 0 else 0.0
        return net, R
    


    # -------- Kademeli ekleme tetikleyici
    def _maybe_add_ladders(self, tr: Trade, last_price: float) -> None:
        """
        K1 açıkken lehimize gittikçe K2 ve K3 eklemelerini yapar.
        - K2: fiyat, girişten itibaren ≥ KADEMELI_2_TRIGGER_ATR * ATR_abs lehe ise
        - K3: T1'e yaklaşım: (t1_hint varsa) giriş→T1 yolunun ≥ KADEMELI_3_NEAR_T1_FRAC'i;
              yoksa mevcut TP'ye göre aynı oran.
        """
        if not KADEMELI_ENABLED:
            return
        # Güvenlik: base_qty_10 yoksa çık
        if tr.base_qty_10 <= 0:
            return

        # ATR abs, giriş fiyatından
        atr_abs = (tr.atr5m / 100.0) * (tr.entry_price_ref or 1.0)

        # K2 şartları
        if tr.ladder_k == 1:
            move_abs = last_price - tr.entry_price_ref
            if move_abs >= KADEMELI_2_TRIGGER_ATR * atr_abs:
                add_qty = tr.base_qty_10 * (KADEMELI_SIZES[1] / 10.0)  # 2. kademe payı
                tr.qty += add_qty
                tr.notional_entry += add_qty * tr.entry_price_ref
                tr.ladder_k = 2
                logging.info(
                    "LADDER[K2] #%d %s | +qty=%.6f (tot=%.6f) | trigger=+%.2f*ATR_abs (%.6f)",
                    tr.trade_id, tr.symbol, add_qty, tr.qty,
                    KADEMELI_2_TRIGGER_ATR, KADEMELI_2_TRIGGER_ATR * atr_abs
                )

        # K3 şartları
        if tr.ladder_k == 2:
            # T1 hedefi varsayımı (hint varsa onu kullan)
            t1 = tr.tp_price_ref
            if tr.t1_price_hint > 0:
                t1 = tr.t1_price_hint
            # fallback güvenlik
            if t1 <= tr.entry_price_ref:
                t1 = tr.tp_price_ref

            path = t1 - tr.entry_price_ref
            progressed = last_price - tr.entry_price_ref
            if path > 0 and progressed >= KADEMELI_3_NEAR_T1_FRAC * path:
                add_qty = tr.base_qty_10 * (KADEMELI_SIZES[2] / 10.0)  # 3. kademe payı
                tr.qty += add_qty
                tr.notional_entry += add_qty * tr.entry_price_ref
                tr.ladder_k = 3
                logging.info(
                    "LADDER[K3] #%d %s | +qty=%.6f (tot=%.6f) | trigger=near %.0f%% of T1 path",
                    tr.trade_id, tr.symbol, add_qty, tr.qty, KADEMELI_3_NEAR_T1_FRAC * 100.0
                )

    # -------- Trailing & BE & winlock
    def _apply_trailing(self, tr: Trade, last_price: float, vol_regime: str) -> None:
        be_thr = {"HOT": BE_R_HOT, "NORMAL": BE_R_NORMAL, "COLD": BE_R_COLD}.get(vol_regime, BE_R_NORMAL)
        net, R = self._compute_net_mtm(tr, last_price)
        # Breakeven
        if (not tr.be_applied) and R >= be_thr:
            be_price = tr.entry_price_ref * (1.0 + self._breakeven_uplift_pct())
            tr.sl_price_ref = max(tr.sl_price_ref, be_price)
            tr.be_applied = True
        # Win-lock (TP'nin belirli kısmına gelince SL'i kilitle)
        tp_move = tr.tp_price_ref - tr.entry_price_ref
        cur_move = last_price - tr.entry_price_ref
        if (not tr.winlock_applied) and tp_move > 0 and cur_move >= WINLOCK_TP_FRACTION * tp_move:
            lock_price = tr.entry_price_ref + WINLOCK_SL_TP_FRAC * tp_move
            tr.sl_price_ref = max(tr.sl_price_ref, lock_price)
            tr.winlock_applied = True

    # -------- Stall (sürünme) çıkışı
    def _stall_exit_check(self, tr: Trade, sym: str, last_price: float) -> Optional[str]:
        bars5 = get_bars(sym, "5m", 40)
        if not bars5:
            return None
        avg5c = sum(b["c"] for b in bars5[-20:]) / max(1, len(bars5[-20:]))
        prices = self.history[sym].uptick_buf
        seq = list(prices)[-40:]
        ups = downs = 0
        prev = None
        for p in seq:
            if prev is not None:
                if p > prev:
                    ups += 1
                elif p < prev:
                    downs += 1
            prev = p
        tot = ups + downs
        upt = (ups / tot) if tot > 0 else 0.5
        if upt < 0.45 and last_price < avg5c:
            return "STALL_EXIT"
        return None

    def _pop_from_open_queue(self, trade_id: int) -> None:
        try:
            self.open_queue.remove(trade_id)
        except ValueError:
            pass

    # -------- İşlem kapatma
    def _close_trade(self, trade_id: int, exit_reason: str, last_price_ref: float, t_now: float) -> None:
        tr = self.active.pop(trade_id, None)
        if not tr:
            return
        sset = self.active_by_symbol.get(tr.symbol)
        if sset:
            sset.discard(trade_id)
            if not sset:
                self.active_by_symbol.pop(tr.symbol, None)
        self._pop_from_open_queue(trade_id)

        net_pnl, realized_R = self._compute_net_mtm(tr, last_price_ref)

        # Supabase insert (queue → background worker)
        if SUPABASE_LOG_ON and (log_closed_trade is not None):
            try:
                meta = f"|POL={tr.policy_name}|VOL={tr.vol_regime}|BRD={tr.breadth_state}|MFE={tr.max_up_pct:.2f}|MAE={tr.max_dn_pct:.2f}|R={realized_R:+.3f}"
                item = dict(
                    project_id=PROJECT_ID, symbol=tr.symbol, pnl=net_pnl,
                    reason=f"{exit_reason} {meta} || {tr.open_reason}",
                    score=tr.score, r1m=tr.r1m, atr5m=tr.atr5m, z1m=tr.z1m,
                    vshock=tr.vshock, upt=tr.upt, trend=tr.trend, volr=tr.volr,
                )
                try:
                    self._log_queue.put_nowait(item)
                except asyncio.QueueFull:
                    logging.error("Supabase log kuyruğu dolu; kayıt atlandı.")
            except Exception as e:
                logging.error("Supabase enqueue HATASI: %s", e)

        # günlük R ve stage
        day_idx = int(time.time() // 86400)
        if day_idx != self.last_day_idx:
            self.last_day_idx = day_idx
            self.cum_R_today = 0.0
            self.peak_R_today = 0.0
        self.cum_R_today += realized_R
        self.peak_R_today = max(self.peak_R_today, self.cum_R_today)
        dd = self.cum_R_today - self.peak_R_today
        if dd <= DD_STEP_1 and self.stage > 1:
            self.stage = max(1, self.stage - 1)
            logging.info("DAILY DD step1: stage ↓ -> %d", self.stage)
        if dd <= DD_STEP_2:
            self.last_entry_open_ts = now_ts() + 120.0
            logging.info("DAILY DD step2: entries cooldown")

        prev_eq = self.equity
        self.equity += net_pnl

        self.trades += 1
        if exit_reason == "TP":
            self.tp_hits += 1
        if exit_reason == "SL":
            self.sl_hits += 1
        pnl_pct_on_equity = (net_pnl / prev_eq) * 100.0 if prev_eq > 0 else 0.0
        self.pnl_pct_sum += pnl_pct_on_equity

        # bandit güncelle (rejime göre anahtar)
        reg_key, _, _, _ = self._btc_vol_regime()
        self.stats_by_regime[reg_key].update(tr.policy_name, realized_R)

        logging.info(
            "CLOSE #%d %s | qty=%.6f | entry %.6f -> exit %.6f | rsn=%s | netPnL %.4f USDT (%s of prev eq) | MFE %s / MAE %s",
            trade_id, tr.symbol, tr.qty, tr.entry_price_ref, last_price_ref, exit_reason,
            net_pnl, pct(pnl_pct_on_equity), pct(tr.max_up_pct), pct(tr.max_dn_pct)
        )
        logging.info(
            "SUMMARY trades=%d | TP=%d | SL=%d | TP%%=%.1f | equity=%.2f USDT | cumPnL%%=%+.2f | open=%d | stage=%d",
            self.trades, self.tp_hits, self.sl_hits,
            (100.0 * self.tp_hits / self.trades) if self.trades else 0.0,
            self.equity, self.pnl_pct_sum, len(self.active), self.stage
        )

    # -------- Breadth flatten (oran + büyüklük, kalıcılık ile)
    def _flatten_on_breadth_loss(self, t_now: float, ev: dict) -> None:
        """
        İki düzey:
        1) Güçlü NEG veto & kalıcılık (mevcut kural)
        2) AND kuralı: DOWN/N ≥ 0.55 VE ortalama düşüş ≤ -0.50%
           → kısa süreli gürültüde tetikleme yapmamak için kalıcılık penceresi uygulanır.
        """
        N = ev.get("N", 0)
        if N < MIN_BREADTH_N:
            return

        adv = ev.get("adv_ratio", 1.0)
        adv_ema = ev.get("adv_ema", adv)
        neg = ev.get("neg_ratio", 0.0)
        avg_dn = ev.get("avg_dn", 0.0)
        adv_eff = 0.5 * adv + 0.5 * adv_ema

        cushion_R = self._portfolio_cushion_R()
        neg_veto = BREADTH_MAX_NEG_RATIO_VETO + (NEG_TOL_IF_CUSHION if cushion_R >= PNL_CUSHION_R_GRACE else 0.0)
        adv_exit = BREADTH_ADV_RATIO_EXIT - (ADV_TOL_IF_CUSHION if cushion_R >= PNL_CUSHION_R_GRACE else 0.0)

        # 1) Eski veto kalır: çok güçlü NEG ise doğrudan kalıcılık sayacı başlatılır
        strong_neg = (neg >= (neg_veto + BREADTH_STRONG_NEG_PAD))
        if strong_neg:
            if self._breadth_loss_t0 is None:
                self._breadth_loss_t0 = t_now

        # 2) AND kuralı (oran + büyüklük)
        and_rule = (neg >= BREADTH_EARLY_EXIT_NEG_RATIO) and (avg_dn <= BREADTH_EARLY_EXIT_AVG_DN)
        # ayrı bir kalıcılık sayacı (büyüklük tabanlı)
        if not hasattr(self, "_mag_loss_t0"):
            self._mag_loss_t0: Optional[float] = None

        if and_rule and self._mag_loss_t0 is None:
            self._mag_loss_t0 = t_now
        if (not and_rule):
            self._mag_loss_t0 = None

        # Persisten koşul var mı?
        persist_ok = False
        if strong_neg:
            persist_ok = (self._breadth_loss_t0 is not None) and ((t_now - self._breadth_loss_t0) >= BREADTH_LOSS_MIN_SEC)
        if self._mag_loss_t0 is not None:
            persist_ok = persist_ok or ((t_now - self._mag_loss_t0) >= BREADTH_LOSS_MIN_SEC)

        if not persist_ok:
            return

        # Cushion varsa önce BE trail uygula
        if cushion_R >= PNL_CUSHION_R_GRACE:
            uplift = self._breakeven_uplift_pct()
            for tid in list(self.open_queue):
                tr = self.active.get(tid)
                if not tr:
                    continue
                be_price = tr.entry_price_ref * (1.0 + uplift)
                tr.sl_price_ref = max(tr.sl_price_ref, be_price)
            logging.info(
                "BREADTH_SOFT: BE_TRAIL (adv=%.2f ema=%.2f neg=%.2f avg_dn=%.2f cushionR=%.2f)",
                adv, adv_ema, neg, avg_dn, cushion_R
            )
            hard_persist = False
            if self._mag_loss_t0 is not None:
                hard_persist = (t_now - self._mag_loss_t0) >= BREADTH_LOSS_HARD_SEC
            if self._breadth_loss_t0 is not None:
                hard_persist = hard_persist or ((t_now - self._breadth_loss_t0) >= BREADTH_LOSS_HARD_SEC)

            if hard_persist and adv_eff < (adv_exit - 0.03):
                closed = 0
                for tid in list(self.open_queue):
                    tr = self.active.get(tid)
                    if (not tr) or ((t_now - tr.entry_ts) < 15):
                        continue
                    last = self.last_quote.get(tr.symbol, tr.entry_price_ref)
                    _, R = self._compute_net_mtm(tr, last)
                    if R >= 0.05:
                        self._close_trade(tid, "BREADTH_WINNER", last, t_now)
                        closed += 1
                if closed:
                    logging.info("BREADTH_SOFT: winners closed=%d (hard persist)", closed)
            return

        # Cushion yoksa kazananları kapat (yumuşak flatten)
        closed = 0
        for tid in list(self.open_queue):
            tr = self.active.get(tid)
            if (not tr) or ((t_now - tr.entry_ts) < 15):
                continue
            last = self.last_quote.get(tr.symbol, tr.entry_price_ref)
            _, R = self._compute_net_mtm(tr, last)
            if R >= 0.05:
                self._close_trade(tid, "BREADTH_WINNER", last, t_now)
                closed += 1
        if closed:
            logging.info("BREADTH_FLATTEN winners closed=%d (adv=%.2f ema=%.2f neg=%.2f avg_dn=%.2f)", closed, adv, adv_ema, neg, avg_dn)

    # -------- Aktif işlemlerin tick bazlı yönetimi
    def _update_trades_on_tick(self, sym: str, last_price: float, t_now: float) -> None:
        tids = list(self.active_by_symbol.get(sym, set()))
        for tid in tids:
            tr = self.active.get(tid)
            if not tr:
                continue
            tr.update_excursions(last_price)
            tr.last_price = last_price
            net, R = self._compute_net_mtm(tr, last_price)
            tr.mtm_usdt = net
            tr.mtm_R = R

            # periyodik log
            if LOG_EVERY_SEC and (t_now - tr.last_log_ts) >= LOG_EVERY_SEC:
                logging.info(
                    "IN-TRADE #%d %s | entry %.6f | now %.6f | MTM %.3fUSDT (%.3fR) | MFE %s | MAE %s",
                    tid, tr.symbol, tr.entry_price_ref, last_price, net, R, pct(tr.max_up_pct), pct(tr.max_dn_pct)
                )
                tr.last_log_ts = t_now

            # Trailing & kademeli ekleme
            self._apply_trailing(tr, last_price, tr.vol_regime)
            self._maybe_add_ladders(tr, last_price)

            # TP/SL
            if last_price >= tr.tp_price_ref:
                self._close_trade(tid, "TP", last_price, t_now)
                continue
            if last_price <= tr.sl_price_ref:
                self._close_trade(tid, "SL", last_price, t_now)
                continue

            # Stall kontrol
            rsn = self._stall_exit_check(tr, sym, last_price)
            if rsn:
                self._close_trade(tid, rsn, last_price, t_now)

    # -------- Stage
    def _update_stage_by_open_queue(self) -> None:
        def sum_first_k_weighted_R(k: int) -> Optional[float]:
            if len(self.open_queue) < k:
                return None
            vals = [self.active.get(tid).notional_entry for tid in self.open_queue if self.active.get(tid)]
            if not vals:
                return None
            med = sorted(vals)[len(vals) // 2] or 1.0
            s = 0.0
            cnt = 0
            for i, tid in enumerate(self.open_queue):
                tr = self.active.get(tid)
                if tr:
                    w = min(2.0, max(0.5, tr.notional_entry / med))
                    s += tr.mtm_R * w
                    cnt += 1
                if i + 1 == k:
                    break
            return s if cnt >= k else None

        if self.stage == 1:
            s3 = sum_first_k_weighted_R(3)
            if (s3 is not None) and (s3 > STAGE_POS_EPS_R):
                self.stage = 2
                logging.info("STAGE 1→2 (sumWR_first3=%.3f)", s3)
        if self.stage == 2:
            s6 = sum_first_k_weighted_R(6)
            if (s6 is not None) and (s6 > STAGE_POS_EPS_R):
                self.stage = 3
                logging.info("STAGE 2→3 (sumWR_first6=%.3f)", s6)

    def _current_cluster_count(self) -> Dict[str, int]:
        cnt = {"BTCBETA": 0}
        for tid in self.open_queue:
            tr = self.active.get(tid)
            if not tr:
                continue
            sim = self._btc_similarity_cached(tr.symbol)
            if sim >= CLUSTER_SIM_THRESHOLD:
                cnt["BTCBETA"] += 1
        return cnt


    # -------- arka plan görevleri
    async def _log_worker(self) -> None:
        """Supabase insert için arka plan işçisi (blocking'i thread'e at)."""
        if (not SUPABASE_LOG_ON) or (log_closed_trade is None):
            return
        while True:
            item = await self._log_queue.get()
            try:
                # blocking olma ihtimaline karşı thread'e at
                await asyncio.to_thread(log_closed_trade, **item)
            except Exception as e:
                logging.error("Supabase insert HATASI: %s", e)

# ====================== Runner ======================

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    for name in ("httpx", "httpcore", "postgrest", "supabase", "storage3", "gotrue"):
        lg = logging.getLogger(name)
        lg.setLevel(logging.WARNING)   # gerekirse logging.ERROR yap
        lg.propagate = False
        
    core = AlphaCore()

    # Supabase workers
    asyncio.create_task(core._log_worker())
    # reject insert için birden fazla worker (backpressure azaltma)
    for _ in range(3):
        asyncio.create_task(core._rej_log_worker())

    async def hb():
        while True:
            openN = len(core.active)
            touched = len(core._touched_syms)
            logging.info(f"HB | open={openN} | touched={touched}")
            await asyncio.sleep(10)
    asyncio.create_task(hb())

    async def on_tick_cb(tick: BookTicker):
        await core.on_tick(tick)

    await consume_bookticker(on_tick_cb, allowed_quote_suffix="USDT", logger_name="AlphaWS")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass



