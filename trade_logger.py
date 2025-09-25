"""
Supabase trade logger (closed trades + entry rejects), NO ENV VARS.

Nasıl kullanılır:
- Aşağıdaki SABİTLERİ doldur: SB_URL, SB_KEY, TABLE_CLOSED, TABLE_REJECTS
- Kapanış insert: log_closed_trade(...)
- Reject insert : log_entry_reject(...), safe_log_entry_reject(...)

Tablolar:
  TABLE_CLOSED  (örn: "closed_trades_simple")
    columns: project_id(text), symbol(text), pnl(numeric), reason(text),
             score, r1m, atr5m, z1m, vshock, upt, trend, volr

  TABLE_REJECTS (örn: "entry_rejects")
    columns: id, ts, project_id, symbol, reason, side, score, spread_bps,
             atr_pct, r_ratio, liq_rank, rank, breadth_adv, breadth_state,
             relax_active, open_cnt, touched, vol_regime, ev_r, tp_pct, sl_pct,
             sim_btc, cluster_bucket, price, bid, ask, dbg, extra(jsonb)
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

try:
    from supabase import create_client, Client  # pip install supabase
except Exception as e:  # pragma: no cover
    raise ImportError("supabase-py gerekli. Kur: pip install supabase") from e


# ============================== SABİTLER (DÜZENLE) =============================

SB_URL = "https://jrdiedgyizhrkmrcaqns.supabase.co"   # ← kendi URL'in
SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpyZGllZGd5aXpocmttcmNhcW5zIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTgzMTAwMDgsImV4cCI6MjA3Mzg4NjAwOH0.1ka5rFreRcNDI-YnltGzVcVy8dR6DUo2p9N8YX5sEmQ"  # ← kendi anon key'in

TABLE_CLOSED  = "closed_trades_simple"  # kapanışların gittiği tablo
TABLE_REJECTS = "entry_rejects"         # entry reddinin gittiği tablo


# ============================== Client Singleton ==============================

_CLIENT: Optional[Client] = None

def _get_client() -> Client:
    """Create/get a cached Supabase client using the constants above."""
    global _CLIENT
    if _CLIENT is None:
        if not SB_URL or not SB_KEY:
            raise RuntimeError("SB_URL / SB_KEY sabitlerini doldurun.")
        _CLIENT = create_client(SB_URL, SB_KEY)
    return _CLIENT


# ============================== Utilities =====================================

_NUM = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)"

_PATTERNS = {
    "score": re.compile(r"\[\s*SCORE\s+(" + _NUM + r")\s*\]"),
    "r1m": re.compile(r"r1m=\s*(" + _NUM + r")\s*%?"),
    "atr5m": re.compile(r"ATR5m%=\s*(" + _NUM + r")"),
    "z1m": re.compile(r"z1m=\s*(" + _NUM + r")"),
    "vshock": re.compile(r"(?:Vshock|VSHOCK)=\s*(" + _NUM + r")"),
    "upt": re.compile(r"upt=\s*(" + _NUM + r")"),
    "trend": re.compile(r"trend=\s*(" + _NUM + r")\s*%?"),
    "volr": re.compile(r"(?:volR|volr)=\s*(" + _NUM + r")"),
}

def _parse_from_dbg(s: Optional[str]) -> Dict[str, Optional[float]]:
    """Pull metrics out of a debug/reason string. Missing -> None."""
    out: Dict[str, Optional[float]] = {
        "score": None, "r1m": None, "atr5m": None, "z1m": None,
        "vshock": None, "upt": None, "trend": None, "volr": None,
    }
    if not s:
        return out
    for k, rx in _PATTERNS.items():
        m = rx.search(s)
        if m:
            try:
                out[k] = float(m.group(1))
            except ValueError:
                out[k] = None
    return out

def _coalesce(*vals: Optional[float]) -> Optional[float]:
    for v in vals:
        if v is not None:
            return float(v)
    return None


# ============================== Entry Reject Logger ===========================

def log_entry_reject(
    project_id: str,
    *,
    symbol: str,
    reason: str,
    side: Optional[str] = None,
    score: Optional[float] = None,
    spread_bps: Optional[float] = None,
    atr_pct: Optional[float] = None,
    r_ratio: Optional[float] = None,
    liq_rank: Optional[int] = None,
    rank: Optional[int] = None,
    breadth_adv: Optional[float] = None,
    breadth_state: Optional[str] = None,
    relax_active: Optional[bool] = None,
    open_cnt: Optional[int] = None,
    touched: Optional[int] = None,
    vol_regime: Optional[str] = None,
    ev_r: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
    sim_btc: Optional[float] = None,
    cluster_bucket: Optional[str] = None,
    price: Optional[float] = None,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    dbg: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    table: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Tekil reject satırı insert eder ve inserted row'u döndürür.
    project_id TEXT olarak yazılır (UUID zorunluluğu yok).
    """
    row = {
        "project_id": str(project_id),
        "symbol": symbol,
        "reason": reason,
        "side": side,
        "score": score,
        "spread_bps": spread_bps,
        "atr_pct": atr_pct,
        "r_ratio": r_ratio,
        "liq_rank": liq_rank,
        "rank": rank,
        "breadth_adv": breadth_adv,
        "breadth_state": breadth_state,
        "relax_active": relax_active,
        "open_cnt": open_cnt,
        "touched": touched,
        "vol_regime": vol_regime,
        "ev_r": ev_r,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "sim_btc": sim_btc,
        "cluster_bucket": cluster_bucket,
        "price": price,
        "bid": bid,
        "ask": ask,
        "dbg": dbg,
        "extra": extra or {},
    }
    client = _get_client()
    table_name = table or TABLE_REJECTS
    resp = client.table(table_name).insert(row).execute()
    if not getattr(resp, "data", None):
        raise RuntimeError(f"Reject insert başarısız: {resp}")
    return resp.data[0]


def safe_log_entry_reject(*args, **kwargs) -> None:
    """
    Fazladan gelen tüm anahtarları extra(jsonb) içine toplar,
    imzada beklenen alanları normal kolonlara yazar.
    """
    try:
        # Pozisyonel kullanım gelirse (nadiren) normalize et
        if args:
            # Beklenen tek positional: project_id
            kwargs.setdefault("project_id", args[0])

        KNOWN = {
            "project_id","symbol","reason","side","score","spread_bps","atr_pct",
            "r_ratio","liq_rank","rank","breadth_adv","breadth_state","relax_active",
            "open_cnt","touched","vol_regime","ev_r","tp_pct","sl_pct","sim_btc",
            "cluster_bucket","price","bid","ask","dbg","extra","table"
        }

        payload = {k: v for k, v in kwargs.items() if k in KNOWN}
        extras  = {k: v for k, v in kwargs.items() if k not in KNOWN}

        # Alias/mapping: çekirdek "cluster_used" yolluyor → tablo "cluster_bucket"
        if "cluster_bucket" not in payload and "cluster_used" in extras:
            payload["cluster_bucket"] = str(extras.pop("cluster_used"))

        # Var olan extra ile birleştir
        extra_dict = payload.get("extra") or {}
        if extras:
            extra_dict = {**extra_dict, **extras}
        payload["extra"] = extra_dict

        # Zorunlu alan kontrolü (erken uyarı)
        assert "project_id" in payload and "symbol" in payload and "reason" in payload, \
            "project_id, symbol, reason zorunlu"

        log_entry_reject(**payload)
    except Exception as e:
        print(f"Reject log hatası: {e}")

# ============================== Closed Trade Logger ===========================

def log_closed_trade(
    project_id: str,
    symbol: str,
    pnl: float,
    reason: str,
    *,
    # optional explicit metrics
    score: Optional[float] = None,
    r1m: Optional[float] = None,
    atr5m: Optional[float] = None,
    z1m: Optional[float] = None,
    vshock: Optional[float] = None,
    upt: Optional[float] = None,
    trend: Optional[float] = None,
    volr: Optional[float] = None,
    # optionally pass a debug string to parse (e.g. open debug)
    dbg: Optional[str] = None,
    table: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Insert one closed trade row. Returns inserted row dict.
    project_id TEXT olarak yazılır (UUID zorunluluğu yok).
    """
    parsed = _parse_from_dbg(dbg or reason)

    row = {
        "project_id": str(project_id),
        "symbol": symbol,
        "pnl": float(pnl),
        "reason": reason,
        "score": _coalesce(score, parsed.get("score")),
        "r1m": _coalesce(r1m, parsed.get("r1m")),
        "atr5m": _coalesce(atr5m, parsed.get("atr5m")),
        "z1m": _coalesce(z1m, parsed.get("z1m")),
        "vshock": _coalesce(vshock, parsed.get("vshock")),
        "upt": _coalesce(upt, parsed.get("upt")),
        "trend": _coalesce(trend, parsed.get("trend")),
        "volr": _coalesce(volr, parsed.get("volr")),
    }

    client = _get_client()
    table_name = table or TABLE_CLOSED
    resp = client.table(table_name).insert(row).execute()
    if not getattr(resp, "data", None):
        raise RuntimeError(f"Insert başarısız: {resp}")
    return resp.data[0]


def safe_log_closed_trade(*args, **kwargs) -> None:
    """Insert hatalarını yutar (opsiyonel)."""
    try:
        log_closed_trade(*args, **kwargs)
    except Exception as e:
        print(f"Closed trade log hatası: {e}")


__all__ = [
    "log_closed_trade",
    "safe_log_closed_trade",
    "log_entry_reject",
    "safe_log_entry_reject",
]
