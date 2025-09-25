# binance_io_V4.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import socket
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import websockets

# ============================ Konfig ============================

# Yalnız combined stream uçları (daha stabil)
WSS_URLS = [
    "wss://fstream.binance.com/stream?streams=!bookTicker",
    "wss://fstream1.binance.com/stream?streams=!bookTicker",
    "wss://fstream2.binance.com/stream?streams=!bookTicker",
    "wss://fstream3.binance.com/stream?streams=!bookTicker",
]

# REST base'leri (dinamik sağlık puanı ile sıralanacak)
REST_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
]

# ---- WS ayarları
DISPATCH_INTERVAL_SEC = 0.25     # coalesced flush penceresi
MAX_DISPATCH_PER_CYCLE = 800     # her flush’ta max sembol
PING_INTERVAL_SEC = 15.0         # websockets’in kendi pingi de açık
PING_TIMEOUT_SEC = 10.0
WS_CONNECT_TIMEOUT = 10.0
WS_READ_MAX_MS = 5000            # reader recv timeout
SILENCE_WATCHDOG_SEC = 30.0      # push gelmezse bu süreden sonra kapat

# ---- Reconnect backoff (sınırı düşük tut: uzun boşluk olmasın)
BACKOFF_MIN = 1.0
BACKOFF_MAX = 6.0
BACKOFF_FACTOR = 1.7

# ---- REST ayarları (V4: daha esnek timeout + daha düşük concurrency)
# (PATCH) tail kesmek için biraz daha agresif yaptık; ağır ağda eski değerlere dönebilirsin.
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=8.0, connect=2.5, sock_read=5.0)
HTTP_CONCURRENCY = 4
HTTP_RETRY = 3
HTTP_RETRY_BASE = 0.35  # exponential jitter

# Global oran sınırlama (token bucket) – başlangıç hedef hızı
RATE_LIMIT_RPS = 8.0     # saniyede ~8 istek (toplam)
RATE_BURST    = 12       # kısa süreli burst kapasitesi

# Ağırlık başlığına göre adaptif throttle (PATCH)
WEIGHT_LIMIT_1M  = 2400        # Binance FAPI typical limit
WEIGHT_SOFT_CAP  = 0.80        # used/limit > 0.80 → yavaşla
MIN_RPS = 3.0
MAX_RPS = 12.0

# 429/418 için
RETRY_AFTER_CAP_SEC = 5.0  # Retry-After gelirse bu kadarla sınırla

# Geçici hata alan anahtarları kısa süre “negatif cache” et
FAIL_COOLDOWN_SEC = 12.0

# TTL (core ayrıca kendi TTL kontrolünü yapıyor ama burada da tutalım)
KLINE_TTL = {"1m": 10.0, "5m": 45.0, "15m": 90.0, "1h": 240.0}

# ExchangeInfo allowlist
UNIVERSE_QUOTE_SUFFIX = "USDT"
EXCHANGEINFO_TTL_SEC = 600.0  # 10 dk'da bir yenile

# (PATCH) Hedged request – p95 düşürmek için alternatif host’a paralel deneme
# 0 → kapalı. 1200 ms iyi bir başlangıç.
HEDGE_AFTER_MS = 1200
# ================================================================

# (PATCH) paylaşılan connector + default header’lar
_HEADERS = {
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}
# EKLE
def _make_connector() -> aiohttp.TCPConnector:
    # Bu fonksiyon bir coroutine içinde çağrılacağı için loop hazır olacak
    return aiohttp.TCPConnector(
        limit=0,               # concurrency’i biz semaforla sınırlıyoruz
        limit_per_host=0,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        family=socket.AF_UNSPEC,
    )


@dataclass
class BookTicker:
    symbol: str
    bid_price: float
    ask_price: float
    ts: float  # local receive time (s)


# ------------------------- ExchangeInfo / Sembol Evreni -------------------------

class _SymbolUniverse:
    """
    /fapi/v1/exchangeInfo çekerek TRADING & PERPETUAL & quote==UNIVERSE_QUOTE_SUFFIX
    sembolleri allowlist'e koyar. TTL süresi dolunca arka planda yeniler.
    """
    def __init__(self) -> None:
        self._last_ts: float = 0.0
        self._allowed: set[str] = set()
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if (self._session is None) or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=HTTP_TIMEOUT,
                raise_for_status=False,
                connector=_make_connector(),   # ← burada oluştur
                headers=_HEADERS,
                auto_decompress=True,
            )
        return self._session


    async def refresh_if_needed(self) -> None:
        async with self._lock:
            now = time.time()
            if (now - self._last_ts) < EXCHANGEINFO_TTL_SEC and self._allowed:
                return
            try:
                session = await self._ensure_session()
                base = random.choice(REST_BASES)
                url = base + "/fapi/v1/exchangeInfo"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        try:
                            _ = await resp.text()
                        except Exception:
                            pass
                        return
                    data = await resp.json(content_type=None)
                    syms = set()
                    for s in (data.get("symbols") or []):
                        try:
                            if (
                                s.get("status") == "TRADING"
                                and s.get("contractType") == "PERPETUAL"
                                and s.get("quoteAsset") == UNIVERSE_QUOTE_SUFFIX
                                and isinstance(s.get("symbol"), str)
                            ):
                                syms.add(s["symbol"])
                        except Exception:
                            continue
                    if syms:
                        self._allowed = syms
                        self._last_ts = now
            except Exception:
                return

    async def is_allowed(self, symbol: str) -> bool:
        await self.refresh_if_needed()
        if not self._allowed:
            return True
        return symbol.upper() in self._allowed


_UNIVERSE = _SymbolUniverse()


# ------------------------- REST Kline Katmanı -------------------------

class _KlineCache:
    """
    TTL'li kline cache + arkaplanda fetch + RPS limiter + Retry-After + negatif-cache.
    get_or_request() senkron ve hızlı döner:
      - Taze cache varsa onu döndürür.
      - Yoksa stale/empty döndürür ve arka planda fetch tetikler.
    """
    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str], Tuple[float, Any]] = {}
        self._pending: set[Tuple[str, str]] = set()
        self._queue: "asyncio.Queue[Tuple[str, str, int]]" = asyncio.Queue(maxsize=4096)
        self._session: Optional[aiohttp.ClientSession] = None
        self._sema = asyncio.Semaphore(HTTP_CONCURRENCY)
        self._worker_task: Optional[asyncio.Task] = None

        # Global token-bucket rate limiter
        self._rate_lock = asyncio.Lock()
        self._tb_tokens = float(RATE_BURST)
        self._tb_updated = time.time()
        self._target_rps = RATE_LIMIT_RPS  # (PATCH) adaptif hedef

        # Negatif cache
        self._neg_cache: Dict[Tuple[str, str], float] = {}

        # Host sağlık puanı (düşük olan tercih edilir)
        self._host_penalty: Dict[str, float] = {h: 0.0 for h in REST_BASES}
        self._host_lock = asyncio.Lock()

        # (PATCH) max-limit aware cache
        self._max_limit: Dict[Tuple[str, str], int] = {}

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if (self._session is None) or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=HTTP_TIMEOUT,
                raise_for_status=False,
                connector=_make_connector(),   # ← burada oluştur
                headers=_HEADERS,
                auto_decompress=True,
            )
        return self._session


    def _ttl_ok(self, interval: str, ts: float) -> bool:
        ttl = KLINE_TTL.get(interval, 30.0)
        return (time.time() - ts) < ttl

    def start(self) -> None:
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker(), name="KLINE_WORKER")

    async def _rate_gate(self) -> None:
        """Global RPS limiter (token bucket) – (PATCH) adaptif refill."""
        async with self._rate_lock:
            while True:
                now = time.time()
                elapsed = now - self._tb_updated
                if elapsed > 0:
                    self._tb_tokens = min(RATE_BURST, self._tb_tokens + elapsed * self._target_rps)
                    self._tb_updated = now
                if self._tb_tokens >= 1.0:
                    self._tb_tokens -= 1.0
                    return
                need = 1.0 - self._tb_tokens
                wait = need / max(1e-9, self._target_rps)
                await asyncio.sleep(min(wait, 0.25))

    async def _tune_rps_by_weight(self, used_w: Optional[str]) -> None:
        """(PATCH) X-MBX-USED-WEIGHT-1M'e göre hedef RPS ayarla."""
        if not used_w:
            return
        try:
            used = int(used_w)
        except Exception:
            return
        ratio = used / max(1, WEIGHT_LIMIT_1M)
        if ratio > WEIGHT_SOFT_CAP:
            self._target_rps = max(MIN_RPS, self._target_rps * 0.8)
        else:
            self._target_rps = min(MAX_RPS, self._target_rps * 1.05)

    async def _pick_base(self) -> str:
        """Host sağlık puanına göre base seç (düşük puan iyidir)."""
        async with self._host_lock:
            items = list(self._host_penalty.items())
            items.sort(key=lambda kv: kv[1] + random.random() * 0.1)
            return items[0][0] if items else random.choice(REST_BASES)

    async def _bump_host(self, base: str, ok: bool, hard: bool = False) -> None:
        async with self._host_lock:
            cur = self._host_penalty.get(base, 0.0)
            if ok:
                self._host_penalty[base] = max(0.0, cur * 0.9 - 0.05)
            else:
                self._host_penalty[base] = cur + (1.0 if hard else 0.4)

    async def _worker(self) -> None:
        while True:
            sym, interval, limit = await self._queue.get()
            key = (sym, interval)
            try:
                await self._fetch_and_store(sym, interval, limit)
            except Exception as e:
                logging.warning("KLINE fetch hata: %s %s L=%s err=%s", sym, interval, limit, e)
                self._neg_cache[key] = time.time()  # cooldown
            finally:
                self._pending.discard(key)

    def get_or_request(self, symbol: str, interval: str, limit: int) -> Any:
        key = (symbol, interval)
        now = time.time()
        item = self._cache.get(key)

        # (PATCH) en büyük limit'i hatırla
        self._max_limit[key] = max(int(limit), self._max_limit.get(key, 0) or 0)

        # Taze cache varsa direkt dön
        if item and self._ttl_ok(interval, item[0]):
            return item[1]

        # Negatif cache: yakın zamanda hata aldıysa tekrar kuyruğa basma
        neg_ts = self._neg_cache.get(key)
        if (neg_ts is not None) and ((now - neg_ts) < FAIL_COOLDOWN_SEC):
            return item[1] if item else []

        # Arka plan fetch tetikle
        self.start()
        if key not in self._pending:
            self._pending.add(key)
            want = self._max_limit[key]
            try:
                self._queue.put_nowait((symbol.upper(), interval, int(want)))
            except asyncio.QueueFull:
                self._pending.discard(key)
        return item[1] if item else []

    async def _do_request(self, base: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """(PATCH) Tek bir HTTP isteğini yap ve sonuç meta ile dön."""
        url = base + "/fapi/v1/klines"
        async with self._sema:
            await self._rate_gate()
            session = await self._ensure_session()
            t0 = time.time()
            try:
                async with session.get(url, params=params) as resp:
                    dt = (time.time() - t0) * 1000.0
                    status = resp.status
                    used_w = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    retry_after = resp.headers.get("Retry-After")
                    if status == 200:
                        data = await resp.json(content_type=None)
                        return {"ok": True, "data": data, "dt": dt, "status": status,
                                "base": base, "used_w": used_w, "retry_after": retry_after}
                    else:
                        body = ""
                        try:
                            body = (await resp.text())[:200]
                        except Exception:
                            pass
                        return {"ok": False, "data": None, "dt": dt, "status": status,
                                "base": base, "used_w": used_w, "retry_after": retry_after, "body": body}
            except Exception as e:
                return {"ok": False, "data": None, "dt": (time.time()-t0)*1000.0, "status": None,
                        "base": base, "used_w": None, "retry_after": None, "exc": repr(e)}

    async def _fetch_and_store(self, symbol: str, interval: str, limit: int) -> None:
        # Evren izinli mi? (allowlist yüklenmediyse engelleme yok)
        try:
            allowed = await _UNIVERSE.is_allowed(symbol)
            if not allowed:
                return
        except Exception:
            pass

        key = (symbol, interval)
        params = {"symbol": symbol.upper(), "interval": interval, "limit": int(limit)}
        last_exc: Optional[Exception] = None

        for attempt in range(1, HTTP_RETRY + 1):
            base1 = await self._pick_base()

            # İlk denemeyi başlat
            task1 = asyncio.create_task(self._do_request(base1, params))

            results: List[Dict[str, Any]] = []

            # Hedge tetiklenmezse ya task1 beklenir, ya da hedge zamanı dolunca task2 açılır
            if HEDGE_AFTER_MS:
                try:
                    res1 = await asyncio.wait_for(task1, timeout=HEDGE_AFTER_MS / 1000.0)
                    results.append(res1)
                except asyncio.TimeoutError:
                    # ikinci host
                    alt = [b for b in REST_BASES if b != base1]
                    base2 = (await self._pick_base()) if not alt else random.choice(alt)
                    task2 = asyncio.create_task(self._do_request(base2, params))
                    done, pending = await asyncio.wait({task1, task2}, return_when=asyncio.FIRST_COMPLETED)
                    results.extend([t.result() for t in done])
                    # Eğer başarı yoksa kalan task’i de bekle (tam sonuç için)
                    if not any(r.get("ok") and isinstance(r.get("data"), list) for r in results):
                        results.extend(await asyncio.gather(*pending, return_exceptions=False))
            else:
                results.append(await task1)

            # Sonuçlardan biri başarılı mı?
            success = next((r for r in results if r.get("ok") and isinstance(r.get("data"), list)), None)

            # RPS tuning (hangi header geldiyse)
            for r in results:
                await self._tune_rps_by_weight(r.get("used_w"))

            if success:
                data = success["data"]
                base_ok = success["base"]
                dt = float(success["dt"] or 0.0)
                self._cache[key] = (time.time(), data)
                self._neg_cache.pop(key, None)
                await self._bump_host(base_ok, ok=True)
                if dt > 1200:
                    logging.info("KLINE slow %s %s L=%s host=%s dt=%.0fms weight=%s",
                                 symbol, interval, limit, base_ok, dt, success.get("used_w") or "-")
                return

            # Başarısız – en anlamlı hatayı seç
            # Önce rate-limit varsa onu baz al, yoksa ilk sonucu
            err = None
            rate = next((r for r in results if r.get("status") in (418, 429)), None)
            chosen = rate or (results[0] if results else None)

            if chosen is not None:
                status = chosen.get("status")
                base_bad = chosen.get("base")
                dt = float(chosen.get("dt") or 0.0)
                used_w = chosen.get("used_w") or "-"
                retry_after = chosen.get("retry_after")
                body = chosen.get("body", "")
                if status in (418, 429):
                    sleep_s = 1.0
                    if retry_after:
                        try:
                            sleep_s = min(RETRY_AFTER_CAP_SEC, float(retry_after))
                        except Exception:
                            pass
                    logging.warning("KLINE rate-limit %s %s L=%s host=%s status=%s retry=%.2fs weight=%s",
                                    symbol, interval, limit, base_bad, status, sleep_s, used_w)
                    await self._bump_host(base_bad, ok=False, hard=False)
                    last_exc = RuntimeError(f"http {status}")
                    await asyncio.sleep(sleep_s)
                elif status and (500 <= status < 600):
                    logging.warning("KLINE 5xx %s %s L=%s host=%s status=%s dt=%.0fms body=%r",
                                    symbol, interval, limit, base_bad, status, dt, body)
                    await self._bump_host(base_bad, ok=False, hard=True)
                    last_exc = RuntimeError(f"http {status}")
                else:
                    # socket/timeout vs
                    await self._bump_host(base_bad or "?", ok=False, hard=False)
                    last_exc = RuntimeError(f"http {status} {body!r}" if status else chosen.get("exc", "error"))
            else:
                last_exc = RuntimeError("unknown fetch error")

            # exponential backoff + jitter
            await asyncio.sleep(HTTP_RETRY_BASE * attempt * (1.0 + random.random()))

        if last_exc:
            raise last_exc

    async def close(self) -> None:
        if self._session and (not self._session.closed):
            await self._session.close()


_KLINES = _KlineCache()


def get_futures_klines(symbol: str, interval: str, limit: int) -> Any:
    """
    Senkron görünen çağrı:
      - Taze cache varsa onu döndürür.
      - Yoksa stale/empty döner ve arka planda fetch başlatır (event loop bloklanmaz).
    """
    return _KLINES.get_or_request(symbol, interval, limit)


def parse_klines(payload: Any) -> List[dict]:
    """
    Binance FAPI klines payload'ını standart dict listesine çevirir.
    [
      [
        0 openTime(ms), 1 "open", 2 "high", 3 "low", 4 "close", 5 "volume",
        6 closeTime(ms), 7 "quoteAssetVolume", 8 trades, 9 "takerBuyBase",
        10 "takerBuyQuote", 11 ignore
      ],
      ...
    ]
    -> {"o","h","l","c","volume","quote","ts"}
    """
    out: List[dict] = []
    if not isinstance(payload, list):
        return out
    for row in payload:
        try:
            o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
            v = float(row[5]); q = float(row[7])
            ts = float(row[6]) / 1000.0  # closeTime'i bar ts olarak kullan
            out.append({"o": o, "h": h, "l": l, "c": c, "volume": v, "quote": q, "ts": ts})
        except Exception:
            continue
    return out


# ------------------------- WS BookTicker Katmanı -------------------------

async def consume_bookticker(
    on_tick_cb,
    *,
    allowed_quote_suffix: str = "USDT",
    logger_name: str = "BinanceWS",
) -> None:
    """
    !bookTicker combined stream akışını tüketir ve coalesced şekilde on_tick_cb(BookTicker) çağırır.
    - allowed_quote_suffix: yalnız USDT pariteleri gibi filtre için
    - on_tick_cb: async callable(BookTicker)
    """
    log = logging.getLogger(logger_name)

    latest: Dict[str, Tuple[float, float, float]] = {}  # sym -> (bid, ask, ts)
    touched: set[str] = set()

    async def dispatcher():
        # coalesced flush
        while True:
            await asyncio.sleep(DISPATCH_INTERVAL_SEC)
            if not touched:
                continue
            symbols = list(touched)[:MAX_DISPATCH_PER_CYCLE]
            touched.difference_update(symbols)
            if not symbols:
                continue
            ticks = []
            for sym in symbols:
                tup = latest.get(sym)
                if not tup:
                    continue
                b, a, ts = tup
                ticks.append(BookTicker(sym, b, a, ts))
            if not ticks:
                continue
            sem = asyncio.Semaphore(128)
            async def _call(tick: BookTicker):
                async with sem:
                    await on_tick_cb(tick)
            await asyncio.gather(*(_call(t) for t in ticks), return_exceptions=True)

    async def reader(ws):
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=WS_READ_MAX_MS / 1000.0)
            except asyncio.TimeoutError:
                continue
            if not raw:
                continue
            try:
                msg = json.loads(raw)
                data = msg.get("data", msg)
                sym = data.get("s")
                if not sym:
                    continue
                if allowed_quote_suffix and (not sym.endswith(allowed_quote_suffix)):
                    continue
                b = float(data.get("b", "0") or 0.0)
                a = float(data.get("a", "0") or 0.0)
                if b <= 0.0 or a <= 0.0:
                    continue
                ts = time.time()
                latest[sym] = (b, a, ts)
                touched.add(sym)
            except Exception:
                continue

    async def watchdog(ws):
        # Push gelmezse kapat → outer loop reconnect
        last_seen = time.time()
        while True:
            await asyncio.sleep(5.0)
            recent = 0.0
            if latest:
                try:
                    recent = max(t[2] for t in latest.values())
                except Exception:
                    recent = 0.0
            last_ts = recent or last_seen
            if (time.time() - last_ts) > SILENCE_WATCHDOG_SEC:
                log.warning("WS watchdog: no tick for %.0fs, closing socket to reconnect", time.time() - last_ts)
                try:
                    await ws.close()
                except Exception:
                    pass
                return

    backoff = BACKOFF_MIN
    while True:
        for url in WSS_URLS:
            try:
                log.info("WS CONNECT %s", url)
                async with websockets.connect(
                    url,
                    ping_interval=PING_INTERVAL_SEC,
                    ping_timeout=PING_TIMEOUT_SEC,
                    close_timeout=3.0,
                    open_timeout=WS_CONNECT_TIMEOUT,
                    max_queue=None,
                ) as ws:
                    # kline worker + evren yenileyici aktif
                    _KLINES.start()
                    try:
                        await _UNIVERSE.refresh_if_needed()
                    except Exception:
                        pass

                    tasks = [
                        asyncio.create_task(dispatcher(), name="BT_DISPATCH"),
                        asyncio.create_task(reader(ws),   name="BT_READER"),
                        asyncio.create_task(watchdog(ws), name="BT_WATCHDOG"),
                    ]
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                    for t in pending:
                        t.cancel()
                    for t in done:
                        exc = t.exception()
                        if exc:
                            raise exc
            except Exception as e:
                emsg = str(e)
                log.warning("WS error: %s (retry in %.1fs)", emsg, backoff)
                await asyncio.sleep(backoff + random.random() * 0.5)
                backoff = min(BACKOFF_MAX, max(BACKOFF_MIN, backoff * BACKOFF_FACTOR))
                continue
            backoff = BACKOFF_MIN

        await asyncio.sleep(min(3.0, backoff))
        backoff = min(BACKOFF_MAX, max(BACKOFF_MIN, backoff * 1.5))


# ------------------------- Yardımcı -------------------------

async def _shutdown():
    await _KLINES.close()


# Basit demo (manual run)
if __name__ == "__main__":
    async def _demo():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
        )

        async def on_tick(t: BookTicker):
            if t.symbol in ("BTCUSDT", "ETHUSDT"):
                pass

        await consume_bookticker(on_tick, allowed_quote_suffix="USDT", logger_name="AlphaWS")

    try:
        asyncio.run(_demo())
    except KeyboardInterrupt:
        pass
