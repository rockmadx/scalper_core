# === entry_gates.py (veya core dosyana direkt ekle) ==========================
import time, logging
from collections import Counter
from dataclasses import dataclass

# ---- Varsayılan knob'lar (istenirse config'ten besle) ----------------------
BASE_SCORE_LONG  = 0.60   # yön-eşikleri (ör: MOM/zscore)
BASE_SCORE_SHORT = 0.60
MAX_SPREAD_BPS_MAJOR = 8   # BTC/ETH/BNB vb için
MAX_SPREAD_BPS_ALT   = 20  # küçük/illiquid için
ATR_CAP_MAJOR_PCT = 0.7/100
ATR_CAP_ALT_PCT   = 1.5/100
R_MIN_DEFAULT     = 1.50   # min beklenen R (TP/SL oranı)
COOLDOWN_TOUCH_SEC = 2.0   # touched→open kaçaklarını engellemek için çok uzun olmasın

# Breadth’e göre dinamik eşik çarpanları
BREADTH_STRONG   = 0.80    # adv>0.80 → long yumuşat
BREADTH_WEAK     = 0.20    # adv<0.20 → short yumuşat
BREADTH_RELAX    = 0.85    # ilgili yön için skor eşiğini 0.90x yap

# “Touched çok, open 0” anında geçici gevşetme
RELAX_PRESSURE_TOUCHED = 300   # touched >= 300
RELAX_PRESSURE_OPEN    = 0     # open == 0
RELAX_DURATION_SEC     = 60.0  # 1 dk sonra eski değerlere dön
RELAX_TOPK             = 5     # sadece en iyi 5 adaya uygula
RELAX_SPREAD_MULT      = 1.50  # spread sınırı 1.5x
RELAX_ATR_MULT         = 1.20  # ATR tavanı 1.2x
RELAX_R_MIN            = 1.20  # R gereksinimi düşür

# Büyük/küçük ayrımı için basit hacim/sıra eşiği (core’daki likidite/sıra metriğini kullan)
MAJOR_LIQUIDITY_RANK_MAX = 50  # rank <=50 ise major say


@dataclass
class CandidateMetrics:
    side: str           # "long" / "short"
    score: float        # MOM/zscore vb pozitif metrik
    spread_bps: float   # (ask-bid)/mid * 1e4
    atr_pct: float      # ATR(14)/mid
    r_ratio: float      # beklenen TP/SL oranı (R)
    liq_rank: int       # 1 en likit
    rank: int | None    # skor sıralaması (0 en iyi). Yoksa None geç
    symbol: str         # sadece log için


class RejectionStats:
    """Ret gerekçelerini sayar ve periyodik olarak loglar."""
    def __init__(self, log_name="ENTRY") -> None:
        self.cnt = Counter()
        self.best_rejected: tuple[float,str,str] | None = None  # (score,symbol,reason)
        self.last_flush = 0.0
        self.log = logging.getLogger(log_name)

    def record(self, reason: str, cand: CandidateMetrics) -> None:
        self.cnt[reason] += 1
        if (self.best_rejected is None) or (cand.score > self.best_rejected[0]):
            self.best_rejected = (cand.score, cand.symbol, reason)

    def flush(self, interval_sec: float = 10.0) -> None:
        now = time.time()
        if now - self.last_flush < interval_sec:
            return
        self.last_flush = now
        if self.cnt:
            top = ", ".join(f"{k}={v}" for k,v in self.cnt.most_common(6))
            self.log.info("REJECTS: %s", top)
            self.cnt.clear()
        if self.best_rejected:
            s, sym, r = self.best_rejected
            self.log.info("BEST_REJECTED: %s score=%.3f reason=%s", sym, s, r)
            self.best_rejected = None


class DynamicGates:
    """Breadth ve baskı (touched/open) ile dinamikleşen giriş kapıları."""
    def __init__(self) -> None:
        self.relax_until_ts = 0.0  # baskı altında geçici gevşetme zamanı

    def begin_cycle(self, *,
                    breadth_adv: float,
                    touched: int,
                    open_cnt: int) -> None:
        """Her seçim turunun başında çağır: baskı altında geçici gevşetmeyi aç/kapat."""
        now = time.time()
        # touched çok ve open 0 → geçici gevşetme
        if (touched >= RELAX_PRESSURE_TOUCHED) and (open_cnt <= RELAX_PRESSURE_OPEN):
            self.relax_until_ts = max(self.relax_until_ts, now + RELAX_DURATION_SEC)
        # süre dolduysa otomatik kapanır (extra iş yok)

        # Breadth’e göre taban eşikleri hazırla
        self.score_long  = BASE_SCORE_LONG
        self.score_short = BASE_SCORE_SHORT
        if breadth_adv >= BREADTH_STRONG:
            # market risk-on → long eşiğini %10 azalt
            self.score_long *= BREADTH_RELAX
        elif breadth_adv <= BREADTH_WEAK:
            # market risk-off → short eşiğini %10 azalt
            self.score_short *= BREADTH_RELAX

    def _is_relax_active_for(self, cand: CandidateMetrics) -> bool:
        if cand.rank is None:
            return False
        if cand.rank >= RELAX_TOPK:
            return False
        return time.time() <= self.relax_until_ts

    def _limits_for(self, cand: CandidateMetrics) -> tuple[float,float,float,float]:
        """cand için (score_th, max_spread_bps, atr_cap_pct, r_min) döndür."""
        # yön skor eşiği
        score_th = self.score_long if cand.side == "long" else self.score_short

        # major/alt spread ve ATR limitleri
        is_major = (cand.liq_rank <= MAJOR_LIQUIDITY_RANK_MAX)
        max_spread = MAX_SPREAD_BPS_MAJOR if is_major else MAX_SPREAD_BPS_ALT
        atr_cap    = ATR_CAP_MAJOR_PCT if is_major else ATR_CAP_ALT_PCT
        r_min      = R_MIN_DEFAULT

        # geçici gevşetme sadece top-K ve süre içinde
        if self._is_relax_active_for(cand):
            max_spread = max_spread * RELAX_SPREAD_MULT
            atr_cap    = atr_cap    * RELAX_ATR_MULT
            r_min      = RELAX_R_MIN

        return score_th, float(max_spread), float(atr_cap), float(r_min)

    def check_candidate(self, cand: CandidateMetrics, rej: RejectionStats | None = None) -> tuple[bool,str]:
        """Tek aday için tüm kapıları uygula. (ok, reason) döner."""
        score_th, max_spread, atr_cap, r_min = self._limits_for(cand)

        if cand.score < score_th:
            if rej: rej.record("score", cand)
            return False, "score"

        if cand.spread_bps > max_spread:
            if rej: rej.record("spread", cand)
            return False, "spread"

        if cand.atr_pct > atr_cap:
            if rej: rej.record("atr_cap", cand)
            return False, "atr_cap"

        if cand.r_ratio < r_min:
            if rej: rej.record("risk_R", cand)
            return False, "risk_R"

        # Opsiyonel: touched sonrası kısa cooldown
        # Core’da symbol bazlı son-deneme/sentetik cooldown tutuyorsan burada kontrol edebilirsin.

        return True, "ok"
# ============================================================================

