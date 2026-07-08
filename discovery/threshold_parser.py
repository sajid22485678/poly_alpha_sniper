"""Deterministic market title/rules parser. NO AI — regex and word rules only.

Extracts asset, market type (up/down vs threshold), threshold price, expiry
hints and computes a parse confidence 0-100. Anything ambiguous is rejected;
this feeds the token mapping validator and the market mapper.

ASSUMPTIONS:
- Polymarket 5-minute crypto series use titles like:
    "Bitcoin Up or Down - July 8, 3:40 PM ET"
    "Ethereum Up or Down - 5 minute window"
    "Will BTC be above $108,500 at 3:45 PM ET?"
- "$108.5k" style thresholds normalize to 108500.
"""
from __future__ import annotations

import re

from poly_alpha_sniper.core.contracts import MarketType, ParsedMarket, RejectReason

ASSET_WORDS: dict[str, list[str]] = {
    "BTC": [r"\bbtc\b", r"\bbitcoin\b", r"\bbtcusdt\b", r"\bxbt\b"],
    "ETH": [r"\beth\b", r"\bethereum\b", r"\bethusdt\b"],
    "SOL": [r"\bsol\b", r"\bsolana\b", r"\bsolusdt\b"],
}

SUBJECTIVE_WORDS = [
    "say", "says", "tweet", "tweets", "announce", "announces", "rumor", "rumour",
    "insider", "approve", "approves", "ban", "sue", "lawsuit", "opinion",
    "sentiment", "etf decision", "hack", "will elon", "trump", "president",
]

UP_DOWN_PATTERNS = [
    r"\bup or down\b", r"\bhigher or lower\b", r"\bup/down\b", r"\bhigher/lower\b",
]

THRESHOLD_DIR_PATTERNS = {
    "above": r"\babove\b", "below": r"\bbelow\b", "higher than": r"\bhigher than\b",
    "lower than": r"\blower than\b", "reach": r"\breach(es)?\b",
    "at or above": r"\bat or above\b", "at or below": r"\bat or below\b",
    "greater than": r"\bgreater than\b", "less than": r"\bless than\b",
}

_PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)\s*([kK])?")
_TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}\s*(?:am|pm)?\s*(?:et|est|edt|utc|gmt)?)\b", re.I)
_DATE_RE = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|october|"
    r"november|december)\s+\d{1,2}\b", re.I)
_DURATION_RE = re.compile(r"\b(\d+)\s*[- ]?\s*(minute|min|hour|hr)s?\b", re.I)


def _find_assets(text: str) -> list[str]:
    found = []
    for asset, patterns in ASSET_WORDS.items():
        if any(re.search(p, text) for p in patterns):
            found.append(asset)
    return found


def parse_market_text(title: str, description: str = "") -> ParsedMarket:
    text = f"{title} {description}".lower()
    notes: list[str] = []
    confidence = 100.0

    # subjective / non-price wording -> fatal
    for w in SUBJECTIVE_WORDS:
        if w in text:
            return ParsedMarket(ok=False, reject_reason=RejectReason.AMBIGUOUS_MARKET,
                                confidence=0.0, notes=[f"subjective word: {w}"])

    assets = _find_assets(text)
    if len(assets) == 0:
        return ParsedMarket(ok=False, reject_reason=RejectReason.AMBIGUOUS_MARKET,
                            confidence=0.0, notes=["no asset found"])
    if len(assets) > 1:
        return ParsedMarket(ok=False, reject_reason=RejectReason.AMBIGUOUS_MARKET,
                            confidence=0.0, notes=[f"multiple assets: {assets}"])
    asset = assets[0]

    # market type
    market_type = MarketType.UNKNOWN
    direction_word = ""
    if any(re.search(p, text) for p in UP_DOWN_PATTERNS):
        market_type = MarketType.UP_DOWN
        direction_word = "up"
    else:
        for word, pat in THRESHOLD_DIR_PATTERNS.items():
            if re.search(pat, text):
                market_type = MarketType.THRESHOLD
                direction_word = word
                break

    if market_type == MarketType.UNKNOWN:
        # price mention alone is not enough to trade safely
        return ParsedMarket(ok=False, asset=asset,
                            reject_reason=RejectReason.AMBIGUOUS_MARKET,
                            confidence=20.0, notes=["unrecognized market phrasing"])

    # threshold extraction
    threshold = None
    if market_type == MarketType.THRESHOLD:
        m = _PRICE_RE.search(text)
        if not m:
            return ParsedMarket(ok=False, asset=asset, market_type=market_type,
                                reject_reason=RejectReason.AMBIGUOUS_MARKET,
                                confidence=10.0, notes=["threshold market without parseable price"])
        threshold = float(m.group(1).replace(",", ""))
        if m.group(2):
            threshold *= 1000.0
        if threshold <= 0:
            return ParsedMarket(ok=False, asset=asset, market_type=market_type,
                                reject_reason=RejectReason.AMBIGUOUS_MARKET,
                                confidence=0.0, notes=["non-positive threshold"])
        # plausibility bands (deterministic sanity: catches parsing the wrong number)
        bands = {"BTC": (1_000, 10_000_000), "ETH": (10, 1_000_000), "SOL": (0.5, 100_000)}
        lo, hi = bands[asset]
        if not (lo <= threshold <= hi):
            confidence -= 40
            notes.append(f"threshold {threshold} outside plausible band for {asset}")

    # expiry hints
    expiry_hint = ""
    tm = _TIME_RE.search(text)
    dm = _DATE_RE.search(text)
    dur = _DURATION_RE.search(text)
    if tm:
        expiry_hint = tm.group(1)
    if dm:
        expiry_hint = (expiry_hint + " " + dm.group(0)).strip()
    if dur:
        expiry_hint = (expiry_hint + f" [{dur.group(1)}{dur.group(2)}]").strip()
        notes.append(f"duration: {dur.group(1)} {dur.group(2)}")
    if not expiry_hint:
        # NOT penalized: the API endDate is the authoritative expiry and the
        # classifier enforces the tradable window from it. Titles like
        # "Bitcoin Up or Down - 5m" are perfectly clear markets.
        notes.append("no visible expiry hint in text (API endDate is authoritative)")

    ok = confidence >= 50
    return ParsedMarket(
        ok=ok, asset=asset, market_type=market_type, direction_word=direction_word,
        threshold=threshold, expiry_hint=expiry_hint,
        reject_reason="" if ok else RejectReason.AMBIGUOUS_MARKET,
        confidence=max(0.0, confidence), notes=notes)
