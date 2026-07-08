"""Shadow vs live execution discrepancy: does live match what shadow assumed?"""
from __future__ import annotations


class ShadowLiveDiscrepancy:
    def __init__(self, maxlen: int = 200):
        self._shadow: dict[str, dict] = {}
        self.pairs: list[dict] = []
        self.maxlen = maxlen

    def record_shadow(self, signal_id: str, hypothetical_fill_price: float,
                      size_usd: float) -> None:
        self._shadow[signal_id] = {"price": hypothetical_fill_price, "size": size_usd}

    def record_live(self, signal_id: str, actual_fill_price: float,
                    size_usd: float) -> None:
        shadow = self._shadow.pop(signal_id, None)
        if shadow is None:
            return
        diff_bps = 0.0
        if shadow["price"] > 0:
            diff_bps = (actual_fill_price - shadow["price"]) / shadow["price"] * 10_000
        self.pairs.append({"signal_id": signal_id, "shadow_price": shadow["price"],
                           "live_price": actual_fill_price, "size_usd": size_usd,
                           "diff_bps": round(diff_bps, 1)})
        if len(self.pairs) > self.maxlen:
            self.pairs = self.pairs[-self.maxlen:]

    def report(self) -> dict:
        if not self.pairs:
            return {"n": 0, "mean_diff_bps": 0.0, "verdict": "no paired data yet"}
        mean = sum(p["diff_bps"] for p in self.pairs) / len(self.pairs)
        verdict = ("live matches shadow" if abs(mean) < 20 else
                   "live meaningfully worse than shadow — recheck fill model"
                   if mean > 0 else "live better than shadow (conservative model)")
        return {"n": len(self.pairs), "mean_diff_bps": round(mean, 1),
                "verdict": verdict, "recent": self.pairs[-10:]}
