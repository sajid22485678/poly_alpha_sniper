"""Experiment registry: deterministic config-hash bookkeeping for research."""
from __future__ import annotations

import hashlib
import json


def config_hash(params: dict) -> str:
    canonical = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


class ExperimentRegistry:
    def __init__(self, store=None):
        self.store = store
        self._experiments: dict[str, dict] = {}
        self._next_id = 1

    def register(self, name: str, params: dict, ts_ms: int = 0) -> str:
        exp_id = f"exp-{self._next_id}"
        self._next_id += 1
        row = {"id_str": exp_id, "name": name, "config_hash": config_hash(params),
               "params": json.dumps(params, default=str)[:2000], "metrics": "",
               "ts_ms": ts_ms}
        self._experiments[exp_id] = row
        if self.store is not None:
            self.store.insert("experiment_registry", {k: v for k, v in row.items()
                                                      if k != "id_str"})
        return exp_id

    def record_result(self, exp_id: str, metrics: dict, ts_ms: int = 0) -> None:
        exp = self._experiments.get(exp_id)
        if exp is None:
            raise KeyError(exp_id)
        exp["metrics"] = json.dumps(metrics, default=str)[:2000]
        if self.store is not None:
            self.store.insert("experiment_registry", {
                "name": exp["name"], "config_hash": exp["config_hash"],
                "params": exp["params"], "metrics": exp["metrics"], "ts_ms": ts_ms})

    def to_records(self) -> list[dict]:
        return list(self._experiments.values())
