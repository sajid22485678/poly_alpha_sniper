import importlib.util
from pathlib import Path


_PATH = Path(__file__).resolve().parent.parent / "scripts" / "replay_lite_history.py"
_SPEC = importlib.util.spec_from_file_location("lite_replay_script", _PATH)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
first_entry_per_window = _MODULE.first_entry_per_window
conflict_metrics = _MODULE.conflict_metrics
metrics = _MODULE.metrics
segments = _MODULE.segments


def _row(identifier, side, pnl, *, window=600_000, asset="BTC"):
    return {
        "id": identifier, "asset": asset, "side": side,
        "entry_ts": 300_000+identifier, "exit_ts": 600_000+identifier,
        "window_close_ts": window,
        "status": "CLOSED_WIN" if pnl > 0 else "CLOSED_LOSS",
        "pnl": pnl,
    }


def test_conflict_free_replay_keeps_only_chronologically_first_direction():
    data = [_row(2, "BUY_NO", -1), _row(1, "BUY_YES", 2)]
    chosen = first_entry_per_window(data)
    assert len(chosen) == 1
    assert chosen[0]["side"] == "BUY_YES"


def test_replay_metrics_use_terminal_net_pnl_without_unresolved_rows():
    data = [_row(1, "BUY_YES", 2), _row(2, "BUY_NO", -1, window=900_000)]
    data.append({**_row(3, "BUY_YES", 999, window=1_200_000),
                 "status": "UNRESOLVED_FINAL", "pnl": None})
    result = metrics(data)
    assert result["completed"] == 2
    assert result["net_realized_pnl"] == 1
    assert result["expectancy"] == 0.5
    assert result["profit_factor"] == 2
    assert result["unresolved"] == 1


def test_chronological_split_leaves_an_untouched_holdout():
    data = [_row(i, "BUY_YES", 1, window=i*300_000) for i in range(1, 11)]
    split = segments(data)
    assert len(split["train"]) == 6
    assert len(split["validation"]) == 2
    assert len(split["holdout"]) == 2
    assert split["train"].isdisjoint(split["holdout"])


def test_conflict_rate_is_explicit_and_candidate_is_zero():
    data = [
        _row(1, "BUY_YES", 1), _row(2, "BUY_NO", -1),
        _row(3, "BUY_YES", 1, window=900_000),
    ]
    before = conflict_metrics(data)
    after = conflict_metrics(first_entry_per_window(data))
    assert before == {
        "asset_windows": 2,
        "opposite_side_conflicts": 1,
        "opposite_side_conflict_rate": 0.5,
    }
    assert after["opposite_side_conflicts"] == 0
