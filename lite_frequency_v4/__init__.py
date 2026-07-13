"""Poly Alpha Lite Frequency V4, an isolated shadow-only research engine.

The package intentionally owns its configuration, contracts, persistence,
runtime files, data adapters, and exports.  It does not import the legacy Lite
lane or any authenticated trading surface.
"""

from .config import (
    FIXED_SHARES,
    MODE,
    STRATEGY_ID,
    FrequencyV4Config,
    V4Config,
    load_frequency_v4_config,
    load_v4_config,
)

__all__ = [
    "FIXED_SHARES",
    "MODE",
    "STRATEGY_ID",
    "FrequencyV4Config",
    "V4Config",
    "load_frequency_v4_config",
    "load_v4_config",
]
