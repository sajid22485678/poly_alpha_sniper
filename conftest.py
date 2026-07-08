"""Pytest bootstrap: make `poly_alpha_sniper.*` importable from repo root."""
import sys
from pathlib import Path

_parent = str(Path(__file__).resolve().parent.parent)
if _parent not in sys.path:
    sys.path.insert(0, _parent)
