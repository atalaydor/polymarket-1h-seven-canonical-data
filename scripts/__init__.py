"""Repository-local production command bootstrap."""

from __future__ import annotations

import sys
from pathlib import Path

# Active Actions matrices install the triggering SHA before queued jobs start, then
# pull main. Always execute canonical modules from the same pulled tree as scripts so
# a repaired control plane cannot be paired with a stale installed wheel.
_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))
