from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure repository root is on sys.path for imports like `ingestion.app...`
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
