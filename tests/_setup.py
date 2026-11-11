"""Makes the modules of kafka/ importable from the tests (the folder is a plain directory of scripts)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "kafka"))
