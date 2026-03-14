import sys
from pathlib import Path

# Ensure the project root is on sys.path so that archive.py (a top-level
# script, not an installed package) can be imported in tests.
sys.path.insert(0, str(Path(__file__).parent))
