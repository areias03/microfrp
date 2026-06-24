"""Make the resource-generation scripts importable as plain modules.

The scoring scripts live in `resource_generation/` and are normally invoked as
standalone scripts, so they are not packaged. Add their directories to the path
so the pure scoring functions can be imported and unit tested.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "resource_generation"))
sys.path.insert(0, str(ROOT / "resource_generation" / "functional_roles"))
