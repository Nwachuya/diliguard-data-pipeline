"""CI lint: none of the known-fake-data sentinel markers may appear in any script
source file. This is the guard against ever re-introducing the old hardcoded
SAP/Siemens/Bolt/Wise/LVMH-style fallback rows that used to ship as if real.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from common.schema import KNOWN_FAKE_SENTINELS  # noqa: E402

REPO_ROOT = Path(__file__).parent.parent
SCAN_DIRS = [REPO_ROOT / "scripts"]
# schema.py itself must be allowed to mention the sentinel strings (it defines them),
# and this test file legitimately quotes them too.
ALLOWLISTED_FILES = {
    REPO_ROOT / "scripts" / "common" / "schema.py",
    REPO_ROOT / "scripts" / "common" / "parquet_io.py",
    REPO_ROOT / "scripts" / "common" / "validate_output.py",
    REPO_ROOT / "scripts" / "update_france_rne.py",  # imports/uses the sentinel list for its own SQL guard
    Path(__file__),
}


def test_no_fake_sentinels_outside_allowlist():
    violations = []
    for scan_dir in SCAN_DIRS:
        for path in scan_dir.rglob("*.py"):
            if path in ALLOWLISTED_FILES:
                continue
            text = path.read_text(encoding="utf-8")
            for sentinel in KNOWN_FAKE_SENTINELS:
                if sentinel in text:
                    violations.append(f"{path}: contains banned sentinel {sentinel!r}")
    assert not violations, "Found fake-data sentinels in source:\n" + "\n".join(violations)
