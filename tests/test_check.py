"""The evidence gate must fail closed: a suite that ran nothing, or skipped, is not a pass."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "dev/scripts/check.py"
spec = importlib.util.spec_from_file_location("check", SCRIPT)
assert spec
assert spec.loader
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


def report(path: Path, **counts: int) -> Path:
    attributes = " ".join(f'{key}="{value}"' for key, value in counts.items())
    path.write_text(f"<testsuites><testsuite {attributes}/></testsuites>")
    return path


def test_a_suite_with_passing_tests_is_accepted(tmp_path: Path) -> None:
    check.require_test_evidence(report(tmp_path / "ok.xml", tests=3, failures=0, skipped=0))


@pytest.mark.parametrize(
    "counts",
    [
        {"tests": 0},
        {"tests": 3, "skipped": 1},
        {"tests": 3, "failures": 1},
        {"tests": 3, "errors": 1},
    ],
)
def test_empty_skipped_or_failing_suites_are_rejected(
    tmp_path: Path, counts: dict[str, int]
) -> None:
    with pytest.raises(ValueError, match="Required suite"):
        check.require_test_evidence(report(tmp_path / "bad.xml", **counts))
