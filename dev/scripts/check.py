"""Shared, fail-closed checks for people, agents and CI; run with uv run --locked."""

import argparse
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / "dev/reports"
COVERAGE_FLOOR = 0.8
VULTURE_FOUND_DEAD_CODE = 3


def run(*command: str) -> None:
    """Print a command, then run it from the repository root; raise when it fails."""
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def require_test_evidence(report: Path) -> None:
    """Reject empty, skipped or unsuccessful suites even if a runner returned zero."""
    suites = list(ET.parse(report).getroot().iter("testsuite"))
    tests = sum(int(suite.get("tests", "0")) for suite in suites)
    problems = sum(
        int(suite.get(key, "0")) for suite in suites for key in ("failures", "errors", "skipped")
    )
    if not tests or problems:
        msg = f"Required suite has {tests} tests and {problems} failures/errors/skips"
        raise ValueError(msg)


def fast() -> None:
    """Check the lock, formatting, lint and types, then test with coverage evidence."""
    run("uv", "lock", "--check")
    run("ruff", "format", "--check", "src", "tests", "dev/scripts")
    run("ruff", "check", "src", "tests", "dev/scripts")
    run("mypy")
    run("lint-imports")
    report = REPORTS / "tests.xml"
    coverage = REPORTS / "coverage.xml"
    REPORTS.mkdir(parents=True, exist_ok=True)
    report.unlink(missing_ok=True)
    coverage.unlink(missing_ok=True)
    run(
        sys.executable,
        "-m",
        "pytest",
        "tests",
        f"--junitxml={report}",
        "--cov=cte2dbt",
        "--cov-branch",
        "--cov-report=term-missing",
        "--cov-report=xml",
        f"--cov-fail-under={COVERAGE_FLOOR * 100:g}",
    )
    require_test_evidence(report)
    root = ET.parse(coverage).getroot()
    possible = sum(int(root.attrib[key]) for key in ("lines-valid", "branches-valid"))
    covered = sum(int(root.attrib[key]) for key in ("lines-covered", "branches-covered"))
    if not possible or covered / possible < COVERAGE_FLOOR:
        msg = f"Combined statement and branch coverage is missing or below {COVERAGE_FLOOR:.0%}"
        raise ValueError(msg)


def dead_code() -> None:
    """Report dead code without failing: dynamic Python can produce false positives."""
    try:
        run("vulture", "src", "tests", "--min-confidence", "100")
    except subprocess.CalledProcessError as error:
        if error.returncode != VULTURE_FOUND_DEAD_CODE:
            raise
        print("ADVISORY: investigate Vulture findings; dynamic Python can produce false positives")


def security() -> None:
    """Audit every locked dependency, development groups included, for known advisories."""
    with TemporaryDirectory(prefix="cte2dbt-audit-") as directory:
        requirements = str(Path(directory) / "requirements.txt")
        run(
            "uv",
            "export",
            "--quiet",
            "--locked",
            "--all-groups",
            "--no-emit-project",
            "--format",
            "requirements-txt",
            "--output-file",
            requirements,
        )
        run("pip-audit", "--require-hashes", "-r", requirements)


def package() -> None:
    """Build the distributions and use the wheel from a clean environment, as users will."""
    dist = REPORTS / "dist"
    run("uv", "build", "--clear", "--out-dir", str(dist))
    wheels = list(dist.glob("*.whl"))
    if len(wheels) != 1:
        msg = f"Expected one wheel in {dist}, found {len(wheels)}"
        raise ValueError(msg)
    with zipfile.ZipFile(wheels[0]) as wheel:
        if "cte2dbt/py.typed" not in wheel.namelist():
            msg = "The wheel does not ship cte2dbt/py.typed"
            raise ValueError(msg)
    with TemporaryDirectory(prefix="cte2dbt-package-") as directory:
        python = str(Path(directory) / "bin/python")
        run("uv", "venv", "--quiet", directory)
        run("uv", "pip", "install", "--quiet", "--python", python, str(wheels[0]))
        run(python, "-I", "-c", "import cte2dbt")
        run(str(Path(directory) / "bin/cte2dbt"), "--help")


def main() -> int:
    """Run the checks of one mode; return 0 when they pass and 1 when one fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("fast", "docs", "security", "verify"),
        default="fast",
        help="verify = fast + dead code (advisory) + docs + security + package",
    )
    parser.add_argument("--external-links", action="store_true", help="also fetch web links")
    args = parser.parse_args()
    if args.external_links and args.mode not in ("docs", "verify"):
        parser.error("--external-links applies to docs or verify")
    try:
        if args.mode in ("fast", "verify"):
            fast()
        if args.mode == "verify":
            dead_code()
        if args.mode in ("docs", "verify"):
            # A separate process keeps this script free of third-party imports.
            links = ["--external-links"] if args.external_links else []
            run(sys.executable, str(ROOT / "dev/scripts/check_docs.py"), *links)
        if args.mode in ("security", "verify"):
            security()
        if args.mode == "verify":
            package()
    except (OSError, subprocess.CalledProcessError, ValueError, ET.ParseError) as error:
        print(f"CHECK FAILED: {error}", file=sys.stderr)
        return 1
    print(f"{args.mode}: passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
