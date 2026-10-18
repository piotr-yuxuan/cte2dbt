"""Check the Markdown documentation: links, writing rules and every Python example."""

import argparse
import re
import sys
import urllib.request
from pathlib import Path

from sybil import Sybil
from sybil.parsers.markdown import PythonCodeBlockParser

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
REPOSITORY = "https://github.com/piotr-yuxuan/cte2dbt/blob/main/"
LINK = re.compile(r"\]\(([^)\s]+)\)")
SCHEME = re.compile(r"[a-z]+:")
FORBIDDEN = {
    "a Unicode dash": re.compile(r"[\u2010-\u2015\u2212]"),
    "an emoji": re.compile(r"[\u2600-\u27bf\u20e3\U0001f300-\U0001faff]"),
    "a home directory": re.compile(r"/home/|/Users/"),
}
TIMEOUT_SECONDS = 15


def local_problems(file: Path) -> list[str]:
    """Return the broken local links and broken writing rules of one Markdown file.

    README.md is also PyPI's description of the package, where relative links break: its
    links must be absolute, and those into this repository must name an existing file.
    """
    text = file.read_text(encoding="utf-8")
    found = [
        f"{name}: {match.group()!r}"
        for name, pattern in FORBIDDEN.items()
        for match in pattern.finditer(text)
    ]
    for target in LINK.findall(text):
        path = target.split("#")[0]
        if file == README and not target.startswith("https://"):
            found.append(f"PyPI cannot follow the relative link {target}")
        elif file == README and path.startswith(REPOSITORY):
            if not (ROOT / path.removeprefix(REPOSITORY)).exists():
                found.append(f"no such file in the repository: {target}")
        elif path and not SCHEME.match(path) and not (file.parent / path).exists():
            found.append(f"broken link {target}")
    return [f"{file.relative_to(ROOT)}: {problem}" for problem in found]


def external_problems(files: list[Path]) -> list[str]:
    """Fetch every web link once and return those that fail."""
    urls = sorted(
        {
            target
            for file in files
            for target in LINK.findall(file.read_text(encoding="utf-8"))
            if target.startswith(("http://", "https://"))
        }
    )
    found = []
    for url in urls:
        request = urllib.request.Request(url, headers={"User-Agent": "cte2dbt-docs-check"})
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS):
                pass
        except (OSError, ValueError) as error:
            found.append(f"{url}: {error}")
    return found


def run_examples(files: list[Path]) -> int:
    """Run every Python code block, failing on the first error; return how many ran."""
    python = Sybil(parsers=[PythonCodeBlockParser()])
    examples = 0
    for file in files:
        for example in python.parse(file):
            example.evaluate()
            examples += 1
    return examples


def main() -> int:
    """Check the documentation; return 0 when it passes and 1 when a problem is found."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-links", action="store_true", help="also fetch web links")
    args = parser.parse_args()
    files = [
        *sorted(ROOT.glob("*.md")),
        *sorted((ROOT / "docs").rglob("*.md")),
        *sorted((ROOT / "dev/templates").glob("*.md")),
    ]
    found = [problem for file in files for problem in local_problems(file)]
    if args.external_links:
        found += external_problems(files)
    examples = run_examples(files)
    if not examples:
        found.append("no executable Python example in the documentation")
    if found:
        print("\n".join(found), file=sys.stderr)
        return 1
    print(f"Documentation: {len(files)} files, links resolved, {examples} Python examples")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
