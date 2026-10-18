"""Split exported CREATE VIEW scripts into dbt models, one per common table expression."""

import argparse
import codecs
import sys
from collections.abc import Sequence
from pathlib import Path

from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError

from cte2dbt.core import View, convert, dbt_files, read_script


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line; return 0 when the models are written and 1 on an error.

    A usage error exits with status 2, through argparse.
    """
    parser = argparse.ArgumentParser(prog="cte2dbt", description=__doc__)
    parser.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        metavar="INPUT",
        help="a .sql file, or a directory searched recursively for .sql files",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="directory to write the models and sources.yml into; it must be absent or empty",
    )
    parser.add_argument(
        "--dialect",
        default="tsql",
        help="sqlglot dialect of the scripts, settings included (default: %(default)s)",
    )
    parser.add_argument(
        "--default-schema",
        default="dbo",
        metavar="SCHEMA",
        help="schema of the tables that a script names without one (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    if args.output.exists() and not (args.output.is_dir() and not any(args.output.iterdir())):
        parser.error(f"{args.output} must be absent or an empty directory")
    try:
        Dialect.get_or_raise(args.dialect)
    except ValueError as error:
        parser.error(str(error))
    paths = dict.fromkeys(
        file
        for path in args.inputs
        for file in (sorted(path.rglob("*.sql")) if path.is_dir() else [path])
    )
    views: list[View] = []
    warnings: list[str] = []
    status = 0
    for path in paths:
        try:
            data = path.read_bytes()
            utf16 = data[:2] in {codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE}
            script = read_script(
                data.decode("utf-16" if utf16 else "utf-8-sig"), dialect=args.dialect
            )
        except (OSError, ValueError, SqlglotError) as error:
            status = _fail(f"{path}: {error}")
            continue
        views.extend(script.views)
        warnings.extend(f"{path}: {warning}" for warning in script.warnings)
    if not views and not status:
        status = _fail("no CREATE VIEW statement found in the inputs")
    if status:
        return status
    try:
        project = convert(views, dialect=args.dialect, default_schema=args.default_schema)
        files = dbt_files(project)
        args.output.mkdir(parents=True, exist_ok=True)
        for name, content in files.items():
            (args.output / name).write_text(content, encoding="utf-8", newline="\n")
    except (OSError, ValueError, SqlglotError) as error:
        return _fail(str(error))
    sys.stderr.writelines(
        f"cte2dbt: warning: {warning}\n" for warning in [*warnings, *project.warnings]
    )
    count = len(project.models)
    print(f"cte2dbt: wrote {count} model{'s' * (count != 1)} and sources.yml to {args.output}")
    return 0


def _fail(message: str) -> int:
    # A sqlglot parse error goes on with an underlined excerpt of the SQL: keep its first line.
    print("cte2dbt: error:", message.partition("\n")[0], file=sys.stderr)
    return 1
