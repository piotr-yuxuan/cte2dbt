# Checks

Install with `uv sync --locked`. Every mode below is
`uv run --locked python dev/scripts/check.py MODE`.

| Mode | Evidence |
| --- | --- |
| `fast` | The lockfile, Ruff formatting and lint (every rule, McCabe complexity at most 10), mypy `--strict`, the Import Linter layers contract, then the tests with at least 80% combined statement and branch coverage. |
| `docs` | In every Markdown file: local links resolve, no Unicode dash, emoji or home directory path, and every Python block runs (Sybil). README links are absolute, as PyPI shows the README, and those into this repository name an existing file. `--external-links` also fetches each web link. |
| `security` | pip-audit over every locked dependency, development groups and the build backend included. Needs network access. |
| `verify` | `fast`, Vulture (advisory: findings print, execution errors fail), `docs`, `security`, then the package: build the distributions into `dev/reports/dist`, check that the wheel ships `py.typed`, install it into a clean environment, import it and run `cte2dbt --help`. |

The test report is deleted before each run. A missing, empty, skipped, failed or erroneous
suite fails the check even when pytest returned zero, as `tests/test_check.py` requires.
Reports go to the ignored `dev/reports/`.

The execution tests compare each converted view with the original on DuckDB, through
sqlglot's T-SQL to DuckDB transpiler for T-SQL. Passing them says nothing about SQL
Server itself.
