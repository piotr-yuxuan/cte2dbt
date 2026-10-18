# 001: Refresh for 2.0

## Scope and authority

cte2dbt 1.0.5 (March 2025) no longer works: sqlglot 28 renamed the `with` argument
to `with_`, so on current sqlglot no CTE is extracted and nothing fails. The
published wheel also declares no dependency on sqlglot, so it cannot be imported
after `pip install cte2dbt`. The owner now works where Excel users read Azure SQL
data through T-SQL views, and wants this repository ready to turn exported view
definitions into dbt models. Migrating those views is a separate, later project.

Outcome: cte2dbt 2.0.0, prepared but not published.

- A pure conversion core that keeps every part of the SQL it does not rewrite,
  understands T-SQL views (CREATE VIEW, GO batches, case-insensitive names,
  recursion, column lists) and converts several views into one dependency graph,
  where a view read by another view becomes `ref()`.
- A `cte2dbt` command line that turns a folder of exported view scripts into a
  dbt models directory with `sources.yml`.
- The owner's house tooling: uv, Ruff `ALL`, mypy `--strict`, a fail-closed
  `dev/scripts/check.py`, Sybil-tested Markdown, the python-playground CI template
  and a publish workflow gated on the checks.
- Documentation restructured along [Diátaxis](https://diataxis.fr/), with a short
  README and no emoji.

Excluded: publishing, tagging or pushing; converting stored procedures, functions
or DML; executing models on SQL Server in CI; editing the owner's `ROADMAP.md`
journal, which is uncommitted in the primary checkout.

Owner and coordinator: piotr-yuxuan. Writer: a delegated agent on branch
`001-refresh` in worktree `.worktrees/001-refresh`. The owner reviews and merges;
the writer has no authority to merge, push or publish.

## Decisions

Agreed with the owner on 2026-10-16:

1. Scope: single-view T-SQL readiness, a multi-view batch, and a command line.
2. Tooling matches the recent repositories. The import path becomes `cte2dbt`
   (the `com.github.piotr_yuxuan` namespace goes). Ruff `select = ["ALL"]` with a
   short commented ignore list, pydocstyle `pep257`, mypy `--strict`.
3. CI is the full house template, plus a compatibility matrix for a library.
4. Documentation is Markdown in `docs/`, read on GitHub.
5. Release 2.0.0 is prepared and not published.
6. `Provider` is replaced by pure functions that return frozen results.

Design, chosen against the simplest viable alternative (keeping `Provider` and
patching the `with` key): the patch alone leaves every verified data-loss defect
in place, so the core is rewritten around one rule, "classify each original table
once, then edit it in place".

- Package: `cte2dbt.core` (pure: no I/O, no logging) below `cte2dbt.cli` (all
  I/O), enforced by an Import Linter layers contract.
- API: `read_script(text, *, dialect) -> Script`,
  `convert(views, *, dialect, default_schema) -> Project` and
  `dbt_files(project) -> dict[str, str]`. The library has no dialect or schema
  default; the command line defaults to `tsql` and `dbo`.
- Results hold text. Jinja is written into the rendered SQL from placeholders,
  never into the syntax tree, and user text that would open Jinja is escaped.
- The 1.x `to_dbt_ref_block` and `to_dbt_source_block` callbacks are dropped:
  rendering `Model.sql` with jinja2 and any `ref` and `source` functions replaces
  them.
- sqlglot `>=30.0,<31`: 30.0 introduced `exp.Expr`, which strict typing needs,
  and sqlglot minors are not semver, so the cap is bumped deliberately.
- Python `>=3.11`: 3.10 reached end of life on 2026-10-01, and dbt-fabric still
  needs a Python below 3.14.

## Acceptance

Agreed by the owner through the approved design. Model SQL is compared with
whitespace normalised.

1. `CREATE VIEW dbo.v_sales AS WITH a AS (SELECT id FROM dbo.Sales) SELECT * FROM a`
   (tsql, default schema `dbo`) gives model `v_sales__a` =
   `SELECT id FROM {{ source("dbo", "Sales") }} AS Sales` and model `v_sales` =
   `SELECT * FROM {{ ref("v_sales__a") }} AS a`; `dbo.v_sales` is not a source.
2. An SSMS export with `SET ANSI_NULLS ON`, `GO` lines and an
   `EXEC sp_addextendedproperty` batch gives the same models and one warning for
   the skipped `EXEC`.
3. `FOR SYSTEM_TIME AS OF`, `WITH (NOLOCK)`, `PIVOT` on a base table,
   `TABLESAMPLE`, a parenthesised join and an alias column list survive unchanged
   around the `source()`.
4. `WITH sales AS (...) SELECT x FROM SALES` in tsql refs the CTE; with
   `"tsql, normalization_strategy=case_sensitive"` it reads the source `SALES`.
5. A recursive CTE stays whole in its own model, `WITH r AS (...) SELECT * FROM r`,
   and no model ever refs itself.
6. View `dbo.v2` reading `dbo.V1`, given before `dbo.v1`, refs `v1`, and `v1` comes
   first in `Project.models`. Views in a cycle raise `ValueError`.
7. `dbo.fn(1)`, `OPENJSON(@j)`, four-part names, `#tmp` and `@t` stay as written,
   each with a warning, and are not sources.
8. `cte2dbt views -o models` writes one `.sql` per model plus `sources.yml` and exits
   0. It exits 2 when `models` is a non-empty directory, and 1, writing nothing,
   when a file cannot be parsed or no `CREATE VIEW` is found.
9. Running each converted chain on DuckDB returns the same rows as the original
   query, for every curated case and for random CTE chains.
10. `uv run --locked python dev/scripts/check.py verify` passes, including a clean
    install of the built wheel. `pip install cte2dbt` would pull sqlglot.

Evidence that no check provides: T-SQL validity rests on sqlglot's T-SQL generator
and on DuckDB runs of transpiled T-SQL. The owner runs the converted views against
a development Azure SQL database with dbt-sqlserver before relying on them.

## Implementation slices

1. Plan the refresh.
2. Package with uv under the flat name; the minimal `Query.ctes` fix makes the 1.x
   tests pass on sqlglot 30.
3. Replace `Provider` with the pure core (single view).
4. Shared check script and strict lint settings; `check.py fast` passes from here.
5. Column lists and recursive CTEs.
6. Several views in one dependency graph.
7. Reading exported scripts.
8. Model files and `sources.yml`.
9. The command line and golden directories.
10. DuckDB execution equivalence and a Hypothesis property.
11. README and Diátaxis documentation; `check.py verify` passes from here.
12. GitHub Actions checks.
13. Gated publishing.
14. Prepare release 2.0.0.
15. Archive this plan.

## Handover

- Owner: piotr-yuxuan. Branch `001-refresh` from `c8e3fdb` (1.0.5), worktree
  `.worktrees/001-refresh`.
- The primary checkout keeps the owner's uncommitted work: the `ROADMAP.md`
  journal, superseded edits to `pyproject.toml`, `poetry.lock` and the old module,
  an Emacs autosave file, and the ignored `dist/`. None is touched here. Before
  merging, keep the journal and stash or discard the superseded edits.

## Outcome and verification

To be completed during finalisation.
