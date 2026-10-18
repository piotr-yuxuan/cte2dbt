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
- sqlglot `>=30.19,<31`: 30.0 introduced `exp.Expr`, which strict typing needs, and
  sqlglot minors are not semver, so the cap is bumped deliberately. The floor was 30.0
  until the handover check: before 30.18 sqlglot printed T-SQL `JSON_VALUE` and
  `JSON_QUERY` as `ISNULL(JSON_QUERY(...), JSON_VALUE(...))`, which changes results, and
  before 30.19 it attached a union's `ORDER BY` to its last `SELECT`.
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

Delivered on branch `001-refresh` from `c8e3fdbe09123baa06d5c0c332add1bbaa9d840d`
(1.0.5). Every slice landed. Slice 6, several views in one dependency graph, landed
within slice 3 (`b4c6071`), because the new core took a sequence of views from the
start. Acceptance examples 1 to 9 are each a test; example 10 is `check.py verify`. One
decision changed during review: the sqlglot floor rose from 30.0 to 30.19, as the
decisions above record. Nothing was tagged, pushed or published.

Evidence at `4055b3b16c044bb27169adc96c32dc5d7ab9ae50`, on NixOS with Python 3.14 and
the locked sqlglot 30.23.0. The commit after it only archives this record.

- `check.py verify` passed: Ruff, mypy, Import Linter, 193 tests at 99.49% combined
  statement and branch coverage (99.69% of lines, 98.72% of branches), the
  documentation (21 files, 13 Python examples), pip-audit with no known vulnerability,
  and the wheel installed in a clean environment, imported and run with `--help`.
  vulture reported nothing. Not covered: the re-raise after a failed batch is parsed
  again for its line numbers, which the second parse always raises first, and one
  branch in `_requalify`.
- The compatibility legs, run in scratch copies as `ci.yml` runs them: Python 3.11 and
  3.15 with sqlglot 30.19.0, and Python 3.11, 3.12, 3.13 and 3.15 with 30.23.0, all
  passed; Python 3.14 ran in `verify`. On 3.15 the DuckDB comparisons are skipped, as
  DuckDB has no wheel for it.
- zizmor and actionlint reported nothing; gitleaks, run through `dev/scripts/tool.sh`,
  found no leak in the 130 commits it scanned.
- Acceptance in dbt: the 12 models of the three DuckDB golden inputs, converted in one
  run with `--dialect duckdb --default-schema main`, built with dbt-core 1.12.5 and
  dbt-duckdb 1.11.0 in `<scratch>/dbt-accept`, with no warning or error. The T-SQL
  golden input was not built.
- Scale, over several runs: `read_script` read 2000 SSMS-style views in 2.3 to 3.0 s,
  against about 0.3 s for 250, and `convert` took 1.0 to 1.4 s for them; 2000
  schema-qualified columns converted in about 0.1 s.

Review, each round by a principles reviewer and a SQL reviewer, whose findings
verifiers then reproduced or rejected with DuckDB as oracle, except where stated:

1. At `f59b281`: 35 findings; 3 blocking and 5 major confirmed, 4 not reproduced.
   Views after an `IF ... DROP VIEW` batch were lost, and a test silenced mypy; nested
   `WITH` scope, column qualifiers, recursive clauses and quoted names were wrong. Fixed
   in `facbd38`, `bd5a847`, `0319468` and `edbff3a`. sqlglot's printing of some T-SQL
   as near-equivalents is not fixed: `2a1c855` documents it and makes the comparison on
   the database a required step.
2. At `2a1c855`: 16 findings, 4 major: quadratic batch padding, gaps in the view guard,
   column qualifiers with a database, and forward references. Fixed in `36671f1`.
3. At `36671f1`: 15 findings, 3 major: the guard fired on prose, `SEARCH` with `CYCLE`
   lost a clause, and two schemas spelt alike merged. Fixed in `c4d2058`.
4. At `c4d2058`: a recursive union in parentheses, a qualified column in a union's
   `ORDER BY`, and comments inside raw text. Fixed in `1144bea` and `d6179d3`.
5. At `d6179d3`: one major. On sqlglot 30.0.0 the guard could not tokenise a skipped
   statement after a `--/*` line. Fixed in `b854402`.
6. At `b854402`, by a principles reviewer alone: no blocking or major finding, after a
   differential run of about 9400 generated scripts at both ends of the sqlglot range.
   Its two test remarks are taken in `c3d5179`. Checking the outcome then showed that
   sqlglot reads most materialised views as plain views, although the conversion rules
   list them as errors, and drops ClickHouse's `TO` clause; `8aa7e5b` refuses them.
7. At `c3d5179`, by a principles reviewer alone: no blocking or major finding, after
   running seven materialised view forms in every sqlglot dialect, about 70 vendor
   forms and 35 plain views at both ends of the range. Its three test remarks, among
   them a plain view with a `MATERIALIZED` common table expression, are taken in
   `269386f`.
8. A handover check at `269386f`, by three read-only checkers of the claims, the
   documentation and the readiness to merge, each followed by an adversarial verifier:
   25 findings, 21 confirmed, of which 4 major, two of them the same. sqlglot below
   30.18 changed `JSON_VALUE` and `JSON_QUERY`, and below 30.19 placed a union's
   `ORDER BY` differently: `beba364` raises the floor to 30.19. The merge commands of the
   development procedure failed in the worktree layout, and the guard and five smaller
   rules were described wrongly: corrected in `6e1df35`. The SSMS export test did not
   assert its SQL: `6f0526f`. The figures of this record and the merge message are
   corrected here.
9. At `6e1df35`, by a principles reviewer alone: one major. The command-line reference
   said that a warning spans lines only for a line break in a string; raw text, comments
   and quoted names do so too. Corrected in `fdeb774`, which also states how Oracle's
   `WITH CHECK OPTION` is read. Its remarks that no test pinned the floor of 30.19 or the
   view forms that are read are taken in `14d5704`.
10. At `fdeb774`, by a principles reviewer alone: no blocking or major finding, every
    corrected sentence and new test holding at sqlglot 30.19.0 and 30.23.0. Its two
    remarks, that warnings are printed only once the files are written and that no test
    pinned Oracle's `WITH CHECK OPTION`, are taken in `4055b3b`.

Limits of the evidence:

- Nothing ran on SQL Server or Azure SQL. T-SQL validity rests on sqlglot's T-SQL
  generator and on DuckDB runs of transpiled T-SQL; the
  [limits](../explanation/conversion.md#limits) list what sqlglot prints differently.
- The workflows have not run on GitHub; the compatibility legs ran locally. The publish
  workflow has never run, and relies on PyPI's existing binding of its file name and
  the `pypi` environment.

Follow-up, each owned by piotr-yuxuan:

- Printing only the table names into the original text would avoid sqlglot's
  near-equivalents altogether. Decide before converting views that use a construct the
  limits list.
- The guard searches strings and quoted names, so an extended property whose text says
  `create view` stops the run, as the conversion rules state. Revisit if a real export
  is refused for it.
- The command line drops sqlglot's notices other than its fallbacks, such as an invalid
  JSON path, without a warning. Revisit with the first item.
- Consider yanking 1.0.5 on PyPI, which cannot be imported, once 2.0.0 is published
  there.

Owner actions, in the primary checkout on `main`:

1. Set aside the three superseded edits that block the merge, and leave the
   `ROADMAP.md` journal, which the branch does not touch:
   `git stash push -m "pre-2.0 edits" -- poetry.lock pyproject.toml src/com/github/piotr_yuxuan/cte2dbt.py`.
2. Complete the draft merge message in the worktree's ignored
   `dev/reports/merge-decision.txt`, then run
   `git merge --no-ff 001-refresh -F .worktrees/001-refresh/dev/reports/merge-decision.txt`
   and `check.py verify` on the merged `main`.
3. Run the converted views on a development Azure SQL database with dbt-sqlserver and
   compare them with `EXCEPT`, as the how-to guide describes.
4. On GitHub, add a ruleset that enforces `CODEOWNERS`, and optionally required
   reviewers on the `pypi` environment; then create the release `2.0.0` from `main`,
   with its release notes as description.
