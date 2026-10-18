# Python API

Everything below is importable from `cte2dbt`. The functions are pure: they read no file,
log nothing themselves and never modify their arguments. sqlglot, which they call, may
log notices to the `sqlglot` logger.

```python
from sqlglot import exp, parse_one

from cte2dbt import Model, Project, Script, Source, View, convert, dbt_files, read_script
```

## read_script

```text
read_script(text: str, *, dialect: str) -> Script
```

Return the `CREATE VIEW` and `CREATE OR ALTER VIEW` statements of a script, in script
order, and a warning for each statement skipped.
[The conversion rules](conversion-rules.md#statements) list how each statement is treated.

Raises `ValueError` for an unknown dialect, and `sqlglot.errors.SqlglotError` for SQL
that cannot be read: `ParseError` for SQL that sqlglot cannot parse, for
`SET QUOTED_IDENTIFIER OFF`, and for a statement that defines a view but cannot be read as
one; `TokenError` for text that does not even split into tokens, such as an unterminated
string.

## convert

```text
convert(views: Iterable[View], *, dialect: str, default_schema: str) -> Project
```

Convert views into dbt models, following [the conversion rules](conversion-rules.md).
`dialect` decides how names compare; `default_schema` is the schema of tables named
without one.

Raises `ValueError` for an unknown dialect; when two models would share a name, ignoring
case; when views refer to one another in a cycle; when two sources would share a dbt
source name; when a table without a schema is read and `default_schema` is empty; when
dropping a schema from a column would bind it to another table; and when the SQL
contains the reserved text `__cte2dbt_`. Raises `sqlglot.errors.UnsupportedError` when
sqlglot cannot print a construct in the dialect.

A query that is not a view converts through a `View` built by hand:

```python
query = parse_one("WITH a AS (SELECT 1 AS x) SELECT x FROM a")
project = convert([View(exp.to_table("final"), query)], dialect="duckdb", default_schema="main")
assert [model.name for model in project.models] == ["final__a", "final"]
```

## dbt_files

```text
dbt_files(project: Project) -> dict[str, str]
```

Return the content of each file to write, keyed by file name: `MODEL.sql` for each model,
ending with a newline, and `sources.yml`, in
[the format of the conversion rules](conversion-rules.md#sourcesyml).

## Results

| Type | Field | Meaning |
| --- | --- | --- |
| `View` | `name: exp.Table` | The name of the view, with its schema and database when written. |
| | `query: exp.Query` | The query of the view. |
| | `columns: tuple[exp.Identifier, ...]` | The column list of the view, empty when there is none. |
| `Script` | `views: tuple[View, ...]` | The views of the script, in script order. |
| | `warnings: tuple[str, ...]` | One `skipped statement: STATEMENT` per statement skipped. |
| `Project` | `models: tuple[Model, ...]` | Every model, each after the models it refs. |
| | `sources: tuple[Source, ...]` | Every source read by a model, sorted, each once. |
| | `warnings: tuple[str, ...]` | What to review, in conversion order. |
| `Model` | `name: str` | The model name, which is also its file name without `.sql`. |
| | `sql: str` | The query, with `{{ ref("MODEL") }}` and `{{ source("SOURCE", "TABLE") }}` in place of tables. |
| | `refs: tuple[str, ...]` | The names of the models it reads, sorted. |
| | `sources: tuple[Source, ...]` | The sources it reads, sorted. |
| `Source` | `database: str` | The database, empty unless the name had three parts. |
| | `schema: str` | The schema. |
| | `table: str` | The table. |
| | `source_name: str` | The dbt source name: the schema, or `DATABASE__SCHEMA`. |

All are frozen dataclasses. `Project`, `Model` and `Source` hold text only. `View` and
`Script` hold sqlglot trees, which `convert` copies before changing them.

```python
source = Source("Archive", "dbo", "Orders")
assert source.source_name == "Archive__dbo"
```
