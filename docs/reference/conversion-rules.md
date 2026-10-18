# Conversion rules

These rules decide what `read_script` keeps from a script and what `convert` makes of
each view. The [explanation](../explanation/conversion.md) gives the reasons.

## Statements

`read_script` treats each statement of a script as follows.

| Statement | Treatment |
| --- | --- |
| `CREATE VIEW`, `CREATE OR ALTER VIEW` | Read as a view, with its column list. View options such as `SCHEMABINDING` are not carried over. |
| A line holding only `GO`, with an optional count, semicolon or `--` comment | Ends a batch, in T-SQL and the dialects derived from it, as sqlcmd does. Each batch is parsed on its own, so a guard such as `IF ... DROP VIEW` ends with its batch. Inside a string or block comment the line ends the batch too, which then fails to parse. |
| `USE`; `SET` options switched `ON`; an empty statement | Ignored. |
| `SET QUOTED_IDENTIFIER OFF` | Error: double-quoted text after it would be read as names. |
| Another `SET` option, such as `SET ANSI_NULLS OFF` | Skipped, with a warning that the views after it are built with the option `ON`. |
| A statement that defines a view but cannot be read as one: a view `WITH CHECK OPTION`, a view created by dynamic SQL in `EXEC`, a view whose body is not a query | Error. |
| SQL that sqlglot cannot parse | Error. |
| Anything else: grants, `EXEC`, `ALTER VIEW`, a guarded `DROP VIEW`, procedures, a query on its own | Skipped, with the warning `skipped statement: STATEMENT`. |

## Tables

`convert` classifies each table that a query reads once, in this order:

1. **Kept as written**, with the warning `MODEL: ITEM kept as written`: a table-valued
   function or other call such as `OPENJSON`, also under `CROSS APPLY` or `OUTER APPLY`, a
   four-part linked-server name, a table variable, a temporary table (`#t`, `##t`), and the
   target of `SELECT INTO`.
2. **Local**: a one-part name declared by a `WITH` nested inside the query stays as it
   is. Inside the definition of one of its common table expressions, only the expressions
   before it are declared, and itself where it is recursive, as for the top level below.
3. **Recursion**: the name of the common table expression being defined, in T-SQL,
   Fabric, Oracle, Snowflake and SQLite, or under `WITH RECURSIVE` in any dialect. The
   expression becomes a recursive model, below.
4. **An earlier common table expression** of the same view, by a one-part name, becomes
   `{{ ref("VIEW__CTE") }}`. A common table expression does not see later ones.
5. **A converted view**, matched by database, schema and name, becomes `{{ ref("VIEW") }}`.
6. **Anything else** becomes `{{ source("SOURCE", "TABLE") }}`.

## Names

- Names compare as the dialect compares identifiers. T-SQL ignores case, inside brackets
  too; Snowflake folds unquoted names to upper case and keeps quoted names; Postgres
  folds unquoted names to lower case. A sqlglot settings string changes this, such as
  `tsql, normalization_strategy=case_sensitive`.
- A table without a schema, or with an empty one as in T-SQL `database..table`, is in the
  default schema.
- A source keeps the spelling of its first occurrence in the whole conversion. Two tables
  that the dialect tells apart but that are spelt alike, such as `Foo` and `"Foo"` in
  Postgres, are an error, as `sources.yml` could not tell them apart.

## Rewriting

- Only the name of a rewritten table changes. Table hints such as `WITH (NOLOCK)`,
  `FOR SYSTEM_TIME`, `PIVOT` and `UNPIVOT`, `TABLESAMPLE`, joins, the alias and its column
  list are kept.
- A rewritten table without an alias takes its written name as alias, and a column
  qualified by the full name of that table, as `schema.table.column`, becomes
  `table.column`. A column binds to the innermost `SELECT` that exposes its table's name:
  when that `SELECT` exposes the name more than once, or for another table, so that the
  shortened column would bind elsewhere, this is an error. A qualifier that names another
  table, such as a property of a column, is left as it is.
- A common table expression or a view with a column list becomes
  `SELECT * FROM (QUERY) AS NAME(COLUMNS)`.
- A recursive common table expression becomes `WITH NAME AS (QUERY) SELECT * FROM NAME`,
  with `RECURSIVE` only where it was written, and with its `MATERIALIZED`, `USING KEY`,
  `SEARCH` and `CYCLE` clauses. No model refs itself.
- `{{`, `{%` and `{#` in strings, comments and names are written `{{ '{' }}{`,
  `{{ '{' }}%` and `{{ '{' }}#`, so that dbt prints them unchanged.
- The arguments of `ref()` and `source()` are double-quoted strings, escaped as in JSON.
- The text `__cte2dbt_` followed by digits and `__` is reserved: SQL that contains it is
  an error.
- sqlglot prints each model in the dialect, indented, with function names as written.

## Model names

- The query of a view becomes the model `VIEW`, and each of its common table expressions
  the model `VIEW__CTE`.
- Each run of characters other than letters, digits and `_` becomes `_`. When this changes
  the name of a view, the warning `MODEL: renamed from VIEW` follows.
- Two models whose names differ only in case are an error, as are views that read one
  another in a cycle.
- Every model comes after the models it refs.

## sources.yml

```yaml
version: 2

sources:
  - name: "dbo"
    schema: "dbo"
    tables:
      - name: "Customers"
  - name: "Archive__dbo"
    database: "Archive"
    schema: "dbo"
    tables:
      - name: "Orders"
```

- One source per database and schema, named after the schema, or
  `DATABASE__SCHEMA` when the table was named with three parts; `database:` appears only
  then. Two sources with the same name are an error.
- Tables are sorted. Every value is a double-quoted string, escaped as in JSON.
- No quoting, description or column is written: dbt-sqlserver, Fabric and DuckDB quote
  names by default, so names keep their case. Other adapters may need `quoting:`.
- With no source, the file holds `sources: []`.
