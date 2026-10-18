# Conversion rules

These rules decide what `read_script` keeps from a script and what `convert` makes of
each view. The [explanation](../explanation/conversion.md) gives the reasons.

## Statements

`read_script` treats each statement of a script as follows.

| Statement | Treatment |
| --- | --- |
| `CREATE VIEW`, `CREATE OR ALTER VIEW` | Read as a view, with its column list. View options such as `SCHEMABINDING` are not carried over. |
| A line holding only `GO`, with an optional count, semicolon or comment | Ends a statement, in T-SQL and the dialects derived from it. As in SQL Server Management Studio, this also applies inside a block comment or a string. |
| `USE`; `SET` options switched `ON`; an empty statement | Ignored. |
| `SET QUOTED_IDENTIFIER OFF` | Error: double-quoted text after it would be read as names. |
| A statement that sqlglot can only keep as text and that mentions `VIEW`, such as a view `WITH CHECK OPTION` | Error: it may hide a view. |
| Anything else: grants, `EXEC`, other `SET` options, `ALTER VIEW`, procedures, a query on its own | Skipped, with the warning `skipped statement: STATEMENT`. |

## Tables

`convert` classifies each table that a query reads once, in this order:

1. **Kept as written**, with the warning `MODEL: ITEM kept as written`: a table-valued
   function or other call such as `OPENJSON`, a four-part linked-server name, a table
   variable, a temporary table (`#t`, `##t`), and the target of `SELECT INTO`.
2. **Local**: a one-part name declared by a `WITH` nested inside the query stays as it is.
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
- A source keeps the spelling of its first occurrence in the whole conversion.

## Rewriting

- Only the name of a rewritten table changes. Table hints such as `WITH (NOLOCK)`,
  `FOR SYSTEM_TIME`, `PIVOT` and `UNPIVOT`, `TABLESAMPLE`, joins, the alias and its column
  list are kept.
- A rewritten table without an alias takes its written name as alias, and a column
  qualified as `schema.table.column` becomes `table.column`. When the same name is exposed
  twice in the query, so that the column would bind to another table, this is an error.
- A common table expression or a view with a column list becomes
  `SELECT * FROM (QUERY) AS NAME(COLUMNS)`.
- A recursive common table expression becomes `WITH NAME AS (QUERY) SELECT * FROM NAME`,
  with `RECURSIVE` only where it was written. No model refs itself.
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
