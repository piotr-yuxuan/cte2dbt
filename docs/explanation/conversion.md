# How views become models

A long view is hard to test, document and change because its steps have no names
outside it. Its common table expressions are those steps, already named by their author.
cte2dbt gives each its own dbt model, so that dbt can build, test and document every
step, and the lineage between steps becomes visible. The
[conversion rules](../reference/conversion-rules.md) state what happens; this page says
why.

## One model per common table expression

A common table expression is the smallest named unit that an author chose. Splitting
further would invent names; splitting less would keep steps hidden. Each model is named
`VIEW__CTE` because common table expressions are local names: across a few hundred views,
names such as `base` or `filtered` repeat, while dbt needs every model name to be unique.
The query of the view keeps the view's own name, which is the name that workbooks and
reports know.

## Tables are edited in place

A table in a parsed query carries much more than its name: hints, temporal clauses,
pivots, samples, joins, and an alias with its column list. Version 1 built new table
nodes and lost all of these, silently changing results. cte2dbt now changes only the
name of a table and keeps every other part of the node, so whatever sqlglot parsed is
printed again.

Each table is classified once, before any change, and replaced by a placeholder that is a
valid SQL name. Jinja is written into the printed text only at the end. The parsed tree
therefore stays valid SQL throughout, and a `source()` can never be read again as a
common table expression.

## Every rewritten table has an alias

A view may refer to a column as `dbo.Sales.id`. Once `dbo.Sales` becomes
`{{ source("dbo", "Sales") }}`, that qualifier no longer names anything, and SQL Server
would reject the query. Giving the table its written name as alias, and dropping the
schema from such columns, keeps every column bound to the same table. When a name is
exposed twice, as with an outer and an inner `Sales`, dropping the schema could bind a
column to the other table, so cte2dbt stops instead of guessing.

## The dialect decides case and recursion

Whether `Sales` and `SALES` are the same table depends on the database: SQL Server
compares names ignoring case under its default collations, Snowflake folds unquoted names
to upper case, and Postgres to lower case. sqlglot knows each rule, so cte2dbt compares
names through it rather than as strings. A database with a case-sensitive collation says
so through a sqlglot settings string.

In the same way, a common table expression that names itself is recursive in T-SQL,
Oracle, Snowflake and SQLite even without the `RECURSIVE` keyword, while elsewhere the
name then means a table of the same name. A recursive expression cannot be split from its
own `WITH`, so it stays whole in one model.

## The results are text

A `Model` holds its SQL as text, with Jinja for `ref()` and `source()`, together with
the names of the models and sources it reads. Text is what dbt reads, and it cannot be
damaged by a later transformation of a parsed tree that does not know about Jinja. The
models come as an ordered sequence, each after the models it reads, so they can be
written to files, rendered and run one by one, compared with the original, or drawn as a
graph, in whatever order of work suits the caller.

## A column list wraps the query

`WITH totals (customer_id, total) AS (...)` renames the columns of its query. Renaming
each projection would fail for `SELECT *` and for set operations, whose columns come from
their first branch. A derived table with a column list, `SELECT * FROM (...) AS
totals(customer_id, total)`, renames the columns in every case, in every dialect that
allows such lists.

## Limits

- sqlglot parses a view and prints it again, so the text of a model differs from the
  original: layout, keyword case, and comments in `/* */` form. Worse, sqlglot 30 prints
  some T-SQL as near-equivalents that change results or that SQL Server rejects:

  | Written | Printed | Effect |
  | --- | --- | --- |
  | `DATEPART(w, d)` | `DATEPART(WEEK, d)` | The weekday becomes the week number. |
  | `CAST(x AS NTEXT)`, `CAST(x AS TEXT)` | `CAST(x AS VARCHAR(MAX))` | Characters outside the code page are lost. |
  | `SYSDATETIME()` | `GETDATE()` | Precision drops from 100 ns to about 3 ms. |
  | `CAST(x AS REAL)` | `CAST(x AS FLOAT)` | Single becomes double precision. |
  | `SQUARE(x)` | `POWER(x, 2)` | The result takes the type of `x` instead of `float`. |
  | `DATEDIFF(day, a, b)` | `DATEDIFF(DAY, CAST(a AS DATETIME2), CAST(b AS DATETIME2))` | Text and `datetimeoffset` arguments convert differently. |
  | `DATENAME(month, d)` | `FORMAT(CAST(d AS DATETIME2), 'MMMM')` | The name follows the culture of the session. |
  | `SUSER_SNAME()`, `SUSER_NAME()`, `SYSTEM_USER` | `CURRENT_USER()` | SQL Server rejects it; the login would become the user. |
  | `APPROX_COUNT_DISTINCT(x)` | `APPROX_DISTINCT(x)` | SQL Server rejects it. |

  The list holds what was observed; it is not complete. Each model is equivalent to its
  view only once compared on the database. Rewriting only the table names inside the
  original text would avoid this, and is future work.
- A `WITH` nested in a subquery stays inside its model; only the top-level common table
  expressions become models.
- Two views with the same name in two schemas would give two models of one name, which is
  an error. Convert one schema at a time; references across schemas then become sources.
- A view read through a three-part name, `database.schema.view`, becomes a source rather
  than a ref.
- A struct field read through a schema-qualified column, such as DuckDB's `raw.st.s.y`,
  keeps its qualifier and no longer binds once its table is aliased; write `st.s.y`.
- A recursive common table expression with `SEARCH` or `CYCLE` is refused rather than
  split: its model would expose the record-typed columns of the clause.
- Table-valued functions, linked servers and temporary tables are kept as written; they
  must be available where dbt builds the models. Pseudo-tables are not recognised:
  Oracle's `DUAL` and DuckDB's `FROM 'file.parquet'` become sources that do not exist.
- Statements other than views are not converted, and `SET` options other than `ON` are not
  carried into the models.
- Model names are not shortened; SQL Server names have at most 128 characters.
- T-SQL output is checked by running it, transpiled, on DuckDB. Only a run on SQL Server
  itself, as the [how-to guide](../how-to/convert-azure-sql-views.md#build-and-compare)
  describes, shows that a model behaves there as its view did.
