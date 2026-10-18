# Release notes

## 2.0.0

cte2dbt 2.0 converts T-SQL views, such as those exported from Azure SQL, as well as
single queries, and comes with a `cte2dbt` command. It works with current sqlglot, and
it keeps every part of the SQL that it does not need to change. Its Python API and the
names of its models change; the table below maps 1.x to 2.0.

### Upgrade from 1.x

| 1.x | 2.0 |
| --- | --- |
| `import com.github.piotr_yuxuan.cte2dbt as cte2dbt` | `import cte2dbt` |
| `Provider(model_name, expr, to_dbt_ref_block, to_dbt_source_block)` | `convert([View(exp.to_table(model_name), expr)], dialect=..., default_schema=...)` |
| `get_dbt_models()`: pairs of a name and a sqlglot expression | `Project.models`: `Model` values, each with its name and its SQL as text |
| `get_sources()` | `Project.sources`: `Source` values with database, schema and table |
| `model_dependencies()` | `Model.refs` and `Model.sources`, kept apart |
| `get_cte_tuples()` | `Query.ctes` in sqlglot |
| `to_dbt_ref_block` and `to_dbt_source_block` | Render `Model.sql` with jinja2 and your own `ref` and `source`, as the [how-to guide](../how-to/run-models-without-dbt.md) shows |
| A model per CTE, named after the CTE | Named `MODEL__CTE`, so that names stay unique across views |
| No dialect: names compared as written | `dialect` is required, and names compare as in that dialect |
| A table without a schema gave `source('', ...)` | It is in `default_schema`, which is required |
| Python 3.8 or later | Python 3.11 or later |
| sqlglot not declared; 24.0 or later expected | sqlglot 30.x, declared, so that `pip install` brings it |
| Importing the module configured logging at INFO level | The library neither configures logging nor logs |

### New

- `read_script` reads exported scripts: `CREATE VIEW` and `CREATE OR ALTER VIEW`, `GO`
  batches, `USE` and `SET` preambles, view column lists, UTF-16 exports through the
  command line.
- `convert` takes several views at once: a view that another view reads becomes a
  `ref()` of its model, and the models come in dependency order.
- `dbt_files` returns the text of each model file and of `sources.yml`.
- The `cte2dbt` command converts files or directories of scripts into a dbt models
  directory.
- The package ships `py.typed`: type checkers read its annotations.

### Fixed

- With sqlglot 28 or later, no common table expression was extracted, and nothing said
  so: sqlglot had renamed the argument that 1.x read.
- `pip install cte2dbt` installed 1.0.5 without sqlglot, so importing it failed.
- Rewriting a table lost its hints, its temporal clause, a pivot or sample on it, the
  tables joined to it inside parentheses, and the column list of its alias.
- Table-valued functions became an empty `source()`; they are now kept and reported.
- The column list of a common table expression was dropped.
- A recursive common table expression became a model that referred to itself.
- A table named in another case than its common table expression became a source.
- `CREATE VIEW` input produced a model holding the `CREATE` statement, with the view
  itself as a source.
- A column qualified as `schema.table.column` no longer matched its table.
- Four-part names lost their schema, so two tables could become one source.
- A source whose callback returned the name of a common table expression became a ref.
- A final model named like one of its common table expressions gave two models of one
  name.
- Names declared by a nested `WITH` became sources.
- The README called a method that no longer existed, and showed stale output.

### Known limits

sqlglot prints each model anew, and prints a few T-SQL functions and types as
near-equivalents that change results or that SQL Server rejects, such as `DATEPART(w, ...)`
or `NTEXT`. Compare each model with its view on the database before relying on it. The
[explanation](../explanation/conversion.md#limits) lists these cases and every other limit.
