# Convert Azure SQL views into a dbt project

This guide turns the views of an Azure SQL or SQL Server database into dbt models built
with the [dbt-sqlserver](https://github.com/dbt-msft/dbt-sqlserver) adapter. You need
cte2dbt, read access to the view definitions, and a dbt project configured for the
database.

## Export the view definitions

Export with [SqlPackage](https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage-extract),
one file per object, grouped by schema and object type:

```sh
SqlPackage /Action:Extract \
  /SourceConnectionString:"Server=tcp:SERVER.database.windows.net,1433;Initial Catalog=DATABASE;Authentication=Active Directory Default;Encrypt=True" \
  /TargetFile:exported \
  /p:ExtractTarget=SchemaObjectType
```

The views of each schema are then in `exported/SCHEMA/Views/`.

Alternatively, in SQL Server Management Studio, use **Tasks > Generate Scripts** on the
views, with **One script file per object**. Under **Advanced**, set **Check for object
existence** to False: otherwise each view is created by dynamic SQL, which cte2dbt
refuses to read.

Do not export from `INFORMATION_SCHEMA.VIEWS`, which cuts definitions at 4000
characters. `sys.sql_modules.definition` holds the full text when you script the export
yourself:

```sql
SELECT s.name AS schema_name, o.name AS view_name, m.definition
FROM sys.sql_modules AS m
JOIN sys.objects AS o ON o.object_id = m.object_id
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
WHERE o.type = 'V';
```

Save each definition as its own `.sql` file. A view created `WITH ENCRYPTION` has no
definition to export.

## Choose the dialect settings

Check the collation of the database:

```sql
SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation');
```

A collation whose name contains `_CI_` compares names ignoring case, which is what
cte2dbt assumes for T-SQL. For a `_CS_` collation, pass
`--dialect "tsql, normalization_strategy=case_sensitive"` in the next step.

Tables named without a schema are read in `dbo`. When the views were written for users
with another default schema, pass `--default-schema` with that schema.

## Convert

Pass the folders that hold views only:

```sh
cte2dbt exported/*/Views --output models/legacy
```

Convert all the views of one dbt project in one run: each run writes its own
`sources.yml`, and dbt rejects a source declared in two files. The output directory must
not exist or must be empty. To convert again, remove it or choose another one, then
compare the two with `diff -r`.

## Act on the warnings

Each warning names a file or a model:

| Warning | Action |
| --- | --- |
| `FILE: skipped statement: ...` | The statement is not a view, such as a grant or an extended property. If the Excel users need a grant, add it with dbt's `grants` configuration. |
| `FILE: skipped statement: SET ... OFF; the views after it are built with this option ON` | Look in the views after it for what the option changes. Under `ANSI_NULLS OFF`, rewrite `= NULL` and `<> NULL` as `IS NULL` and `IS NOT NULL`. |
| `MODEL: ITEM kept as written` | A table-valued function, a temporary table or a linked-server name stays as written. Check that the database where dbt builds the model can run it; a user-defined function must exist there. |
| `MODEL: renamed from VIEW` | The view name holds characters that a dbt model name cannot have. Add `{{ config(alias='VIEW') }}` as the first line of `MODEL.sql`, so that the database object keeps the name the workbooks use. |

## Configure the models

In `dbt_project.yml`, build the models as views:

```yaml
models:
  your_project:
    legacy:
      +materialized: view
```

dbt creates each model in the target schema of the profile. For the workbooks to read
the same `schema.view` names as before, either target that schema, or set `+schema` on the
folder and override `generate_schema_name` as the dbt documentation on
[custom schemas](https://docs.getdbt.com/docs/build/custom-schemas) shows.

The models named `VIEW__CTE` are intermediate steps. To build them as ephemeral models,
which dbt-sqlserver 1.12 and later supports, move them into a subfolder with
`+materialized: ephemeral`, except any model whose SQL starts with `WITH`: dbt injects an
ephemeral model as a common table expression, and T-SQL cannot nest a `WITH` in one.

## Build and compare

Build into a development schema:

```sh
dbt build --select legacy --target dev
```

For each view, compare the rows of the original view with those of its model, both ways:

```sql
SELECT * FROM dbo.vOrders EXCEPT SELECT * FROM dev_legacy.vOrders;
SELECT * FROM dev_legacy.vOrders EXCEPT SELECT * FROM dbo.vOrders;
SELECT (SELECT COUNT(*) FROM dbo.vOrders) - (SELECT COUNT(*) FROM dev_legacy.vOrders);
```

The two `EXCEPT` queries return no rows and the difference of counts is zero when the
model returns the rows of the view.
