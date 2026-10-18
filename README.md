# cte2dbt

[![PyPI - Downloads](https://img.shields.io/pypi/dm/cte2dbt)](https://pypi.org/project/cte2dbt/)
[![PyPI - Status](https://img.shields.io/pypi/status/cte2dbt)](https://pypi.org/project/cte2dbt/)
[![GitHub License](https://img.shields.io/github/license/piotr-yuxuan/cte2dbt)](https://github.com/piotr-yuxuan/cte2dbt?tab=EUPL-1.2-1-ov-file#readme)
[![GitHub Issues](https://img.shields.io/github/issues/piotr-yuxuan/cte2dbt)](https://github.com/piotr-yuxuan/cte2dbt/issues)

![A robotic arm working beside screens of schematics in a laboratory.](https://raw.githubusercontent.com/piotr-yuxuan/cte2dbt/main/docs/social-media-preview.png)

cte2dbt splits SQL views into [dbt](https://www.getdbt.com/) models. Each common table
expression (CTE) of a view becomes a model of its own, a view that another view reads
becomes a `ref()` of its model, and every other table becomes a `source()` listed in
`sources.yml`. Long views become small models that dbt can build, test and document one
at a time.

It was written to move T-SQL views, such as those that Excel workbooks read from Azure
SQL, into a dbt project, and it reads any dialect that
[sqlglot](https://github.com/tobymao/sqlglot) parses.

## Install

```sh
pip install cte2dbt
```

The command line alone installs with `uv tool install cte2dbt`. Python 3.11 or later is
required.

## Use

From the command line, on a folder of view scripts exported from SQL Server Management
Studio or SqlPackage:

```sh
cte2dbt exported/dbo/Views --output models/legacy
```

From Python:

```python
from cte2dbt import convert, dbt_files, read_script

script = read_script(
    "CREATE VIEW dbo.v_sales AS WITH a AS (SELECT id FROM dbo.Sales) SELECT * FROM a",
    dialect="tsql",
)
project = convert(script.views, dialect="tsql", default_schema="dbo")

assert [model.name for model in project.models] == ["v_sales__a", "v_sales"]
assert project.models[1].sql == 'SELECT\n  *\nFROM {{ ref("v_sales__a") }} AS a'
assert sorted(dbt_files(project)) == ["sources.yml", "v_sales.sql", "v_sales__a.sql"]
```

## Limits

sqlglot parses each view and prints it again, so a model returns the same rows as the
view it comes from but its text differs: layout, keyword case, comments, and sometimes
an explicit cast. Only `CREATE VIEW` statements are converted; a table that cannot become
a `ref()` or a `source()`, such as a table-valued function, is kept as written and
reported. The [conversion rules](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/reference/conversion-rules.md)
list every case.

## Documentation

| Page | Purpose |
| --- | --- |
| [Split your first view](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/tutorials/split-your-first-view.md) | Learn how views become models, refs and sources. |
| [Convert Azure SQL views into a dbt project](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/how-to/convert-azure-sql-views.md) | Export, convert, review and build a set of views. |
| [Run the models without dbt](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/how-to/run-models-without-dbt.md) | Render the models into plain SQL and run them. |
| [Command line](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/reference/command-line.md) | Look up options, output files and exit codes. |
| [Python API](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/reference/python-api.md) | Look up functions, results and errors. |
| [How views become models](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/explanation/conversion.md) | Understand the design and its limits. |

All pages are listed in the [documentation index](https://github.com/piotr-yuxuan/cte2dbt/blob/main/docs/README.md).

## Licence

EUPL 1.2, see [LICENSE](https://github.com/piotr-yuxuan/cte2dbt/blob/main/LICENSE).
