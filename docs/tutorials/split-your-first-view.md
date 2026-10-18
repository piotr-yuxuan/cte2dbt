# Split your first view

In this tutorial you turn two T-SQL views into dbt models with the Python API of
cte2dbt, and look at each part of the result: the models, their refs, the sources and
the files to write. You need Python 3.11 or later and `pip install cte2dbt`. Every step
below runs as written; run them in order in one Python session.

## 1. Read a script

Here is a script with two views, as SQL Server Management Studio exports them, with a
`GO` line after each statement. The second view reads the first.

```python
from cte2dbt import convert, dbt_files, read_script

text = """
CREATE VIEW dbo.active_customers AS
WITH recent_orders AS (
    SELECT customer_id, amount FROM dbo.Orders WHERE order_date >= '2026-01-01'
)
SELECT c.id, c.name, SUM(r.amount) AS total
FROM dbo.Customers AS c
JOIN recent_orders AS r ON r.customer_id = c.id
GROUP BY c.id, c.name
GO
CREATE VIEW dbo.top_customers AS
SELECT TOP 10 id, name, total FROM dbo.active_customers ORDER BY total DESC
GO
"""

script = read_script(text, dialect="tsql")
assert [view.name.name for view in script.views] == ["active_customers", "top_customers"]
assert script.warnings == ()
```

`read_script` found both views, and there was nothing to warn about.

## 2. Convert the views

```python
project = convert(script.views, dialect="tsql", default_schema="dbo")
assert [model.name for model in project.models] == [
    "active_customers__recent_orders",
    "active_customers",
    "top_customers",
]
```

There are three models: the common table expression `recent_orders` became a model of
its own, named after its view, and each view became a model named after itself. A model
always comes after the models it reads.

## 3. Read the first model

```python
recent_orders = project.models[0]
assert recent_orders.sql == """SELECT
  customer_id,
  amount
FROM {{ source("dbo", "Orders") }} AS Orders
WHERE
  order_date >= '2026-01-01'"""
```

The table `dbo.Orders` became a dbt `source()`. The table keeps a name, `Orders`, so
that the rest of the query can still refer to it.

## 4. Follow the refs

```python
active_customers = project.models[1]
assert active_customers.refs == ("active_customers__recent_orders",)
assert '{{ ref("active_customers__recent_orders") }} AS r' in active_customers.sql

top_customers = project.models[2]
assert top_customers.refs == ("active_customers",)
assert '{{ ref("active_customers") }} AS active_customers' in top_customers.sql
```

Where `active_customers` read `recent_orders`, its model now reads the model of that
expression through `ref()`. Where `top_customers` read the view `dbo.active_customers`,
its model reads the model of that view: a view defined in the same conversion is a
model, not a source.

## 5. List the sources

```python
assert [(source.schema, source.table) for source in project.sources] == [
    ("dbo", "Customers"),
    ("dbo", "Orders"),
]
```

These are the tables the views read and do not define. dbt needs them declared in
`sources.yml`.

## 6. See the files to write

```python
files = dbt_files(project)
assert sorted(files) == [
    "active_customers.sql",
    "active_customers__recent_orders.sql",
    "sources.yml",
    "top_customers.sql",
]
assert files["sources.yml"] == """version: 2

sources:
  - name: "dbo"
    schema: "dbo"
    tables:
      - name: "Customers"
      - name: "Orders"
"""
```

Each model is one `.sql` file, and `sources.yml` declares the sources. These are the
files that the `cte2dbt` command writes into a dbt models directory.

## 7. Meet a warning

Some tables cannot become a `ref()` or a `source()`. Convert a view that splits a column
with the table-valued function `STRING_SPLIT`:

```python
tags = read_script(
    "CREATE VIEW dbo.tags AS"
    " SELECT p.id, t.value FROM dbo.Products AS p CROSS JOIN STRING_SPLIT(p.tags, ',') AS t",
    dialect="tsql",
)
project = convert(tags.views, dialect="tsql", default_schema="dbo")
assert project.warnings == ("tags: STRING_SPLIT(p.tags, ',') AS t kept as written",)
```

The function call stays in the model exactly as it was, and the warning tells you to
check it: here nothing needs to change, as dbt passes the call on to the database.

## What you have done

You read an exported script, converted its views into models, followed refs between
models and views, listed the sources and produced the files of a dbt project. To convert
your own views, follow [Convert Azure SQL views into a dbt project](../how-to/convert-azure-sql-views.md).
The [conversion rules](../reference/conversion-rules.md) list every case that cte2dbt
handles.
