# Run the models without dbt

This guide renders the models of a conversion into plain SQL and runs them in order, for
example to compare intermediate results or to inspect each step of a long view. It uses
[jinja2](https://jinja.palletsprojects.com/), the template engine of dbt, which you
install with `pip install jinja2`, and an in-memory SQLite database.

## Render each model and create it as a view

Give jinja2 a `ref` function that names the relation of a model and a `source` function
that names a table, then create the models in the order of `Project.models`, which puts
every model after the models it reads:

```python
import sqlite3

import jinja2

from cte2dbt import convert, read_script

script = read_script(
    "CREATE VIEW main.big_orders AS"
    " WITH big AS (SELECT id, amount FROM orders WHERE amount > 10)"
    " SELECT COUNT(*) AS n FROM big",
    dialect="sqlite",
)
project = convert(script.views, dialect="sqlite", default_schema="main")

connection = sqlite3.connect(":memory:")
connection.execute("CREATE TABLE orders (id INTEGER, amount INTEGER)")
connection.executemany("INSERT INTO orders VALUES (?, ?)", [(1, 5), (2, 15), (3, 25)])

environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
for model in project.models:
    sql = environment.from_string(model.sql).render(
        ref=lambda name: f'"{name}"',
        source=lambda schema, table: f'"{schema}"."{table}"',
    )
    connection.execute(f'CREATE VIEW "{model.name}" AS {sql}')
```

## Query any step

Each common table expression is now a view you can query on its own:

```python
assert connection.execute('SELECT id FROM "big_orders__big" ORDER BY id').fetchall() == [
    (2,),
    (3,),
]
assert connection.execute('SELECT n FROM "big_orders"').fetchall() == [(2,)]
```

To run the models on another database, convert with its dialect and connect with its
driver; the rendering stays the same. For a source in a three-part name, the `source`
function receives `DATABASE__SCHEMA` as its first argument: look it up in
`Project.sources`, where each `Source` holds the database, schema and table.
