# [`cte2dbt` 🚀](https://github.com/piotr-yuxuan/cte2dbt)

[![PyPI - Downloads](https://img.shields.io/pypi/dm/cte2dbt)](https://pypi.org/project/cte2dbt/)
[![PyPI - Status](https://img.shields.io/pypi/status/cte2dbt)](https://pypi.org/project/cte2dbt/)
[![GitHub License](https://img.shields.io/github/license/piotr-yuxuan/cte2dbt)](https://github.com/piotr-yuxuan/cte2dbt?tab=EUPL-1.2-1-ov-file#readme)
[![GitHub Issues](https://img.shields.io/github/issues/piotr-yuxuan/cte2dbt)](https://github.com/piotr-yuxuan/cte2dbt/issues)

## Overview

![](./doc/social-media-preview.png)

This small Python module is transforms large SQL queries with multiple
Common Table Expressions (CTE) into modular, reusable dbt models.

If you have ever struggled with maintaining long, monolithic SQL
queries filled with CTE, this tool is for you! `cte2dbt` automates the
extraction of CTE and converts them into structured dbt models,
preserving their dependencies and making them easier to test,
document, and reuse.

## Why use `cte2dbt`?

- ✅ **Automates SQL-to-dbt migration** – No more manually splitting
  queries into models.
- ✅ **Preserves dependencies** – Ensures each dbt model is built in
  the correct order.
- ✅ **Enables testing & modularisation** – Improves maintainability
  and performance.
- ✅ **Customisable transformation functions** – Tailor how CTE and
  source tables are processed.
- ✅ **Iterable design** – Process, validate, or visualise models
  however you like.

## Example Usage

### Transforming a SQL query into dbt models

#### 1️⃣ Import `cte2dbt` and `sqlglot`

```python
import com.github.piotr_yuxuan.cte2dbt as cte2dbt
from sqlglot import parse_one
```

#### 2️⃣ Define a SQL query

``` python
sql_query = """
WITH
  customers_cte AS (
    SELECT id, name FROM customers
  ),
  orders_cte AS (
    SELECT c.id, o.amount
    FROM customers_cte AS c
    JOIN prod.retails.orders AS o ON c.id = o.customer_id
  )
SELECT * FROM orders_cte;
"""
```

#### 3️⃣ Define dbt transformation functions

``` python
to_dbt_ref_block = lambda name: f"{{{{ ref('{name}') }}}}"
to_dbt_source_block = lambda table: f"{{{{ source('{table.db}', '{table.name}') }}}}"
```

#### 4️⃣ Initialise the provider

``` python
provider = cte2dbt.Provider(
    model_name="final_model",
    expr=parse_one(sql_query),
    to_dbt_ref_block=to_dbt_ref_block,
    to_dbt_source_block=to_dbt_source_block
)
```

#### 5️⃣ Iterate over dbt models in execution order

The order guarantees that current model only relies on models that
came earlier in the iteration.

``` python
for model_name, model_expr in provider.iter_dbt_models():
    print(f"-- Model: {model_name}")
    print(model_expr.sql(pretty=True))
```

Output:
``` sql
-- Model: cte1
SELECT
  id,
  name
FROM {{ source('my_source', 'customers') }} AS customers

-- Model: cte2
SELECT
  cte1.id,
  orders.amount
FROM {{ ref('cte1') }} AS cte1
JOIN {{ source('my_source', 'orders') }} AS orders
  ON cte1.id = orders.customer_id

-- Model: final_model_name
SELECT
  *
FROM {{ ref('cte2') }} AS cte2
```

#### 6️⃣ Generate the model dependency graph

``` python
print(provider.model_dependencies())
# Output:
# {'customers_cte': {'customers'},
#  'orders_cte': {'customers_cte', 'prod.retails.orders'},
#  'final_model': {'orders_cte'}}
```

## Installation

Install cte2dbt using Poetry:

``` zsh
poetry add cte2dbt
```

## Use Cases

Beyond just transforming SQL queries into dbt models, `cte2dbt`
provides an iterable interface that unlocks multiple possibilities:

### 🔹 Run SQL transformations dynamically

Iterate over models and execute each as a temporary table:

``` python
for model_name, model_expr in provider.iter_dbt_models():
    conn.execute(f"CREATE TEMPORARY TABLE {model_name} AS {model_expr.sql()}")
```

### 🔹 Inspect intermediate data structures

Use `DESCRIBE TABLE` or `DESCRIBE RESULT LAST_QUERY_ID()`to log or
visualise schema changes:

``` python
for model_name, _ in provider.iter_dbt_models():
    print(conn.execute(f"DESCRIBE TABLE {model_name}").fetchall())
```

### 🔹 Validate data consistency

Compare model output with existing tables to [measure
similarity](https://docs.snowflake.com/en/sql-reference/functions/approximate_similarity):

``` python
for model_name, model_expr in provider.iter_dbt_models():
    similarity_score = compare_with_reference(conn, model_expr.sql(), reference_table)
    print(f"{model_name}: similarity {similarity_score}%")
```

### 🔹 Generate dependency graphs

Understand how changes in one CTE ripple through your final model:

``` python
import networkx as nx
import matplotlib.pyplot as plt

graph = nx.DiGraph(provider.model_dependencies())
nx.draw(graph, with_labels=True)
plt.show()
```

### 🔹 Export models to files

Save each model as a `.sql` file with a structured naming strategy:

``` python
for model_name, model_expr in provider.iter_dbt_models():
    with open(f"models/{model_name}.sql", "w") as f:
        f.write(model_expr.sql(pretty=True))
```

Extract column lists and generate corresponding .yml files:

``` python
for model_name, model_expr in provider.iter_dbt_models():
    columns = extract_columns(model_expr)
    write_yaml(f"models/{model_name}.yml", {"columns": columns})
```

## How It Works

### 📌 SQL Parsing & CTE Extraction

`cte2dbt` leverages [sqlglot](https://github.com/tobymao/sqlglot) to
parse SQL queries at the token level. It then identifies CTE, source
tables, and their dependencies.

### 📌 Modular dbt Model Generation

- CTE are transformed into separate dbt models;
- Source tables are converted into source() blocks;
- CTE references are replaced with ref() calls;
- Execution order is preserved to guarantee correctness.

### 📌 Fully Flexible API

Rather than enforcing a rigid output format, cte2dbt gives you full
control over how models are transformed, stored, or executed.

## Technical Details

- Uses [sqlglot](https://github.com/tobymao/sqlglot) for robust SQL
  parsing;
- Implements dependency resolution to determine the correct order of
  model execution;
- Supports fully customisable transformation functions;
- Can be used programmatically or integrated into dbt workflows.

## 🚀 Conclusion

`cte2dbt` makes SQL-to-dbt migrations effortless, ensuring cleaner,
modular, and testable SQL transformations. Whether you want to
refactor legacy queries, improve maintainability, or generate
structured dbt models automatically, this tool provides an elegant and
flexible solution.

Give it a try and simplify your dbt workflow today! 🎯
