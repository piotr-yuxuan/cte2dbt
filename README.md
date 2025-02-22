# `cte2dbt`

## Overview

This small Python module is designed to transform large SQL queries
containing numerous Common Table Expressions (CTE) into modular,
reusable dbt models.

## Example usage

- Import `cte2dbt`:
``` python
import com.github.piotr_yuxuan.cte2dbt as cte2dbt
from sqlglot import parse_one
```

- Store a SQL query as string variable `sql_query`:
``` SQL
WITH
  cte1 AS (
    SELECT id
         , name
    FROM customers
)
, cte2 AS (
    SELECT cte1.id
         , orders.amount
    FROM cte1
    JOIN orders ON cte1.id = orders.customer_id
)
SELECT *
FROM cte2;
```

- You may define custom transformation functions:
``` python
to_dbt_ref_block = lambda name: f"{{{{ ref('{name}') }}}}"
to_dbt_source_block = lambda table: f"{{{{ source('{table.db}', '{table.name}') }}}}"
```

- Lastly Initialize the model provider:
``` python
provider = cte2dbt.Provider(
    model_name="final_model",
    expr=parse_one(sql_query),
    to_dbt_ref_block=to_dbt_ref_block,
    to_dbt_source_block=to_dbt_source_block
)
```

- Now you can iterate over the dbt models:
``` python
for model_name, model_expr in provider.iter_dbt_models():
    print(f"---\nModel: {model_name}")
    print(model_expr.sql(pretty=True))
```

## Installation

Install `cte2dbt` from the central Python package repository:

``` zsh
poetry add cte2dbt
```

## Rationale

Data analysts often write complex, monolithic SQL queries with
multiple CTEs, making them difficult to maintain and test. This tool
automates the extraction of CTEs, converting them into structured dbt
models that integrate seamlessly into modern data transformation
workflows.

## Use Case

This module is particularly useful for teams working with dbt who need
to migrate existing SQL queries into a well-structured, modular format
while preserving dependencies and relationships between different
parts of the query.

## Technical Notes

This module:

- Uses `sqlglot` for SQL parsing, enabling analysis and transformation
  at the token level.
- Identifies and classifies tables as either CTEs or source tables.
- Replaces CTE references with Jinja blocks calling dbt `ref()` calls
  and source tables with `source()` blocks. The transformations are
  provided by the user who retains full flexibility.
- Provides an iterable interface for processing extracted dbt models
  in various ways, such as writing them to files, executing them, or
  constructing dependency graphs.
- Implements a bottom-up approach to processing SQL, first handling
  individual CTEs before reconstructing the final query.

This tool streamlines SQL-to-dbt migrations, ensuring cleaner,
testable, and reusable SQL transformations. 🚀
