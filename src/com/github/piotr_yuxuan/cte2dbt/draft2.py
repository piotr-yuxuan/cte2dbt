from typing import Callable, Dict, List, Tuple, Optional

from pydantic import BaseModel
from sqlglot import exp


def replace_table_name(
    replacements: Dict,
    expression: exp.Expression,
    table_hook_fn: Callable = None,
) -> exp.Expression:
    """Return a deep copy of the expression with table names replaced
    where appropriate according to the replacements.

    Known bug: this approach doesn't handle nested definitions that
    shadow one another, for example:

    ``` sql
    WITH cte1 AS ( -- Outer CTE
      WITH cte1 AS ( -- Inner CTE
        SELECT *
        FROM table
      )
      SELECT cte1.*
      FROM cte1
    )
    SELECT cte1.*
    FROM cte1
    ```

    In this case it is not correct for the inner FROM to be changed as
    `FROM stg_cte1 AS cte1`, but that would happen anyway. You would
    then need to fix the SQL manually, either by renaming the inner
    CTE in the input, or fixing the output.

    """

    def transformer(node: exp.Expression):
        transformed = node
        if isinstance(node, exp.Table):
            table: exp.Table = node
            if not table.db and not table.catalog and table.name in replacements:
                transformed = exp.Table(
                    this=exp.to_identifier(
                        replacements[node.name],
                        # Not quoting the name can make the SQL
                        # invalid, but we want to insert raw jinja
                        # template ‑ invalid SQL in themselves.
                        quoted=False,
                    ),
                    alias=exp.to_identifier(
                        node.alias if table.alias else node.name,
                    ),
                )
            if table.name not in replacements and table_hook_fn:
                # Intended to be used to register and replace tables.
                table_hook_fn(table)
        return transformed

    return expression.copy().transform(transformer)


def get_cte_name_and_exprs(
    select: exp.Select,
) -> List[Tuple[str, exp.Expression]]:
    if isinstance(select.args.get("with"), exp.With):
        return [(cte.alias, cte.this) for cte in select.args.get("with")]
    else:
        return []


class ModelDefinition(BaseModel):
    # We could expose the exp.Expression type outside but for the main
    # entrypoint the API simpler to deal with text. However, as this
    # stage this is premature optimisation: for exemple, we can't know
    # if it's always fine to prettyprint the sql.
    original_id: Optional[str]  # None for the final model
    renamed_id: str
    original_sql: str
    transformed_sql: str


def get_all_name_and_exprs(
    replacement_name: Callable,
    final_model_name: str,
    initial_expression: exp.Expression,
    table_hook_fn: Callable = None,
) -> List[Dict]:
    # I'm not very convinced that this API is currently great. I want
    # to favour organic growth for now, but at some point we'll need
    # to check whether we could do better.
    final_select_expr: exp.Expression = initial_expression.copy()
    final_select_expr.args.pop("with")
    cte_name_and_exprs = get_cte_name_and_exprs(initial_expression)

    # FIXME: shall we do an actual reduction here, and iteratively
    # collect replacements? This current implementation is easier to
    # read but suboptimal.
    replacements = {name: replacement_name(name) for name, _ in cte_name_and_exprs}

    return [
        {
            "cte_name": cte_name,
            "cte_expr": cte_expr,
            "model_name": replacement_name(cte_name),
            "model_expr": replace_table_name(
                replacements,
                cte_expr,
                table_hook_fn,
            ),
        }
        for cte_name, cte_expr in cte_name_and_exprs
    ] + [
        {
            # "cte_name": … # Note: no cte name here.
            "cte_expr": final_select_expr,
            "model_name": final_model_name,
            "model_expr": replace_table_name(
                replacements,
                final_select_expr,
                table_hook_fn,
            ),
        }
    ]
