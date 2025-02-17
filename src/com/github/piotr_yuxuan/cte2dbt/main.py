from typing import Callable, Dict, List, Tuple

from sqlglot import exp


def has_table_qualified_name(
    table: exp.Table,
) -> bool:
    return bool(table.db or table.catalog)


def is_table_a_cte(
    cte_names: Dict[str, str],
    table: exp.Table,
) -> bool:
    return not has_table_qualified_name(table) and table.name in cte_names


def is_table_a_source(
    cte_names: Dict[str, str],
    table: exp.Table,
) -> bool:
    # ? Dubious?
    # return has_table_qualified_name(table) and table.name not in replacements
    return (has_table_qualified_name(table)) or not (
        has_table_qualified_name(table) or table.name in cte_names
    )


def cte_table_fn(
    cte_table: exp.Table,
    cte_names: Dict[str, str],
) -> exp.Expression:
    return exp.Table(
        this=exp.to_identifier(
            cte_names[cte_table.name],
            # Not quoting the name can make the SQL
            # invalid, but we want to insert raw jinja
            # template ‑ invalid SQL in themselves.
            quoted=False,
        ),
        alias=exp.to_identifier(
            cte_table.alias if cte_table.alias else cte_table.name,
        ),
    )


def transform_cte_tables(
    cte_expr: exp.Expression,
    cte_names: Dict[str, str],
):
    return cte_expr.copy().transform(
        lambda node: (
            cte_table_fn(node, cte_names)
            if isinstance(node, exp.Table)
            and is_table_a_cte(
                cte_names,
                node,
            )
            else node
        )
    )


def to_fully_qualified_name(table):
    fully_qualified_name = ".".join(
        [
            segment
            for segment in [
                table.db,
                table.catalog,
                table.name,
            ]
            if segment
        ]
    )
    return fully_qualified_name


def source_table_fn(
    source_table: exp.Table,
    source_names: Dict,
    to_source_name: Callable,
) -> exp.Expression:
    """Beware: because of the walk pattern used, this function uses
    mutable arguments.

    """
    fully_qualified_name = to_fully_qualified_name(source_table)

    if fully_qualified_name not in source_names:
        source_names[fully_qualified_name] = to_source_name(source_table)

    return exp.Table(
        this=exp.to_identifier(
            source_names[fully_qualified_name],
            # Not quoting the name can make the SQL
            # invalid, but we want to insert raw jinja
            # template ‑ invalid SQL in themselves.
            quoted=False,
        ),
        alias=exp.to_identifier(
            source_table.alias if source_table.alias else source_table.name,
        ),
    )


def transform_source_tables(
    cte_expr: exp.Expression,
    cte_names: Dict[str, str],
    source_names: Dict[str, str],
    to_source_name: Callable,
):
    new_source_names = source_names.copy()
    new_expr = cte_expr.copy().transform(
        lambda node: (
            source_table_fn(node, new_source_names, to_source_name)
            if isinstance(node, exp.Table)
            and is_table_a_source(
                cte_names,
                node,
            )
            else node
        )
    )
    return new_expr, new_source_names


def get_cte_name_expr_tuples(
    select: exp.Select,
) -> List[Tuple[str, exp.Expression]]:
    if isinstance(select.args.get("with"), exp.With):
        return [(cte.alias, cte.this) for cte in select.args.get("with")]
    else:
        return []


def _to_model_expr(
    cte_names,
    source_names,
    cte_expr,
    to_source_name,
    model_name,
    cte_name: str = None,
):
    if cte_name:
        cte_names[cte_name] = model_name

    model_expr = cte_expr
    model_expr, source_names = transform_source_tables(
        model_expr,
        cte_names,
        source_names,
        to_source_name,
    )
    model_expr = transform_cte_tables(cte_expr, cte_names)

    return model_expr


def process_expression(
    parent_expr: exp.Expression,
    parent_model_name: str,
    to_model_name: Callable,
    to_source_name: Callable,
    expr_fn: Callable = lambda expr: expr,
) -> List[Dict]:
    # I'm not very convinced that this API is currently great. I want
    # to favour organic growth for now, but at some point we'll need
    # to check whether we could do better.
    #
    # Also, I'm shocked at the argument order in Python, so different
    # from what we would do in Clojure.
    final_select_expr: exp.Expression = parent_expr.copy()
    final_select_expr.args.pop("with")
    cte_name_and_exprs = get_cte_name_expr_tuples(parent_expr)

    cte_names: Dict[str, str] = dict({})
    source_names: Dict[str, str] = dict({})
    models: Dict = dict()

    for cte_name, cte_expr in cte_name_and_exprs:
        model_name = to_model_name(cte_name)
        models[model_name] = {
            "cte_name": cte_name,
            "cte_expr": expr_fn(cte_expr),
            "model_expr": expr_fn(
                _to_model_expr(
                    cte_names,
                    source_names,
                    cte_expr,
                    to_source_name,
                    model_name=model_name,
                    cte_name=cte_name,
                )
            ),
        }
    models[parent_model_name] = {
        "cte_expr": expr_fn(cte_expr),
        "model_expr": expr_fn(
            _to_model_expr(
                cte_names,
                source_names,
                final_select_expr,
                to_source_name,
                model_name=parent_model_name,
            )
        ),
    }

    return {
        "cte_names": cte_names,
        "source_names": source_names,
        "models": models,
    }
