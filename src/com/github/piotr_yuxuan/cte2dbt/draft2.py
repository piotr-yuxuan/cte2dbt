from typing import Callable, Dict, List, Tuple

from sqlglot import exp


def has_table_qualified_name(
    table: exp.Table,
) -> bool:
    return table.db or table.catalog


def is_table_a_cte(
    replacements: Dict[str, str],
    table: exp.Table,
) -> bool:
    return not has_table_qualified_name(table) and table.name in replacements


def is_table_a_source(
    cte_names: Dict[str, str],
    table: exp.Table,
) -> bool:
    # ? Dubious?
    # return has_table_qualified_name(table) and table.name not in replacements
    return table.name not in cte_names


def cte_table_fn(
    replacements: Dict[str, str],
    cte_table: exp.Table,
) -> exp.Expression:
    return exp.Table(
        this=exp.to_identifier(
            replacements[cte_table.name],
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
    replacements: Dict[str, str],
    expression: exp.Expression,
):
    return expression.copy().transform(
        lambda node: (
            cte_table_fn(replacements, node)
            if isinstance(node, exp.Table)
            and is_table_a_cte(
                replacements,
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
    to_source_name: Callable,
    source_names: Dict,
    cte_names: Dict,
    source_table: exp.Table,
) -> exp.Expression:
    fully_qualified_name = to_fully_qualified_name(source_table)
    if fully_qualified_name not in source_names:
        source_names[fully_qualified_name] = to_source_name(source_table)
    return fully_qualified_name


def transform_source_tables(
    to_source_name: Callable,
    source_names: Dict[str, str],
    cte_names: Dict[str, str],
    expression: exp.Expression,
):
    return expression.copy().transform(
        lambda node: (
            source_table_fn(to_source_name, source_names, cte_names, node)
            if isinstance(node, exp.Table)
            and is_table_a_source(
                cte_names,
                node,
            )
            else node
        )
    )


def get_cte_name_and_exprs(
    select: exp.Select,
) -> List[Tuple[str, exp.Expression]]:
    if isinstance(select.args.get("with"), exp.With):
        return [(cte.alias, cte.this) for cte in select.args.get("with")]
    else:
        return []


def get_all_name_and_exprs(
    to_model_name: Callable,
    to_source_name: Callable,
    final_model_name: str,
    initial_expression: exp.Expression,
) -> List[Dict]:
    # I'm not very convinced that this API is currently great. I want
    # to favour organic growth for now, but at some point we'll need
    # to check whether we could do better.
    final_select_expr: exp.Expression = initial_expression.copy()
    final_select_expr.args.pop("with")
    cte_name_and_exprs = get_cte_name_and_exprs(initial_expression)

    cte_names: Dict[str, str] = dict({})
    source_names: Dict[str, str] = dict({})
    result: List[Dict] = list()

    def transform_cte_expr(
        cte_names,
        source_names,
        cte_expr,
        model_name: str = None,
        cte_name: str = None,
    ):
        if cte_name:
            cte_names[cte_name] = model_name

        model_expr = cte_expr
        model_expr = transform_source_tables(
            to_source_name, source_names, cte_names, model_expr
        )
        model_expr = transform_cte_tables(cte_names, cte_expr)

        return {
            "cte_name": cte_name,
            "cte_expr": cte_expr,
            "model_name": model_name,
            "model_expr": model_expr,
        }

    for cte_name, cte_expr in cte_name_and_exprs:
        model_name = to_model_name(cte_name)
        result.append(
            transform_cte_expr(
                cte_names,
                source_names,
                cte_expr,
                model_name=model_name,
                cte_name=cte_name,
            )
        )
    result.append(
        transform_cte_expr(
            cte_names,
            source_names,
            final_select_expr,
            model_name=final_model_name,
        )
    )

    print(f"cte_names={cte_names}")
    print(f"source_names={source_names}")
    return result
