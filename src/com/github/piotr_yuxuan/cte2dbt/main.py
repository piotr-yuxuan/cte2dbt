from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Tuple

from pydantic import BaseModel
from sqlglot import exp


def table_has_qualified_name(
    table: exp.Table,
) -> bool:
    return bool(table.db or table.catalog)


def table_is_a_cte(
    cte_names: Dict[str, str],
    table: exp.Table,
) -> bool:
    return not table_has_qualified_name(table) and table.name in cte_names


def table_is_a_source(
    cte_names: Dict[str, str],
    table: exp.Table,
) -> bool:
    # ? Dubious?
    # return has_table_qualified_name(table) and table.name not in replacements
    return (table_has_qualified_name(table)) or not (
        table_has_qualified_name(table) or table.name in cte_names
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


def transform_tables(
    expr: exp.Expression,
    table_predicate: Callable[[exp.Table], bool],
    table_transform: Callable[[exp.Table], exp.Expression],
):
    return expr.copy().transform(
        lambda node: (
            table_transform(node)
            if isinstance(node, exp.Table) and table_predicate(node)
            else node
        )
    )


def transform_cte_tables(
    cte_expr: exp.Expression,
    dbt_ref_blocks: Dict[str, str],
):
    return transform_tables(
        cte_expr,
        table_predicate=lambda node: table_is_a_cte(dbt_ref_blocks, node),
        table_transform=lambda table: cte_table_fn(table, dbt_ref_blocks),
    )


def to_fully_qualified_name(table) -> str:
    return ".".join(filter(None, [table.db, table.catalog, table.name]))


def source_table_fn(
    source_table: exp.Table,
    dbt_source_blocks: Dict,
    to_dbt_source_block: Callable,
) -> exp.Expression:
    """Beware: because of the walk pattern used, this function uses
    mutable arguments.

    """
    fully_qualified_name = to_fully_qualified_name(source_table)

    if fully_qualified_name not in dbt_source_blocks:
        dbt_source_blocks[fully_qualified_name] = to_dbt_source_block(source_table)

    return exp.Table(
        this=exp.to_identifier(
            dbt_source_blocks[fully_qualified_name],
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
    dbt_ref_blocks: Dict[str, str],
    dbt_source_blocks: Dict[str, str],
    to_dbt_source_block: Callable,
):
    new_source_names = dbt_source_blocks.copy()

    return (
        transform_tables(
            cte_expr,
            table_predicate=lambda node: table_is_a_source(dbt_ref_blocks, node),
            table_transform=lambda table: source_table_fn(
                table, new_source_names, to_dbt_source_block
            ),
        ),
        new_source_names,
    )


def get_cte_name_expr_tuples(
    select: exp.Select,
) -> List[Tuple[str, exp.Expression]]:
    if isinstance(select.args.get("with", None), exp.With):
        return [(cte.alias, cte.this) for cte in select.args.get("with")]
    else:
        return []


class Metadata(BaseModel):
    dbt_ref_blocks: Dict[str, str] = dict({})
    dbt_source_blocks: Dict[str, str] = dict({})
    models: Dict = dict()


class MetadataExtractor(ABC):
    """Abstract base class for transforming SQL tables."""

    def __init__(
        self,
        dbt_ref_blocks: Dict[str, str] = None,
    ):
        self.dbt_ref_blocks: Dict[str, str] = (
            dbt_ref_blocks.copy() if dbt_ref_blocks else dict()
        )

    @abstractmethod
    def extract(self, sql_expression: exp.Expression) -> exp.Expression: ...


class CTEMetadataExtractor(MetadataExtractor):
    def __init__(self, dbt_ref_blocks: Dict[str, str] = None):
        super().__init__(dbt_ref_blocks)

    def extract(self, sql_expression: exp.Expression) -> exp.Expression:
        return transform_tables(
            sql_expression,
            table_predicate=lambda node: table_is_a_cte(self.dbt_ref_blocks, node),
            table_transform=lambda table: cte_table_fn(table, self.dbt_ref_blocks),
        )


class SourceMetadataExtractor(MetadataExtractor):
    def __init__(
        self,
        dbt_ref_blocks: Dict[str, str] = None,
        dbt_source_blocks: Dict[str, str] = None,
        to_dbt_source_block: Callable[[exp.Table], str] = None,
    ):
        super().__init__(dbt_ref_blocks)
        self.dbt_source_blocks: Dict[str, str] = (
            dbt_source_blocks.copy() if dbt_source_blocks else dict()
        )

        def noop(s: str):
            return s

        self.to_dbt_source_block: Callable[[str], str] = to_dbt_source_block or noop

    def extract(self, sql_expression: exp.Expression) -> exp.Expression:
        return transform_tables(
            sql_expression,
            table_predicate=lambda node: table_is_a_source(self.dbt_ref_blocks, node),
            table_transform=lambda table: self.table_transform(table),
        )

    def table_transform(self, source_table: exp.Table) -> exp.Expression:
        source_name: str = to_fully_qualified_name(source_table)

        if source_name not in self.dbt_ref_blocks:
            self.dbt_source_blocks[source_name] = self.to_dbt_source_block(source_table)

        return exp.Table(
            this=exp.to_identifier(
                self.dbt_source_blocks[source_name],
                # Not quoting the name can make the SQL
                # invalid, but we want to insert raw jinja
                # template ‑ invalid SQL in themselves.
                quoted=False,
            ),
            alias=exp.to_identifier(
                source_table.alias if source_table.alias else source_table.name,
            ),
        )


def process_expression(
    parent_expr: exp.Expression,
    parent_model_name: str,
    to_dbt_ref_block: Callable[[str], str],
    to_dbt_source_block: Callable[[exp.Table], str],
    # Quite unpure, intended mostly for tests.
    expr_fn: Callable = lambda expr: expr,
) -> Metadata:
    final_select_expr: exp.Expression = parent_expr.copy()
    final_select_expr.args.pop("with", None)
    cte_name_and_exprs = get_cte_name_expr_tuples(parent_expr)

    models: Dict = dict()

    source_extractor = SourceMetadataExtractor(to_dbt_source_block=to_dbt_source_block)
    cte_extractor = CTEMetadataExtractor()

    for cte_name, cte_expr in cte_name_and_exprs:
        dbt_ref_block = to_dbt_ref_block(cte_name)
        # Duplicate code because we do not share mutable object.
        source_extractor.dbt_ref_blocks[cte_name] = dbt_ref_block
        cte_extractor.dbt_ref_blocks[cte_name] = dbt_ref_block
        models[dbt_ref_block] = {
            "cte_name": cte_name,
            "cte_expr": expr_fn(cte_expr),
            "model_expr": expr_fn(
                cte_extractor.extract(source_extractor.extract(cte_expr))
            ),
        }

    models[parent_model_name] = {
        "cte_expr": expr_fn(final_select_expr),
        "model_expr": expr_fn(
            cte_extractor.extract(source_extractor.extract(final_select_expr))
        ),
    }

    return Metadata(
        dbt_ref_blocks=cte_extractor.dbt_ref_blocks,
        dbt_source_blocks=source_extractor.dbt_source_blocks,
        models=models,
    )
