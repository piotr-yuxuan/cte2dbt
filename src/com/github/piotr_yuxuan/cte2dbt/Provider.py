import logging
from abc import ABC, abstractmethod
from itertools import chain
from typing import Callable, Dict, Iterator, Tuple

from sqlglot import exp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def table_has_qualified_name(table: exp.Table) -> bool:
    """Check if a table has a qualified name (database or catalog)."""
    result = bool(table.db or table.catalog)
    logger.debug(f"Checking if table '{table}' has a qualified name: {result}")
    return result


def table_is_a_cte(
    cte_names: Dict[str, str],
    table: exp.Table,
) -> bool:
    """Determine if a table is a Common Table Expression (CTE)."""
    result = not table_has_qualified_name(table) and table.name in cte_names
    logger.debug(f"Checking if table '{table}' is a CTE: {result}")
    return result


def table_is_a_source(
    cte_names: Dict[str, str],
    table: exp.Table,
) -> bool:
    """Check if a table is a source table (not a CTE)."""
    result = table_has_qualified_name(table) or table.name not in cte_names
    logger.debug(f"Checking if table '{table}' is a source: {result}")
    return result


def cte_table_fn(
    cte_table: exp.Table,
    cte_names: Dict[str, str],
) -> exp.Expression:
    """Transform a CTE table name into its Jinja block."""
    logger.info(f"Transforming CTE table '{cte_table.name}'")
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
    logger.debug(f"Transforming tables in expression: {expr}")
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
    logger.info("Transforming CTE tables")
    return transform_tables(
        cte_expr,
        table_predicate=lambda node: table_is_a_cte(
            dbt_ref_blocks,
            node,
        ),
        table_transform=lambda table: cte_table_fn(
            table,
            dbt_ref_blocks,
        ),
    )


def to_fully_qualified_name(table: exp.Table) -> str:
    """Return the fully qualified name of a table."""
    name = ".".join(filter(None, [table.db, table.catalog, table.name]))
    logger.debug(f"Computed fully qualified name: {name}")
    return name


def source_table_fn(
    source_table: exp.Table,
    dbt_source_blocks: Dict,
    to_dbt_source_block: Callable,
) -> exp.Expression:
    """Transform a source table name into a Jinja block."""
    fully_qualified_name = to_fully_qualified_name(source_table)
    logger.info(f"Processing source table: {fully_qualified_name}")

    if fully_qualified_name not in dbt_source_blocks:
        dbt_source_blocks[fully_qualified_name] = to_dbt_source_block(source_table)
        logger.debug(f"Added new source block: {fully_qualified_name}")

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
    logger.info("Transforming source tables")
    new_source_names = dbt_source_blocks.copy()

    return (
        transform_tables(
            cte_expr,
            table_predicate=lambda node: table_is_a_source(
                dbt_ref_blocks,
                node,
            ),
            table_transform=lambda table: source_table_fn(
                table,
                new_source_names,
                to_dbt_source_block,
            ),
        ),
        new_source_names,
    )


class MetadataExtractor(ABC):
    """Abstract base class for extracting and transforming metadata
    from SQL expressions."""

    def __init__(self):
        self.dbt_ref_blocks: Dict[str, str] = dict()

    @abstractmethod
    def extract(self, sql_expression: exp.Expression) -> exp.Expression: ...


class CTEMetadataExtractor(MetadataExtractor):
    """Extracts metadata related to Common Table Expressions (CTEs) in
    a SQL expression."""

    def __init__(self):
        super().__init__()

    def extract(self, sql_expression: exp.Expression) -> exp.Expression:
        logger.info("Extracting metadata from CTEs")
        return transform_tables(
            sql_expression,
            table_predicate=lambda node: table_is_a_cte(
                self.dbt_ref_blocks,
                node,
            ),
            table_transform=lambda table: cte_table_fn(
                table,
                self.dbt_ref_blocks,
            ),
        )


class SourceMetadataExtractor(MetadataExtractor):
    """Extracts metadata related to source tables in a SQL expression."""

    def __init__(self, to_dbt_source_block: Callable[[exp.Table], str]):
        super().__init__()
        self.dbt_source_blocks: Dict[str, str] = dict()
        self.to_dbt_source_block: Callable[[exp.Table], str] = to_dbt_source_block

    def extract(self, sql_expression: exp.Expression) -> exp.Expression:
        return transform_tables(
            sql_expression,
            table_predicate=lambda node: table_is_a_source(self.dbt_ref_blocks, node),
            table_transform=lambda table: self.table_transform(table),
        )

    def table_transform(self, source_table: exp.Table) -> exp.Expression:
        source_name: str = to_fully_qualified_name(source_table)
        logger.debug(f"Transforming source table: {source_name}")

        if source_name not in self.dbt_ref_blocks:
            self.dbt_source_blocks[source_name] = self.to_dbt_source_block(source_table)
            logger.info(f"New source block added: {source_name}")

        return exp.Table(
            this=exp.to_identifier(
                self.dbt_source_blocks[source_name],
                # Not quoting the name can make the SQL
                # invalid, but we want to insert raw jinja
                # template ‑ invalid SQL in themselves.
                quoted=False,
            ),
            alias=exp.to_identifier(
                source_table.alias if source_table.alias else source_table.name
            ),
        )


class Provider:
    def __init__(
        self,
        model_name: str,
        expr: exp.Expression,
        to_dbt_ref_block: Callable[[str], str],
        to_dbt_source_block: Callable[[exp.Table], str],
    ):
        self.expr = expr
        self.model_name = model_name
        self.to_dbt_ref_block = to_dbt_ref_block
        self.to_dbt_source_block = to_dbt_source_block

        self.source_extractor = SourceMetadataExtractor(self.to_dbt_source_block)
        self.cte_extractor = CTEMetadataExtractor()

    def iter_cte_tuples(self) -> Iterator[Tuple[str, exp.Expression]]:
        """Yield CTE name and expr from the parent expression."""
        if with_expr := self.expr.args.get("with", None):
            logger.debug("Extracting CTE tuples")
            yield from ((cte.alias, cte.this) for cte in with_expr)

    def iter_sources(self) -> Iterator[Tuple[str, str]]:
        """Yield source table names from the extracted sources."""
        logger.info("Iterating over source tables")
        for _ in self.iter_dbt_models():
            # Realise the dependent iterator so as to avoid a complex
            # API with dependent iterators.
            pass
        return iter(self.source_extractor.dbt_source_blocks.items())

    def iter_dbt_models(self) -> Iterator[Tuple[str, exp.Expression]]:
        """Yield instances of DbtModel."""
        logger.info("Iterating over DBT models")
        final_select_expr = self.expr.copy()
        final_select_expr.args.pop("with", None)

        for cte_name, cte_expr in chain(
            self.iter_cte_tuples(),
            [(self.model_name, final_select_expr)],
        ):
            logger.debug(f"Processing DBT model: {cte_name}")

            dbt_ref_block = self.to_dbt_ref_block(cte_name)
            self.source_extractor.dbt_ref_blocks[cte_name] = dbt_ref_block
            self.cte_extractor.dbt_ref_blocks[cte_name] = dbt_ref_block

            model_expr = cte_expr
            model_expr = self.source_extractor.extract(model_expr)
            model_expr = self.cte_extractor.extract(model_expr)

            yield (cte_name, model_expr)
