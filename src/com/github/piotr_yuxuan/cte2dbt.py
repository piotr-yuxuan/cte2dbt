import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import partial
from itertools import chain
from typing import Any, Callable, Dict, Iterator, Set, Tuple

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
    dbt_ref_blocks: Dict[str, str],
    cte_table: exp.Table,
) -> exp.Expression:
    """Transform a CTE table name into its Jinja block."""
    logger.info(f"Transforming CTE table '{cte_table.name}'")
    return exp.Table(
        this=exp.to_identifier(
            dbt_ref_blocks[cte_table.name],
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
    table_transform: Callable[[str, exp.Table], exp.Expression],
):
    logger.debug(f"Transforming tables in expression: {expr}")
    return expr.transform(
        lambda node: (
            table_transform(node)
            if isinstance(node, exp.Table) and table_predicate(node)
            else node
        )
    )


def to_fully_qualified_name(table: exp.Table) -> str:
    """Return the fully qualified name of a table in the order catalog.db.name.

    If a component is None, an empty string, or just whitespace, it is ignored.
    """
    # Normalise and trim each component
    catalog = (table.catalog or "").strip()
    db = (table.db or "").strip()
    name = (table.name or "").strip()

    # Arrange components in the desired order (catalog, then db, then name)
    components = [catalog, db, name]

    # Filter out any empty components
    full_name = ".".join(comp for comp in components if comp)

    logger.debug(f"Computed fully qualified name: {full_name}")
    return full_name


class BaseBlockTransformer(ABC):
    def __init__(self):
        self.dbt_ref_blocks: Dict[str, str] = dict()
        self.dependencies: Dict[str, Set[str]] = defaultdict(set)

    @abstractmethod
    def extract(self, sql_expression: exp.Expression) -> exp.Expression: ...


class CTEBlockTransformer(BaseBlockTransformer):
    def __init__(self):
        super().__init__()

    def extract(self, cte_name: str, sql_expression: exp.Expression) -> exp.Expression:
        logger.info("Extracting metadata from CTEs")
        return transform_tables(
            sql_expression,
            table_predicate=partial(table_is_a_cte, self.dbt_ref_blocks),
            table_transform=partial(self.table_transform, cte_name),
        )

    def table_transform(
        self,
        cte_name: str,
        cte_table: exp.Table,
    ) -> exp.Expression:
        """Transform a CTE table name into its Jinja block."""
        logger.info(f"Transforming CTE table '{cte_table.name}'")
        self.dependencies[cte_name].add(cte_table.name)
        return exp.Table(
            this=exp.to_identifier(
                self.dbt_ref_blocks[cte_table.name],
                # Not quoting the name can make the SQL
                # invalid, but we want to insert raw jinja
                # template ‑ invalid SQL in themselves.
                quoted=False,
            ),
            alias=exp.to_identifier(
                cte_table.alias if cte_table.alias else cte_table.name,
            ),
        )


class SourceBlockTransformer(BaseBlockTransformer):
    def __init__(self, to_dbt_source_block: Callable[[exp.Table], str]):
        super().__init__()
        self.dbt_source_blocks: Dict[str, str] = dict()
        self.to_dbt_source_block: Callable[[exp.Table], str] = to_dbt_source_block

    def extract(self, cte_name: str, sql_expression: exp.Expression) -> exp.Expression:
        return transform_tables(
            sql_expression,
            table_predicate=partial(table_is_a_source, self.dbt_ref_blocks),
            table_transform=partial(self.table_transform, cte_name),
        )

    def table_transform(self, cte_name: str, source_table: exp.Table) -> exp.Expression:
        source_name: str = to_fully_qualified_name(source_table)
        logger.debug(f"Transforming source table: {source_name}")

        if source_name not in self.dbt_ref_blocks:
            self.dbt_source_blocks[source_name] = self.to_dbt_source_block(source_table)
            self.dependencies[cte_name].add(source_name)
            logger.info(f"New source block added: {source_name}")

        return exp.Table(
            this=exp.to_identifier(
                self.dbt_source_blocks[source_name],
                # Not quoting can make the SQL invalid, but we want to
                # insert a jinja block ‑ itself invalid SQL.
                quoted=False,
            ),
            alias=exp.to_identifier(
                source_table.alias if source_table.alias else source_table.name
            ),
        )


def merge_dicts_of_sets(
    left: Dict[Any, str],
    right: Dict[Any, str],
) -> Dict[Any, str]:
    result = defaultdict(set)
    for key in left.keys():
        result[key] |= left.get(key)
    for key in right.keys():
        result[key] |= right.get(key)
    return result


class Provider:
    def __init__(
        self,
        model_name: str,
        expr: exp.Expression,
        to_dbt_ref_block: Callable[[str], str],
        to_dbt_source_block: Callable[[exp.Table], str],
    ):
        self.expr = expr.copy()
        self.model_name = model_name
        self.to_dbt_ref_block = to_dbt_ref_block
        self.to_dbt_source_block = to_dbt_source_block

        self.source_extractor = SourceBlockTransformer(self.to_dbt_source_block)
        self.cte_extractor = CTEBlockTransformer()

    def iter_cte_tuples(self) -> Iterator[Tuple[str, exp.Expression]]:
        """Yield CTE name and expr from the parent expression."""
        if with_expr := self.expr.args.get("with", None):
            logger.debug("Extracting CTE tuples")
            yield from ((cte.alias, cte.this) for cte in with_expr)

    def iter_sources(self) -> Iterator[Tuple[str, str]]:
        """Yield source table names from the extracted sources."""
        logger.info("Iterating over source tables")
        self._populate_stateful_accumulators()
        return iter(self.source_extractor.dbt_source_blocks.items())

    def model_dependencies(self) -> Dict[str, Set[str]]:
        """Return a dependency dictionary where CTE whose name is key
        depends on sources and CTE whose names are in value.

        """
        self._populate_stateful_accumulators()
        return merge_dicts_of_sets(
            self.source_extractor.dependencies,
            self.cte_extractor.dependencies,
        )

    def _populate_stateful_accumulators(self):
        """Realise the main iterator as to avoid a complex API with
        dependent iterators.

        """
        for _ in self.iter_dbt_models():
            pass

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
            model_expr = self.source_extractor.extract(cte_name, model_expr)
            model_expr = self.cte_extractor.extract(cte_name, model_expr)

            yield (cte_name, model_expr)
