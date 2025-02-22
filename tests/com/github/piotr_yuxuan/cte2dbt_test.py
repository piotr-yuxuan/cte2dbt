import importlib
from typing import List, Tuple

import com.github.piotr_yuxuan.cte2dbt as cte2dbt
import pytest
from sqlglot import exp, parse_one

importlib.reload(cte2dbt)


def test_has_table_qualified_name():
    assert (
        cte2dbt.table_has_qualified_name(
            exp.Table(
                db="db",
                catalog="catalog",
                name="name",
            )
        )
        is True
    )
    assert (
        cte2dbt.table_has_qualified_name(
            exp.Table(
                catalog="catalog",
                name="name",
            )
        )
        is True
    )
    assert (
        cte2dbt.table_has_qualified_name(
            exp.Table(
                this=exp.to_identifier("name"),
                alias="alias",
            ),
        )
        is False
    )
    assert cte2dbt.table_has_qualified_name(exp.Table(name="name")) is False


@pytest.mark.parametrize(
    "cte_names, table, expected",
    [
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("db.catalog.name"),
            ),
            False,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("db.catalog.cte1"),
            ),
            False,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("catalog.cte1"),
            ),
            False,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("cte1"),
            ),
            True,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("cte1"),
                alias="cte1",
            ),
            True,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("cte1"),
                alias="alias",
            ),
            True,
        ),
    ],
)
def test_is_table_a_cte(cte_names, table: exp.Table, expected):
    assert expected == cte2dbt.table_is_a_cte(cte_names, table)


@pytest.mark.parametrize(
    "cte_names, table, expected",
    [
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("db.catalog.name"),
            ),
            True,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("db.catalog.cte1"),
            ),
            True,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("catalog.cte1"),
            ),
            True,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("cte1"),
            ),
            False,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("cte1"),
                alias="cte1",
            ),
            False,
        ),
        (
            {"cte1": "stg_cte1"},
            exp.Table(
                this=exp.to_identifier("cte1"),
                alias="alias",
            ),
            False,
        ),
    ],
)
def test_is_table_a_source(cte_names, table: exp.Table, expected):
    assert expected == cte2dbt.table_is_a_source(cte_names, table)


@pytest.mark.parametrize(
    "cte_names, query_text, expected",
    [
        (
            {"cte1": "stg_cte1"},
            "SELECT * FROM cte1",
            "SELECT * FROM stg_cte1 AS cte1",
        ),
        (
            {"cte1": "stg_cte1"},
            "SELECT cte1.* FROM cte1",
            "SELECT cte1.* FROM stg_cte1 AS cte1",
        ),
        (
            {"cte1": "stg_cte1"},
            "SELECT * FROM cte1 alias",
            "SELECT * FROM stg_cte1 AS alias",
        ),
        (
            {"my_alias": "stg_my_alias"},
            "SELECT * FROM cte1 my_alias",
            "SELECT * FROM cte1 AS my_alias",
        ),
        (
            {"cte1": "stg_cte1"},
            "SELECT * FROM cte1 JOIN cte1 c on cte1.id = c.id",
            "SELECT * FROM stg_cte1 AS cte1 JOIN stg_cte1 AS c on cte1.id = c.id",
        ),
        (
            {"cte1": "stg_cte1"},
            "SELECT * FROM (SELECT * FROM table) cte1",
            "SELECT * FROM (SELECT * FROM table) cte1",
        ),
        (
            {"cte1": "stg_cte1"},
            "SELECT * FROM (SELECT * FROM cte1) cte1",
            "SELECT * FROM (SELECT * FROM stg_cte1 as cte1) cte1",
        ),
        (
            {"cte1": "stg_cte1"},
            "WITH cte1 as (SELECT * FROM table) SELECT * FROM cte1",
            "WITH cte1 as (SELECT * FROM table) SELECT * FROM stg_cte1 as cte1",
        ),
        (
            {
                "cte1": "stg_cte1",
                "cte2": "stg_cte2",
            },
            # Query text:
            """WITH RECURSIVE
            cte1 as (SELECT * FROM cte2),
            cte2 as (SELECT * FROM cte1)
            SELECT cte1.*, cte2.*
            FROM cte1
            JOIN cte2""",
            # Expected:
            """WITH RECURSIVE
            cte1 as (SELECT * FROM stg_cte2 AS cte2),
            cte2 as (SELECT * FROM stg_cte1 AS cte1)
            SELECT cte1.*, cte2.*
            FROM stg_cte1 AS cte1
            JOIN stg_cte2 AS cte2
            """,
        ),
        (
            {"cte1": "stg_cte1"},
            "SELECT * FROM schema.cte1",
            "SELECT * FROM schema.cte1",
        ),
        (
            {"cte1": "stg_cte1"},
            "SELECT * FROM db.schema.cte1 cte1",
            "SELECT * FROM db.schema.cte1 AS cte1",
        ),
    ],
)
def test_table_name_with_alias_get_replaced(cte_names, query_text, expected):
    actual = cte2dbt.transform_cte_tables(parse_one(query_text), cte_names)
    assert parse_one(expected).sql() == actual.sql()


@pytest.mark.parametrize(
    "query_text, expected_source_tuples, expected_cte_tuples, expected_model_tuples",
    [
        (
            "SELECT 1",
            [],
            [],
            [
                ("final_model_name", "SELECT 1"),
            ],
        ),
        (
            "SELECT * from source1",
            [("source1", "{{ source('my_source', 'source1') }}")],
            [],
            [
                (
                    "final_model_name",
                    "SELECT * FROM {{ source('my_source', 'source1') }} AS source1",
                ),
            ],
        ),
        (
            "WITH cte1 as (SELECT 1) SELECT 2",
            [],
            [("cte1", "SELECT 1")],
            [("cte1", "SELECT 1"), ("final_model_name", "SELECT 2")],
        ),
        (
            "WITH cte1 as (SELECT 1), cte2 as (SELECT 2) SELECT 3",
            [],
            [("cte1", "SELECT 1"), ("cte2", "SELECT 2")],
            [
                ("cte1", "SELECT 1"),
                ("cte2", "SELECT 2"),
                ("final_model_name", "SELECT 3"),
            ],
        ),
        (
            "WITH cte1 as (SELECT 1) WITH cte2 as (SELECT 2) SELECT 3",
            [],
            [("cte1", "SELECT 1"), ("cte2", "SELECT 2")],
            [
                (
                    "cte1",
                    "SELECT 1",
                ),
                (
                    "cte2",
                    "SELECT 2",
                ),
                (
                    "final_model_name",
                    "SELECT 3",
                ),
            ],
        ),
        (
            "WITH cte1 as (SELECT 1 FROM source1) SELECT * FROM cte1 NATURAL JOIN source2",
            [
                (
                    "source1",
                    "{{ source('my_source', 'source1') }}",
                ),
                (
                    "source2",
                    "{{ source('my_source', 'source2') }}",
                ),
            ],
            [
                (
                    "cte1",
                    "SELECT 1 FROM source1",
                ),
            ],
            [
                (
                    "cte1",
                    "SELECT 1 FROM {{ source('my_source', 'source1') }} AS source1",
                ),
                (
                    "final_model_name",
                    "SELECT * FROM {{ ref('cte1') }} AS cte1 NATURAL JOIN {{ source('my_source', 'source2') }} AS source2",
                ),
            ],
        ),
        (
            # Ideally we would walk through deeper CTE and use a file
            # path strategy to keep code organised.
            "WITH cte1 as (WITH cte2 as (SELECT 1) SELECT 2) SELECT 3",
            [],
            [
                (
                    "cte1",
                    "WITH cte2 AS (SELECT 1) SELECT 2",
                ),
            ],
            [
                (
                    "cte1",
                    "WITH cte2 AS (SELECT 1) SELECT 2",
                ),
                (
                    "final_model_name",
                    "SELECT 3",
                ),
            ],
        ),
    ],
)
def test_provider(
    query_text: str,
    expected_source_tuples: List[Tuple],
    expected_cte_tuples: List[Tuple],
    expected_model_tuples: List[Tuple],
):
    """The goal of this non-unit test is to hepl the rewriting by
    making sure the former main interface keeps correct while its
    internals are being rewritten.

    It also serves as usage example.

    """

    def to_dbt_ref_block(cte_name: str) -> str:
        return f"{{{{ ref('{cte_name}') }}}}"

    def to_dbt_source_block(table: exp.Table) -> str:
        return f"{{{{ source('my_source', '{table.name}') }}}}"

    provider = cte2dbt.Provider(
        "final_model_name",
        parse_one(query_text),
        to_dbt_ref_block,
        to_dbt_source_block,
    )
    assert expected_source_tuples == list(provider.iter_sources()), "iter_sources"
    assert expected_cte_tuples == list(
        map(
            lambda tuple: (tuple[0], tuple[1].sql(pretty=False)),
            provider.iter_cte_tuples(),
        )
    ), "iter_cte_tuples"
    assert expected_model_tuples == list(
        map(
            lambda tuple: (tuple[0], tuple[1].sql(pretty=False)),
            provider.iter_dbt_models(),
        )
    ), "iter_dbt__models"
