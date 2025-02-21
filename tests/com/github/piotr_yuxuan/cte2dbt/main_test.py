import importlib

import com.github.piotr_yuxuan.cte2dbt.main as main
import pytest
from sqlglot import exp, parse_one

importlib.reload(main)


def test_has_table_qualified_name():
    assert (
        main.table_has_qualified_name(
            exp.Table(
                db="db",
                catalog="catalog",
                name="name",
            )
        )
        is True
    )
    assert (
        main.table_has_qualified_name(
            exp.Table(
                catalog="catalog",
                name="name",
            )
        )
        is True
    )
    assert (
        main.table_has_qualified_name(
            exp.Table(
                this=exp.to_identifier("name"),
                alias="alias",
            ),
        )
        is False
    )
    assert main.table_has_qualified_name(exp.Table(name="name")) is False


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
    assert expected == main.table_is_a_cte(cte_names, table)


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
    assert expected == main.table_is_a_source(cte_names, table)


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
    actual = main.transform_cte_tables(parse_one(query_text), cte_names)
    assert parse_one(expected).sql() == actual.sql()


@pytest.mark.parametrize(
    "query_text, expected_names",
    [
        (
            "SELECT 1",
            [],
        ),
        (
            "WITH cte1 as (SELECT 1) SELECT 2",
            ["cte1"],
        ),
        (
            "WITH cte1 as (SELECT 1) WITH cte2 as (SELECT 1) SELECT 2",
            ["cte1", "cte2"],
        ),
        (
            "WITH cte1 as (WITH cte2 as (SELECT 1) SELECT 2) SELECT 3",
            ["cte1"],
        ),
    ],
)
def test_get_name_and_text_for_all_cte(query_text, expected_names):
    assert expected_names == [
        name
        for name, _ in main.iter_cte_tuples(
            parse_one(
                query_text,
            )
        )
    ]


@pytest.mark.parametrize(
    "query_text, expected_cte_expr, expected_source_names",
    [
        (
            "SELECT 1 FROM db.catalog.table",
            "SELECT 1 FROM {{ source('db') }} AS table",
            {"catalog.db.table": "{{ source('db') }}"},
        ),
        (
            "SELECT 1 FROM db.catalog.table as table",
            "SELECT 1 FROM {{ source('db') }} AS table",
            {"catalog.db.table": "{{ source('db') }}"},
        ),
        (
            "SELECT 1 FROM db.catalog.table as other",
            "SELECT 1 FROM {{ source('db') }} AS other",
            {"catalog.db.table": "{{ source('db') }}"},
        ),
    ],
)
def test_transform_source_tables_implicit_source_names(
    query_text,
    expected_cte_expr,
    expected_source_names,
):
    def to_dbt_source_block(table: exp.Table) -> str:
        return "{{ source('" + (table.catalog or table.name) + "') }}"

    actual_cte_expr, actual_source_names = main.transform_source_tables(
        cte_expr=parse_one(query_text),
        dbt_ref_blocks=dict(),
        dbt_source_blocks=dict(),
        to_dbt_source_block=to_dbt_source_block,
    )

    assert expected_cte_expr == actual_cte_expr.sql()
    assert expected_source_names == actual_source_names


@pytest.mark.parametrize(
    "query_text, dbt_source_blocks, expected_cte_expr, expected_source_names",
    [
        (
            "SELECT 1 FROM db.catalog.table",
            {},
            "SELECT 1 FROM {{ source('db') }} AS table",
            {"catalog.db.table": "{{ source('db') }}"},
        ),
        (
            "SELECT 1 FROM db.catalog.table as table",
            {"catalog.db.table": "{{ source('source_name', 'table') }}"},
            "SELECT 1 FROM {{ source('source_name', 'table') }} AS table",
            {"catalog.db.table": "{{ source('source_name', 'table') }}"},
        ),
        (
            "SELECT 1 FROM db.catalog.table as other",
            {"catalog.db.table": "{{ source('source_name', 'table') }}"},
            "SELECT 1 FROM {{ source('source_name', 'table') }} AS other",
            {"catalog.db.table": "{{ source('source_name', 'table') }}"},
        ),
    ],
)
def test_transform_source_tables(
    query_text,
    dbt_source_blocks,
    expected_cte_expr,
    expected_source_names,
):
    def to_dbt_source_block(table: exp.Table) -> str:
        return "{{ source('" + (table.catalog or table.name) + "') }}"

    actual_cte_expr, actual_source_names = main.transform_source_tables(
        cte_expr=parse_one(query_text),
        dbt_ref_blocks=dict(),
        dbt_source_blocks=dbt_source_blocks,
        to_dbt_source_block=to_dbt_source_block,
    )

    assert expected_cte_expr == actual_cte_expr.sql()
    assert expected_source_names == actual_source_names


@pytest.mark.parametrize(
    "query_text, expected_metadata",
    [
        (
            "SELECT 1",
            main.MetadataDeprecated(
                dbt_ref_blocks={},
                dbt_source_blocks={},
                models={
                    "final_model_name": {
                        "cte_expr": "SELECT 1",
                        "model_expr": "SELECT 1",
                    }
                },
            ),
        ),
        (
            "WITH cte1 as (SELECT 1 FROM source1) SELECT * FROM cte1 NATURAL JOIN source2",
            main.MetadataDeprecated(
                dbt_ref_blocks={"cte1": "{{ ref('cte1') }}"},
                dbt_source_blocks={
                    "source1": "{{ source('source1') }}",
                    "source2": "{{ source('source2') }}",
                },
                models={
                    "{{ ref('cte1') }}": {
                        "cte_name": "cte1",
                        "cte_expr": "SELECT 1 FROM source1",
                        "model_expr": "SELECT 1 FROM {{ source('source1') }} AS source1",
                    },
                    "final_model_name": {
                        "cte_expr": "SELECT * FROM cte1 NATURAL JOIN source2",
                        "model_expr": "SELECT * FROM {{ ref('cte1') }} AS cte1 NATURAL JOIN {{ source('source2') }} AS source2",
                    },
                },
            ),
        ),
        (
            "WITH cte1 as (SELECT 1) SELECT 2",
            main.MetadataDeprecated(
                dbt_ref_blocks={"cte1": "{{ ref('cte1') }}"},
                dbt_source_blocks={},
                models={
                    "{{ ref('cte1') }}": {
                        "cte_name": "cte1",
                        "cte_expr": "SELECT 1",
                        "model_expr": "SELECT 1",
                    },
                    "final_model_name": {
                        "cte_expr": "SELECT 2",
                        "model_expr": "SELECT 2",
                    },
                },
            ),
        ),
        (
            "WITH cte1 as (SELECT 1) WITH cte2 as (SELECT 1) SELECT 2",
            main.MetadataDeprecated(
                dbt_ref_blocks={
                    "cte1": "{{ ref('cte1') }}",
                    "cte2": "{{ ref('cte2') }}",
                },
                dbt_source_blocks={},
                models={
                    "{{ ref('cte1') }}": {
                        "cte_name": "cte1",
                        "cte_expr": "SELECT 1",
                        "model_expr": "SELECT 1",
                    },
                    "{{ ref('cte2') }}": {
                        "cte_name": "cte2",
                        "cte_expr": "SELECT 1",
                        "model_expr": "SELECT 1",
                    },
                    "final_model_name": {
                        "cte_expr": "SELECT 2",
                        "model_expr": "SELECT 2",
                    },
                },
            ),
        ),
        (
            # Ideally we would walk through deeper CTE and use a file
            # path strategy to keep code organised.
            "WITH cte1 as (WITH cte2 as (SELECT 1) SELECT 2) SELECT 3",
            main.MetadataDeprecated(
                dbt_ref_blocks={"cte1": "{{ ref('cte1') }}"},
                dbt_source_blocks={},
                models={
                    "{{ ref('cte1') }}": {
                        "cte_name": "cte1",
                        "cte_expr": "WITH cte2 AS (SELECT 1) SELECT 2",
                        "model_expr": "WITH cte2 AS (SELECT 1) SELECT 2",
                    },
                    "final_model_name": {
                        "cte_expr": "SELECT 3",
                        "model_expr": "SELECT 3",
                    },
                },
            ),
        ),
    ],
)
def test_refactoring_process_expression(
    query_text: str,
    expected_metadata: main.MetadataDeprecated,
):
    """The goal of this non-unit test is to hepl the rewriting by
    making sure the former main interface keeps correct while its
    internals are being rewritten.

    It also serves as usage example.

    """

    def to_dbt_ref_block(cte_name: str) -> str:
        return f"{{{{ ref('{cte_name}') }}}}"

    def to_dbt_source_block(table: exp.Table) -> str:
        return f"{{{{ source('{table.catalog or table.name}') }}}}"

    assert expected_metadata == main.process_expression(
        parse_one(query_text),
        "final_model_name",
        to_dbt_ref_block,
        to_dbt_source_block,
        expr_fn=lambda x: x.sql(pretty=False),
    )
