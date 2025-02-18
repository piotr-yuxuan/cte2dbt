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
        for name, _ in main.get_cte_name_expr_tuples(
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
    def to_source_name(table: exp.Table) -> str:
        return "{{ source('" + (table.catalog or table.name) + "') }}"

    actual_cte_expr, actual_source_names = main.transform_source_tables(
        cte_expr=parse_one(query_text),
        cte_names=dict(),
        source_names=dict(),
        to_source_name=to_source_name,
    )

    assert expected_cte_expr == actual_cte_expr.sql()
    assert expected_source_names == actual_source_names


@pytest.mark.parametrize(
    "query_text, source_names, expected_cte_expr, expected_source_names",
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
    source_names,
    expected_cte_expr,
    expected_source_names,
):
    def to_source_name(table: exp.Table) -> str:
        return "{{ source('" + (table.catalog or table.name) + "') }}"

    actual_cte_expr, actual_source_names = main.transform_source_tables(
        cte_expr=parse_one(query_text),
        cte_names=dict(),
        source_names=source_names,
        to_source_name=to_source_name,
    )

    assert expected_cte_expr == actual_cte_expr.sql()
    assert expected_source_names == actual_source_names
