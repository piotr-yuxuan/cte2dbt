import importlib
from pathlib import Path

import com.github.piotr_yuxuan.cte2dbt.draft2 as draft2
import pytest
from sqlglot import exp, parse_one

importlib.reload(draft2)


@pytest.mark.parametrize(
    "replacements, query_text, expected",
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
def test_table_name_with_alias_get_replaced(replacements, query_text, expected):
    actual = draft2.replace_table_name(
        replacements,
        parse_one(query_text),
    )
    print(f"actual:{actual.sql(pretty=True)}")
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
        for name, _ in draft2.get_cte_name_and_exprs(
            parse_one(
                query_text,
            )
        )
    ]
