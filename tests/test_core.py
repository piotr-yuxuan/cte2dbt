"""Conversion rules, each checked against an expectation written by hand from the rule."""

import dataclasses
import re

import pytest
import sqlglot
from sqlglot import exp

from cte2dbt import Project, Source, View, convert


def views(dialect: str, *named: tuple[str, str]) -> list[View]:
    """Build views from (name, query) pairs, as a caller with bare queries would."""
    built = []
    for name, sql in named:
        query = sqlglot.parse_one(sql, read=dialect)
        assert isinstance(query, exp.Query)
        built.append(View(exp.to_table(name, dialect=dialect), query))
    return built


def flat(sql: str) -> str:
    """Ignore layout: collapse whitespace, and drop it around parentheses and commas."""
    return re.sub(r"\s*([(),])\s*", r"\1", " ".join(sql.split()))


def models(project: Project) -> dict[str, str]:
    return {model.name: flat(model.sql) for model in project.models}


TSQL = ("tsql", "dbo")
DUCKDB = ("duckdb", "main")
SNOWFLAKE = ("snowflake", "PUBLIC")

CASES = [
    pytest.param(
        TSQL,
        [("dbo.v_sales", "WITH a AS (SELECT id FROM dbo.Sales) SELECT * FROM a")],
        {
            "v_sales__a": 'SELECT id FROM {{ source("dbo", "Sales") }} AS Sales',
            "v_sales": 'SELECT * FROM {{ ref("v_sales__a") }} AS a',
        },
        [],
        id="each CTE becomes a model, the view its query",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT * FROM dbo.Sales WITH (NOLOCK)")],
        {"v": 'SELECT * FROM {{ source("dbo", "Sales") }} AS Sales WITH (NOLOCK)'},
        [],
        id="table hints are kept",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT * FROM dbo.Prices FOR SYSTEM_TIME AS OF '2024-01-01'")],
        {
            "v": 'SELECT * FROM {{ source("dbo", "Prices") }}'
            " FOR SYSTEM_TIME AS OF '2024-01-01' AS Prices"
        },
        [],
        id="temporal clauses are kept",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT * FROM dbo.Sales PIVOT (SUM(amt) FOR m IN ([1], [2])) AS p")],
        {
            "v": 'SELECT * FROM {{ source("dbo", "Sales") }} AS Sales'
            " PIVOT(SUM(amt) FOR m IN ([1], [2])) AS p"
        },
        [],
        id="a pivot on a base table is kept",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT * FROM dbo.Sales TABLESAMPLE (10 PERCENT)")],
        {"v": 'SELECT * FROM {{ source("dbo", "Sales") }} AS Sales TABLESAMPLE (10 PERCENT)'},
        [],
        id="table samples are kept",
    ),
    pytest.param(
        DUCKDB,
        [("main.v", "SELECT * FROM (raw.t1 JOIN raw.t2 ON t1.id = t2.id)")],
        {
            "v": 'SELECT * FROM ({{ source("raw", "t1") }} AS t1'
            ' JOIN {{ source("raw", "t2") }} AS t2 ON t1.id = t2.id)'
        },
        [],
        id="both tables of a parenthesised join are sources",
    ),
    pytest.param(
        SNOWFLAKE,
        [("v", 'SELECT "MyT".id, x.a FROM raw.t AS "MyT" CROSS JOIN raw.u AS x(a, b)')],
        {
            "v": 'SELECT "MyT".id, x.a FROM {{ source("raw", "t") }} AS "MyT"'
            ' CROSS JOIN {{ source("raw", "u") }} AS x(a, b)'
        },
        [],
        id="aliases keep their quoting and column lists",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "WITH sales AS (SELECT 1 AS x) SELECT x FROM SALES")],
        {
            "v__sales": "SELECT 1 AS x",
            "v": 'SELECT x FROM {{ ref("v__sales") }} AS SALES',
        },
        [],
        id="T-SQL names compare ignoring case",
    ),
    pytest.param(
        ("tsql, normalization_strategy=case_sensitive", "dbo"),
        [("dbo.v", "WITH sales AS (SELECT 1 AS x) SELECT x FROM SALES")],
        {
            "v__sales": "SELECT 1 AS x",
            "v": 'SELECT x FROM {{ source("dbo", "SALES") }} AS SALES',
        },
        [],
        id="a case-sensitive collation is a dialect setting",
    ),
    pytest.param(
        SNOWFLAKE,
        [
            (
                "v",
                (
                    'WITH foo AS (SELECT 1 AS a), "Bar" AS (SELECT 2 AS b)'
                    ' SELECT * FROM FOO CROSS JOIN bar CROSS JOIN "Bar"'
                ),
            )
        ],
        {
            "v__foo": "SELECT 1 AS a",
            "v__Bar": "SELECT 2 AS b",
            "v": 'SELECT * FROM {{ ref("v__foo") }} AS FOO'
            ' CROSS JOIN {{ source("PUBLIC", "bar") }} AS bar'
            ' CROSS JOIN {{ ref("v__Bar") }} AS "Bar"',
        },
        [],
        id="Snowflake folds unquoted names and keeps quoted ones",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT * FROM dbo.fn_sales(2024) AS f CROSS JOIN #tmp CROSS JOIN ##g")],
        {"v": "SELECT * FROM dbo.fn_sales(2024) AS f CROSS JOIN #tmp CROSS JOIN ##g"},
        [
            "v: dbo.fn_sales(2024) AS f kept as written",
            "v: #tmp kept as written",
            "v: ##g kept as written",
        ],
        id="functions and temporary tables are kept and reported",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT * FROM LinkedSrv.MyDb.dbo.Sales")],
        {"v": "SELECT * FROM LinkedSrv.MyDb.dbo.Sales"},
        ["v: LinkedSrv.MyDb.dbo.Sales kept as written"],
        id="linked-server names are kept and reported",
    ),
    pytest.param(
        TSQL,
        [
            ("dbo.v2", "SELECT * FROM dbo.V1"),
            ("dbo.v1", "SELECT id FROM dbo.Sales"),
            ("dbo.v3", "SELECT * FROM other.v1"),
        ],
        {
            "v1": 'SELECT id FROM {{ source("dbo", "Sales") }} AS Sales',
            "v3": 'SELECT * FROM {{ source("other", "v1") }} AS v1',
            "v2": 'SELECT * FROM {{ ref("v1") }} AS V1',
        },
        [],
        id="a view read by another view is a ref, whatever the input order",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT dbo.Sales.id, Sales.x FROM dbo.Sales WHERE dbo.Sales.id < 5")],
        {
            "v": 'SELECT Sales.id, Sales.x FROM {{ source("dbo", "Sales") }} AS Sales'
            " WHERE Sales.id < 5"
        },
        [],
        id="schema-qualified columns keep binding to their table",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "SELECT * FROM Sales JOIN DBO.SALES AS s2 ON 1 = 1"
                    " JOIN MyDb..Orders ON 1 = 1 JOIN Archive.dbo.Orders AS o2 ON 1 = 1"
                ),
            )
        ],
        {
            "v": 'SELECT * FROM {{ source("dbo", "Sales") }} AS Sales'
            ' JOIN {{ source("dbo", "Sales") }} AS s2 ON 1 = 1'
            ' JOIN {{ source("MyDb__dbo", "Orders") }} AS Orders ON 1 = 1'
            ' JOIN {{ source("Archive__dbo", "Orders") }} AS o2 ON 1 = 1'
        },
        [],
        id="the default schema fills a missing schema and the first spelling wins",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "WITH [a.b] AS (SELECT 1 AS x) SELECT x FROM [a.b] AS c"
                    " CROSS JOIN [a].[b] AS d CROSS JOIN [dbo].[a.b] AS e"
                    " CROSS JOIN [dbo.a].[b] AS f"
                ),
            )
        ],
        {
            "v__a_b": "SELECT 1 AS x",
            "v": 'SELECT x FROM {{ ref("v__a_b") }} AS c'
            ' CROSS JOIN {{ source("a", "b") }} AS d'
            ' CROSS JOIN {{ source("dbo", "a.b") }} AS e'
            ' CROSS JOIN {{ source("dbo.a", "b") }} AS f',
        },
        [],
        id="names with dots stay distinct",
    ),
    pytest.param(
        TSQL,
        [
            (
                "[dbo].[Sales Report]",
                (
                    "WITH [my cte] AS (SELECT 1 AS x), base AS (SELECT 2 AS y)"
                    " SELECT * FROM [my cte] CROSS JOIN base"
                ),
            ),
            ("dbo.w", "WITH base AS (SELECT 3 AS z) SELECT * FROM base"),
        ],
        {
            "Sales_Report__my_cte": "SELECT 1 AS x",
            "Sales_Report__base": "SELECT 2 AS y",
            "Sales_Report": 'SELECT * FROM {{ ref("Sales_Report__my_cte") }} AS [my cte]'
            ' CROSS JOIN {{ ref("Sales_Report__base") }} AS base',
            "w__base": "SELECT 3 AS z",
            "w": 'SELECT * FROM {{ ref("w__base") }} AS base',
        },
        ["Sales_Report: renamed from Sales Report"],
        id="model names are prefixed by their view and safe as file names",
    ),
    pytest.param(
        DUCKDB,
        [
            (
                "main.v",
                (
                    "WITH a AS (SELECT 1 AS v) SELECT * FROM a"
                    " CROSS JOIN (WITH a AS (SELECT 2 AS v) SELECT * FROM a) AS s"
                ),
            )
        ],
        {
            "v__a": "SELECT 1 AS v",
            "v": 'SELECT * FROM {{ ref("v__a") }} AS a'
            " CROSS JOIN (WITH a AS (SELECT 2 AS v) SELECT * FROM a) AS s",
        },
        [],
        id="a nested WITH keeps its own names",
    ),
    pytest.param(
        DUCKDB,
        [
            (
                "main.v",
                (
                    "WITH c AS (SELECT 1 AS id)"
                    " SELECT x FROM raw.t1 WHERE EXISTS (SELECT 1 FROM c WHERE c.id = t1.id)"
                ),
            )
        ],
        {
            "v__c": "SELECT 1 AS id",
            "v": 'SELECT x FROM {{ source("raw", "t1") }} AS t1'
            ' WHERE EXISTS (SELECT 1 FROM {{ ref("v__c") }} AS c WHERE c.id = t1.id)',
        },
        [],
        id="references in subqueries are rewritten",
    ),
    pytest.param(
        DUCKDB,
        [("main.v", "SELECT '{{ x }}' AS a, '{%' AS b, '{#' AS c FROM raw.t")],
        {
            "v": "SELECT '{{ '{' }}{ x }}' AS a, '{{ '{' }}%' AS b, '{{ '{' }}#' AS c"
            ' FROM {{ source("raw", "t") }} AS t'
        },
        [],
        id="text that would open Jinja is escaped",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT * FROM dbo.[O'Brien] AS a CROSS JOIN dbo.[x\") }}{{ evil] AS b")],
        {
            "v": 'SELECT * FROM {{ source("dbo", "O\'Brien") }} AS a'
            ' CROSS JOIN {{ source("dbo", "x\\") }}{{ evil") }} AS b'
        },
        [],
        id="names are quoted as Jinja strings",
    ),
]


@pytest.mark.parametrize(("dialect", "named", "expected", "warnings"), CASES)
def test_conversion(
    dialect: tuple[str, str],
    named: list[tuple[str, str]],
    expected: dict[str, str],
    warnings: list[str],
) -> None:
    project = convert(views(dialect[0], *named), dialect=dialect[0], default_schema=dialect[1])
    assert models(project) == {name: flat(sql) for name, sql in expected.items()}
    assert list(project.warnings) == warnings


def test_models_come_after_the_models_they_ref() -> None:
    project = convert(
        views(
            "tsql",
            ("dbo.v2", "WITH a AS (SELECT * FROM dbo.v1) SELECT * FROM a"),
            ("dbo.v1", "SELECT 1 AS x"),
        ),
        dialect="tsql",
        default_schema="dbo",
    )
    names = [model.name for model in project.models]
    for model in project.models:
        assert all(names.index(ref) < names.index(model.name) for ref in model.refs)
    assert {model.name: model.refs for model in project.models} == {
        "v1": (),
        "v2__a": ("v1",),
        "v2": ("v2__a",),
    }


def test_sources_are_listed_once_per_table() -> None:
    project = convert(
        views(
            "tsql",
            (
                "dbo.v",
                "SELECT * FROM Sales JOIN dbo.Orders AS o ON 1 = 1 JOIN DBO.SALES AS s ON 1 = 1",
            ),
        ),
        dialect="tsql",
        default_schema="dbo",
    )
    expected = (Source("", "dbo", "Orders"), Source("", "dbo", "Sales"))
    assert project.sources == expected
    assert project.models[0].sources == expected


@pytest.mark.parametrize(
    ("named", "message"),
    [
        pytest.param(
            [("dbo.a", "WITH b AS (SELECT 1 AS x) SELECT x FROM b"), ("dbo.a__b", "SELECT 1 AS x")],
            "two models would be named a__b",
            id="a view named like another view's CTE model",
        ),
        pytest.param(
            [("dbo.V", "SELECT 1 AS x"), ("sales.v", "SELECT 2 AS x")],
            "two models would be named v",
            id="views of the same name in two schemas",
        ),
        pytest.param(
            [("dbo.a", "SELECT * FROM dbo.b"), ("dbo.b", "SELECT * FROM dbo.a")],
            "cycle",
            id="views reading one another",
        ),
        pytest.param([("dbo.a", "SELECT * FROM a")], "cycle", id="a view reading itself"),
        pytest.param(
            [("dbo.v", "SELECT '__cte2dbt_0__' AS x")],
            "reserved text",
            id="the placeholder text in the SQL",
        ),
        pytest.param(
            [
                (
                    "dbo.v",
                    (
                        "SELECT id FROM dbo.Sales WHERE EXISTS"
                        " (SELECT 1 FROM archive.Sales WHERE archive.Sales.y = dbo.Sales.y)"
                    ),
                )
            ],
            "would bind to another table",
            id="a schema-qualified column that would become ambiguous",
        ),
        pytest.param(
            [("dbo.v", "SELECT * FROM [a__b].t CROSS JOIN a.b.t AS u")],
            "two dbt sources would be named a__b",
            id="two sources sharing a dbt source name",
        ),
    ],
)
def test_conversion_errors(named: list[tuple[str, str]], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        convert(views("tsql", *named), dialect="tsql", default_schema="dbo")


def test_an_unknown_dialect_is_an_error() -> None:
    with pytest.raises(ValueError, match="nosuch"):
        convert([], dialect="nosuch", default_schema="dbo")


def test_the_input_views_are_not_modified() -> None:
    given = views(
        "tsql", ("dbo.v", "WITH a AS (SELECT * FROM dbo.Sales) SELECT dbo.a.x FROM dbo.a")
    )
    before = [(view.name.sql(), view.query.sql()) for view in given]
    convert(given, dialect="tsql", default_schema="dbo")
    assert [(view.name.sql(), view.query.sql()) for view in given] == before


def test_results_are_frozen() -> None:
    project = convert(
        views("tsql", ("dbo.v", "SELECT 1 AS x")), dialect="tsql", default_schema="dbo"
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        project.models[0].name = "w"  # type: ignore[misc]
