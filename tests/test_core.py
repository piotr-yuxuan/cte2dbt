"""Conversion rules, each checked against an expectation written by hand from the rule."""

import dataclasses
import re

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.errors import UnsupportedError

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


CASES += [
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "WITH a (x, y) AS (SELECT p, q FROM dbo.t UNION ALL SELECT 1, 2)"
                    " SELECT x, y FROM a"
                ),
            )
        ],
        {
            "v__a": (
                'SELECT * FROM (SELECT p, q FROM {{ source("dbo", "t") }} AS t'
                " UNION ALL SELECT 1, 2) AS a(x, y)"
            ),
            "v": 'SELECT x, y FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="a CTE column list names the columns of its model",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM R WHERE n < 10),"
                    " s AS (SELECT n FROM r) SELECT n FROM s"
                ),
            )
        ],
        {
            "v__r": (
                "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM R WHERE n < 10)"
                " SELECT * FROM r"
            ),
            "v__s": 'SELECT n FROM {{ ref("v__r") }} AS r',
            "v": 'SELECT n FROM {{ ref("v__s") }} AS s',
        },
        [],
        id="a recursive CTE keeps its WITH in its own model",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "WITH base AS (SELECT id FROM dbo.t), r (id) AS (SELECT id FROM base"
                    " UNION ALL SELECT r.id + 1 FROM r JOIN dbo.u ON 1 = 0) SELECT id FROM r"
                ),
            )
        ],
        {
            "v__base": 'SELECT id FROM {{ source("dbo", "t") }} AS t',
            "v__r": (
                'WITH r(id) AS (SELECT id FROM {{ ref("v__base") }} AS base'
                ' UNION ALL SELECT r.id + 1 FROM r JOIN {{ source("dbo", "u") }} AS u ON 1 = 0)'
                " SELECT * FROM r"
            ),
            "v": 'SELECT id FROM {{ ref("v__r") }} AS r',
        },
        [],
        id="a recursive CTE still refs earlier CTEs and reads sources",
    ),
    pytest.param(
        ("postgres", "public"),
        [
            (
                "v",
                (
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3)"
                    " SELECT n FROM r"
                ),
            )
        ],
        {
            "v__r": (
                "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3)"
                " SELECT * FROM r"
            ),
            "v": 'SELECT n FROM {{ ref("v__r") }} AS r',
        },
        [],
        id="WITH RECURSIVE is kept where the dialect needs it",
    ),
    pytest.param(
        DUCKDB,
        [
            (
                "main.v",
                (
                    "WITH orders AS (SELECT * FROM orders WHERE amount > 10)"
                    " SELECT COUNT(*) AS n FROM orders"
                ),
            )
        ],
        {
            "v__orders": 'SELECT * FROM {{ source("main", "orders") }} AS orders WHERE amount > 10',
            "v": 'SELECT COUNT(*) AS n FROM {{ ref("v__orders") }} AS orders',
        },
        [],
        id="without RECURSIVE a CTE named like a table reads the table",
    ),
]


CASES += [
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT dbo.t1.id FROM dbo.t1 UNION ALL SELECT dbo.t1.id FROM dbo.t1")],
        {
            "v": 'SELECT t1.id FROM {{ source("dbo", "t1") }} AS t1'
            ' UNION ALL SELECT t1.id FROM {{ source("dbo", "t1") }} AS t1'
        },
        [],
        id="each branch of a union requalifies its own columns",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "SELECT dbo.t1.id FROM dbo.t1"
                    " WHERE dbo.t1.id IN (SELECT id FROM dbo.t1 WHERE x > 10)"
                ),
            )
        ],
        {
            "v": 'SELECT t1.id FROM {{ source("dbo", "t1") }} AS t1 WHERE t1.id IN'
            ' (SELECT id FROM {{ source("dbo", "t1") }} AS t1 WHERE x > 10)'
        },
        [],
        id="a subquery over the same table does not hide the outer one",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT other.Sales.id, s.Location.Lat FROM dbo.Sales CROSS JOIN dbo.s")],
        {
            "v": 'SELECT other.Sales.id, s.Location.Lat FROM {{ source("dbo", "Sales") }}'
            ' AS Sales CROSS JOIN {{ source("dbo", "s") }} AS s'
        },
        [],
        id="a qualifier is shortened only when it names the same table",
    ),
    pytest.param(
        DUCKDB,
        [
            (
                "main.v",
                (
                    "WITH a AS (SELECT * FROM"
                    " (WITH orders AS (SELECT * FROM orders WHERE amount > 10)"
                    " SELECT * FROM orders) q)"
                    " SELECT * FROM a"
                ),
            )
        ],
        {
            "v__a": "SELECT * FROM (WITH orders AS (SELECT * FROM"
            ' {{ source("main", "orders") }} AS orders WHERE amount > 10)'
            " SELECT * FROM orders) AS q",
            "v": 'SELECT * FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="a nested CTE does not see itself without RECURSIVE",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT t.id, f.x FROM dbo.t AS t CROSS APPLY dbo.fn_tvf(t.id) AS f")],
        {
            "v": 'SELECT t.id, f.x FROM {{ source("dbo", "t") }} AS t'
            " CROSS APPLY dbo.fn_tvf(t.id) AS f"
        },
        ["v: CROSS APPLY dbo.fn_tvf(t.id) AS f kept as written"],
        id="a function under APPLY is reported",
    ),
]


CASES += [
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT dbo.t1.id, Sales.dbo.t2.id FROM Sales.dbo.t1 CROSS JOIN dbo.t2")],
        {
            "v": 'SELECT t1.id, t2.id FROM {{ source("Sales__dbo", "t1") }} AS t1'
            ' CROSS JOIN {{ source("dbo", "t2") }} AS t2'
        },
        [],
        id="a database written on one side only still names the same table",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "SELECT dbo.t1.id FROM dbo.t1"
                    " WHERE EXISTS (SELECT 1 FROM dbo.t2 WHERE dbo.t2.id = dbo.t1.id)"
                ),
            )
        ],
        {
            "v": 'SELECT t1.id FROM {{ source("dbo", "t1") }} AS t1 WHERE EXISTS'
            ' (SELECT 1 FROM {{ source("dbo", "t2") }} AS t2 WHERE t2.id = t1.id)'
        },
        [],
        id="a correlated column binds to the outer table",
    ),
    pytest.param(
        ("sqlite", "main"),
        [("v", "WITH a AS (SELECT x FROM b), b AS (SELECT 1 AS x) SELECT x FROM a")],
        {
            "v__b": "SELECT 1 AS x",
            "v__a": 'SELECT x FROM {{ ref("v__b") }} AS b',
            "v": 'SELECT x FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="SQLite lets a CTE read a later one",
    ),
    pytest.param(
        ("sqlite", "main"),
        [
            (
                "v",
                (
                    "WITH a AS (SELECT * FROM (WITH c AS (SELECT * FROM d), d AS (SELECT 7 AS id)"
                    " SELECT * FROM c) AS q) SELECT * FROM a"
                ),
            )
        ],
        {
            "v__a": "SELECT * FROM (WITH c AS (SELECT * FROM d), d AS (SELECT 7 AS id)"
            " SELECT * FROM c) AS q",
            "v": 'SELECT * FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="SQLite lets a nested CTE read a later one",
    ),
    pytest.param(
        ("postgres", "public"),
        [
            (
                "v",
                (
                    "WITH a AS (SELECT * FROM (WITH RECURSIVE r(n) AS"
                    " (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r) AS q)"
                    " SELECT * FROM a"
                ),
            )
        ],
        {
            "v__a": "SELECT * FROM (WITH RECURSIVE r(n) AS"
            " (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r) AS q",
            "v": 'SELECT * FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="a nested recursive CTE reads itself",
    ),
    pytest.param(
        DUCKDB,
        [
            (
                "main.v",
                (
                    "WITH a AS (SELECT * FROM (WITH RECURSIVE r(n) AS"
                    " (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r) AS q)"
                    " SELECT * FROM a"
                ),
            )
        ],
        {
            "v__a": "SELECT * FROM (WITH RECURSIVE r(n) AS"
            " (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r) AS q",
            "v": 'SELECT * FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="a nested recursive CTE reads itself without forward references",
    ),
    pytest.param(
        ("snowflake", "PUBLIC"),
        [
            (
                "v",
                (
                    "WITH a AS (SELECT * FROM (WITH r (n) AS"
                    " (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r) AS q)"
                    " SELECT * FROM a"
                ),
            )
        ],
        {
            "v__a": "SELECT * FROM (WITH r(n) AS"
            " (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r) AS q",
            "v": 'SELECT * FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="a nested CTE reads itself where recursion needs no keyword",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                (
                    "SELECT t.id, f.x FROM dbo.t AS t"
                    " CROSS APPLY dbo.fn((SELECT MAX(id) FROM dbo.u)) AS f"
                ),
            )
        ],
        {
            "v": 'SELECT t.id, f.x FROM {{ source("dbo", "t") }} AS t'
            ' CROSS APPLY dbo.fn((SELECT MAX(id) FROM {{ source("dbo", "u") }} AS u)) AS f'
        },
        ["v: CROSS APPLY dbo.fn((SELECT MAX(id) FROM dbo.u)) AS f kept as written"],
        id="a function under APPLY is reported as written",
    ),
]


CASES += [
    pytest.param(
        ("postgres", "public"),
        [("v", "WITH RECURSIVE a AS (SELECT x FROM b), b AS (SELECT 7 AS x) SELECT x FROM a")],
        {
            "v__b": "SELECT 7 AS x",
            "v__a": 'SELECT x FROM {{ ref("v__b") }} AS b',
            "v": 'SELECT x FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="Postgres under RECURSIVE lets a CTE read a later one",
    ),
    pytest.param(
        DUCKDB,
        [("main.v", "WITH RECURSIVE a AS (SELECT x FROM b), b AS (SELECT 7 AS x) SELECT x FROM a")],
        {
            "v__a": 'SELECT x FROM {{ source("main", "b") }} AS b',
            "v__b": "SELECT 7 AS x",
            "v": 'SELECT x FROM {{ ref("v__a") }} AS a',
        },
        [],
        id="DuckDB under RECURSIVE does not",
    ),
    pytest.param(
        DUCKDB,
        [
            (
                "main.v",
                (
                    "WITH b AS (SELECT 5 AS id)"
                    " SELECT * FROM (WITH RECURSIVE b AS (SELECT id + 1 AS id FROM b)"
                    " SELECT * FROM b) AS q"
                ),
            )
        ],
        {
            "v__b": "SELECT 5 AS id",
            "v": "SELECT * FROM (WITH RECURSIVE b AS"
            ' (SELECT id + 1 AS id FROM {{ ref("v__b") }} AS b) SELECT * FROM b) AS q',
        },
        [],
        id="only a union reads itself under RECURSIVE",
    ),
    pytest.param(
        TSQL,
        [("dbo.v", "SELECT b.dbo.t1.id FROM a.dbo.t1")],
        {"v": 'SELECT b.dbo.t1.id FROM {{ source("a__dbo", "t1") }} AS t1'},
        [],
        id="a column naming another database keeps its qualifier",
    ),
    pytest.param(
        TSQL,
        [
            (
                "dbo.v",
                "SELECT t.id FROM dbo.t AS t CROSS APPLY (SELECT MAX(id) AS m FROM dbo.u) AS x",
            )
        ],
        {
            "v": 'SELECT t.id FROM {{ source("dbo", "t") }} AS t'
            ' CROSS APPLY (SELECT MAX(id) AS m FROM {{ source("dbo", "u") }} AS u) AS x'
        },
        [],
        id="a subquery under APPLY is not reported",
    ),
]


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SEARCH DEPTH FIRST BY n SET ord", id="SEARCH"),
        pytest.param("CYCLE n SET is_cycle USING path", id="CYCLE"),
    ],
)
def test_a_recursive_cte_with_search_or_cycle_is_refused(sql: str) -> None:
    query = f"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) {sql} SELECT n FROM r"
    with pytest.raises(ValueError, match="SEARCH or CYCLE"):
        convert(views("postgres", ("v", query)), dialect="postgres", default_schema="public")


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param('SELECT * FROM RAW.t1 JOIN "RAW".t2 ON TRUE', id="two schemas spelt alike"),
        pytest.param('SELECT * FROM Foo JOIN "Foo" ON TRUE', id="two tables spelt alike"),
        pytest.param(
            'SELECT * FROM Db.s1.t JOIN "Db".s2.u ON TRUE', id="two databases spelt alike"
        ),
    ],
)
def test_names_that_only_quoting_tells_apart_are_refused(sql: str) -> None:
    with pytest.raises(ValueError, match="would name two different relations"):
        convert(views("postgres", ("v", sql)), dialect="postgres", default_schema="public")


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT a.dbo.t1.id FROM a.dbo.t1 JOIN b.dbo.t1 ON 1 = 1", id="two exposures"),
        pytest.param(
            "SELECT dbo.t1.id FROM dbo.t1 CROSS JOIN (SELECT 1 AS id) AS t1x"
            " WHERE EXISTS (SELECT 1 FROM (SELECT 2 AS id) AS t1 WHERE dbo.t1.id = 1)",
            id="an exposure by a user alias",
        ),
    ],
)
def test_a_column_that_would_bind_elsewhere_is_refused(sql: str) -> None:
    with pytest.raises(ValueError, match="would bind to another table"):
        convert(views("tsql", ("dbo.v", sql)), dialect="tsql", default_schema="dbo")


CASES += [
    pytest.param(
        DUCKDB,
        [("main.v", "WITH RECURSIVE b AS (SELECT id + 1 AS id FROM b) SELECT * FROM b")],
        {
            "v__b": 'SELECT id + 1 AS id FROM {{ source("main", "b") }} AS b',
            "v": 'SELECT * FROM {{ ref("v__b") }} AS b',
        },
        [],
        id="a CTE that is not a union reads the table of its name under RECURSIVE",
    ),
]


@pytest.mark.parametrize("dialect", ["tsql", "oracle", "snowflake", "sqlite"])
def test_a_self_reference_is_recursion_where_no_keyword_is_needed(dialect: str) -> None:
    project = convert(
        views(
            dialect,
            ("v", "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r) SELECT n FROM r"),
        ),
        dialect=dialect,
        default_schema="s",
    )
    recursive, final = project.models
    assert recursive.name == "v__r"
    assert flat(recursive.sql) == flat(
        "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r) SELECT * FROM r"
    )
    assert (recursive.refs, recursive.sources) == ((), ())
    assert final.refs == ("v__r",)


def test_a_view_column_list_names_the_columns_of_its_model() -> None:
    query = sqlglot.parse_one("SELECT p, q FROM dbo.t", read="tsql")
    assert isinstance(query, exp.Query)
    columns = (exp.to_identifier("a"), exp.to_identifier("b"))
    view = View(exp.to_table("dbo.v", dialect="tsql"), query, columns)
    parents = [column.parent for column in columns]
    project = convert([view], dialect="tsql", default_schema="dbo")
    assert models(project) == {
        "v": flat('SELECT * FROM (SELECT p, q FROM {{ source("dbo", "t") }} AS t) AS v(a, b)')
    }
    assert [column.parent for column in view.columns] == parents


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
            [("dbo.v", "SELECT 1 AS x"), ("sales.V", "SELECT 2 AS x")],
            "two models would be named V",
            id="views of the same name in another case",
        ),
        pytest.param(
            [("dbo.a", "SELECT * FROM dbo.b"), ("dbo.b", "SELECT * FROM dbo.a")],
            "cycle",
            id="views reading one another",
        ),
        pytest.param([("dbo.a", "SELECT * FROM a")], "cycle", id="a view reading itself"),
        pytest.param(
            [
                (
                    "dbo.v",
                    "SELECT dbo.t1.id FROM dbo.t1 UNION SELECT id FROM dbo.t2 ORDER BY dbo.t1.id",
                )
            ],
            "would bind to another table",
            id="a schema-qualified column in the ORDER BY of a union",
        ),
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
        views("tsql", ("dbo.v", "SELECT x FROM dbo.t")), dialect="tsql", default_schema="dbo"
    )
    for result in (project, project.models[0], project.sources[0]):
        for item in dataclasses.fields(result):
            with pytest.raises(dataclasses.FrozenInstanceError):
                setattr(result, item.name, None)


def test_sql_that_the_dialect_cannot_print_is_an_error() -> None:
    query = sqlglot.parse_one("SELECT * FROM dbo.t PIVOT (SUM(a) FOR m IN ([1])) AS p", read="tsql")
    assert isinstance(query, exp.Query)
    with pytest.raises(UnsupportedError):
        convert([View(exp.to_table("v"), query)], dialect="sqlite", default_schema="main")


def test_the_target_of_select_into_is_kept() -> None:
    query = sqlglot.parse_one("SELECT a INTO dbo.archive FROM dbo.s", read="tsql")
    assert isinstance(query, exp.Query)
    project = convert([View(exp.to_table("v"), query)], dialect="tsql", default_schema="dbo")
    assert project.warnings == ("v: dbo.archive kept as written",)
    assert project.sources == (Source("", "dbo", "s"),)


def test_malformed_dialect_settings_are_a_value_error() -> None:
    with pytest.raises(ValueError, match="malformed dialect settings"):
        convert([], dialect="tsql, normalization_strategy", default_schema="dbo")
