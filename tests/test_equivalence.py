"""Executed equivalence: the converted models return the rows of the original views.

Each script runs twice on DuckDB, in fresh in-memory databases: once as written, and once
as the converted models, rendered by jinja2 and created as views in dependency order.
T-SQL is transpiled to DuckDB on both sides, so it checks the conversion, not SQL Server.
"""

from typing import TYPE_CHECKING

import jinja2
import pytest
import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st

from cte2dbt import Project, Source, View, convert, read_script

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

duckdb = pytest.importorskip("duckdb")

TABLES = """
CREATE SCHEMA raw;
CREATE SCHEMA dbo;
CREATE TABLE raw.t1 AS SELECT * FROM (VALUES (1, 10), (2, 20), (2, 20), (3, NULL)) AS t(id, x);
CREATE TABLE raw.t2 AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (4, NULL)) AS t(id, y);
CREATE TABLE raw.sales AS
    SELECT * FROM (VALUES (1, 'JAN', 5), (1, 'FEB', 7), (2, 'JAN', 3)) AS t(id, month, amount);
CREATE TABLE main.orders AS SELECT * FROM (VALUES (1, 5), (2, 15), (3, 25)) AS t(id, amount);
CREATE TABLE dbo.t1 AS SELECT * FROM raw.t1;
"""

JINJA = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=False)

Rows = list[tuple[object, ...]]


def connect() -> "DuckDBPyConnection":
    connection: DuckDBPyConnection = duckdb.connect()
    connection.execute(TABLES)
    return connection


def rows(connection: "DuckDBPyConnection", relation: str) -> Rows:
    return sorted(connection.execute(f"SELECT * FROM {relation}").fetchall(), key=repr)


def original_rows(script: str, dialect: str, last: View) -> Rows:
    connection = connect()
    for statement in sqlglot.transpile(script, read=dialect, write="duckdb"):
        connection.execute(statement)
    return rows(connection, last.name.sql(dialect="duckdb"))


def converted_rows(project: Project, dialect: str, last: View) -> Rows:
    """Create each model as a view in schema m, checking that its Jinja calls match it."""
    connection = connect()
    connection.execute("CREATE SCHEMA m")
    sources = {(source.source_name, source.table): source for source in project.sources}
    for model in project.models:
        refs: set[str] = set()
        read: set[Source] = set()

        def ref(name: str, refs: set[str] = refs) -> str:
            refs.add(name)
            return f'"m"."{name}"'

        def source(name: str, table: str, read: set[Source] = read) -> str:
            found = sources[name, table]
            read.add(found)
            return f'"{found.schema}"."{found.table}"'

        sql = JINJA.from_string(model.sql).render(ref=ref, source=source)
        assert refs == set(model.refs)
        assert read == set(model.sources)
        [body] = sqlglot.transpile(sql, read=dialect, write="duckdb")
        connection.execute(f'CREATE VIEW "m"."{model.name}" AS {body}')
    return rows(connection, f'"m"."{last.name.name}"')


def check(script: str, dialect: str, default_schema: str) -> Project:
    """Convert a script and require the rows of its last view, unchanged input and stability."""
    views = read_script(script, dialect=dialect).views
    before = [view.query.sql(dialect=dialect) for view in views]
    project = convert(views, dialect=dialect, default_schema=default_schema)
    assert [view.query.sql(dialect=dialect) for view in views] == before
    assert convert(views, dialect=dialect, default_schema=default_schema) == project
    assert converted_rows(project, dialect, views[-1]) == original_rows(script, dialect, views[-1])
    return project


DUCKDB_VIEWS = {
    "a sample": "SELECT COUNT(*) AS n FROM raw.t1 TABLESAMPLE 2 ROWS",
    "a pivot on a base table": (
        "SELECT * FROM raw.sales PIVOT (SUM(amount) FOR month IN ('JAN', 'FEB') GROUP BY id)"
    ),
    "a pivot on a CTE": (
        "WITH s AS (SELECT * FROM raw.sales)"
        " SELECT * FROM s PIVOT (SUM(amount) FOR month IN ('JAN', 'FEB') GROUP BY id)"
    ),
    "a parenthesised join": "SELECT * FROM (raw.t1 JOIN raw.t2 ON t1.id = t2.id)",
    "an alias column list": "SELECT a, b FROM raw.t1 AS x(a, b)",
    "a schema-qualified column": "SELECT raw.t1.x FROM raw.t1",
    "a CTE column list": "WITH c(a, b) AS (SELECT id, x FROM raw.t1) SELECT a, b FROM c",
    "a CTE name in another case": "WITH Foo AS (SELECT 1 AS a) SELECT * FROM foo",
    "a nested WITH shadowing a CTE": (
        "WITH a AS (SELECT 1 AS v)"
        " SELECT * FROM (WITH a AS (SELECT 2 AS v) SELECT * FROM a) AS s CROSS JOIN a"
    ),
    "a recursive CTE": (
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5)"
        " SELECT n FROM r"
    ),
    "a CTE shadowing its base table": (
        "WITH orders AS (SELECT * FROM orders WHERE amount > 10) SELECT COUNT(*) AS n FROM orders"
    ),
    "EXISTS over a union": (
        "WITH c AS (SELECT 1 AS id UNION ALL SELECT 3)"
        " SELECT x FROM raw.t1 WHERE EXISTS (SELECT 1 FROM c WHERE c.id = t1.id)"
    ),
    "text that would open Jinja": "SELECT '{{ x }}' AS a, '{%' AS b, '{#' AS c, x FROM raw.t1",
    "a chain of CTEs": (
        "WITH a AS (SELECT id, x FROM raw.t1), b AS (SELECT id, x * 2 AS y FROM a),"
        " c AS (SELECT a.id, b.y FROM a JOIN b ON a.id = b.id) SELECT * FROM c"
    ),
    "a self-join": "SELECT l.id, r.y FROM raw.t1 AS l JOIN raw.t2 AS r ON l.id = r.id",
}


@pytest.mark.parametrize("sql", DUCKDB_VIEWS.values(), ids=DUCKDB_VIEWS.keys())
def test_duckdb_views(sql: str) -> None:
    check(f"CREATE VIEW main.v AS {sql}", "duckdb", "main")


def test_a_view_column_list() -> None:
    check("CREATE VIEW main.v (p, q) AS SELECT id, x FROM raw.t1", "duckdb", "main")


def test_views_reading_one_another() -> None:
    project = check(
        "CREATE VIEW main.v1 AS WITH a AS (SELECT id, x FROM raw.t1) SELECT * FROM a;"
        " CREATE VIEW main.v2 AS SELECT v1.id FROM main.v1 WHERE x > 10;"
        " CREATE VIEW main.v3 AS SELECT * FROM main.v2",
        "duckdb",
        "main",
    )
    assert [model.name for model in project.models] == ["v1__a", "v1", "v2", "v3"]


TSQL_VIEWS = {
    "a bracketed CTE name in another case": (
        "WITH [Recent] AS (SELECT id FROM dbo.t1) SELECT r.id FROM [recent] AS r"
    ),
    "schema-qualified columns": "SELECT dbo.t1.id, dbo.t1.x FROM dbo.t1 WHERE dbo.t1.id < 3",
    "a table named without a schema": "SELECT id FROM t1",
    "TOP and ORDER BY in a CTE": (
        "WITH top2 AS (SELECT TOP 2 id, x FROM dbo.t1 ORDER BY id DESC) SELECT * FROM top2"
    ),
    "a CTE column list": "WITH c (a, b) AS (SELECT id, x FROM dbo.t1) SELECT a, b FROM c",
}


@pytest.mark.parametrize("sql", TSQL_VIEWS.values(), ids=TSQL_VIEWS.keys())
def test_t_sql_views_transpiled(sql: str) -> None:
    # Transpiled, an unqualified name must find the default schema, as SQL Server would.
    check(f"SET search_path = 'dbo'; CREATE VIEW dbo.v AS {sql}", "tsql", "dbo")


@st.composite
def chains(draw: st.DrawFn) -> tuple[str, list[int | None]]:
    """Draw a view over a chain of CTEs, each reading raw.t1 or an earlier CTE (its edge)."""
    count = draw(st.integers(1, 5))
    edges: list[int | None] = []
    ctes = []
    for i in range(count):
        edge = draw(st.sampled_from([None, *range(i)]))
        edges.append(edge)
        read = "raw.t1" if edge is None else draw(st.sampled_from([f"c{edge}", f"C{edge}"]))
        alias = draw(st.sampled_from(["", " AS s"]))
        prefix = "s." if alias else ""
        renamed = draw(st.booleans())
        columns = " (id, x)" if renamed else ""
        projection = f"{prefix}id AS p, {prefix}x AS q" if renamed else f"{prefix}id, {prefix}x"
        modulus = draw(st.integers(2, 3))
        where = draw(st.sampled_from(["", f" WHERE {prefix}x % {modulus} = 0"]))
        ctes.append(f"c{i}{columns} AS (SELECT {projection} FROM {read}{alias}{where})")
    last = draw(st.integers(0, count - 1))
    edges.append(last)
    return f"CREATE VIEW main.v AS WITH {', '.join(ctes)} SELECT id, x FROM c{last}", edges


@settings(max_examples=50, deadline=None)
@given(chains())
def test_random_chains_of_ctes(chain: tuple[str, list[int | None]]) -> None:
    sql, edges = chain
    project = check(sql, "duckdb", "main")
    names = [f"v__c{i}" for i in range(len(edges) - 1)] + ["v"]
    assert {model.name: model.refs for model in project.models} == {
        name: () if edge is None else (f"v__c{edge}",)
        for name, edge in zip(names, edges, strict=True)
    }
    order = [model.name for model in project.models]
    for model in project.models:
        assert all(order.index(ref) < order.index(model.name) for ref in model.refs)
