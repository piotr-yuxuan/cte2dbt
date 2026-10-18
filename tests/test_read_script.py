"""Reading exported scripts: which statements become views, which are skipped, which fail."""

import pytest
from sqlglot.errors import ParseError, SqlglotError

from cte2dbt import convert, read_script

SSMS_EXPORT = (
    "USE [Sales]\r\n"
    "GO\r\n"
    "/****** Object:  View [dbo].[v]    Script Date: 16/10/2026 ******/\r\n"
    "SET ANSI_NULLS ON\r\n"
    "go 2\r\n"
    "SET QUOTED_IDENTIFIER ON\r\n"
    "  GO;  \r\n"
    "CREATE VIEW [dbo].[v] AS\r\n"
    "WITH a AS (SELECT id FROM dbo.Sales) SELECT * FROM a\r\n"
    "GO -- done\r\n"
    "EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPaneCount', @value=1,"
    " @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=N'v'\r\n"
    "GO\r\n"
    "CREATE OR ALTER VIEW dbo.w AS SELECT 1 AS x\r\n"
)


def test_an_ssms_export_gives_its_views_in_order() -> None:
    script = read_script(SSMS_EXPORT, dialect="tsql")
    assert [(view.name.db, view.name.name) for view in script.views] == [("dbo", "v"), ("dbo", "w")]
    assert len(script.warnings) == 1
    assert script.warnings[0].startswith("skipped statement: EXEC")
    project = convert(script.views, dialect="tsql", default_schema="dbo")
    assert {model.name: model.refs for model in project.models} == {
        "v__a": (),
        "v": ("v__a",),
        "w": (),
    }


@pytest.mark.parametrize(
    ("text", "skipped"),
    [
        pytest.param("SELECT 1 AS x", ["SELECT 1 AS x"], id="a bare query"),
        pytest.param(
            ";WITH c AS (SELECT 1 AS a) SELECT a FROM c",
            ["WITH c AS (SELECT 1 AS a) SELECT a FROM c"],
            id="a query led by a semicolon",
        ),
        pytest.param(
            "SET ANSI_NULLS OFF",
            ["SET ANSI_NULLS OFF; the views after it are built with this option ON"],
            id="a SET option off",
        ),
        pytest.param(
            "IF EXISTS (SELECT * FROM sys.views WHERE name = 'v') DROP VIEW dbo.v",
            # Cut after 80 characters.
            ["IF EXISTS(SELECT * FROM sys.views WHERE name = 'v') BEGIN DROP VIEW dbo.v"],
            id="a guarded drop",
        ),
        pytest.param(
            "GRANT SELECT ON dbo.v TO excel_readers",
            ["GRANT SELECT ON dbo.v TO excel_readers"],
            id="a grant",
        ),
        pytest.param("PRINT 'hello'", ["PRINT 'hello'"], id="a statement kept as text"),
    ],
)
def test_other_statements_are_skipped_with_a_warning(text: str, skipped: list[str]) -> None:
    script = read_script(text, dialect="tsql")
    assert script.views == ()
    assert list(script.warnings) == [f"skipped statement: {sql}" for sql in skipped]


@pytest.mark.parametrize(
    ("text", "message"),
    [
        pytest.param(
            'SET QUOTED_IDENTIFIER OFF\nGO\nCREATE VIEW v AS SELECT "a" AS x',
            "QUOTED_IDENTIFIER OFF",
            id="double quotes delimit strings",
        ),
        pytest.param(
            "CREATE VIEW v AS SELECT 1 AS x WITH CHECK OPTION",
            "defines a view that cannot be read",
            id="a view sqlglot cannot parse",
        ),
        pytest.param(
            "IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'v')\n"
            "EXEC dbo.sp_executesql @statement = N'CREATE VIEW dbo.v AS SELECT 1 AS x'\nGO",
            "defines a view that cannot be read",
            id="a view created by dynamic SQL",
        ),
        pytest.param(
            "SET ANSI_NULLS ON\nGO\nCREATE VIEW v AS\nSELECT (1",
            "Line 4",
            id="invalid SQL, with the line number of the file",
        ),
    ],
)
def test_statements_that_cannot_be_read_faithfully_fail(text: str, message: str) -> None:
    with pytest.raises(ParseError, match=message):
        read_script(text, dialect="tsql")


def test_a_view_column_list_is_read() -> None:
    text = "CREATE VIEW [dbo].[v] (a, b) AS SELECT p, q FROM dbo.t"
    [view] = read_script(text, dialect="tsql").views
    assert [column.name for column in view.columns] == ["a", "b"]


def test_a_column_list_with_comments_keeps_the_names() -> None:
    [view] = read_script(
        "CREATE VIEW v (a COMMENT 'first', b) AS SELECT p, q FROM t", dialect="snowflake"
    ).views
    assert [column.sql(dialect="snowflake") for column in view.columns] == ["a", "b"]


def test_go_is_a_batch_separator_in_t_sql_only() -> None:
    [view] = read_script("CREATE VIEW v AS SELECT\ngo\nFROM t", dialect="duckdb").views
    assert view.query.sql(dialect="duckdb") == "SELECT go FROM t"


def test_an_unknown_dialect_is_an_error() -> None:
    with pytest.raises(ValueError, match="nosuch"):
        read_script("SELECT 1", dialect="nosuch")


def test_a_go_line_ends_every_statement_before_it() -> None:
    script = read_script(
        "USE [Sales]\nGO\n"
        "IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[v1]'))\n"
        "DROP VIEW [dbo].[v1]\nGO\n"
        "SET ANSI_NULLS ON\nGO\n"
        "CREATE VIEW [dbo].[v1] AS SELECT id FROM dbo.Sales\nGO\n"
        "IF OBJECT_ID('dbo.v2', 'V') IS NOT NULL DROP VIEW dbo.v2;\nGO\n"
        "CREATE VIEW dbo.v2 AS SELECT id FROM dbo.v1\nGO\n",
        dialect="tsql",
    )
    assert [view.name.name for view in script.views] == ["v1", "v2"]
    assert [warning.split("(")[0] for warning in script.warnings] == [
        "skipped statement: IF EXISTS",
        "skipped statement: IF NOT OBJECT_ID",
    ]


def test_a_go_line_inside_a_string_ends_the_batch_as_sqlcmd_would() -> None:
    with pytest.raises(SqlglotError):
        read_script("CREATE VIEW dbo.a AS SELECT 'x\nGO\ny' AS x", dialect="tsql")


def test_a_view_whose_body_is_not_a_query_fails() -> None:
    with pytest.raises(ParseError, match="defines a view that cannot be read"):
        read_script("CREATE VIEW main.v AS VALUES (1), (2)", dialect="duckdb")
