"""Reading exported scripts: which statements become views, which are skipped, which fail."""

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
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
        pytest.param("SET NOCOUNT OFF", ["SET NOCOUNT OFF"], id="an option off by default"),
        pytest.param("SET DATEFORMAT dmy", ["SET DATEFORMAT dmy"], id="an option without ON"),
        pytest.param(
            "GRANT CREATE VIEW TO reporting",
            ["GRANT CREATE VIEW TO reporting"],
            id="a grant of the permission to create views",
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
            "CREATE OR ALTER VIEW dbo.v WITH SCHEMABINDING, VIEW_METADATA AS SELECT 1 AS x",
            "defines a view that cannot be read",
            id="a view with options sqlglot keeps as text",
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
    assert len(script.warnings) == 2
    assert all(warning.startswith("skipped statement: IF") for warning in script.warnings)


def test_a_go_line_inside_a_string_ends_the_batch_as_sqlcmd_would() -> None:
    with pytest.raises(SqlglotError):
        read_script("CREATE VIEW dbo.a AS SELECT 'x\nGO\ny' AS x", dialect="tsql")


def test_a_view_whose_body_is_not_a_query_fails() -> None:
    with pytest.raises(ParseError, match="defines a view that cannot be read"):
        read_script("CREATE VIEW main.v AS VALUES (1), (2)", dialect="duckdb")


def test_a_guarded_drop_is_skipped() -> None:
    script = read_script(
        "-- Replaces CREATE VIEW dbo.old\n"
        "IF EXISTS (SELECT * FROM sys.views WHERE name = 'v') DROP VIEW dbo.v",
        dialect="tsql",
    )
    assert script.views == ()
    [warning] = script.warnings
    assert warning.startswith("skipped statement: IF EXISTS")


@pytest.mark.parametrize(
    ("text", "dialect"),
    [
        pytest.param(
            'CREATE OR REPLACE FORCE EDITIONABLE VIEW "S"."V" ("A") AS SELECT 1 A FROM dual',
            "oracle",
            id="an Oracle view with modifiers",
        ),
        pytest.param(
            "CREATE MATERIALIZED VIEW public.mv AS SELECT 1 AS x WITH NO DATA",
            "postgres",
            id="a materialised view",
        ),
        pytest.param(
            "CREATE RECURSIVE VIEW v (n) AS SELECT 1 UNION ALL SELECT n + 1 FROM v",
            "postgres",
            id="a recursive view",
        ),
        pytest.param(
            "CREATE OR REPLACE NOFORCE VIEW s.v AS SELECT 1 AS a FROM dual",
            "oracle",
            id="an Oracle NOFORCE view",
        ),
        pytest.param(
            "CREATE/*x*/VIEW dbo.v AS SELECT 1 AS a WITH CHECK OPTION",
            "tsql",
            id="a comment between CREATE and VIEW",
        ),
        pytest.param(
            "CREATE OR REFRESH MATERIALIZED VIEW v SCHEDULE CRON '0 0 * * * ?' AS SELECT 1 AS a",
            "databricks",
            id="a Databricks refreshed view",
        ),
        pytest.param("CREATE LIVE VIEW lv AS SELECT 1 AS a", "clickhouse", id="a live view"),
        pytest.param(
            "CREATE JSON RELATIONAL DUALITY VIEW dv AS SELECT d.id FROM dept d",
            "oracle",
            id="an Oracle duality view",
        ),
        pytest.param(
            "CREATE WINDOW VIEW wv AS"
            " SELECT count(id) FROM t GROUP BY tumble(ts, INTERVAL '10' SECOND)",
            "clickhouse",
            id="a window view",
        ),
    ],
)
def test_views_with_modifiers_sqlglot_keeps_as_text_fail(text: str, dialect: str) -> None:
    with pytest.raises(ParseError, match="defines a view that cannot be read"):
        read_script(text, dialect=dialect)


def test_only_options_that_dbt_switches_on_are_flagged() -> None:
    script = read_script("SET ANSI_WARNINGS OFF\nGO\nSET NUMERIC_ROUNDABORT OFF", dialect="tsql")
    assert list(script.warnings) == [
        (
            "skipped statement: SET ANSI_WARNINGS OFF;"
            " the views after it are built with this option ON"
        ),
        "skipped statement: SET NUMERIC_ROUNDABORT OFF",
    ]


def test_each_batch_is_parsed_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    # The text before a batch is not parsed again with it, so the time stays linear.
    batch = "CREATE VIEW dbo.v{} AS SELECT {} AS x\n"
    text = "GO\n".join(batch.format(i, i) for i in range(50))
    parsed: list[str] = []
    parse = sqlglot.parse

    def spy(sql: str, read: Dialect) -> list[exp.Expr | None]:
        parsed.append(sql)
        return parse(sql, read=read)

    monkeypatch.setattr(sqlglot, "parse", spy)
    assert len(read_script(text, dialect="tsql").views) == 50
    # A batch keeps the newline that ends its GO line; the text before it would be longer.
    assert max(len(sql) for sql in parsed) < 2 * len(batch.format(49, 49))


@pytest.mark.parametrize(
    ("text", "dialect"),
    [
        pytest.param(
            "CREATE VIEW dbo.v AS SELECT 1 AS a\nGO\n"
            "EXEC sys.sp_addextendedproperty @name=N'MS_Description',"
            " @value=N'To change it, create a new view and retire this one.'",
            "tsql",
            id="prose about views in a description",
        ),
        pytest.param(
            "CREATE VIEW dbo.v AS SELECT 1 AS a\nGO\n-- Replaces CREATE VIEW dbo.old\nEXEC sp_who",
            "tsql",
            id="a comment about a view",
        ),
        pytest.param(
            "CREATE VIEW dbo.v AS SELECT 1 AS a\nGO\nGRANT CREATE VIEW ON SCHEMA dbo TO bob",
            "tsql",
            id="a parsed grant of the permission",
        ),
        pytest.param(
            "CREATE VIEW dbo.v AS SELECT 1 AS a\nGO\n"
            "CREATE TRIGGER dbo.trg ON dbo.t AFTER INSERT AS BEGIN\n"
            " -- CREATE VIEW dbo.x\n SELECT 1 END",
            "tsql",
            id="a comment inside a trigger kept as text",
        ),
        pytest.param(
            "CREATE VIEW dbo.v AS SELECT 1 AS a\nGO\n--/*\nGRANT SELECT ON dbo.v TO app\n--*/\n",
            "tsql",
            id="a block comment opened inside a line comment",
        ),
        pytest.param(
            "CREATE VIEW v AS SELECT 1 AS a; REVOKE CREATE VIEW ON SCHEMA db.s FROM ROLE r",
            "snowflake",
            id="a revoke of the permission",
        ),
    ],
)
def test_statements_that_mention_views_without_defining_one_are_skipped(
    text: str, dialect: str
) -> None:
    script = read_script(text, dialect=dialect)
    assert len(script.views) == 1
    [warning] = script.warnings
    assert warning.startswith("skipped statement: ")


def test_malformed_dialect_settings_are_a_value_error() -> None:
    with pytest.raises(ValueError, match="malformed dialect settings"):
        read_script("SELECT 1", dialect="tsql, normalization_strategy")
