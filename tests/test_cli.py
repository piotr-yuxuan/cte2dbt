"""The command line: golden directories, the output rule, encodings, errors and exit codes."""

import re
from collections.abc import Callable
from pathlib import Path

import pytest

from cte2dbt.cli import main

RESOURCES = Path(__file__).parent / "test_resources"
DUCKDB = ["--dialect", "duckdb", "--default-schema", "main"]
CASES = {
    "dependant_cte": (DUCKDB, []),
    "nested_subqueries": (DUCKDB, []),
    "no_cross_cte_use": (DUCKDB, []),
    "ssms_export": (
        [],
        ["cte2dbt: warning: input/dbo.vOrders.View.sql: skipped statement: EXEC"],
    ),
}


def flat(sql: str) -> str:
    """Ignore layout: collapse whitespace, and drop it around parentheses and commas."""
    return re.sub(r"\s*([(),])\s*", r"\1", " ".join(sql.split()))


@pytest.mark.parametrize("case", sorted(CASES))
def test_golden_directories(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    options, warnings = CASES[case]
    expected = RESOURCES / case / "expected"
    output = tmp_path / "models"
    monkeypatch.chdir(RESOURCES / case)
    assert main(["input", "-o", str(output), *options]) == 0
    assert sorted(path.name for path in output.iterdir()) == sorted(
        path.name for path in expected.iterdir()
    )
    for path in expected.glob("*.sql"):
        written = (output / path.name).read_text(encoding="utf-8")
        assert flat(written) == flat(path.read_text(encoding="utf-8")), path.name
    assert (output / "sources.yml").read_bytes() == (expected / "sources.yml").read_bytes()
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == len(warnings)
    assert all(line.startswith(prefix) for line, prefix in zip(lines, warnings, strict=True))


def write(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


VIEW = "CREATE VIEW dbo.v AS SELECT x FROM dbo.t"


@pytest.mark.parametrize("encoding", ["utf-16", "utf-8-sig", "utf-8"])
def test_exports_in_unicode_encodings_are_read(tmp_path: Path, encoding: str) -> None:
    script = write(tmp_path / "v.sql", VIEW.encode(encoding))
    assert main([str(script), "-o", str(tmp_path / "out")]) == 0
    assert (tmp_path / "out/v.sql").exists()


def test_a_directory_is_searched_for_sql_files_once_each(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write(tmp_path / "in/a/v.sql", VIEW.encode())
    write(tmp_path / "in/notes.txt", b"not SQL")
    out = tmp_path / "out"
    assert main([str(tmp_path / "in"), str(tmp_path / "in/a/v.sql"), "-o", str(out)]) == 0
    assert sorted(path.name for path in out.iterdir()) == ["sources.yml", "v.sql"]
    assert capsys.readouterr().out == f"cte2dbt: wrote 1 model and sources.yml to {out}\n"


def test_kept_items_are_reported_on_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    script = write(tmp_path / "v.sql", b"CREATE VIEW dbo.v AS SELECT * FROM dbo.fn(1) AS f")
    assert main([str(script), "-o", str(tmp_path / "out")]) == 0
    assert capsys.readouterr().err == "cte2dbt: warning: v: dbo.fn(1) AS f kept as written\n"


def test_the_dialect_and_default_schema_options(tmp_path: Path) -> None:
    script = write(
        tmp_path / "v.sql",
        b"CREATE VIEW v AS WITH a AS (SELECT 1 AS x) SELECT x FROM A CROSS JOIN t",
    )
    out = tmp_path / "out"
    options = ["--dialect", "tsql, normalization_strategy=case_sensitive"]
    assert main([str(script), "-o", str(out), *options, "--default-schema", "sales"]) == 0
    written = flat((out / "v.sql").read_text(encoding="utf-8"))
    assert written == flat(
        'SELECT x FROM {{ source("sales", "A") }} AS A CROSS JOIN {{ source("sales", "t") }} AS t'
    )


@pytest.mark.parametrize(
    "output",
    [
        pytest.param(
            lambda path: write(path / "out/existing.sql", b""), id="a non-empty directory"
        ),
        pytest.param(lambda path: write(path / "out", b""), id="a file"),
    ],
)
def test_an_existing_output_is_a_usage_error_and_untouched(
    tmp_path: Path, output: Callable[[Path], Path]
) -> None:
    script = write(tmp_path / "v.sql", VIEW.encode())
    existing = output(tmp_path)
    with pytest.raises(SystemExit) as raised:
        main([str(script), "-o", str(tmp_path / "out")])
    assert raised.value.code == 2
    assert existing.read_bytes() == b""


@pytest.mark.parametrize(
    "arguments",
    [
        pytest.param(["v.sql"], id="no output"),
        pytest.param(["v.sql", "-o", "out", "--dialect", "nosuch"], id="an unknown dialect"),
    ],
)
def test_usage_errors_exit_with_status_2(arguments: list[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(arguments)
    assert raised.value.code == 2


@pytest.mark.parametrize(
    ("scripts", "options", "messages"),
    [
        pytest.param({}, [], ["missing.sql", "other.sql"], id="every unreadable file is reported"),
        pytest.param(
            {"v.sql": b"CREATE VIEW v AS\nSELECT (1"}, [], ["v.sql", "Line 2"], id="invalid SQL"
        ),
        pytest.param(
            {"v.sql": b"CREATE VIEW v AS SELECT 'caf\xe9' AS x"},
            [],
            ["v.sql", "utf-8"],
            id="bytes that are not Unicode",
        ),
        pytest.param(
            {"a.sql": VIEW.encode(), "b.sql": VIEW.encode()},
            [],
            ["two models would be named v"],
            id="two views of the same name",
        ),
        pytest.param({"v.sql": b"SELECT 1 AS x"}, [], ["no CREATE VIEW"], id="no view"),
        pytest.param(
            {"v.sql": b"CREATE VIEW v AS SELECT x FROM t"},
            ["--default-schema", ""],
            ["t has no schema"],
            id="an empty default schema",
        ),
    ],
)
def test_errors_exit_with_status_1_and_write_nothing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    scripts: dict[str, bytes],
    options: list[str],
    messages: list[str],
) -> None:
    paths = [str(write(tmp_path / name, data)) for name, data in scripts.items()] or [
        str(tmp_path / "missing.sql"),
        str(tmp_path / "other.sql"),
    ]
    out = tmp_path / "out"
    assert main([*paths, "-o", str(out), *options]) == 1
    errors = capsys.readouterr().err
    assert all(message in errors for message in messages)
    assert all(line.startswith("cte2dbt: error: ") for line in errors.splitlines())
    assert not out.exists()
