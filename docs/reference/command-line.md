# Command line

`cte2dbt` reads view scripts and writes the dbt models they become. The
[Python API](python-api.md) does the conversion; this page describes the command.

## Synopsis

```text
cte2dbt [-h] -o OUTPUT [--dialect DIALECT] [--default-schema SCHEMA] INPUT [INPUT ...]
```

## Arguments

| Argument | Value and default | Meaning |
| --- | --- | --- |
| `INPUT` | One or more paths | A `.sql` file, or a directory searched recursively for `*.sql` files in sorted order. The same path given twice is read once. |
| `-o`, `--output` | Required directory | Where to write the models and `sources.yml`. It must not exist or must be empty; it is created with its parents. |
| `--dialect` | sqlglot dialect, `tsql` by default | The dialect of the scripts, with any sqlglot settings, such as `"tsql, normalization_strategy=case_sensitive"` for a case-sensitive collation. |
| `--default-schema` | Schema name, `dbo` by default | The schema of tables and views named without one. |
| `-h`, `--help` | No value | Show the usage and exit. |

## Inputs

A file that starts with a UTF-16 byte order mark is read as UTF-16, the encoding of the
"Unicode text" that SQL Server Management Studio saves. Any other file is read as UTF-8,
with or without a byte order mark; other encodings are an error.

A file may hold several statements. How each is treated is in
[the conversion rules](conversion-rules.md#statements).

## Output

```text
OUTPUT/VIEW.sql          the query of a view, without its WITH clause
OUTPUT/VIEW__CTE.sql     one common table expression of a view
OUTPUT/sources.yml       the tables that the views read but do not define
```

Files are UTF-8 with `\n` line endings. Model names follow
[the naming rules](conversion-rules.md#model-names).

Nothing is written until every input has been read and converted. An existing file is
never overwritten: the output directory must be empty. If writing itself fails, for
instance on a full disk, the directory may hold some of the files; remove it before
running again.

## Messages

Standard output receives one line when the files are written:

```text
cte2dbt: wrote 4 models and sources.yml to models/legacy
```

Standard error receives each warning and error. An error takes one line; a warning that
quotes a statement spans several lines where the quoted text does:

```text
cte2dbt: warning: FILE: skipped statement: STATEMENT
cte2dbt: warning: MODEL: ITEM kept as written
cte2dbt: warning: MODEL: renamed from VIEW
cte2dbt: error: MESSAGE
```

A statement in a warning is cut after 80 characters. An error from sqlglot keeps its
first line, which for a parse error gives the line and column in the file.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | The files are written. Warnings may have been printed. Also after `--help`. |
| 1 | An input could not be read, decoded or parsed; no input defines a view; the views could not be converted; or a file could not be written. Every unreadable input is reported before exiting. |
| 2 | A usage error: an unknown option, a missing `--output`, an unknown dialect or malformed dialect settings, or an output that exists and is a file or a directory that is not empty. |
