# Architecture

cte2dbt has two modules. The conversion decides everything and touches nothing; the
command line touches files and decides nothing about SQL.

```text
cte2dbt.cli    arguments, files, encodings, messages, exit codes        all I/O
     |
cte2dbt.core   read_script  ->  convert  ->  dbt_files                  pure
     |
sqlglot        parsing, comparing names, printing SQL
```

An [Import Linter](https://import-linter.readthedocs.io/) contract in `pyproject.toml`
keeps `cte2dbt.core` from importing `cte2dbt.cli`. The core reads no file, prints and logs
nothing, and returns frozen results, so every rule is tested by calling a function. The
library configures no logging, so applications keep control of their own. The command
line is such an application: it keeps sqlglot's notices off standard error. Each
statement that sqlglot can only keep as text is reported as a skipped statement or an
error; its other notices, such as an invalid JSON path, are dropped.

## Data flow

1. `cli` lists the input files, decodes each, and passes its text to `read_script`.
2. `read_script` parses the text with sqlglot and returns the views it defines.
3. `convert` turns all the views into models at once, so that a view read by another one
   becomes a `ref()`, and orders the models.
4. `dbt_files` prints the models and `sources.yml` as text.
5. `cli` writes the files, then prints the warnings and a summary.

## Failures

Every input is read and every view converted in memory before the first file is written,
so an error in a script or in the conversion leaves nothing behind. Only a failure while
writing, such as a full disk, can leave some files. The
[command-line reference](../reference/command-line.md#exit-codes) gives the exit codes.
