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
line is such an application: it keeps sqlglot's notices off standard error, as each of
them is also reported as a `cte2dbt:` warning or error.

## Data flow

1. `cli` lists the input files, decodes each, and passes its text to `read_script`.
2. `read_script` parses the text with sqlglot and returns the views it defines.
3. `convert` turns all the views into models at once, so that a view read by another one
   becomes a `ref()`, and orders the models.
4. `dbt_files` prints the models and `sources.yml` as text.
5. `cli` writes the files, then prints the warnings and a summary.

## Failures

| Failure | Exit code | Written |
| --- | --- | --- |
| Usage error, unknown dialect, output exists and is not empty | 2 | Nothing |
| An input cannot be read, decoded or parsed | 1 | Nothing; every such input is reported |
| No input defines a view | 1 | Nothing |
| Conversion error: name clash, cycle, reserved text, ambiguous column | 1 | Nothing |
| Writing a file fails | 1 | The files written before the failure |
| Success, with or without warnings | 0 | Every model and `sources.yml` |
