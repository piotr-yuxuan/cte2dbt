# Continuous integration

`.github/workflows/ci.yml` runs on pull requests, on pushes to `main`, on demand, and on
two schedules. Every job runs on `ubuntu-24.04` with a time limit, reads the repository
with `contents: read` and without persisted credentials, and uses actions pinned to a
full commit hash.

| Job | When | Evidence |
| --- | --- | --- |
| `checks` | Not on schedules | `check.py verify`; zizmor and actionlint on the workflows; the tool catalogue is valid Compose; gitleaks over the whole history. Reports are kept for 30 days. |
| `checks` | Daily, 03:17 UTC | `check.py security`, for advisories published since the last change. |
| `checks` | Weekly, Monday 08:41 UTC | `check.py docs --external-links`. |
| `compatibility` | Not on the weekly schedule | The tests on Python 3.11 and 3.15 against the oldest and the newest allowed sqlglot, and on 3.12 and 3.13 against the newest. The lock is re-resolved in this job only. |
| `dependency-review` | Pull requests | A pull request that adds a dependency with a known advisory fails. |
| `codeql` | Not on schedules | CodeQL for Python and for the workflows. The only job allowed to write security events. |

On Python 3.15 DuckDB has no wheel yet, so the execution tests skip there; `checks` runs
them on the Python of `.python-version` and fails on any skipped test.

## Pinned tools

`dev/resources/ci/compose.yaml` lists actionlint and gitleaks as images pinned by digest.
`dev/scripts/tool.sh` runs one of them with the repository mounted read-only, no
capabilities, and no access to the container engine. Dependabot proposes updates to the
uv dependencies, the actions and these images every Monday; nothing is merged
automatically.

## Run a job locally

```sh
uv run --locked python dev/scripts/check.py verify
uv run --locked zizmor --offline .github/workflows
bash dev/scripts/tool.sh actionlint -color .github/workflows/*.yml
bash dev/scripts/tool.sh gitleaks git /repo --redact
```

`tool.sh` uses `docker` unless `CTE2DBT_CONTAINER_ENGINE` names another engine, such as
`podman`. For one compatibility leg, in a scratch copy of the repository, so that the
re-resolved lock is not committed:

```sh
UV_PYTHON=3.11 uv sync --resolution lowest-direct --upgrade --no-default-groups --group test
UV_PYTHON=3.11 uv run --no-sync python -m pytest
```

## Repository settings

`.github/CODEOWNERS` names the owner of the workflows, the development scripts and the
dependency files; only a repository ruleset that requires code-owner review makes it a
gate.
