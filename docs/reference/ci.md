# Continuous integration

`.github/workflows/ci.yml` runs on pull requests, on pushes to `main`, on demand, and on
two schedules. Every job runs on `ubuntu-24.04` with a time limit, reads the repository
with `contents: read` and without persisted credentials, and uses actions pinned to a
full commit hash.

| Job | When | Evidence |
| --- | --- | --- |
| `checks` | Not on schedules | `check.py verify`; zizmor on the workflows and the Dependabot configuration, and actionlint on the workflows; the tool catalogue is valid Compose; gitleaks over the whole history. Reports are kept for 30 days. |
| `checks` | Daily, 03:17 UTC | `check.py security`, for advisories published since the last change. |
| `checks` | Weekly, Monday 08:41 UTC | `check.py docs --external-links`. |
| `compatibility` | Not on the weekly schedule | The tests on Python 3.11 and 3.15 against the oldest and the newest allowed sqlglot, and on 3.12 and 3.13 against the newest. The lock is re-resolved in this job only. |
| `dependency-review` | Pull requests | A pull request that adds a dependency with a known advisory fails. |
| `codeql` | Not on schedules | CodeQL for Python and for the workflows. The only job allowed to write security events. |

On Python 3.15 DuckDB has no wheel yet, so the execution tests skip there; `checks` runs
them on the Python of `.python-version` and fails on any skipped test.

## Publishing

`.github/workflows/python-publish.yml` runs when a GitHub release is published:

1. **Check and build** fails unless the release tag equals the project version given by
   `uv version --short`, such as `2.0.0` without a `v`. It runs `check.py verify`, which
   builds the distributions into `dev/reports/dist`, and keeps them as an artefact.
2. **Publish to PyPI** uploads those same files through PyPI's Trusted Publishing, with
   attestations of their origin. It alone receives an `id-token: write` permission, and
   it runs in the `pypi` environment, where GitHub can require an approval.

PyPI trusts this file name and the `pypi` environment: renaming either stops publishing
until the Trusted Publisher on PyPI is updated. One release is published at a time.

## Pinned tools

`dev/resources/ci/compose.yaml` lists actionlint and gitleaks as images pinned by digest.
`dev/scripts/tool.sh` runs one of them with the repository mounted read-only, no
capabilities, and no access to the container engine. Dependabot proposes updates to the
uv dependencies, the actions and these images every Monday, for releases at least seven
days old; nothing is merged automatically.

`tool.sh` uses `docker` unless `CTE2DBT_CONTAINER_ENGINE` names another engine, such as
`podman`. [The development procedure](../how-to/develop.md#run-the-ci-jobs-locally) shows
how to run each job locally.

## Repository settings

`.github/CODEOWNERS` names the owner of the workflows, the development scripts and the
dependency files; only a repository ruleset that requires code-owner review makes it a
gate.
