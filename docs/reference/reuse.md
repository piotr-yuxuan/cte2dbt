# Reuse and attribution

Files adapted from the owner's other repositories, and what changed.

## From piotr-yuxuan/next-word-list

Adapted from commit `b6fe2bee60071aa706002bfb3acec5054e4f8876`:

- `dev/templates/plan.md`: drops learning mode and the live-model evidence.
- `AGENTS.md`: keeps the definitions, the start, the constraints and the review rules;
  points at the `principles` skills instead of vendored guides; drops learning
  ownership; adds the pure-core rule and the writing rules.
- `dev/scripts/check.py`: keeps the fail-closed test evidence and the coverage floor;
  runs mypy instead of BasedPyright; runs the documentation check as a separate process,
  so that the script needs no third-party package; adds the package step; is rewritten to
  pass Ruff's full rule set, with docstrings, named constants and messages assigned before
  `raise`. Parsing the XML that the same run's pytest and coverage write is accepted
  through a commented per-file ignore of rule S314.
- `dev/scripts/check_docs.py`: keeps the local link check and the Sybil loop; adds the
  README rules for PyPI, the writing rules and the external link check.
- `tests/test_check.py`: typed for mypy `--strict`.
- `docs/how-to/develop.md` and `docs/reference/checks.md`: describe this repository's
  procedure and checks.

## From piotr-yuxuan/python-playground

Adapted from commit `2569c51d5555441f1c389655b568846fb807d7a4`:

- `dev/scripts/check.py`: the `--external-links` option.
- `.github/workflows/ci.yml`: keeps the `checks`, `dependency-review` and `codeql` jobs
  and the schedules; drops Node, the service stack and the image audit; adds the
  `compatibility` matrix.
- `dev/resources/ci/compose.yaml`: keeps the actionlint and gitleaks images and digests;
  drops trivy, as this repository builds no image.
- `dev/scripts/tool.sh`: reads the image of a tool from the Compose file with awk and
  checks its digest in bash, so that neither `images.py` nor PyYAML is needed.
- `.github/dependabot.yml` and `.github/CODEOWNERS`: without npm, the platform images and
  `.agents/`.
