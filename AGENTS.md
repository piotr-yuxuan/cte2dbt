# Repository guidance

cte2dbt is a small published library and command line that splits SQL views into dbt
models. Its process is a lighter adaptation of `piotr-yuxuan/next-word-list`; see
[reuse](docs/reference/reuse.md).

## Definitions

- **Owner:** the human who sets goals, agrees acceptance examples, grants any delegated
  authority and merges into `main`.
- **Author:** the person or agent who implements, tests and documents one change on its
  own branch.
- **Reviewer:** a person or agent who independently assesses a stable change and reports
  findings without editing the author's work. Review does not grant authority to merge.

## Start here

Read [the roadmap](docs/work/README.md), the assigned plan and
[the development procedure](docs/how-to/develop.md). The
[architecture](docs/explanation/architecture.md) and the
[conversion rules](docs/reference/conversion-rules.md) describe the code.

- One plan per change, numbered `NNN-slug` in `docs/work/`, implemented on branch
  `NNN-slug` in the worktree `.worktrees/NNN-slug` by one author.
- When the goal is to create, ask all currently known questions in one batch, then wait
  for replies.
- Agree acceptance examples with the owner before implementation. Preserve unrelated and
  uncommitted work, in the primary checkout above all.

## Design quality

Optimise for the context a maintainer must hold to understand, change and verify the
code. Apply the `principles:programming-principles` and `principles:karpathy-guidelines`
skills when they are installed, and read a principle's guide when a decision depends on
it.

## Repository constraints

- **The core stays pure.** `cte2dbt.core` reads no file and prints or logs nothing; all
  I/O sits in `cte2dbt.cli`. The Import Linter layers contract records this; do not
  weaken it.
- **No single-use private helpers.** Keep one-off steps inline; extract shared logic only
  for existing production callers. Test calls do not count as reuse.
- Ruff enforces every rule and `C901` with a maximum McCabe complexity of **10**. Do not
  suppress a check in code or move complexity merely to satisfy the metric; a per-file
  ignore in `pyproject.toml` needs a comment that justifies it.
- **Input SQL is untrusted.** Parse it at the boundary, and fail rather than write a model
  that may not mean what the view meant.
- sqlglot changes its API in minor versions. Support only the range in `pyproject.toml`.
  Prefer its public properties, such as `Query.ctes`; read or set an argument key only
  where no property gives the node, and cover each such key with a test, which the
  compatibility job runs at both ends of the range.

## Implement simply

Prefer explicit data flow and ordinary functions and classes; introduce an abstraction
only when its current benefit outweighs its cost. Use small coherent commits. Development
scripts live in `dev/scripts/`; do not add Make or a task framework. Reuse the entry
points:

```sh
uv run --locked python dev/scripts/check.py fast
uv run --locked python dev/scripts/check.py docs
uv run --locked python dev/scripts/check.py security
uv run --locked python dev/scripts/check.py verify
```

## Review and integrate

Before integration, have the change reviewed independently, by the
`principles:principles-reviewer` agent where it is installed, with the base commit and the
intent. Weakening checks, exclusions or approval rules needs explicitly agreed scope.
Merge with `--no-ff` only with passing evidence and the owner's approval, recording the
decision in the merge message as [the development procedure](docs/how-to/develop.md)
shows. Never push or publish: the owner does.

## Keep documentation useful

Update affected documentation in the same change. Keep current documentation separate
from plans and dated records. Python examples in Markdown run under `check.py docs`;
keep them executable.

## Writing

- British spelling, sentence-case headings, and plain, precise prose.
- No Unicode dash, emoji, or home directory path in a tracked file: the repository is
  public. `check.py docs` checks the Markdown.
- Commit titles are in sentence case and the imperative, at most 50 characters; body
  lines are at most 72 characters and say only what would otherwise be lost.
- A commit message carries no `Co-Authored-By` or other attribution trailer.
