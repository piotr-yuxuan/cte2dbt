# Develop and integrate a change

The owner coordinates this repository and approves integration. One author owns each
change on its own branch. Independent reviewers assess a stable diff; a review neither
grants permission to merge nor waives failed evidence.

## Design readiness

Agree observable acceptance examples, failure cases and scope, and compare the approach
with the simplest viable design. For a change to the conversion, state the rule in
[the conversion rules](../reference/conversion-rules.md) first, with the SQL it applies
to and the models it gives.

Write the plan from [the template](../../dev/templates/plan.md) as
`docs/work/NNN-slug.md`, taking the next number from [the roadmap](../work/README.md),
and work on a branch of the same name in a worktree:

```sh
git worktree add .worktrees/NNN-slug -b NNN-slug
```

## Work and check

Keep small coherent commits, each passing the checks. Run them from the repository root:

```sh
uv sync --locked
uv run --locked python dev/scripts/check.py fast
uv run --locked python dev/scripts/check.py verify
```

See [the checks reference](../reference/checks.md) for what each mode runs. Never weaken
a check to obtain a passing run.

On NixOS, the DuckDB wheel used by the execution tests needs the C++ runtime. When the
import error names `libstdc++.so.6` and `nix-ld` is configured, run the checks with a
command-local library path:

```sh
LD_LIBRARY_PATH="$NIX_LD_LIBRARY_PATH" uv run --locked python dev/scripts/check.py verify
```

Other Linux distributions provide the runtime through their system libraries.

## Finalise, review and merge

1. Update the documentation and the plan's outcome, then move the plan to
   `docs/changelog/` and remove its roadmap row.
2. Run `verify` and record the full commit IDs and the results.
3. Have the change reviewed independently, with its base commit and intent.
4. Resolve blocking findings and consequential questions. A correction needs a new
   review; remarks alone do not.
5. With the owner's approval, merge and record the decision in the merge message:

```sh
git switch main
git merge --no-ff NNN-slug -F dev/reports/merge-decision.txt
```

The decision states the scope, the reviewed base and head commits, the evidence and its
limits, how each finding was settled, and any exception. Run `verify` again on the
merged `main`.

## Cut a release

1. On the branch of the change, set the version, such as `uv version 2.1.0`, and add its
   section to [the release notes](../reference/release-notes.md).
2. After the merge, once the checks pass on `main`, create a GitHub release from `main`
   whose tag is the version without a `v`, such as `2.1.0`, with the section of the
   release notes as its description.
3. Publishing the release runs `.github/workflows/python-publish.yml`, which checks again,
   builds and uploads to PyPI, as [the CI reference](../reference/ci.md#publishing)
   describes. Approve the `pypi` environment if it asks.
