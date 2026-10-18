# NNN: Short feature title

## Scope and authority

Problem, intended outcome and exclusions. Name the owner and matching branch.
State whether final integration needs human approval or has explicit delegated
authority, including the agreed limits. Keep assignment status in the roadmap.
Name the coordinator. Use one task and writer per feature. A small correction may
use a few lines under these headings; keep scope, acceptance and authority explicit.

## Decisions

Use the [design-readiness checklist](../../docs/how-to/develop.md#design-readiness)
to record relevant choices and their rationale. Compare the selected approach
with the simplest viable design and justify any added complexity. Identify
dependencies, delegated choices and unresolved questions before coding.

## Acceptance

Record user-agreed examples with concrete inputs and observable outcomes, plus
important failures. State who agreed them. Include affected documentation and
runtime behaviour. Identify the required evidence using the
[development procedure](../../docs/how-to/develop.md): a passing `verify` says
nothing about SQL Server or dbt behaviour that no check exercises.

## Implementation slices

Describe small coherent increments. Split the feature if its review becomes
too large; avoid a speculative task hierarchy.
Line-count limits are warnings, not targets. Split by observable behaviour.

## Handover

Keep this compact while active: current owner, branch/worktree, base/head IDs,
uncommitted or useful ignored work, decisions/dependencies, evidence with its
revisions and limits, open findings and next step. Omit secrets.

## Outcome and verification

Complete during finalisation: what changed, what ran, its results and limits,
and any remaining follow-up with owner and review date or condition. Record each
required check's revision, result and limitations. Unavailable or failed evidence
remains so even if the coordinator accepts an explicit exception.

Retain the final reviewed base/head IDs, finding dispositions, approval scope and
exception decisions in the merge commit message (`git merge --no-ff -F`).

Move the completed plan to `docs/changelog/` before final validation. Its presence
on `main`, rather than in an unmerged branch, establishes completion.
