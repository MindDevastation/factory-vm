# Local Workspace Hygiene

This is the canonical local hygiene guide for Codex work in this repo. Keep it short, practical, and aligned with the restore -> preflight -> apply flow.

## What It Means

Local workspace hygiene means the checkout is easy to trust and easy to review.

It keeps the slice visible, separates repo work from machine noise, and prevents local-only state from leaking into portable repo files.

## Where Active Work Should Not Live

Do not keep an active Codex checkout inside synced or cloud-managed folders such as:

- OneDrive
- Google Drive
- Dropbox-like sync locations

Those paths can introduce file churn, delayed sync, or hidden edits that make the worktree harder to trust.

## What Counts As Local Noise

Local noise is anything created by the machine, editor, shell, or sync layer rather than by the slice itself.

Common examples include:

- editor swap files, temp files, or backup files
- local caches, build artifacts, or test output
- virtual environments or dependency caches
- OS metadata files
- machine-specific config or secrets
- sync-tool side effects

## What The Difference Is

- Intended slice changes: the exact files needed for the task.
- Unrelated local changes: real repo edits, but outside the slice.
- Machine-local config/artifacts: files that only matter on one machine and should not become portable repo state unless intentionally generalized.

## What Blocks Work Immediately

Stop immediately when:

- the checkout lives in a synced folder and file churn is interfering with trust
- you cannot tell intended slice changes from unrelated local edits
- local noise touches files that matter to the slice
- machine-local settings are already embedded in tracked portable files
- the worktree state is too ambiguous to review safely

## What Is Tolerable For Docs/Process Slices

For docs or process-only slices, unrelated local noise can be tolerable if:

- it is clearly unrelated
- it does not touch the files in the slice
- it does not change the meaning of the doc work
- it is reported honestly

Even then, do not let tolerated noise become part of the publication path.

## Preparing A Clean Worktree Before Publication

Before publishing a slice:

1. Review `git status` and identify the intended slice files.
2. Separate unrelated tracked edits from the slice.
3. Move or ignore machine-local artifacts outside the portable repo state.
4. Confirm the final commit only contains the intended slice.
5. Recheck the branch history and publish path before push.

Clean worktree discipline reduces review noise, but it does not replace remote SHA verification after push.

## Reporting Local-Noise Blockers

When local noise blocks work, report only what you observed:

- the exact files or paths involved
- whether the issue is intended slice, unrelated repo change, or machine-local noise
- why the state is unsafe to publish or continue from
- whether the block is immediate or only affects publication

Do not guess. If the boundary is unclear, say so.

## How This Fits The Workflow

- Restore: identify the branch, worktree, and local noise situation before relying on the checkout.
- Preflight: decide whether the slice is still bounded and whether local noise is tolerable.
- Apply: keep edits confined to the slice and stop if unrelated or machine-local state starts to leak in.

## Guardrails

- Clean workspace discipline does not replace remote SHA verification after push.
- Local-only fixes or settings should stay out of portable repo files unless they are intentionally generalized for everyone.

## Cross-Links

- Workflow canon: `docs/workflows/CODEX_WORKFLOW.md`
- Environment readiness: `docs/workflows/ENVIRONMENT_HEALTHCHECK.md`
- PR hygiene: `docs/workflows/PR_HYGIENE.md`
- Testing canon: `docs/workflows/TESTING.md`
