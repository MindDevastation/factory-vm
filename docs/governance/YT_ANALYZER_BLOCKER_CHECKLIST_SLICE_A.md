# YouTube Performance Metrics Analyzer — Blocker Closure Checklist (Slice A Scaffold)

Status date: 2026-04-09 (UTC)
Canonical branch: `feature/youtube-performance-metrics-analyzer`
Scope: Slice A only (state alignment + deterministic evidence harness scaffolding)

## Purpose
This checklist is a tracking scaffold for blocker-closure execution. It intentionally does **not** mark any blocker as closed in Slice A.

## Blocker tracker
| ID | Blocker | Current status | Evidence contract (required before closure) |
| --- | --- | --- | --- |
| B1 | analyzer header entry + real analyzer UI surface family | CLOSED (Slice B) | UI/API evidence pack showing real entrypoint wiring, route availability, and parity tests. |
| B2 | real user-facing charts and animated charts | CLOSED (Slice C) | Screenshot/video evidence + deterministic UI tests proving chart data and animation hooks. |
| B3 | full required external YouTube metrics breadth | CLOSED (Slice D re-apply) | Contract and integration evidence for complete required metric set, alias normalization, and explicit coverage-state visibility. |
| B4 | real historical backfill feature flow | OPEN | End-to-end flow evidence for backfill trigger, processing, and persisted history windows. |
| B5 | real planning assistant feature surface | OPEN | API + UI/consumer evidence for planning assistant behavior with production-shaped payloads. |
| B6 | real Telegram analyzer operator surface | OPEN | API + operator surface evidence validating Telegram output contract and behavior. |
| B7 | truthful export coverage for planning outputs and comparison outputs | OPEN | Export fixture diff evidence proving planning/comparison outputs are represented faithfully. |
| B8 | full automated evidence for the completed feature set | OPEN | Repeatable automation matrix with pass/fail outcomes and artifact links for all blockers. |

## Deterministic evidence harness (Slice A scaffold)
1. Use canonical branch head only (`origin/feature/youtube-performance-metrics-analyzer`).
2. Run baseline test suite command:
   - `python -m unittest discover -s tests -v`
3. Store blocker-specific evidence under a single predictable root:
   - `artifacts/blocker_closure/<blocker_id>/...`
4. Record outcomes in the JSON scaffold:
   - `docs/governance/YT_ANALYZER_BLOCKER_TEST_MATRIX_SLICE_A.json`
5. Blocker status may move from `OPEN` only when linked evidence artifacts and automated checks are both present.

## Slice A constraints acknowledged
- No blocker is declared closed here.
- No Slice B implementation work is included here.
- This file is intended to reduce ambiguity and provide deterministic review checkpoints.
