# Epic 7 MF2-S2 — Stage Contracts (DISCOVERY / REQ / SPEC / DEV / REVIEW / QA)

Owner: Documentation Governance (Epic 7)
Primary doc class: WORKFLOW_DOC
Lifecycle/status: CANONICAL
Canonical-for: MF2-S2 stage contract baseline
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Contract-level expansion of MF2-S1 skeleton without handoff matrix or MF2-S3+ linkage scope.

## Scope boundary (MF2-S2 only)

This slice defines contract-level stage requirements for:
- DISCOVERY
- REQ
- SPEC
- DEV
- REVIEW
- QA

This slice does not define full sender/receiver handoff matrix (MF2-S4) or operator/release linkage (MF2-S3).

## Stage contracts

### DISCOVERY
- Purpose: establish problem/opportunity framing and candidate scope.
- Required inputs: prior context links, relevant runtime/doc signals.
- Required outputs: discovery summary, candidate scope statement, open questions list.
- Entry gate: problem statement exists.
- Exit gate: candidate scope and open questions are explicit.

### REQ
- Purpose: freeze requirement intent and acceptance constraints.
- Required inputs: discovery outputs.
- Required outputs: requirement artifact in canonical req location, acceptance criteria draft.
- Entry gate: discovery outputs are present and linked.
- Exit gate: requirement artifact is explicit and reviewable.

### SPEC
- Purpose: convert requirements into implementable spec contract.
- Required inputs: requirement artifact.
- Required outputs: spec artifact in canonical spec location, scope boundaries, non-goals.
- Entry gate: requirement artifact exists.
- Exit gate: spec contract is explicit and bounded.

### DEV
- Purpose: implement scoped slice against approved spec.
- Required inputs: spec artifact and acceptance contract.
- Required outputs: scoped code/docs change set, slice commit, validation evidence.
- Entry gate: spec scope approved for slice.
- Exit gate: slice changes + evidence are committed.

### REVIEW
- Purpose: validate scope, correctness, and contract adherence.
- Required inputs: committed slice diff and validation evidence.
- Required outputs: review verdict, required fixes list (if any).
- Entry gate: slice commit exists.
- Exit gate: pass or explicit actionable follow-ups.

### QA
- Purpose: verify behavioral/documentary acceptance against contract.
- Required inputs: reviewed slice and test/validation instructions.
- Required outputs: QA verdict with evidence.
- Entry gate: review verdict available.
- Exit gate: pass or explicit defect report.

## Contract rules

- Stages are normative and ordered.
- Entry/exit gates must be explicit for each stage.
- Stage contracts must reference canonical artifacts; no parallel canon creation.
- Ownership/SSOT logic remains in MF1 artifacts and is referenced, not duplicated.

## Cross-links

- MF2-S1 structure: `docs/workflows/MF2_S1_CANONICAL_WORKFLOW_STRUCTURE.md`
- MF1 ownership map: `docs/ssot/MF1_S3_OWNERSHIP_MAP.md`
- MF1 SSOT map: `docs/ssot/MF1_S3_SSOT_MAP.md`
