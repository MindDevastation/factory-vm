# Epic 7 MF5-S3 — Troubleshooting Guide

Owner: Documentation Governance (Epic 7)
Primary doc class: RUNBOOK
Lifecycle/status: CANONICAL
Canonical-for: Troubleshooting decision aid linked to incident/recovery paths
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Troubleshooting guide supports incident/recovery runbooks and preserves mode separation.

## Symptom-to-path mapping

| Symptom | First check | Path |
|---|---|---|
| API unreachable | service status + health endpoint | Incident response runbook |
| Worker heartbeat missing | worker process/heartbeat endpoint | Incident response runbook |
| Post-restore verification failure | restore checkpoints and verification logs | Recovery runbook |
| Repeated failure after deploy | deploy checklist + smoke evidence | Deploy/update runbook then incident if unresolved |

## Cross-links

- Incident: `docs/runbooks/MF5_S3_INCIDENT_RESPONSE_RUNBOOK.md`
- Recovery: `docs/runbooks/MF5_S3_RECOVERY_RUNBOOK.md`
- Deploy/update: `docs/runbooks/MF5_S2_DEPLOY_UPDATE_RUNBOOK.md`
