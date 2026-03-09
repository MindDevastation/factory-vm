# FVR-S2 follow-up handoff (row summary)

## Scope confirmation
- Runtime behavior is unchanged in this follow-up slice.
- `track_analysis_flat` remains a derived table built from `tracks` + raw analyzer payload JSON (`track_features.payload_json`, `track_tags.payload_json`, `track_scores.payload_json`).
- Raw JSON payload tables remain the canonical source of truth.
- No report switch is included in this slice.

## Concrete persisted `track_analysis_flat` row fragment (test-backed)
Source: persisted row asserted by `tests/integration/test_track_analysis_flat_sync.py::test_sync_upserts_flat_row`.

```json
{
  "track_pk": 1,
  "channel_slug": "darkwood-reverie",
  "track_id": "001",
  "analysis_computed_at": 1234.5,
  "analysis_status": "COMPLETE",
  "analyzer_version": "adv",
  "schema_version": "v1",
  "duration_sec": 12.5,
  "true_peak_dbfs": -2.1,
  "yamnet_top_tags_text": "Music",
  "voice_flag": 1,
  "speech_flag": 0,
  "dominant_texture": "smooth",
  "prohibited_cues_summary": "No prohibited cues detected by fallback analyzer.",
  "dsp_score": 0.8,
  "legacy_scene": null,
  "legacy_mood": null,
  "human_readable_notes": "No prohibited cues detected by fallback analyzer. | weighted components | ok | voice_prob=0.9 | speech_prob=0.0"
}
```

`updated_at` is persisted and asserted as a non-empty ISO timestamp string in the same integration test.

## Sync-after-analyze proof
- In `analyze_tracks`, flat sync is called only after `track_features`, `track_tags`, and `track_scores` upserts execute successfully inside the same transaction.
- This preserves the derived-only contract for `track_analysis_flat` and does not change legacy payload behavior.

## Backfill summary status
- Backfill summary: **N/A in FVR-S2 (not implemented in this slice)**.
