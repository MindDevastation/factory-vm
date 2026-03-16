# Playlist Builder month_batch backfill

For previously discovered tracks with `tracks.month_batch IS NULL`, run discovery again so month folder names from `Audio/<Batch>/...` are persisted.

```bash
python scripts/backfill_track_month_batch.py --channel-slug <channel-slug>
```

Optional explicit root override:

```bash
python scripts/backfill_track_month_batch.py --channel-slug <channel-slug> --gdrive-root-id <gdrive-root-id>
```

This is safe/idempotent and updates existing rows by `gdrive_file_id`.
