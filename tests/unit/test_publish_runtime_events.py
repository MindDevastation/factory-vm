from __future__ import annotations

import json
import unittest

from services.publish_runtime.events import (
    append_publish_lifecycle_event,
    publish_lifecycle_events_path,
    read_publish_lifecycle_events,
)
from tests._helpers import temp_env


class TestPublishRuntimeEvents(unittest.TestCase):
    def test_append_and_read_latest_first(self) -> None:
        with temp_env() as (_td, env):
            append_publish_lifecycle_event(storage_root=env.storage_root, event={"event": "one", "seq": 1})
            append_publish_lifecycle_event(storage_root=env.storage_root, event={"event": "two", "seq": 2})

            rows = read_publish_lifecycle_events(storage_root=env.storage_root, limit=10)
            self.assertEqual([row["event"] for row in rows], ["two", "one"])

    def test_read_ignores_malformed_json_lines(self) -> None:
        with temp_env() as (_td, env):
            path = publish_lifecycle_events_path(env.storage_root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "{" + "\n" + json.dumps({"event": "ok"}) + "\n",
                encoding="utf-8",
            )

            rows = read_publish_lifecycle_events(storage_root=env.storage_root, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event"], "ok")

    def test_enrichment_event_append_is_additive_only(self) -> None:
        with temp_env() as (_td, env):
            append_publish_lifecycle_event(
                storage_root=env.storage_root,
                event={
                    "event_name": "publish.enrichment",
                    "job_id": 7,
                    "publish_state_before": "ready_to_publish",
                    "publish_state_after": "ready_to_publish",
                    "changed_fields": ["publish_reason_detail"],
                },
            )

            rows = read_publish_lifecycle_events(storage_root=env.storage_root, limit=1)
            self.assertEqual(rows[0]["publish_state_before"], rows[0]["publish_state_after"])
            self.assertEqual(rows[0]["changed_fields"], ["publish_reason_detail"])


if __name__ == "__main__":
    unittest.main()
