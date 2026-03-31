from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner import cover_assignment_service as svc
from tests._helpers import seed_minimal_db, temp_env


class TestCoverCandidateContract(unittest.TestCase):
    def test_candidate_payload_contract_has_provenance_and_no_thumbnail_entity(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r', 'd', '[]', NULL, NULL, 'origin-cover-contract', NULL, 1.0)
                        """,
                        (int(channel["id"]),),
                    ).lastrowid
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=int(channel["id"]),
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://cover-contract",
                    name="cover-contract.png",
                    path="/tmp/cover-contract.png",
                )
                payload = svc.create_cover_candidate_reference(
                    conn,
                    release_id=release_id,
                    cover_asset_id=cover_asset_id,
                    source_provider_family="manual_provider",
                    source_reference="manual://cover-contract",
                    input_payload_id=None,
                    selection_mode="manual",
                    template_ref={"template_id": 1},
                    created_by="tester",
                )
                required_keys = {
                    "candidate_id",
                    "release_id",
                    "source_provider_family",
                    "source_reference",
                    "input_payload_id",
                    "selection_mode",
                    "is_manual_selection",
                    "template_ref",
                    "cover_asset",
                    "created_at",
                }
                self.assertTrue(required_keys.issubset(payload.keys()))
                self.assertNotIn("thumbnail_asset_id", payload)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
