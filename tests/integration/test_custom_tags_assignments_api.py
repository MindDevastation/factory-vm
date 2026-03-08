from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsAssignmentsApi(unittest.TestCase):
    def _create_tag(self, client: TestClient, headers: dict[str, str], *, code: str, category: str, is_active: bool = True) -> int:
        resp = client.post(
            "/v1/track-catalog/custom-tags/catalog",
            headers=headers,
            json={"code": code, "label": code.title(), "category": category, "description": None, "is_active": is_active},
        )
        self.assertEqual(resp.status_code, 200)
        return int(resp.json()["tag"]["id"])

    def _insert_track(self, env: Env) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("darkwood-reverie", "trk-1", "gdrv-1", "gdrive", "f.wav", "title", "artist", 10.0, 1000.0, None),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_get_effective_tags_and_assignments(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            track_pk = self._insert_track(env)
            auto_tag_id = self._create_tag(client, h, code="nebula", category="VISUAL")
            manual_tag_id = self._create_tag(client, h, code="solar", category="VISUAL")
            suppressed_tag_id = self._create_tag(client, h, code="calm", category="MOOD")

            conn = dbm.connect(env)
            try:
                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, auto_tag_id, "AUTO", "2025-01-01", "2025-01-01"),
                )
                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, manual_tag_id, "MANUAL", "2025-01-01", "2025-01-01"),
                )
                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, suppressed_tag_id, "SUPPRESSED", "2025-01-01", "2025-01-01"),
                )
            finally:
                conn.close()

            resp = client.get(f"/v1/track-catalog/tracks/{track_pk}/custom-tags", headers=h)
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(payload["track_pk"], str(track_pk))
            self.assertEqual(sorted(payload["effective_tags"].keys()), ["MOOD", "THEME", "VISUAL"])
            visual_codes = [item["code"] for item in payload["effective_tags"]["VISUAL"]]
            self.assertEqual(visual_codes, ["nebula", "solar"])
            sources = {item["code"]: item["source"] for item in payload["effective_tags"]["VISUAL"]}
            self.assertEqual(sources["nebula"], "auto")
            self.assertEqual(sources["solar"], "manual")
            self.assertEqual(payload["effective_tags"]["MOOD"], [])

            states = {a["code"]: a["state"] for a in payload["assignments"]}
            self.assertEqual(states["nebula"], "AUTO")
            self.assertEqual(states["solar"], "MANUAL")
            self.assertEqual(states["calm"], "SUPPRESSED")

    def test_post_manual_add_and_delete_suppressed_semantics(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            track_pk = self._insert_track(env)
            tag_id = self._create_tag(client, h, code="solar", category="VISUAL")

            add1 = client.post(f"/v1/track-catalog/tracks/{track_pk}/custom-tags", headers=h, json={"tag_id": tag_id})
            self.assertEqual(add1.status_code, 200)
            self.assertEqual(add1.json()["state"], "MANUAL")

            add2 = client.post(
                f"/v1/track-catalog/tracks/{track_pk}/custom-tags",
                headers=h,
                json={"tag_code": "solar", "category": "VISUAL"},
            )
            self.assertEqual(add2.status_code, 200)
            self.assertEqual(add2.json()["state"], "MANUAL")

            delete1 = client.delete(f"/v1/track-catalog/tracks/{track_pk}/custom-tags/{tag_id}", headers=h)
            self.assertEqual(delete1.status_code, 200)
            self.assertEqual(delete1.json()["state"], "SUPPRESSED")

            delete2 = client.delete(f"/v1/track-catalog/tracks/{track_pk}/custom-tags/{tag_id}", headers=h)
            self.assertEqual(delete2.status_code, 200)
            self.assertEqual(delete2.json()["state"], "SUPPRESSED")

            add3 = client.post(f"/v1/track-catalog/tracks/{track_pk}/custom-tags", headers=h, json={"tag_id": tag_id})
            self.assertEqual(add3.status_code, 200)
            self.assertEqual(add3.json()["state"], "MANUAL")

    def test_delete_absent_creates_suppressed_and_not_found_errors(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            track_pk = self._insert_track(env)
            tag_id = self._create_tag(client, h, code="space", category="THEME")

            delete_absent = client.delete(f"/v1/track-catalog/tracks/{track_pk}/custom-tags/{tag_id}", headers=h)
            self.assertEqual(delete_absent.status_code, 200)
            self.assertEqual(delete_absent.json()["state"], "SUPPRESSED")

            missing_track = client.get("/v1/track-catalog/tracks/999999/custom-tags", headers=h)
            self.assertEqual(missing_track.status_code, 404)
            self.assertEqual(missing_track.json()["error"]["code"], "CTA_TRACK_NOT_FOUND")

            missing_tag = client.delete(f"/v1/track-catalog/tracks/{track_pk}/custom-tags/999999", headers=h)
            self.assertEqual(missing_tag.status_code, 404)
            self.assertEqual(missing_tag.json()["error"]["code"], "CTA_TAG_NOT_FOUND")

    def test_post_rejects_inactive_new_assignment_and_invalid_input(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            track_pk = self._insert_track(env)
            inactive_id = self._create_tag(client, h, code="hidden", category="MOOD", is_active=False)

            reject = client.post(f"/v1/track-catalog/tracks/{track_pk}/custom-tags", headers=h, json={"tag_id": inactive_id})
            self.assertEqual(reject.status_code, 400)
            self.assertEqual(reject.json()["error"]["code"], "CTA_INVALID_INPUT")

            bad_selector = client.post(
                f"/v1/track-catalog/tracks/{track_pk}/custom-tags",
                headers=h,
                json={"tag_id": inactive_id, "tag_code": "hidden", "category": "MOOD"},
            )
            self.assertEqual(bad_selector.status_code, 400)
            self.assertEqual(bad_selector.json()["error"]["code"], "CTA_INVALID_INPUT")

            missing_category = client.post(
                f"/v1/track-catalog/tracks/{track_pk}/custom-tags",
                headers=h,
                json={"tag_code": "hidden"},
            )
            self.assertEqual(missing_category.status_code, 400)
            self.assertEqual(missing_category.json()["error"]["code"], "CTA_INVALID_INPUT")

            conn = dbm.connect(env)
            try:
                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, inactive_id, "MANUAL", "2025-01-01", "2025-01-01"),
                )
            finally:
                conn.close()

            listed = client.get(f"/v1/track-catalog/tracks/{track_pk}/custom-tags", headers=h)
            self.assertEqual(listed.status_code, 200)
            assignments = {a["tag_id"]: a["state"] for a in listed.json()["assignments"]}
            self.assertEqual(assignments[inactive_id], "MANUAL")

    def test_invalid_path_params_return_cta_invalid_input_envelope(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            track_pk = self._insert_track(env)

            bad_get = client.get("/v1/track-catalog/tracks/not-an-int/custom-tags", headers=h)
            self.assertEqual(bad_get.status_code, 400)
            bad_get_payload = bad_get.json()
            self.assertIn("error", bad_get_payload)
            self.assertEqual(bad_get_payload["error"]["code"], "CTA_INVALID_INPUT")
            self.assertIn("details", bad_get_payload["error"])
            self.assertNotIn("detail", bad_get_payload)

            bad_delete = client.delete(f"/v1/track-catalog/tracks/{track_pk}/custom-tags/not-an-int", headers=h)
            self.assertEqual(bad_delete.status_code, 400)
            bad_delete_payload = bad_delete.json()
            self.assertIn("error", bad_delete_payload)
            self.assertEqual(bad_delete_payload["error"]["code"], "CTA_INVALID_INPUT")
            self.assertIn("details", bad_delete_payload["error"])
            self.assertNotIn("detail", bad_delete_payload)


if __name__ == "__main__":
    unittest.main()
