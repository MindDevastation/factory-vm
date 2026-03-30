from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataChannelVisualStyleTemplateApi(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _insert_release(self, env, *, channel_slug: str = "darkwood-reverie") -> int:
        from services.common import db as dbm

        conn = dbm.connect(env)
        try:
            channel = dbm.get_channel_by_slug(conn, channel_slug)
            assert channel is not None
            cur = conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, 'r', 'd', '[]', NULL, NULL, ?, 1.0)
                """,
                (int(channel["id"]), f"meta-visual-{channel_slug}"),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_crud_archive_activate_set_default_and_contract_shape(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            create = client.post(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Main Visual",
                    "template_payload": _payload("forest"),
                    "make_default": True,
                },
            )
            self.assertEqual(create.status_code, 200)
            created = create.json()
            template_id = int(created["id"])
            self.assertEqual(
                sorted(created.keys()),
                [
                    "archived_at",
                    "channel_slug",
                    "created_at",
                    "id",
                    "is_default",
                    "last_validated_at",
                    "status",
                    "template_name",
                    "template_payload",
                    "updated_at",
                    "validation_errors",
                    "validation_status",
                ],
            )
            self.assertEqual(created["status"], "ACTIVE")
            self.assertTrue(created["is_default"])
            self.assertEqual(created["validation_status"], "VALID")

            detail = client.get(f"/v1/metadata/channel-visual-style-templates/{template_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.json()["template_name"], "Main Visual")

            patched = client.patch(
                f"/v1/metadata/channel-visual-style-templates/{template_id}",
                headers=headers,
                json={"template_name": "Edited Visual", "template_payload": _payload("mist")},
            )
            self.assertEqual(patched.status_code, 200)
            self.assertEqual(patched.json()["template_name"], "Edited Visual")

            archived = client.post(f"/v1/metadata/channel-visual-style-templates/{template_id}/archive", headers=headers)
            self.assertEqual(archived.status_code, 200)
            self.assertEqual(archived.json()["status"], "ARCHIVED")
            self.assertFalse(archived.json()["is_default"])

            set_default_archived = client.post(
                f"/v1/metadata/channel-visual-style-templates/{template_id}/set-default",
                headers=headers,
            )
            self.assertEqual(set_default_archived.status_code, 422)
            self.assertEqual(set_default_archived.json()["error"]["code"], "CVST_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_DEFAULT")

            activated = client.post(f"/v1/metadata/channel-visual-style-templates/{template_id}/activate", headers=headers)
            self.assertEqual(activated.status_code, 200)
            self.assertEqual(activated.json()["status"], "ACTIVE")
            self.assertFalse(activated.json()["is_default"])

            set_default_active = client.post(
                f"/v1/metadata/channel-visual-style-templates/{template_id}/set-default",
                headers=headers,
            )
            self.assertEqual(set_default_active.status_code, 200)
            self.assertTrue(set_default_active.json()["is_default"])

    def test_validation_failure_and_channel_scope_and_default_switching(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            invalid = client.post(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Bad",
                    "template_payload": {"palette_guidance": "x"},
                    "make_default": False,
                },
            )
            self.assertEqual(invalid.status_code, 422)
            self.assertEqual(invalid.json()["error"]["code"], "CVST_PAYLOAD_REQUIRED_KEY")

            first = client.post(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Default A",
                    "template_payload": _payload("forest"),
                    "make_default": True,
                },
            )
            second = client.post(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Default B",
                    "template_payload": _payload("mist"),
                    "make_default": True,
                },
            )
            other_channel = client.post(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                json={
                    "channel_slug": "channel-b",
                    "template_name": "Other",
                    "template_payload": _payload("night"),
                    "make_default": False,
                },
            )
            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(other_channel.status_code, 200)

            darkwood_list = client.get(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                params={"channel_slug": "darkwood-reverie", "status": "active"},
            )
            self.assertEqual(darkwood_list.status_code, 200)
            self.assertEqual(len(darkwood_list.json()["items"]), 2)

            all_items = client.get("/v1/metadata/channel-visual-style-templates", headers=headers).json()["items"]
            darkwood_defaults = [
                row for row in all_items if row["channel_slug"] == "darkwood-reverie" and row["status"] == "ACTIVE" and row["is_default"]
            ]
            channel_b_defaults = [
                row for row in all_items if row["channel_slug"] == "channel-b" and row["status"] == "ACTIVE" and row["is_default"]
            ]
            self.assertEqual(len(darkwood_defaults), 1)
            self.assertEqual(len(channel_b_defaults), 0)

    def test_effective_resolution_source_visibility_and_clear_override(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            release_id = self._insert_release(env)

            default_row = client.post(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Default",
                    "template_payload": _payload("forest"),
                    "make_default": True,
                },
            ).json()
            override_row = client.post(
                "/v1/metadata/channel-visual-style-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Override",
                    "template_payload": _payload("mist"),
                    "make_default": False,
                },
            ).json()

            before = client.get(
                f"/v1/metadata/channel-visual-style-templates/releases/{release_id}/effective",
                headers=headers,
            )
            self.assertEqual(before.status_code, 200)
            body_before = before.json()
            self.assertEqual(body_before["source"], "channel_default")
            self.assertFalse(body_before["is_override"])
            self.assertFalse(body_before["has_override"])
            self.assertEqual(body_before["default_template_id"], int(default_row["id"]))
            self.assertEqual(body_before["effective_template"]["id"], int(default_row["id"]))

            set_override = client.post(
                f"/v1/metadata/channel-visual-style-templates/releases/{release_id}/override",
                headers=headers,
                json={"template_id": int(override_row["id"])},
            )
            self.assertEqual(set_override.status_code, 200)
            set_body = set_override.json()
            self.assertEqual(set_body["release_id"], release_id)
            self.assertEqual(set_body["template_id"], int(override_row["id"]))

            after = client.get(
                f"/v1/metadata/channel-visual-style-templates/releases/{release_id}/effective",
                headers=headers,
            )
            self.assertEqual(after.status_code, 200)
            body_after = after.json()
            self.assertEqual(body_after["source"], "release_override")
            self.assertTrue(body_after["is_override"])
            self.assertTrue(body_after["has_override"])
            self.assertEqual(body_after["override_template_id"], int(override_row["id"]))
            self.assertEqual(body_after["default_template_id"], int(default_row["id"]))
            self.assertEqual(body_after["effective_template"]["id"], int(override_row["id"]))

            clear = client.post(
                f"/v1/metadata/channel-visual-style-templates/releases/{release_id}/override/clear",
                headers=headers,
            )
            self.assertEqual(clear.status_code, 200)
            self.assertEqual(clear.json(), {"release_id": release_id, "cleared": True})

            after_clear = client.get(
                f"/v1/metadata/channel-visual-style-templates/releases/{release_id}/effective",
                headers=headers,
            )
            self.assertEqual(after_clear.status_code, 200)
            body_after_clear = after_clear.json()
            self.assertEqual(body_after_clear["source"], "channel_default")
            self.assertFalse(body_after_clear["is_override"])
            self.assertFalse(body_after_clear["has_override"])
            self.assertEqual(body_after_clear["effective_template"]["id"], int(default_row["id"]))

    def test_effective_resolution_returns_none_when_no_templates_and_release_fields_unchanged(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            release_id = self._insert_release(env, channel_slug="channel-b")

            effective = client.get(
                f"/v1/metadata/channel-visual-style-templates/releases/{release_id}/effective",
                headers=headers,
            )
            self.assertEqual(effective.status_code, 200)
            body = effective.json()
            self.assertEqual(body["source"], "none")
            self.assertFalse(body["is_override"])
            self.assertFalse(body["has_override"])
            self.assertIsNone(body["effective_template"])
            self.assertIsNone(body["override_template_id"])
            self.assertIsNone(body["default_template_id"])

            from services.common import db as dbm

            conn = dbm.connect(env)
            try:
                release_row = conn.execute(
                    "SELECT title, description, tags_json FROM releases WHERE id = ?",
                    (release_id,),
                ).fetchone()
                assert release_row is not None
                self.assertEqual(str(release_row["title"]), "r")
                self.assertEqual(str(release_row["description"]), "d")
                self.assertEqual(str(release_row["tags_json"]), "[]")
            finally:
                conn.close()



def _payload(motif: str) -> dict[str, object]:
    return {
        "palette_guidance": "Muted earth tones",
        "typography_rules": "Use clean sans serif titles",
        "text_layout_rules": "Center align title block",
        "composition_framing_rules": "Subject centered with margin",
        "allowed_motifs": [motif],
        "banned_motifs": ["neon"],
        "branding_rules": "Keep logo in lower right",
        "output_profile_guidance": "16:9 high contrast",
        "background_compatibility_guidance": "Works on dark backgrounds",
        "cover_composition_guidance": "Leave top third for text",
    }


if __name__ == "__main__":
    unittest.main()
