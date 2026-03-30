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
