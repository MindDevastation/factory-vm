from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataTitleGenApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_release(self, conn, *, channel_slug: str = "darkwood-reverie", planned_at: str | None = "2026-04-09T12:00:00Z", title: str = "Current"):
        ch = dbm.get_channel_by_slug(conn, channel_slug)
        assert ch is not None
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, "d", "[]", planned_at, "f", f"meta-{channel_slug}-{planned_at}-{title}", dbm.now_ts()),
        )
        return int(cur.lastrowid)

    def _seed_template(
        self,
        conn,
        *,
        channel_slug: str = "darkwood-reverie",
        is_default: bool = True,
        status: str = "ACTIVE",
        validation_status: str = "VALID",
        body: str = "{{channel_display_name}} {{release_year}}",
        name: str = "tmpl",
    ) -> int:
        return dbm.create_title_template(
            conn,
            channel_slug=channel_slug,
            template_name=name,
            template_body=body,
            status=status,
            is_default=is_default,
            validation_status=validation_status,
            validation_errors_json=None,
            last_validated_at="2026-01-01T00:00:00+00:00",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
            archived_at=None,
        )

    def _release_row(self, env, release_id: int):
        conn = dbm.connect(env)
        try:
            return dict(conn.execute("SELECT * FROM releases WHERE id = ?", (release_id,)).fetchone())
        finally:
            conn.close()

    def test_context_endpoint_with_default_present(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                default_id = self._seed_template(conn, is_default=True, name="d")
                self._seed_template(conn, is_default=False, name="a")
                self._seed_template(conn, status="ARCHIVED", is_default=False, name="arch")
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(
                set(body.keys()),
                {
                    "release_id",
                    "channel_slug",
                    "current_title",
                    "has_existing_title",
                    "default_template",
                    "active_templates",
                    "can_generate_with_default",
                },
            )
            self.assertEqual(body["release_id"], release_id)
            self.assertEqual(body["channel_slug"], "darkwood-reverie")
            self.assertTrue(body["has_existing_title"])
            self.assertTrue(body["can_generate_with_default"])
            self.assertEqual(body["default_template"]["id"], default_id)
            self.assertEqual(
                set(body["default_template"].keys()),
                {"id", "template_name", "status", "is_default"},
            )
            self.assertEqual(body["default_template"]["status"], "ACTIVE")
            self.assertTrue(body["default_template"]["is_default"])
            self.assertEqual(len(body["active_templates"]), 2)
            expected_item_keys = {"id", "template_name", "status", "is_default"}
            for item in body["active_templates"]:
                self.assertEqual(set(item.keys()), expected_item_keys)
                self.assertEqual(item["status"], "ACTIVE")

    def test_context_endpoint_without_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIsNone(body["default_template"])
            self.assertFalse(body["can_generate_with_default"])

    def test_generate_with_default_template_and_no_release_mutation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="  Keep Me  ")
                template_id = self._seed_template(conn, is_default=True)
                before = conn.execute("SELECT title FROM releases WHERE id = ?", (release_id,)).fetchone()["title"]
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={})
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(
                set(body.keys()),
                {
                    "release_id",
                    "used_template",
                    "current_title",
                    "has_existing_title",
                    "overwrite_required",
                    "proposed_title",
                    "normalized_length",
                    "generation_fingerprint",
                    "warnings",
                },
            )
            self.assertEqual(body["release_id"], release_id)
            self.assertEqual(body["used_template"]["id"], template_id)
            self.assertTrue(body["used_template"]["is_default_channel_template"])
            self.assertEqual(body["proposed_title"], "Darkwood Reverie 2026")
            self.assertEqual(body["normalized_length"], len("Darkwood Reverie 2026"))
            self.assertTrue(all(isinstance(w, str) for w in body["warnings"]))
            self.assertIn("overwrite", body["warnings"][0].lower())

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT title FROM releases WHERE id = ?", (release_id,)).fetchone()["title"]
            finally:
                conn.close()
            self.assertEqual(before, after)

    def test_generate_with_explicit_template_override_and_rejections(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
                good = self._seed_template(conn, is_default=False, body="{{channel_slug}}")
                mismatch = self._seed_template(conn, channel_slug="channel-b", is_default=False)
                archived = self._seed_template(conn, is_default=False, status="ARCHIVED")
                invalid = self._seed_template(conn, is_default=False, validation_status="INVALID")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            ok = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": good})
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(ok.json()["used_template"]["id"], good)
            self.assertFalse(ok.json()["used_template"]["is_default_channel_template"])

            bad_mismatch = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": mismatch})
            self.assertEqual(bad_mismatch.status_code, 422)
            self.assertEqual(bad_mismatch.json()["error"]["code"], "MTG_TEMPLATE_CHANNEL_MISMATCH")

            bad_archived = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": archived})
            self.assertEqual(bad_archived.status_code, 422)
            self.assertEqual(bad_archived.json()["error"]["code"], "MTG_TEMPLATE_NOT_ACTIVE")

            bad_invalid = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": invalid})
            self.assertEqual(bad_invalid.status_code, 422)
            self.assertEqual(bad_invalid.json()["error"]["code"], "MTG_TEMPLATE_INVALID")

    def test_explicit_generate_does_not_mutate_channel_default_template(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
                default_id = self._seed_template(conn, is_default=True, body="{{channel_display_name}}", name="default")
                explicit_id = self._seed_template(conn, is_default=False, body="{{channel_slug}}", name="explicit")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/generate",
                headers=headers,
                json={"template_id": explicit_id},
            )
            self.assertEqual(generated.status_code, 200)
            self.assertEqual(generated.json()["used_template"]["id"], explicit_id)

            context = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=headers)
            self.assertEqual(context.status_code, 200)
            self.assertEqual(context.json()["default_template"]["id"], default_id)

    def test_can_generate_with_default_only_reflects_default_presence(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="", planned_at=None)
                self._seed_template(conn, is_default=True, body="{{channel_display_name}} {{release_year}}")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            context_resp = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=headers)
            self.assertEqual(context_resp.status_code, 200)
            context_payload = context_resp.json()
            self.assertTrue(context_payload["can_generate_with_default"])
            self.assertIsNotNone(context_payload["default_template"])

            generate_resp = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={})
            self.assertEqual(generate_resp.status_code, 422)
            self.assertEqual(generate_resp.json()["error"]["code"], "MTG_REQUIRED_CONTEXT_MISSING")

    def test_generate_without_default_or_missing_schedule(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_no_default = self._seed_release(conn, title="", channel_slug="channel-b")
                release_missing_planned = self._seed_release(conn, title="", planned_at=None)
                self._seed_template(conn, is_default=True, body="{{channel_display_name}} {{release_year}}")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            no_default = client.post(f"/v1/metadata/releases/{release_no_default}/titlegen/generate", headers=headers, json={})
            self.assertEqual(no_default.status_code, 422)
            self.assertEqual(no_default.json()["error"]["code"], "MTG_DEFAULT_TEMPLATE_NOT_CONFIGURED")

            missing_planned = client.post(f"/v1/metadata/releases/{release_missing_planned}/titlegen/generate", headers=headers, json={})
            self.assertEqual(missing_planned.status_code, 422)
            self.assertEqual(missing_planned.json()["error"]["code"], "MTG_REQUIRED_CONTEXT_MISSING")

    def test_generate_and_apply_template_not_found_maps_to_404(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            missing_generate = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/generate",
                headers=headers,
                json={"template_id": 999_999},
            )
            self.assertEqual(missing_generate.status_code, 404)
            self.assertEqual(missing_generate.json()["error"]["code"], "MTG_TEMPLATE_NOT_FOUND")

            missing_apply = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/apply",
                headers=headers,
                json={
                    "template_id": 999_999,
                    "generation_fingerprint": "x",
                    "overwrite_confirmed": False,
                },
            )
            self.assertEqual(missing_apply.status_code, 404)
            self.assertEqual(missing_apply.json()["error"]["code"], "MTG_TEMPLATE_NOT_FOUND")

    def test_apply_after_generate_updates_only_release_title(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="Manual")
                self._seed_template(conn, is_default=True, body="{{channel_display_name}} {{release_year}}")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={}).json()
            before = self._release_row(env, release_id)

            applied = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/apply",
                headers=headers,
                json={
                    "generation_fingerprint": generated["generation_fingerprint"],
                    "overwrite_confirmed": True,
                },
            )
            self.assertEqual(applied.status_code, 200)
            body = applied.json()
            self.assertTrue(body["title_updated"])
            self.assertEqual(body["title_before"], "Manual")

            after = self._release_row(env, release_id)
            changed_release_cols = {k for k in before if before[k] != after[k]}
            self.assertEqual(changed_release_cols, {"title"})

    def test_apply_overwrite_requires_confirmation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="Existing")
                self._seed_template(conn, is_default=True, body="{{channel_display_name}}")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={}).json()
            resp = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/apply",
                headers=headers,
                json={"generation_fingerprint": generated["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTG_OVERWRITE_CONFIRMATION_REQUIRED")

    def test_apply_stale_fingerprint_blocks_apply_for_template_or_schedule_changes(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
                template_id = self._seed_template(conn, is_default=True, body="{{channel_display_name}} {{release_year}}")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={}).json()

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE title_templates SET updated_at = ? WHERE id = ?", ("2026-03-03T00:00:00+00:00", template_id))
                conn.commit()
            finally:
                conn.close()
            stale_template = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/apply",
                headers=headers,
                json={"generation_fingerprint": generated["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(stale_template.status_code, 422)
            self.assertEqual(stale_template.json()["error"]["code"], "MTG_PREVIEW_STALE")

            regenerated = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={}).json()
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE releases SET planned_at = ? WHERE id = ?", ("2027-04-09T12:00:00Z", release_id))
                conn.commit()
            finally:
                conn.close()
            stale_schedule = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/apply",
                headers=headers,
                json={"generation_fingerprint": regenerated["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(stale_schedule.status_code, 422)
            self.assertEqual(stale_schedule.json()["error"]["code"], "MTG_PREVIEW_STALE")

    def test_apply_default_and_explicit_template_paths_and_same_title_noop(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_default = self._seed_release(conn, title="Darkwood Reverie")
                release_explicit = self._seed_release(conn, title="")
                default_template = self._seed_template(conn, is_default=True, body="{{channel_display_name}}")
                explicit_template = self._seed_template(conn, is_default=False, body="{{channel_slug}}", name="explicit")
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            gen_default = client.post(f"/v1/metadata/releases/{release_default}/titlegen/generate", headers=headers, json={}).json()
            noop_resp = client.post(
                f"/v1/metadata/releases/{release_default}/titlegen/apply",
                headers=headers,
                json={"generation_fingerprint": gen_default["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(noop_resp.status_code, 200)
            self.assertFalse(noop_resp.json()["title_updated"])
            self.assertEqual(noop_resp.json()["used_template_id"], default_template)

            gen_explicit = client.post(
                f"/v1/metadata/releases/{release_explicit}/titlegen/generate",
                headers=headers,
                json={"template_id": explicit_template},
            ).json()
            applied_explicit = client.post(
                f"/v1/metadata/releases/{release_explicit}/titlegen/apply",
                headers=headers,
                json={
                    "template_id": explicit_template,
                    "generation_fingerprint": gen_explicit["generation_fingerprint"],
                    "overwrite_confirmed": False,
                },
            )
            self.assertEqual(applied_explicit.status_code, 200)
            self.assertTrue(applied_explicit.json()["title_updated"])
            self.assertEqual(applied_explicit.json()["used_template_id"], explicit_template)

    def test_apply_stale_when_title_and_template_change_between_generate_apply(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="Original")
                template_id = self._seed_template(conn, is_default=True, body="{{channel_display_name}}")
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={}).json()

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("Changed by user", release_id))
                conn.execute("UPDATE title_templates SET updated_at = ? WHERE id = ?", ("2026-02-02T00:00:00+00:00", template_id))
                conn.commit()
            finally:
                conn.close()

            resp = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/apply",
                headers=headers,
                json={"generation_fingerprint": generated["generation_fingerprint"], "overwrite_confirmed": True},
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTG_PREVIEW_STALE")


if __name__ == "__main__":
    unittest.main()
