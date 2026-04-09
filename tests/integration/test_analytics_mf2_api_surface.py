from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.analytics_center.external_sync import create_sync_run, run_external_youtube_ingestion, transition_sync_run
from services.analytics_center.write_service import write_snapshot
from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env
from tests.analytics_fixtures import make_snapshot_input, make_sync_run_payload


class TestAnalyticsMf2ApiSurface(unittest.TestCase):
    class _OkProvider:
        def fetch_channel_metrics(self, **_: object) -> dict:
            return {
                "channel_slug": "darkwood-reverie",
                "metrics": {"views": 10, "impressions": 100},
                "metric_families_returned": ["views", "impressions"],
                "metric_families_unavailable": ["ctr"],
                "freshness_status": "STALE",
                "freshness_basis": "window_end",
                "incomplete_backfill": True,
            }

    class _FailProvider:
        def fetch_channel_metrics(self, **_: object) -> dict:
            raise RuntimeError("provider unavailable")

    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_manual_refresh_and_run_list_detail_contract(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            created = client.post(
                "/v1/analytics/external/manual-refresh",
                headers=h,
                json={
                    "provider_name": "YOUTUBE",
                    "target_scope_type": "CHANNEL",
                    "target_scope_ref": "darkwood-reverie",
                    "refresh_mode": "MANUAL_REFRESH",
                    "force": False,
                    "metrics_subset": ["views", "impressions", "ctr"],
                },
            )
            self.assertEqual(created.status_code, 200)
            body = created.json()
            self.assertEqual(body["sync_state"], "RUNNING")
            self.assertEqual(body["target_scope_type"], "CHANNEL")
            self.assertEqual(body["manual_refresh_contract"]["action"], "MANUAL_REFRESH")
            self.assertEqual(body["manual_refresh_contract"]["run_mode"], "MANUAL_REFRESH")
            self.assertIn("not_scheduled_selector_alias", body["manual_refresh_contract"]["manual_refresh_contract"]["invariants"])
            run_id = int(body["run_id"])

            listed = client.get("/v1/analytics/external/runs", headers=h, params={"target_scope_type": "CHANNEL", "target_scope_ref": "darkwood-reverie"})
            self.assertEqual(listed.status_code, 200)
            self.assertGreaterEqual(len(listed.json()["items"]), 1)

            detail = client.get(f"/v1/analytics/external/runs/{run_id}", headers=h)
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(int(detail.json()["id"]), run_id)

    def test_scheduled_refresh_selector_contract_and_rejection(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            created = client.post(
                "/v1/analytics/external/scheduled-refresh",
                headers=h,
                json={
                    "target_scope_type": "CHANNEL",
                    "target_scope_ref": "darkwood-reverie",
                    "refresh_selector": "EVERY_12_HOURS",
                    "metrics_subset": ["views", "impressions"],
                },
            )
            self.assertEqual(created.status_code, 200)
            body = created.json()
            self.assertEqual(body["run_mode"], "SCHEDULED_SYNC")
            self.assertEqual(body["refresh_selector"], "EVERY_12_HOURS")
            self.assertEqual(int(body["refresh_interval_seconds"]), 43200)
            self.assertEqual(
                body["scheduled_refresh_contract"]["allowed_refresh_selectors"],
                ["HOURLY", "EVERY_12_HOURS", "DAILY"],
            )

            rejected = client.post(
                "/v1/analytics/external/scheduled-refresh",
                headers=h,
                json={
                    "target_scope_type": "CHANNEL",
                    "target_scope_ref": "darkwood-reverie",
                    "refresh_selector": "EVERY_6_HOURS",
                },
            )
            self.assertEqual(rejected.status_code, 422)
            self.assertEqual(rejected.json()["detail"]["code"], "E5A_INVALID_REFRESH_MODE")

    def test_status_and_coverage_semantics_not_yet_synced_and_partial_stale(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            not_yet = client.get("/v1/analytics/external/status", headers=h, params={"target_scope_type": "CHANNEL", "target_scope_ref": "darkwood-reverie"})
            self.assertEqual(not_yet.status_code, 200)
            self.assertEqual(not_yet.json()["source_availability_status"], "NOT_YET_SYNCED")

            conn = dbm.connect(env)
            try:
                run_id = create_sync_run(conn, **make_sync_run_payload(run_mode="MANUAL_REFRESH"))
                transition_sync_run(
                    conn,
                    run_id=run_id,
                    to_sync_state="PARTIAL",
                    metric_families_returned=["views"],
                    metric_families_unavailable=["ctr"],
                    incomplete_backfill=True,
                    freshness_status="STALE",
                    freshness_basis="window_end",
                )
            finally:
                conn.close()

            status = client.get("/v1/analytics/external/status", headers=h, params={"target_scope_type": "CHANNEL", "target_scope_ref": "darkwood-reverie"})
            self.assertEqual(status.status_code, 200)
            self.assertEqual(status.json()["sync_state"], "PARTIAL")
            self.assertEqual(status.json()["freshness_status"], "STALE")

            coverage = client.get("/v1/analytics/external/coverage", headers=h, params={"target_scope_type": "CHANNEL", "target_scope_ref": "darkwood-reverie"})
            self.assertEqual(coverage.status_code, 200)
            self.assertIn("metric_family_coverage", coverage.json())
            self.assertIn("unavailable_by_permission", coverage.json())
            self.assertGreaterEqual(len(coverage.json()["metric_family_coverage"]), 1)

    def test_source_unavailable_and_internal_paths_stay_usable(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                write_snapshot(
                    conn,
                    make_snapshot_input(
                        entity_type="CHANNEL",
                        entity_ref=str(channel_id),
                        source_family="INTERNAL_OPERATIONAL",
                        payload_json={"queue_depth": 1},
                    ),
                )
                run_id = create_sync_run(conn, **make_sync_run_payload(run_mode="MANUAL_REFRESH"))
                run_external_youtube_ingestion(
                    conn,
                    run_id=run_id,
                    provider=self._FailProvider(),
                    channel_slug="darkwood-reverie",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                )
            finally:
                conn.close()

            status = client.get("/v1/analytics/external/status", headers=h, params={"target_scope_type": "CHANNEL", "target_scope_ref": "darkwood-reverie"})
            self.assertEqual(status.status_code, 200)
            self.assertEqual(status.json()["source_availability_status"], "SOURCE_UNAVAILABLE")

            page = client.get("/v1/analytics/channels/darkwood-reverie", headers=h)
            self.assertEqual(page.status_code, 200)
            self.assertIn("freshness_summary", page.json())


if __name__ == "__main__":
    unittest.main()
