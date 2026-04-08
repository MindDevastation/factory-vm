from __future__ import annotations

import unittest

from services.analytics_center.literals import (
    ANALYZER_DEFAULT_MUTATION_POLICY,
    ANALYZER_PROFILE_AXES,
    ANALYZER_REFRESH_SELECTOR_VALUES,
    ANALYZER_REQUIRED_METRIC_DIMENSIONS,
    ANALYTICS_EXTERNAL_PROVIDER_NAMES,
    ANALYTICS_EXTERNAL_RUN_MODES,
    ANALYTICS_EXTERNAL_SYNC_STATES,
    ANALYTICS_EXTERNAL_TARGET_SCOPE_TYPES,
    ANALYTICS_ENTITY_TYPES,
    ANALYTICS_FRESHNESS_STATUSES,
    ANALYTICS_MF4_BASELINE_FAMILIES,
    ANALYTICS_MF4_COMPARISON_FAMILIES,
    ANALYTICS_MF4_CONFIDENCE_CLASSES,
    ANALYTICS_MF4_PREDICTION_FAMILIES,
    ANALYTICS_MF4_RUN_KINDS,
    ANALYTICS_MF4_SCOPE_TYPES,
    ANALYTICS_MF4_VARIANCE_CLASSES,
    ANALYTICS_MF5_CONFIDENCE_CLASSES,
    ANALYTICS_MF5_LIFECYCLE_STATUSES,
    ANALYTICS_MF5_RECOMMENDATION_FAMILIES,
    ANALYTICS_MF5_RECOMPUTE_MODES,
    ANALYTICS_MF5_RUN_STATES,
    ANALYTICS_MF5_SCOPE_TYPES,
    ANALYTICS_MF5_SEVERITY_CLASSES,
    ANALYTICS_MF5_TARGET_DOMAINS,
    ANALYTICS_MF6_ARTIFACT_TYPES,
    ANALYTICS_MF6_GENERATION_STATUSES,
    ANALYTICS_MF6_REPORT_SCOPE_TYPES,
    ANALYTICS_OPERATIONAL_KPI_FAMILIES,
    ANALYTICS_OPERATIONAL_KPI_STATUS_CLASSES,
    ANALYTICS_OPERATIONAL_RECOMPUTE_MODES,
    ANALYTICS_OPERATIONAL_RUN_STATES,
    ANALYTICS_OPERATIONAL_SCOPE_TYPES,
    ANALYTICS_ROLLUP_RELATION_TYPES,
    ANALYTICS_SNAPSHOT_STATUSES,
    ANALYTICS_SOURCE_FAMILIES,
    ANALYTICS_WINDOW_TYPES,
    ANALYTICS_YT_LINKAGE_CONFIDENCE,
    ANALYTICS_YT_LINKAGE_SOURCE,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestAnalyticsSchemaFoundation(unittest.TestCase):
    def test_literals_match_frozen_contract(self) -> None:
        self.assertEqual(
            ANALYZER_PROFILE_AXES,
            ("CHANNEL_STRATEGY_PROFILE", "FORMAT_PROFILE"),
        )
        self.assertEqual(ANALYZER_DEFAULT_MUTATION_POLICY, "NO_AUTO_APPLY")
        self.assertEqual(ANALYZER_REFRESH_SELECTOR_VALUES, ("HOURLY", "EVERY_12_HOURS", "DAILY"))
        self.assertEqual(
            ANALYZER_REQUIRED_METRIC_DIMENSIONS,
            (
                "views",
                "impressions",
                "ctr",
                "watch_time",
                "average_view_duration",
                "retention",
                "subscribers",
                "monetization",
                "unique_viewers",
                "new_casual_regular_returning_viewers",
                "traffic_sources",
                "youtube_search_terms",
                "viewers_when_on_youtube",
                "retention_key_moments",
                "retention_typical_benchmark",
                "top_geographies",
                "subscriber_conversion_context",
            ),
        )

        self.assertEqual(
            ANALYTICS_ENTITY_TYPES,
            ("CHANNEL", "RELEASE", "BATCH", "JOB_RUNTIME", "PORTFOLIO"),
        )
        self.assertEqual(
            ANALYTICS_SOURCE_FAMILIES,
            (
                "EXTERNAL_YOUTUBE",
                "INTERNAL_OPERATIONAL",
                "DERIVED_ROLLUP",
                "COMPARISON_BASELINE",
                "EXPLAINABILITY_OUTPUT",
            ),
        )
        self.assertEqual(
            ANALYTICS_WINDOW_TYPES,
            (
                "POINT_IN_TIME",
                "BOUNDED_WINDOW",
                "ROLLING_BASELINE",
                "LAST_KNOWN_CURRENT",
                "MONTHLY_BATCH",
            ),
        )
        self.assertEqual(
            ANALYTICS_SNAPSHOT_STATUSES,
            ("CURRENT", "HISTORICAL", "SUPERSEDED", "PARTIAL", "FAILED"),
        )
        self.assertEqual(
            ANALYTICS_FRESHNESS_STATUSES,
            ("FRESH", "STALE", "PARTIAL", "UNKNOWN"),
        )
        self.assertEqual(
            ANALYTICS_ROLLUP_RELATION_TYPES,
            ("CHANNEL_TO_RELEASE", "RELEASE_TO_JOB_RUNTIME", "RELEASE_TO_BATCH", "PORTFOLIO_TO_ENTITY"),
        )
        self.assertEqual(ANALYTICS_YT_LINKAGE_CONFIDENCE, ("EXACT", "INFERRED"))
        self.assertEqual(
            ANALYTICS_YT_LINKAGE_SOURCE,
            ("UPLOAD_ARTIFACT", "RELEASE_BINDING", "MANUAL_LINK", "RECONCILED"),
        )
        self.assertEqual(ANALYTICS_EXTERNAL_PROVIDER_NAMES, ("YOUTUBE",))
        self.assertEqual(ANALYTICS_EXTERNAL_TARGET_SCOPE_TYPES, ("CHANNEL", "RELEASE_VIDEO"))
        self.assertEqual(
            ANALYTICS_EXTERNAL_RUN_MODES,
            ("INITIAL_BACKFILL", "SCHEDULED_SYNC", "MANUAL_REFRESH", "PARTIAL_REFRESH", "STALE_RESYNC"),
        )
        self.assertEqual(ANALYTICS_EXTERNAL_SYNC_STATES, ("RUNNING", "SUCCEEDED", "PARTIAL", "FAILED"))
        self.assertEqual(ANALYTICS_OPERATIONAL_SCOPE_TYPES, ("CHANNEL", "RELEASE", "BATCH_MONTH", "PORTFOLIO"))
        self.assertEqual(ANALYTICS_OPERATIONAL_KPI_STATUS_CLASSES, ("NORMAL", "ANOMALY", "RISK"))
        self.assertEqual(
            ANALYTICS_OPERATIONAL_KPI_FAMILIES,
            (
                "PIPELINE_TIMING",
                "QA_STATUS",
                "UPLOAD_OUTCOME",
                "PUBLISH_OUTCOME",
                "RETRY_BURDEN",
                "READINESS",
                "DRIFT_RECONCILE",
                "CADENCE_ADHERENCE",
                "BATCH_COMPLETENESS",
            ),
        )
        self.assertEqual(
            ANALYTICS_OPERATIONAL_RECOMPUTE_MODES,
            ("FULL_RECOMPUTE", "INCREMENTAL_RECOMPUTE", "TARGETED_RECOMPUTE"),
        )
        self.assertEqual(ANALYTICS_OPERATIONAL_RUN_STATES, ("RUNNING", "SUCCEEDED", "PARTIAL", "FAILED"))
        self.assertEqual(ANALYTICS_MF4_SCOPE_TYPES, ("CHANNEL", "RELEASE", "BATCH_MONTH", "PORTFOLIO"))
        self.assertEqual(
            ANALYTICS_MF4_BASELINE_FAMILIES,
            ("CHANNEL_HISTORICAL", "RELEASE_VS_CHANNEL", "BATCH_MONTH_HISTORICAL", "PORTFOLIO_COMPARISON"),
        )
        self.assertEqual(
            ANALYTICS_MF4_COMPARISON_FAMILIES,
            ("RELEASE_VS_CHANNEL_BASELINE", "CHANNEL_VS_SELF_HISTORY", "BATCH_MONTH_VS_RECENT_CHANNEL", "CHANNEL_VS_PORTFOLIO"),
        )
        self.assertEqual(ANALYTICS_MF4_VARIANCE_CLASSES, ("NORMAL", "ANOMALY", "RISK"))
        self.assertEqual(
            ANALYTICS_MF4_PREDICTION_FAMILIES,
            ("WEAK_RELEASE_RISK", "PUBLISH_WINDOW_QUALITY", "CHANNEL_MOMENTUM", "CADENCE_DEGRADATION_RISK", "OPERATIONAL_ANOMALY_RISK"),
        )
        self.assertEqual(ANALYTICS_MF4_CONFIDENCE_CLASSES, ("LOW", "MEDIUM", "HIGH"))
        self.assertEqual(
            ANALYTICS_MF4_RUN_KINDS,
            ("BASELINE_RECOMPUTE", "COMPARISON_RECOMPUTE", "PREDICTION_RECOMPUTE", "FULL_STACK_RECOMPUTE"),
        )

        self.assertEqual(ANALYTICS_MF5_SCOPE_TYPES, ("CHANNEL", "RELEASE", "BATCH_MONTH", "PORTFOLIO"))
        self.assertEqual(
            ANALYTICS_MF5_RECOMMENDATION_FAMILIES,
            (
                "PUBLISH_TIMING_SUGGESTION",
                "CADENCE_BATCH_HEALTH_SUGGESTION",
                "WEAK_RELEASE_ATTENTION",
                "OPERATIONAL_REMEDIATION",
                "CHANNEL_OPTIMIZATION",
                "ANOMALY_RISK_ALERT",
                "CONTENT_PACKAGING_SUGGESTION",
            ),
        )
        self.assertEqual(ANALYTICS_MF5_TARGET_DOMAINS, ("PUBLISH", "METADATA", "VISUALS", "PLANNER", "OPERATIONAL_TROUBLESHOOTING"))
        self.assertEqual(ANALYTICS_MF5_SEVERITY_CLASSES, ("INFO", "WARNING", "CRITICAL"))
        self.assertEqual(ANALYTICS_MF5_CONFIDENCE_CLASSES, ("LOW", "MEDIUM", "HIGH"))
        self.assertEqual(ANALYTICS_MF5_LIFECYCLE_STATUSES, ("OPEN", "ACKNOWLEDGED", "DISMISSED", "SUPERSEDED"))
        self.assertEqual(ANALYTICS_MF5_RECOMPUTE_MODES, ("FULL_RECOMPUTE", "INCREMENTAL_RECOMPUTE", "TARGETED_RECOMPUTE"))
        self.assertEqual(ANALYTICS_MF5_RUN_STATES, ("RUNNING", "SUCCEEDED", "PARTIAL", "FAILED"))
        self.assertEqual(ANALYTICS_MF6_REPORT_SCOPE_TYPES, ("OVERVIEW", "CHANNEL", "RELEASE", "BATCH_MONTH"))
        self.assertEqual(ANALYTICS_MF6_ARTIFACT_TYPES, ("XLSX", "STRUCTURED_REPORT", "API_REPORT"))
        self.assertEqual(ANALYTICS_MF6_GENERATION_STATUSES, ("PENDING", "READY", "FAILED"))

    def test_migrate_creates_required_analytics_tables_and_indexes(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                tables = {
                    str(row["name"])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                for table in (
                    "analytics_external_identities",
                    "analytics_scope_links",
                    "analytics_snapshots",
                    "analytics_rollup_links",
                    "analytics_youtube_video_links",
                    "analytics_external_sync_runs",
                    "analytics_external_scope_status",
                    "analytics_external_audit_events",
                    "analytics_operational_kpi_runs",
                    "analytics_operational_kpi_snapshots",
                    "analytics_operational_kpi_events",
                    "analytics_prediction_runs",
                    "analytics_baseline_snapshots",
                    "analytics_comparison_snapshots",
                    "analytics_prediction_snapshots",
                    "analytics_prediction_events",
                    "analytics_recommendation_runs",
                    "analytics_recommendation_snapshots",
                    "analytics_recommendation_events",
                    "analytics_report_records",
                    "analytics_ui_events",
                ):
                    self.assertIn(table, tables)

                snapshots_cols = {
                    str(row["name"]) for row in conn.execute("PRAGMA table_info(analytics_snapshots)").fetchall()
                }
                self.assertIn("comparison_baseline_snapshot_id", snapshots_cols)
                self.assertIn("payload_json", snapshots_cols)
                self.assertIn("explainability_json", snapshots_cols)
                self.assertIn("lineage_json", snapshots_cols)
                self.assertIn("anomaly_markers_json", snapshots_cols)

                indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_snapshots)").fetchall()
                }
                self.assertIn("idx_as_current_scope_unique", indexes)
                self.assertIn("idx_as_read_filters", indexes)
                self.assertIn("idx_as_scope_current", indexes)

                aei_indexes = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA index_list(analytics_external_identities)").fetchall()
                }
                self.assertIn("idx_aei_entity", aei_indexes)
                self.assertIn("idx_aei_source_external", aei_indexes)

                yt_link_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_youtube_video_links)").fetchall()
                }
                self.assertIn("idx_analytics_youtube_video_links_channel_video", yt_link_indexes)

                sync_run_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_external_sync_runs)").fetchall()
                }
                self.assertIn("idx_analytics_external_sync_runs_scope_time", sync_run_indexes)
                self.assertIn("idx_analytics_external_sync_runs_state_time", sync_run_indexes)

                scope_status_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_external_scope_status)").fetchall()
                }
                self.assertIn("idx_analytics_external_scope_status_scope", scope_status_indexes)

                audit_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_external_audit_events)").fetchall()
                }
                self.assertIn("idx_analytics_external_audit_events_scope_time", audit_indexes)

                kpi_run_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_operational_kpi_runs)").fetchall()
                }
                self.assertIn("idx_aokr_scope_time", kpi_run_indexes)
                self.assertIn("idx_aokr_state_time", kpi_run_indexes)

                kpi_snapshot_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_operational_kpi_snapshots)").fetchall()
                }
                self.assertIn("idx_aoks_scope_family_current", kpi_snapshot_indexes)
                self.assertIn("idx_aoks_status_class", kpi_snapshot_indexes)
                kpi_events_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_operational_kpi_events)").fetchall()
                }
                self.assertIn("idx_aoke_scope_time", kpi_events_indexes)
                prediction_run_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_prediction_runs)").fetchall()
                }
                self.assertIn("idx_apr_scope_kind_time", prediction_run_indexes)
                self.assertIn("idx_apr_state_time", prediction_run_indexes)
                baseline_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_baseline_snapshots)").fetchall()
                }
                self.assertIn("idx_abs_scope_family_current", baseline_indexes)
                self.assertIn("idx_abs_variance_class", baseline_indexes)
                comparison_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_comparison_snapshots)").fetchall()
                }
                self.assertIn("idx_acs_scope_family_current", comparison_indexes)
                self.assertIn("idx_acs_variance_class", comparison_indexes)
                prediction_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_prediction_snapshots)").fetchall()
                }
                self.assertIn("idx_aps_scope_family_current", prediction_indexes)
                self.assertIn("idx_aps_variance_confidence", prediction_indexes)
                prediction_events_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_prediction_events)").fetchall()
                }
                self.assertIn("idx_ape_scope_time", prediction_events_indexes)
                recommendation_run_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_recommendation_runs)").fetchall()
                }
                self.assertIn("idx_arr_scope_family_mode_time", recommendation_run_indexes)
                self.assertIn("idx_arr_state_time", recommendation_run_indexes)
                recommendation_snapshot_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_recommendation_snapshots)").fetchall()
                }
                self.assertIn("idx_ars_current_open_scope_issue", recommendation_snapshot_indexes)
                self.assertIn("idx_ars_scope_family_status", recommendation_snapshot_indexes)
                self.assertIn("idx_ars_queue_order", recommendation_snapshot_indexes)
                recommendation_event_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_recommendation_events)").fetchall()
                }
                self.assertIn("idx_are_scope_time", recommendation_event_indexes)
                report_record_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_report_records)").fetchall()
                }
                self.assertIn("idx_analytics_report_records_scope_time", report_record_indexes)
                self.assertIn("idx_analytics_report_records_status_time", report_record_indexes)
                ui_event_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_ui_events)").fetchall()
                }
                self.assertIn("idx_analytics_ui_events_scope_time", ui_event_indexes)

                external_snapshot_indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_snapshots)").fetchall()
                }
                self.assertIn("idx_analytics_snapshots_external_scope_time", external_snapshot_indexes)
            finally:
                conn.close()

    def test_migration_regression_does_not_break_release_job_planner_foundation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()
                assert channel is not None
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                        VALUES(?, 'analytics-smoke', 'd', '[]', NULL, NULL, 'meta-analytics-smoke', ?)
                        """,
                        (int(channel["id"]), dbm.now_ts()),
                    ).lastrowid
                )
                job_id = dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=release_id,
                    job_type="UI",
                    state="DRAFT",
                    stage="DRAFT",
                    priority=0,
                    attempt=0,
                    created_at=dbm.now_ts(),
                    updated_at=dbm.now_ts(),
                )
                self.assertGreater(job_id, 0)
                planned_id = conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                    VALUES('darkwood-reverie', 'LONG', 'p', '2026-03-01T00:00:00Z', NULL, 'PLANNED', '2026-03-01T00:00:00Z', '2026-03-01T00:00:00Z')
                    """
                ).lastrowid
                self.assertGreater(int(planned_id), 0)
            finally:
                conn.close()

    def test_mf2_external_unique_constraints(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO analytics_external_scope_status(
                        provider_name, target_scope_type, target_scope_ref,
                        last_successful_sync_at, last_attempted_sync_at, sync_state, freshness_status,
                        coverage_payload_json, availability_status, updated_at
                    ) VALUES('YOUTUBE', 'CHANNEL', 'darkwood-reverie', NULL, 1.0, 'RUNNING', 'UNKNOWN', '{}', 'NOT_YET_SYNCED', 1.0)
                    """
                )
                with self.assertRaises(Exception):
                    conn.execute(
                        """
                        INSERT INTO analytics_external_scope_status(
                            provider_name, target_scope_type, target_scope_ref,
                            last_successful_sync_at, last_attempted_sync_at, sync_state, freshness_status,
                            coverage_payload_json, availability_status, updated_at
                        ) VALUES('YOUTUBE', 'CHANNEL', 'darkwood-reverie', NULL, 2.0, 'RUNNING', 'UNKNOWN', '{}', 'NOT_YET_SYNCED', 2.0)
                        """
                    )

                conn.execute(
                    """
                    INSERT INTO analytics_youtube_video_links(
                        channel_slug, youtube_video_id, release_id, job_id, youtube_channel_id,
                        linkage_confidence, linkage_source, payload_json, created_at, updated_at
                    ) VALUES('darkwood-reverie', 'yt-video-1', NULL, NULL, NULL, 'EXACT', 'MANUAL_LINK', '{}', 1.0, 1.0)
                    """
                )
                with self.assertRaises(Exception):
                    conn.execute(
                        """
                        INSERT INTO analytics_youtube_video_links(
                            channel_slug, youtube_video_id, release_id, job_id, youtube_channel_id,
                            linkage_confidence, linkage_source, payload_json, created_at, updated_at
                        ) VALUES('darkwood-reverie', 'yt-video-1', NULL, NULL, NULL, 'EXACT', 'MANUAL_LINK', '{}', 2.0, 2.0)
                        """
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
