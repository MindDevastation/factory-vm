from __future__ import annotations

import json
import sqlite3
import time
import re
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.common.env import Env


# Canonical UI/render jobs state domain used by Jobs page filtering.
# Keep this list ordered for stable API/UI presentation.
UI_JOB_STATES: tuple[str, ...] = (
    "DRAFT",
    "WAITING_INPUTS",
    "FETCHING_INPUTS",
    "READY_FOR_RENDER",
    "RENDERING",
    "RENDER_FAILED",
    "FAILED",
    "QA_RUNNING",
    "QA_FAILED",
    "UPLOADING",
    "UPLOAD_FAILED",
    "WAIT_APPROVAL",
    "APPROVED",
    "REJECTED",
    "PUBLISHED",
    "CANCELLED",
    "CLEANED",
)


def _dict_factory(cursor: sqlite3.Cursor, row: Tuple[Any, ...]) -> Dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def connect(env: Env) -> sqlite3.Connection:
    Path(env.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(env.db_path, timeout=30, isolation_level=None)
    conn.row_factory = _dict_factory
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    _ensure_track_analyzer_schema_tables(conn)

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            youtube_channel_id TEXT UNIQUE,
            kind TEXT NOT NULL,
            weight REAL NOT NULL DEFAULT 1.0,
            render_profile TEXT NOT NULL,
            autopublish_enabled INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS render_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            video_w INTEGER NOT NULL,
            video_h INTEGER NOT NULL,
            fps REAL NOT NULL,
            vcodec_required TEXT NOT NULL,
            audio_sr INTEGER NOT NULL,
            audio_ch INTEGER NOT NULL,
            acodec_required TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            tags_json TEXT NOT NULL,
            planned_at TEXT,
            origin_release_folder_id TEXT,
            origin_meta_file_id TEXT UNIQUE,
            current_open_job_id INTEGER NULL,
            created_at REAL NOT NULL,
            FOREIGN KEY(channel_id) REFERENCES channels(id),
            FOREIGN KEY(current_open_job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS planned_releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_slug TEXT NOT NULL,
            content_type TEXT NOT NULL,
            title TEXT NULL,
            publish_at TEXT NULL,
            notes TEXT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK(status IN ('PLANNED','LOCKED','FAILED')),
            UNIQUE(channel_slug, publish_at)
        );

        CREATE INDEX IF NOT EXISTS idx_pr_channel_slug ON planned_releases(channel_slug);
        CREATE INDEX IF NOT EXISTS idx_pr_content_type ON planned_releases(content_type);
        CREATE INDEX IF NOT EXISTS idx_pr_publish_at ON planned_releases(publish_at);
        CREATE INDEX IF NOT EXISTS idx_pr_status ON planned_releases(status);
        CREATE INDEX IF NOT EXISTS idx_pr_title ON planned_releases(title);

        CREATE TABLE IF NOT EXISTS monthly_planning_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            template_name TEXT NOT NULL,
            content_type TEXT NULL,
            status TEXT NOT NULL,
            usage_summary_json TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            archived_at TEXT NULL,
            created_by TEXT NULL,
            updated_by TEXT NULL,
            archived_by TEXT NULL,
            FOREIGN KEY(channel_id) REFERENCES channels(id),
            CHECK(status IN ('ACTIVE','ARCHIVED'))
        );

        CREATE INDEX IF NOT EXISTS idx_mpt_channel_status
            ON monthly_planning_templates(channel_id, status);

        CREATE INDEX IF NOT EXISTS idx_mpt_channel_name
            ON monthly_planning_templates(channel_id, template_name);

        CREATE TABLE IF NOT EXISTS monthly_planning_template_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            item_key TEXT NOT NULL,
            slot_code TEXT NOT NULL,
            position INTEGER NOT NULL,
            title TEXT NOT NULL,
            day_of_month INTEGER NULL,
            notes TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(template_id) REFERENCES monthly_planning_templates(id) ON DELETE CASCADE,
            UNIQUE(template_id, item_key),
            UNIQUE(template_id, slot_code),
            UNIQUE(template_id, position)
        );

        CREATE INDEX IF NOT EXISTS idx_mpti_template_position
            ON monthly_planning_template_items(template_id, position);

        CREATE TABLE IF NOT EXISTS monthly_planning_template_apply_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            target_month TEXT NOT NULL,
            preview_fingerprint TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT NULL,
            status TEXT NOT NULL,
            request_id TEXT NOT NULL,
            created_count INTEGER NOT NULL DEFAULT 0,
            blocked_duplicate_count INTEGER NOT NULL DEFAULT 0,
            blocked_invalid_date_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(template_id) REFERENCES monthly_planning_templates(id),
            FOREIGN KEY(channel_id) REFERENCES channels(id),
            CHECK(status IN ('STARTED','COMPLETED','FAILED'))
        );

        CREATE INDEX IF NOT EXISTS idx_mptar_template_month
            ON monthly_planning_template_apply_runs(template_id, target_month);

        CREATE INDEX IF NOT EXISTS idx_mptar_channel_month
            ON monthly_planning_template_apply_runs(channel_id, target_month);

        CREATE TABLE IF NOT EXISTS monthly_planning_template_apply_run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apply_run_id INTEGER NOT NULL,
            template_item_key TEXT NOT NULL,
            slot_code TEXT NOT NULL,
            position INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            planned_release_id INTEGER NULL,
            reason_code TEXT NULL,
            reason_message TEXT NULL,
            FOREIGN KEY(apply_run_id) REFERENCES monthly_planning_template_apply_runs(id) ON DELETE CASCADE,
            FOREIGN KEY(planned_release_id) REFERENCES planned_releases(id),
            CHECK(outcome IN ('CREATED','BLOCKED_DUPLICATE','BLOCKED_INVALID_DATE','FAILED_INTERNAL'))
        );

        CREATE INDEX IF NOT EXISTS idx_mptari_apply_run_position
            ON monthly_planning_template_apply_run_items(apply_run_id, position);

        CREATE TABLE IF NOT EXISTS planner_release_links (
            planned_release_id INTEGER PRIMARY KEY,
            release_id INTEGER NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            created_by TEXT NULL,
            FOREIGN KEY(planned_release_id) REFERENCES planned_releases(id),
            FOREIGN KEY(release_id) REFERENCES releases(id)
        );

        CREATE INDEX IF NOT EXISTS idx_planner_release_links_release_id
            ON planner_release_links(release_id);

        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            origin TEXT NOT NULL,
            origin_id TEXT,
            name TEXT,
            path TEXT,
            sha256 TEXT,
            duration_sec REAL,
            created_at REAL NOT NULL,
            FOREIGN KEY(channel_id) REFERENCES channels(id)
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            state TEXT NOT NULL,
            stage TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            attempt INTEGER NOT NULL DEFAULT 0,
            locked_by TEXT,
            locked_at REAL,
            retry_at REAL,
            progress_pct REAL NOT NULL DEFAULT 0.0,
            progress_text TEXT,
            progress_updated_at REAL,
            error_reason TEXT,
            approval_notified_at REAL,
            published_at REAL,
            delete_mp4_at REAL,
            retry_of_job_id INTEGER UNIQUE,
            root_job_id INTEGER NOT NULL,
            attempt_no INTEGER NOT NULL DEFAULT 1,
            force_refetch_inputs INTEGER NOT NULL DEFAULT 0,
            publish_state TEXT,
            publish_target_visibility TEXT,
            publish_delivery_mode_effective TEXT,
            publish_resolved_scope TEXT,
            publish_reason_code TEXT,
            publish_reason_detail TEXT,
            publish_scheduled_at REAL,
            publish_attempt_count INTEGER NOT NULL DEFAULT 0,
            publish_retry_at REAL,
            publish_last_error_code TEXT,
            publish_last_error_message TEXT,
            publish_in_progress_at REAL,
            publish_last_transition_at REAL,
            publish_hold_active INTEGER NOT NULL DEFAULT 0,
            publish_hold_reason_code TEXT,
            publish_manual_ack_at REAL,
            publish_manual_completed_at REAL,
            publish_manual_published_at REAL,
            publish_manual_video_id TEXT,
            publish_manual_url TEXT,
            publish_drift_detected_at REAL,
            publish_observed_visibility TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id),
            FOREIGN KEY(retry_of_job_id) REFERENCES jobs(id),
            FOREIGN KEY(root_job_id) REFERENCES jobs(id),
            CHECK(attempt_no >= 1),
            CHECK(
                publish_state IS NULL
                OR publish_state IN (
                    'private_uploaded',
                    'policy_blocked',
                    'waiting_for_schedule',
                    'ready_to_publish',
                    'publish_in_progress',
                    'retry_pending',
                    'manual_handoff_pending',
                    'manual_handoff_acknowledged',
                    'manual_publish_completed',
                    'published_public',
                    'published_unlisted',
                    'publish_failed_terminal',
                    'publish_state_drift_detected'
                )
            ),
            CHECK(
                publish_target_visibility IS NULL
                OR publish_target_visibility IN ('public', 'unlisted')
            ),
            CHECK(
                publish_delivery_mode_effective IS NULL
                OR publish_delivery_mode_effective IN ('automatic', 'manual')
            ),
            CHECK(
                publish_resolved_scope IS NULL
                OR publish_resolved_scope IN ('project', 'channel', 'item')
            ),
            CHECK(publish_state != 'retry_pending' OR publish_retry_at IS NOT NULL)
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_state_priority ON jobs(state, priority, created_at);

        CREATE TABLE IF NOT EXISTS job_inputs (
            job_id INTEGER NOT NULL,
            asset_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            order_index INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(asset_id) REFERENCES assets(id)
        );

        CREATE TABLE IF NOT EXISTS job_outputs (
            job_id INTEGER NOT NULL,
            asset_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(asset_id) REFERENCES assets(id)
        );

        CREATE TABLE IF NOT EXISTS qa_reports (
            job_id INTEGER PRIMARY KEY,
            hard_ok INTEGER NOT NULL,
            warnings_json TEXT NOT NULL,
            info_json TEXT NOT NULL,
            duration_expected REAL,
            duration_actual REAL,
            vcodec TEXT,
            acodec TEXT,
            fps REAL,
            width INTEGER,
            height INTEGER,
            sr INTEGER,
            ch INTEGER,
            mean_volume_db REAL,
            max_volume_db REAL,
            created_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS approvals (
            job_id INTEGER PRIMARY KEY,
            decision TEXT NOT NULL,
            comment TEXT NOT NULL,
            decided_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS youtube_uploads (
            job_id INTEGER PRIMARY KEY,
            video_id TEXT NOT NULL,
            url TEXT NOT NULL,
            studio_url TEXT NOT NULL,
            privacy TEXT NOT NULL,
            uploaded_at REAL NOT NULL,
            error TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS tg_messages (
            job_id INTEGER PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            created_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS tg_pending (
            user_id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS worker_heartbeats (
            worker_id TEXT PRIMARY KEY,
            role TEXT NOT NULL,
            pid INTEGER NOT NULL,
            hostname TEXT NOT NULL,
            details_json TEXT NOT NULL,
            last_seen REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_last_seen ON worker_heartbeats(last_seen);

        CREATE TABLE IF NOT EXISTS ui_job_drafts (
            job_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            tags_csv TEXT NOT NULL,
            cover_name TEXT,
            cover_ext TEXT,
            background_name TEXT NOT NULL,
            background_ext TEXT NOT NULL,
            audio_ids_text TEXT NOT NULL,
            playlist_builder_override_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(channel_id) REFERENCES channels(id)
        );

        CREATE TABLE IF NOT EXISTS release_visual_configs (
            release_id INTEGER PRIMARY KEY,
            intent_config_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id)
        );

        CREATE TABLE IF NOT EXISTS release_visual_preview_snapshots (
            id TEXT PRIMARY KEY,
            release_id INTEGER NOT NULL,
            intent_snapshot_json TEXT NOT NULL,
            preview_package_json TEXT NOT NULL,
            created_by TEXT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id)
        );

        CREATE INDEX IF NOT EXISTS idx_release_visual_preview_snapshots_release_created
            ON release_visual_preview_snapshots(release_id, created_at);

        CREATE TABLE IF NOT EXISTS release_visual_approved_previews (
            release_id INTEGER PRIMARY KEY,
            preview_id TEXT NOT NULL UNIQUE,
            approved_by TEXT NULL,
            approved_at TEXT NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id),
            FOREIGN KEY(preview_id) REFERENCES release_visual_preview_snapshots(id)
        );

        CREATE TABLE IF NOT EXISTS release_visual_applied_packages (
            release_id INTEGER PRIMARY KEY,
            background_asset_id INTEGER NOT NULL,
            cover_asset_id INTEGER NOT NULL,
            source_preview_id TEXT NULL,
            applied_by TEXT NULL,
            applied_at TEXT NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id),
            FOREIGN KEY(background_asset_id) REFERENCES assets(id),
            FOREIGN KEY(cover_asset_id) REFERENCES assets(id),
            FOREIGN KEY(source_preview_id) REFERENCES release_visual_preview_snapshots(id)
        );

        CREATE TABLE IF NOT EXISTS playlist_builder_channel_settings (
            channel_slug TEXT PRIMARY KEY,
            default_generation_mode TEXT NOT NULL,
            min_duration_min INTEGER NOT NULL,
            max_duration_min INTEGER NOT NULL,
            tolerance_min INTEGER NOT NULL,
            preferred_month_batch TEXT,
            preferred_batch_ratio INTEGER NOT NULL DEFAULT 70,
            allow_cross_channel INTEGER NOT NULL DEFAULT 0,
            novelty_target_min REAL NOT NULL DEFAULT 0.50,
            novelty_target_max REAL NOT NULL DEFAULT 0.80,
            position_memory_window INTEGER NOT NULL DEFAULT 20,
            strictness_mode TEXT NOT NULL DEFAULT 'balanced',
            vocal_policy TEXT NOT NULL,
            reuse_policy TEXT NOT NULL DEFAULT 'avoid_recent',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS playlist_history (
            id INTEGER PRIMARY KEY,
            channel_slug TEXT NOT NULL,
            job_id INTEGER,
            history_stage TEXT NOT NULL,
            source_preview_id TEXT,
            generation_mode TEXT NOT NULL,
            strictness_mode TEXT NOT NULL,
            playlist_duration_sec REAL NOT NULL,
            tracks_count INTEGER NOT NULL,
            set_fingerprint TEXT NOT NULL,
            ordered_fingerprint TEXT NOT NULL,
            prefix_fingerprint_n3 TEXT NOT NULL,
            prefix_fingerprint_n5 TEXT NOT NULL,
            novelty_against_prev REAL,
            batch_overlap_score REAL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_playlist_history_channel_stage_created
            ON playlist_history(channel_slug, history_stage, created_at);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_playlist_history_unique_draft_preview
            ON playlist_history(source_preview_id)
            WHERE history_stage = 'DRAFT' AND source_preview_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS playlist_history_items (
            id INTEGER PRIMARY KEY,
            history_id INTEGER NOT NULL,
            position_index INTEGER NOT NULL,
            track_pk INTEGER NOT NULL,
            month_batch TEXT,
            duration_sec REAL,
            channel_slug TEXT NOT NULL,
            FOREIGN KEY(history_id) REFERENCES playlist_history(id)
        );

        CREATE INDEX IF NOT EXISTS idx_playlist_history_items_track_pos
            ON playlist_history_items(track_pk, position_index);

        CREATE INDEX IF NOT EXISTS idx_playlist_history_items_history_pos
            ON playlist_history_items(history_id, position_index);

        CREATE TABLE IF NOT EXISTS playlist_build_previews (
            id TEXT PRIMARY KEY,
            job_id INTEGER,
            channel_slug TEXT NOT NULL,
            effective_brief_json TEXT NOT NULL,
            preview_result_json TEXT NOT NULL,
            created_by TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metadata_preview_sessions (
            id TEXT PRIMARY KEY,
            release_id INTEGER NOT NULL,
            channel_slug TEXT NOT NULL,
            session_status TEXT NOT NULL,
            requested_fields_json TEXT NOT NULL,
            current_bundle_json TEXT NOT NULL,
            proposed_bundle_json TEXT NOT NULL,
            sources_json TEXT NOT NULL,
            field_statuses_json TEXT NOT NULL,
            dependency_fingerprints_json TEXT NOT NULL,
            warnings_json TEXT NOT NULL,
            errors_json TEXT NOT NULL,
            fields_snapshot_json TEXT NOT NULL DEFAULT '{}',
            effective_source_selection_json TEXT NOT NULL DEFAULT '{}',
            effective_source_provenance_json TEXT NOT NULL DEFAULT '{}',
            created_by TEXT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            applied_at TEXT NULL,
            CHECK(session_status IN ('OPEN','APPLIED','EXPIRED','INVALIDATED'))
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_preview_sessions_release_id
            ON metadata_preview_sessions(release_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_metadata_preview_sessions_expires_at
            ON metadata_preview_sessions(expires_at);

        CREATE INDEX IF NOT EXISTS idx_metadata_preview_sessions_status
            ON metadata_preview_sessions(session_status, created_at);

        CREATE TABLE IF NOT EXISTS metadata_bulk_preview_sessions (
            id TEXT PRIMARY KEY,
            planner_context_json TEXT NOT NULL,
            selected_item_ids_json TEXT NOT NULL,
            requested_fields_json TEXT NOT NULL,
            selected_channels_json TEXT NOT NULL,
            session_status TEXT NOT NULL,
            aggregate_summary_json TEXT NOT NULL,
            item_states_json TEXT NOT NULL,
            created_by TEXT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            applied_at TEXT NULL,
            CHECK(session_status IN ('OPEN','APPLIED','EXPIRED','INVALIDATED'))
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_bulk_preview_sessions_status
            ON metadata_bulk_preview_sessions(session_status, created_at);

        CREATE INDEX IF NOT EXISTS idx_metadata_bulk_preview_sessions_expires_at
            ON metadata_bulk_preview_sessions(expires_at);

        CREATE TABLE IF NOT EXISTS planner_mass_action_sessions (
            id TEXT PRIMARY KEY,
            action_type TEXT NOT NULL,
            planner_scope_fingerprint TEXT NOT NULL,
            selected_item_ids_json TEXT NOT NULL,
            preview_status TEXT NOT NULL,
            aggregate_preview_json TEXT NOT NULL,
            item_preview_json TEXT NOT NULL,
            created_by TEXT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            executed_at TEXT NULL,
            CHECK(action_type IN ('BATCH_MATERIALIZE_SELECTED','BATCH_CREATE_JOBS_FOR_SELECTED')),
            CHECK(preview_status IN ('OPEN','EXECUTED','EXPIRED','INVALIDATED'))
        );

        CREATE INDEX IF NOT EXISTS idx_planner_mass_action_sessions_status
            ON planner_mass_action_sessions(preview_status, created_at);

        CREATE INDEX IF NOT EXISTS idx_planner_mass_action_sessions_expires_at
            ON planner_mass_action_sessions(expires_at);

        CREATE TABLE IF NOT EXISTS title_templates (
            id INTEGER PRIMARY KEY,
            channel_slug TEXT NOT NULL,
            template_name TEXT NOT NULL,
            template_body TEXT NOT NULL,
            status TEXT NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            validation_status TEXT NOT NULL,
            validation_errors_json TEXT NULL,
            last_validated_at TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            archived_at TEXT NULL,
            CHECK(status IN ('ACTIVE','ARCHIVED')),
            CHECK(validation_status IN ('VALID','INVALID')),
            CHECK(LENGTH(TRIM(template_name)) > 0),
            CHECK(LENGTH(TRIM(template_body)) > 0)
        );

        CREATE INDEX IF NOT EXISTS idx_title_templates_channel_slug
            ON title_templates(channel_slug);

        CREATE INDEX IF NOT EXISTS idx_title_templates_channel_slug_status
            ON title_templates(channel_slug, status);

        CREATE INDEX IF NOT EXISTS idx_title_templates_channel_slug_updated_at
            ON title_templates(channel_slug, updated_at);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_title_templates_active_default_unique
            ON title_templates(channel_slug)
            WHERE status = 'ACTIVE' AND is_default = 1;

        CREATE TABLE IF NOT EXISTS description_templates (
            id INTEGER PRIMARY KEY,
            channel_slug TEXT NOT NULL,
            template_name TEXT NOT NULL,
            template_body TEXT NOT NULL,
            status TEXT NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            validation_status TEXT NOT NULL,
            validation_errors_json TEXT NULL,
            last_validated_at TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            archived_at TEXT NULL,
            CHECK(status IN ('ACTIVE','ARCHIVED')),
            CHECK(validation_status IN ('VALID','INVALID')),
            CHECK(LENGTH(TRIM(template_name)) > 0),
            CHECK(LENGTH(TRIM(template_body)) > 0)
        );

        CREATE INDEX IF NOT EXISTS idx_description_templates_channel_slug
            ON description_templates(channel_slug);

        CREATE INDEX IF NOT EXISTS idx_description_templates_channel_slug_status
            ON description_templates(channel_slug, status);

        CREATE INDEX IF NOT EXISTS idx_description_templates_channel_slug_updated_at
            ON description_templates(channel_slug, updated_at);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_description_templates_active_default_unique
            ON description_templates(channel_slug)
            WHERE status = 'ACTIVE' AND is_default = 1;

        CREATE TABLE IF NOT EXISTS video_tag_presets (
            id INTEGER PRIMARY KEY,
            channel_slug TEXT NOT NULL,
            preset_name TEXT NOT NULL,
            preset_body_json TEXT NOT NULL,
            status TEXT NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            validation_status TEXT NOT NULL,
            validation_errors_json TEXT NULL,
            last_validated_at TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            archived_at TEXT NULL,
            CHECK(status IN ('ACTIVE','ARCHIVED')),
            CHECK(validation_status IN ('VALID','INVALID')),
            CHECK(LENGTH(TRIM(preset_name)) > 0),
            CHECK(json_valid(preset_body_json)),
            CHECK(json_type(preset_body_json) = 'array'),
            CHECK(json_array_length(preset_body_json) > 0)
        );

        CREATE INDEX IF NOT EXISTS idx_video_tag_presets_channel_slug
            ON video_tag_presets(channel_slug);

        CREATE INDEX IF NOT EXISTS idx_video_tag_presets_channel_slug_status
            ON video_tag_presets(channel_slug, status);

        CREATE INDEX IF NOT EXISTS idx_video_tag_presets_channel_slug_updated_at
            ON video_tag_presets(channel_slug, updated_at);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_video_tag_presets_active_default_unique
            ON video_tag_presets(channel_slug)
            WHERE status = 'ACTIVE' AND is_default = 1;

        CREATE TABLE IF NOT EXISTS channel_visual_style_templates (
            id INTEGER PRIMARY KEY,
            channel_slug TEXT NOT NULL,
            template_name TEXT NOT NULL,
            template_payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            validation_status TEXT NOT NULL,
            validation_errors_json TEXT NULL,
            last_validated_at TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            archived_at TEXT NULL,
            CHECK(status IN ('ACTIVE','ARCHIVED')),
            CHECK(validation_status IN ('VALID','INVALID')),
            CHECK(LENGTH(TRIM(template_name)) > 0),
            CHECK(json_valid(template_payload_json))
        );

        CREATE INDEX IF NOT EXISTS idx_channel_visual_style_templates_channel_slug
            ON channel_visual_style_templates(channel_slug);

        CREATE INDEX IF NOT EXISTS idx_channel_visual_style_templates_channel_slug_status
            ON channel_visual_style_templates(channel_slug, status);

        CREATE INDEX IF NOT EXISTS idx_channel_visual_style_templates_channel_slug_updated_at
            ON channel_visual_style_templates(channel_slug, updated_at);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_visual_style_templates_active_default_unique
            ON channel_visual_style_templates(channel_slug)
            WHERE status = 'ACTIVE' AND is_default = 1;

        CREATE TABLE IF NOT EXISTS channel_metadata_defaults (
            channel_slug TEXT PRIMARY KEY,
            default_title_template_id INTEGER NULL,
            default_description_template_id INTEGER NULL,
            default_video_tag_preset_id INTEGER NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(channel_slug) REFERENCES channels(slug)
        );

        CREATE TABLE IF NOT EXISTS publish_audit_status_project_defaults (
            singleton_key INTEGER PRIMARY KEY CHECK(singleton_key = 1),
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            last_reason TEXT NOT NULL,
            last_request_id TEXT NOT NULL,
            CHECK(status IN ('unknown', 'pending', 'approved', 'rejected', 'manual-only', 'suspended'))
        );

        CREATE TABLE IF NOT EXISTS publish_audit_status_channel_overrides (
            channel_slug TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            last_reason TEXT NOT NULL,
            last_request_id TEXT NOT NULL,
            FOREIGN KEY(channel_slug) REFERENCES channels(slug),
            CHECK(status IN ('unknown', 'pending', 'approved', 'rejected', 'manual-only', 'suspended'))
        );

        CREATE INDEX IF NOT EXISTS idx_publish_audit_status_channel_overrides_status
            ON publish_audit_status_channel_overrides(status);

        CREATE TABLE IF NOT EXISTS publish_audit_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_type TEXT NOT NULL,
            channel_slug TEXT NULL,
            previous_status TEXT NULL,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            request_id TEXT NOT NULL,
            actor_identity TEXT NOT NULL,
            created_at TEXT NOT NULL,
            CHECK(scope_type IN ('project_default', 'channel_override')),
            CHECK(status IN ('unknown', 'pending', 'approved', 'rejected', 'manual-only', 'suspended'))
        );

        CREATE INDEX IF NOT EXISTS idx_publish_audit_status_history_created_at
            ON publish_audit_status_history(created_at, id);

        CREATE INDEX IF NOT EXISTS idx_publish_audit_status_history_scope
            ON publish_audit_status_history(scope_type, channel_slug, created_at, id);

        CREATE TABLE IF NOT EXISTS publish_policy_project_defaults (
            singleton_key INTEGER PRIMARY KEY CHECK(singleton_key = 1),
            publish_mode TEXT NULL,
            target_visibility TEXT NULL,
            reason_code TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            last_reason TEXT NOT NULL,
            last_request_id TEXT NOT NULL,
            CHECK(publish_mode IS NULL OR publish_mode IN ('auto', 'manual_only', 'hold')),
            CHECK(target_visibility IS NULL OR target_visibility IN ('public', 'unlisted'))
        );

        CREATE TABLE IF NOT EXISTS publish_policy_channel_overrides (
            channel_slug TEXT PRIMARY KEY,
            publish_mode TEXT NULL,
            target_visibility TEXT NULL,
            reason_code TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            last_reason TEXT NOT NULL,
            last_request_id TEXT NOT NULL,
            FOREIGN KEY(channel_slug) REFERENCES channels(slug),
            CHECK(publish_mode IS NULL OR publish_mode IN ('auto', 'manual_only', 'hold')),
            CHECK(target_visibility IS NULL OR target_visibility IN ('public', 'unlisted'))
        );

        CREATE INDEX IF NOT EXISTS idx_publish_policy_channel_overrides_mode
            ON publish_policy_channel_overrides(publish_mode);

        CREATE TABLE IF NOT EXISTS publish_policy_item_overrides (
            release_id INTEGER PRIMARY KEY,
            publish_mode TEXT NULL,
            target_visibility TEXT NULL,
            reason_code TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            last_reason TEXT NOT NULL,
            last_request_id TEXT NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id),
            CHECK(publish_mode IS NULL OR publish_mode IN ('auto', 'manual_only', 'hold')),
            CHECK(target_visibility IS NULL OR target_visibility IN ('public', 'unlisted'))
        );

        CREATE INDEX IF NOT EXISTS idx_publish_policy_item_overrides_mode
            ON publish_policy_item_overrides(publish_mode);

        CREATE TABLE IF NOT EXISTS publish_global_controls (
            singleton_key INTEGER PRIMARY KEY CHECK(singleton_key = 1),
            auto_publish_paused INTEGER NOT NULL DEFAULT 0 CHECK(auto_publish_paused IN (0, 1)),
            reason TEXT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT NOT NULL
        );


        CREATE TABLE IF NOT EXISTS publish_action_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_type TEXT NOT NULL,
            request_id TEXT NOT NULL,
            job_id INTEGER NOT NULL,
            actor_identity TEXT NOT NULL,
            reason TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            UNIQUE(action_type, request_id)
        );

        CREATE INDEX IF NOT EXISTS idx_publish_action_log_job_id
            ON publish_action_log(job_id, created_at, id);

        CREATE TABLE IF NOT EXISTS publish_bulk_action_sessions (
            id TEXT PRIMARY KEY,
            action_type TEXT NOT NULL,
            action_payload_json TEXT NOT NULL DEFAULT '{}',
            selection_fingerprint TEXT NOT NULL,
            selected_job_ids_json TEXT NOT NULL,
            preview_status TEXT NOT NULL,
            aggregate_preview_json TEXT NOT NULL,
            item_preview_json TEXT NOT NULL,
            invalidation_reason_code TEXT NULL,
            created_by TEXT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            executed_at TEXT NULL,
            CHECK(action_type IN ('retry','move_to_manual','acknowledge','reschedule','hold','unblock')),
            CHECK(preview_status IN ('OPEN','EXECUTED','EXPIRED','INVALIDATED'))
        );

        CREATE INDEX IF NOT EXISTS idx_publish_bulk_action_sessions_status
            ON publish_bulk_action_sessions(preview_status, created_at);

        CREATE INDEX IF NOT EXISTS idx_publish_bulk_action_sessions_expires_at
            ON publish_bulk_action_sessions(expires_at);

        CREATE TABLE IF NOT EXISTS publish_reconcile_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger_mode TEXT NOT NULL,
            status TEXT NOT NULL,
            error_code TEXT NULL,
            error_message TEXT NULL,
            total_jobs INTEGER NOT NULL DEFAULT 0,
            compared_jobs INTEGER NOT NULL DEFAULT 0,
            drift_count INTEGER NOT NULL DEFAULT 0,
            no_drift_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            CHECK(trigger_mode IN ('manual')),
            CHECK(status IN ('completed', 'source_unavailable'))
        );

        CREATE INDEX IF NOT EXISTS idx_publish_reconcile_runs_created_at
            ON publish_reconcile_runs(created_at, id);

        CREATE TABLE IF NOT EXISTS publish_reconcile_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            job_id INTEGER NOT NULL,
            release_id INTEGER NOT NULL,
            channel_slug TEXT NOT NULL,
            publish_state_snapshot TEXT NOT NULL,
            expected_visibility TEXT NOT NULL,
            observed_visibility TEXT NOT NULL,
            drift_classification TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES publish_reconcile_runs(id),
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(release_id) REFERENCES releases(id),
            CHECK(drift_classification IN ('drift_detected', 'no_drift'))
        );

        CREATE INDEX IF NOT EXISTS idx_publish_reconcile_items_run_id
            ON publish_reconcile_items(run_id, id);

        CREATE INDEX IF NOT EXISTS idx_publish_reconcile_items_classification
            ON publish_reconcile_items(drift_classification, run_id, id);

        CREATE TABLE IF NOT EXISTS canon_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_forbidden (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_palettes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_thresholds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_slug TEXT NOT NULL,
            track_id TEXT NOT NULL,
            gdrive_file_id TEXT NOT NULL UNIQUE,
            source TEXT,
            filename TEXT,
            title TEXT,
            artist TEXT,
            duration_sec REAL,
            month_batch TEXT,
            discovered_at REAL NOT NULL,
            analyzed_at REAL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_tracks_channel_slug_track_id
            ON tracks(channel_slug, track_id);

        CREATE TABLE IF NOT EXISTS track_features (
            track_pk INTEGER PRIMARY KEY,
            payload_json TEXT NOT NULL,
            computed_at REAL NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id)
        );

        CREATE TABLE IF NOT EXISTS track_tags (
            track_pk INTEGER PRIMARY KEY,
            payload_json TEXT NOT NULL,
            computed_at REAL NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id)
        );

        CREATE TABLE IF NOT EXISTS track_scores (
            track_pk INTEGER PRIMARY KEY,
            payload_json TEXT NOT NULL,
            computed_at REAL NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id)
        );

        CREATE TABLE IF NOT EXISTS track_analysis_flat (
            track_pk INTEGER PRIMARY KEY,
            channel_slug TEXT NOT NULL,
            track_id TEXT NOT NULL,
            gdrive_file_id TEXT NULL,
            analysis_computed_at REAL NOT NULL,
            analysis_status TEXT NOT NULL,
            analyzer_version TEXT NULL,
            schema_version TEXT NULL,
            duration_sec REAL NULL,
            true_peak_dbfs REAL NULL,
            spikes_found INTEGER NOT NULL DEFAULT 0,
            yamnet_top_tags_text TEXT NULL,
            yamnet_top_classes_json TEXT NULL,
            voice_flag INTEGER NOT NULL DEFAULT 0,
            voice_flag_reason TEXT NULL,
            speech_flag INTEGER NOT NULL DEFAULT 0,
            speech_flag_reason TEXT NULL,
            dominant_texture TEXT NULL,
            texture_confidence REAL NULL,
            texture_reason TEXT NULL,
            prohibited_cues_summary TEXT NULL,
            prohibited_cues_flags_json TEXT NULL,
            dsp_score REAL NULL,
            dsp_score_version TEXT NULL,
            dsp_notes TEXT NULL,
            legacy_scene TEXT NULL,
            legacy_mood TEXT NULL,
            legacy_safety REAL NULL,
            legacy_scene_match REAL NULL,
            human_readable_notes TEXT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id)
        );

        CREATE TABLE IF NOT EXISTS track_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            channel_slug TEXT,
            status TEXT NOT NULL,
            payload_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_track_jobs_type_status
            ON track_jobs(job_type, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_track_jobs_channel
            ON track_jobs(job_type, channel_slug, status, created_at);

        CREATE TABLE IF NOT EXISTS track_job_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            level TEXT,
            message TEXT NOT NULL,
            ts REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES track_jobs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_track_job_logs
            ON track_job_logs(job_id, ts);

        CREATE TABLE IF NOT EXISTS custom_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            label TEXT NOT NULL,
            category TEXT NOT NULL,
            description TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK(category IN ('VISUAL','MOOD','THEME')),
            UNIQUE(category, code)
        );

        CREATE INDEX IF NOT EXISTS idx_custom_tags_category
            ON custom_tags(category);

        CREATE INDEX IF NOT EXISTS idx_custom_tags_is_active
            ON custom_tags(is_active);

        CREATE TABLE IF NOT EXISTS custom_tag_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag_id INTEGER NOT NULL,
            source_path TEXT NOT NULL,
            operator TEXT NOT NULL,
            value_json TEXT NOT NULL,
            match_mode TEXT NOT NULL DEFAULT 'ALL',
            priority INTEGER NOT NULL DEFAULT 100,
            weight REAL,
            required INTEGER NOT NULL DEFAULT 0,
            stop_after_match INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(tag_id) REFERENCES custom_tags(id),
            CHECK(match_mode IN ('ALL','ANY'))
        );

        CREATE INDEX IF NOT EXISTS idx_ctr_tag_id
            ON custom_tag_rules(tag_id);

        CREATE INDEX IF NOT EXISTS idx_ctr_priority
            ON custom_tag_rules(priority);

        CREATE INDEX IF NOT EXISTS idx_ctr_active
            ON custom_tag_rules(is_active);

        CREATE TABLE IF NOT EXISTS custom_tag_channel_bindings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag_id INTEGER NOT NULL,
            channel_slug TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(tag_id) REFERENCES custom_tags(id),
            UNIQUE(tag_id, channel_slug)
        );

        CREATE INDEX IF NOT EXISTS idx_ctcb_tag_id
            ON custom_tag_channel_bindings(tag_id);

        CREATE INDEX IF NOT EXISTS idx_ctcb_channel_slug
            ON custom_tag_channel_bindings(channel_slug);

        CREATE TABLE IF NOT EXISTS track_custom_tag_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_pk INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            state TEXT NOT NULL,
            assigned_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id),
            FOREIGN KEY(tag_id) REFERENCES custom_tags(id),
            CHECK(state IN ('AUTO','MANUAL','SUPPRESSED')),
            UNIQUE(track_pk, tag_id)
        );

        CREATE INDEX IF NOT EXISTS idx_tcta_track_pk
            ON track_custom_tag_assignments(track_pk);

        CREATE INDEX IF NOT EXISTS idx_tcta_tag_id
            ON track_custom_tag_assignments(tag_id);

        CREATE INDEX IF NOT EXISTS idx_tcta_track_state
            ON track_custom_tag_assignments(track_pk, state);
        """
    )

    # Backward-compatible additive migrations for older DBs (SQLite doesn't support IF NOT EXISTS for ADD COLUMN).
    _ensure_jobs_columns(conn)
    _ensure_channels_columns(conn)
    _ensure_ui_job_drafts_columns(conn)
    _ensure_tracks_columns(conn)
    _ensure_metadata_preview_sessions_columns(conn)
    _ensure_planned_release_materialization_binding(conn)
    _ensure_planned_releases_template_preview_foundation(conn)
    _ensure_monthly_planning_template_apply_schema(conn)
    _ensure_releases_current_open_job_relation(conn)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return {str(r.get("name")) for r in rows if isinstance(r, dict) and r.get("name")}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _next_legacy_table_name(conn: sqlite3.Connection, table: str) -> str:
    base = f"{table}__legacy"
    if not _table_exists(conn, base):
        return base

    ts = int(time.time())
    name = f"{base}_{ts}"
    while _table_exists(conn, name):
        ts += 1
        name = f"{base}_{ts}"
    return name


def _rename_table_to_legacy(conn: sqlite3.Connection, table: str) -> None:
    new_name = _next_legacy_table_name(conn, table)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"invalid table name: {table}")
    conn.execute(f"ALTER TABLE {table} RENAME TO {new_name}")


def _ensure_track_analyzer_schema_tables(conn: sqlite3.Connection) -> None:
    expected = {
        "canon_channels": {"id", "value"},
        "canon_tags": {"id", "value"},
        "canon_forbidden": {"id", "value"},
        "canon_palettes": {"id", "value"},
        "canon_thresholds": {"id", "value"},
    }
    for table, expected_cols in expected.items():
        if not _table_exists(conn, table):
            continue
        if _table_columns(conn, table) != expected_cols:
            _rename_table_to_legacy(conn, table)


def _ensure_jobs_columns(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "jobs")
    lineage_cols = {"retry_of_job_id", "root_job_id", "attempt_no", "force_refetch_inputs"}
    if not lineage_cols.issubset(cols):
        _migrate_jobs_retry_lineage(conn)
        cols = _table_columns(conn, "jobs")

    if "retry_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN retry_at REAL;")

    # These columns were added after the initial MVP schema. For older DBs created before
    # progress/error/approval fields existed, we add them additively.
    if "progress_pct" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN progress_pct REAL NOT NULL DEFAULT 0.0;")
    if "progress_text" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN progress_text TEXT;")
    if "progress_updated_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN progress_updated_at REAL;")
    if "error_reason" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN error_reason TEXT;")
    if "approval_notified_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN approval_notified_at REAL;")
    if "published_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN published_at REAL;")
    if "delete_mp4_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN delete_mp4_at REAL;")

    # Create index only after ensuring the column exists.
    with suppress(Exception):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_state_retry ON jobs(state, retry_at, priority, created_at);")
    with suppress(Exception):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_retry_of_job_id ON jobs(retry_of_job_id);")
    with suppress(Exception):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_root_job_id_attempt_no ON jobs(root_job_id, attempt_no);")
    if "publish_state" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_state TEXT;")
    if "publish_target_visibility" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_target_visibility TEXT;")
    if "publish_delivery_mode_effective" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_delivery_mode_effective TEXT;")
    if "publish_resolved_scope" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_resolved_scope TEXT;")
    if "publish_reason_code" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_reason_code TEXT;")
    if "publish_reason_detail" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_reason_detail TEXT;")
    if "publish_scheduled_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_scheduled_at REAL;")
    if "publish_attempt_count" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_attempt_count INTEGER NOT NULL DEFAULT 0;")
    if "publish_retry_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_retry_at REAL;")
    if "publish_last_error_code" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_last_error_code TEXT;")
    if "publish_last_error_message" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_last_error_message TEXT;")
    if "publish_in_progress_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_in_progress_at REAL;")
    if "publish_last_transition_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_last_transition_at REAL;")
    if "publish_hold_active" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_hold_active INTEGER NOT NULL DEFAULT 0;")
    if "publish_hold_reason_code" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_hold_reason_code TEXT;")
    if "publish_manual_ack_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_manual_ack_at REAL;")
    if "publish_manual_completed_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_manual_completed_at REAL;")
    if "publish_manual_published_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_manual_published_at REAL;")
    if "publish_manual_video_id" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_manual_video_id TEXT;")
    if "publish_manual_url" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_manual_url TEXT;")
    if "publish_drift_detected_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_drift_detected_at REAL;")
    if "publish_observed_visibility" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN publish_observed_visibility TEXT;")

    with suppress(Exception):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_publish_runtime_state_id
            ON jobs(publish_state ASC, id ASC)
            WHERE publish_state IS NOT NULL;
            """
        )
    with suppress(Exception):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_publish_runtime_retry_due
            ON jobs(publish_state ASC, publish_retry_at ASC, id ASC)
            WHERE publish_state = 'retry_pending' AND publish_retry_at IS NOT NULL;
            """
        )


def _ensure_planned_release_materialization_binding(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "planned_releases")
    if "materialized_release_id" not in cols:
        with suppress(Exception):
            conn.execute(
                """
                ALTER TABLE planned_releases
                ADD COLUMN materialized_release_id INTEGER NULL REFERENCES releases(id);
                """
            )
    with suppress(Exception):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_materialized_release_unique
            ON planned_releases(materialized_release_id)
            WHERE materialized_release_id IS NOT NULL;
            """
        )


def _ensure_planned_releases_template_preview_foundation(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "planned_releases")
    if "planning_slot_code" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE planned_releases ADD COLUMN planning_slot_code TEXT NULL;")
    if "source_template_id" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE planned_releases ADD COLUMN source_template_id INTEGER NULL;")
    if "source_template_item_key" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE planned_releases ADD COLUMN source_template_item_key TEXT NULL;")
    if "source_template_target_month" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE planned_releases ADD COLUMN source_template_target_month TEXT NULL;")
    if "source_template_apply_run_id" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE planned_releases ADD COLUMN source_template_apply_run_id INTEGER NULL;")

    with suppress(Exception):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pr_preview_slot_month
            ON planned_releases(channel_slug, planning_slot_code, publish_at);
            """
        )
    with suppress(Exception):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pr_preview_provenance_month
            ON planned_releases(source_template_id, source_template_item_key, source_template_target_month);
            """
        )
    with suppress(Exception):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_slot_month_unique
            ON planned_releases(channel_slug, planning_slot_code, substr(publish_at, 1, 7))
            WHERE planning_slot_code IS NOT NULL AND publish_at IS NOT NULL;
            """
        )
    with suppress(Exception):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_provenance_month_unique
            ON planned_releases(source_template_id, source_template_item_key, source_template_target_month)
            WHERE source_template_id IS NOT NULL
              AND source_template_item_key IS NOT NULL
              AND source_template_target_month IS NOT NULL;
            """
        )


def _ensure_monthly_planning_template_apply_schema(conn: sqlite3.Connection) -> None:
    with suppress(Exception):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_planning_template_apply_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                target_month TEXT NOT NULL,
                preview_fingerprint TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NULL,
                status TEXT NOT NULL,
                request_id TEXT NOT NULL,
                created_count INTEGER NOT NULL DEFAULT 0,
                blocked_duplicate_count INTEGER NOT NULL DEFAULT 0,
                blocked_invalid_date_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(template_id) REFERENCES monthly_planning_templates(id),
                FOREIGN KEY(channel_id) REFERENCES channels(id),
                CHECK(status IN ('STARTED','COMPLETED','FAILED'))
            );
            """
        )
    with suppress(Exception):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mptar_template_month
            ON monthly_planning_template_apply_runs(template_id, target_month);
            """
        )
    with suppress(Exception):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mptar_channel_month
            ON monthly_planning_template_apply_runs(channel_id, target_month);
            """
        )

    with suppress(Exception):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_planning_template_apply_run_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                apply_run_id INTEGER NOT NULL,
                template_item_key TEXT NOT NULL,
                slot_code TEXT NOT NULL,
                position INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                planned_release_id INTEGER NULL,
                reason_code TEXT NULL,
                reason_message TEXT NULL,
                FOREIGN KEY(apply_run_id) REFERENCES monthly_planning_template_apply_runs(id) ON DELETE CASCADE,
                FOREIGN KEY(planned_release_id) REFERENCES planned_releases(id),
                CHECK(outcome IN ('CREATED','BLOCKED_DUPLICATE','BLOCKED_INVALID_DATE','FAILED_INTERNAL'))
            );
            """
        )
    with suppress(Exception):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mptari_apply_run_position
            ON monthly_planning_template_apply_run_items(apply_run_id, position);
            """
        )


def _ensure_releases_current_open_job_relation(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "releases")
    if "current_open_job_id" not in cols:
        with suppress(Exception):
            conn.execute(
                """
                ALTER TABLE releases
                ADD COLUMN current_open_job_id INTEGER NULL REFERENCES jobs(id);
                """
            )
    with suppress(Exception):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_releases_current_open_job_unique
            ON releases(current_open_job_id)
            WHERE current_open_job_id IS NOT NULL;
            """
        )


def _migrate_jobs_retry_lineage(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "jobs")

    def _sel(col: str, default_expr: str) -> str:
        return col if col in cols else default_expr

    conn.execute("PRAGMA foreign_keys=OFF;")
    try:
        conn.execute(
            """
            CREATE TABLE jobs__new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            state TEXT NOT NULL,
            stage TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            attempt INTEGER NOT NULL DEFAULT 0,
            locked_by TEXT,
            locked_at REAL,
            retry_at REAL,
            progress_pct REAL NOT NULL DEFAULT 0.0,
            progress_text TEXT,
            progress_updated_at REAL,
            error_reason TEXT,
            approval_notified_at REAL,
            published_at REAL,
            delete_mp4_at REAL,
            retry_of_job_id INTEGER UNIQUE,
            root_job_id INTEGER NOT NULL,
            attempt_no INTEGER NOT NULL DEFAULT 1,
            force_refetch_inputs INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id),
            FOREIGN KEY(retry_of_job_id) REFERENCES jobs__new(id),
            FOREIGN KEY(root_job_id) REFERENCES jobs__new(id),
            CHECK(attempt_no >= 1)
        );
            """
        )
        conn.execute(
            f"""
            INSERT INTO jobs__new (
                id, release_id, job_type, state, stage, priority, attempt, locked_by, locked_at,
                retry_at, progress_pct, progress_text, progress_updated_at, error_reason,
                approval_notified_at, published_at, delete_mp4_at, retry_of_job_id, root_job_id,
                attempt_no, force_refetch_inputs, created_at, updated_at
            )
            SELECT
                id, release_id, job_type, state, stage, {_sel('priority', '0')}, {_sel('attempt', '0')},
                {_sel('locked_by', 'NULL')}, {_sel('locked_at', 'NULL')}, {_sel('retry_at', 'NULL')},
                {_sel('progress_pct', '0.0')}, {_sel('progress_text', 'NULL')}, {_sel('progress_updated_at', 'NULL')},
                {_sel('error_reason', 'NULL')}, {_sel('approval_notified_at', 'NULL')}, {_sel('published_at', 'NULL')},
                {_sel('delete_mp4_at', 'NULL')}, NULL, id, 1, 0, created_at, {_sel('updated_at', 'created_at')}
            FROM jobs;
            """
        )
        conn.execute("DROP TABLE jobs;")
        conn.execute("ALTER TABLE jobs__new RENAME TO jobs;")
        conn.execute("CREATE INDEX idx_jobs_state_priority ON jobs(state, priority, created_at);")
        conn.execute("CREATE INDEX idx_jobs_state_retry ON jobs(state, retry_at, priority, created_at);")
        conn.execute("CREATE INDEX idx_jobs_retry_of_job_id ON jobs(retry_of_job_id);")
        conn.execute("CREATE INDEX idx_jobs_root_job_id_attempt_no ON jobs(root_job_id, attempt_no);")
    finally:
        conn.execute("PRAGMA foreign_keys=ON;")


def _ensure_channels_columns(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "channels")
    if "youtube_channel_id" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE channels ADD COLUMN youtube_channel_id TEXT;")

    with suppress(Exception):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_youtube_channel_id_unique
            ON channels(youtube_channel_id)
            WHERE youtube_channel_id IS NOT NULL;
            """
        )


def _ensure_ui_job_drafts_columns(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "ui_job_drafts")
    if "playlist_builder_override_json" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE ui_job_drafts ADD COLUMN playlist_builder_override_json TEXT;")


def _ensure_tracks_columns(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "tracks")
    if "month_batch" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE tracks ADD COLUMN month_batch TEXT;")


def _ensure_metadata_preview_sessions_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "metadata_preview_sessions"):
        return
    cols = _table_columns(conn, "metadata_preview_sessions")
    if "fields_snapshot_json" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE metadata_preview_sessions ADD COLUMN fields_snapshot_json TEXT NOT NULL DEFAULT '{}';")
    if "effective_source_selection_json" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE metadata_preview_sessions ADD COLUMN effective_source_selection_json TEXT NOT NULL DEFAULT '{}';")
    if "effective_source_provenance_json" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE metadata_preview_sessions ADD COLUMN effective_source_provenance_json TEXT NOT NULL DEFAULT '{}';")


def now_ts() -> float:
    return time.time()


def json_dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def get_channel_by_slug(conn: sqlite3.Connection, slug: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM channels WHERE slug = ?", (slug,))
    return cur.fetchone()


def get_channel_by_id(conn: sqlite3.Connection, channel_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM channels WHERE id = ?", (channel_id,))
    return cur.fetchone()


def get_channel_by_youtube_channel_id(conn: sqlite3.Connection, youtube_channel_id: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM channels WHERE youtube_channel_id = ?", (youtube_channel_id,))
    return cur.fetchone()


def create_channel(
    conn: sqlite3.Connection,
    *,
    slug: str,
    display_name: str,
    kind: str = "LONG",
    weight: float = 1.0,
    render_profile: str = "long_1080p24",
    autopublish_enabled: int = 0,
    youtube_channel_id: str | None = None,
) -> Dict[str, Any]:
    cur = conn.execute(
        """
        INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled, youtube_channel_id)
        VALUES(?,?,?,?,?,?,?)
        """,
        (slug, display_name, kind, weight, render_profile, autopublish_enabled, youtube_channel_id),
    )
    channel_id = int(cur.lastrowid)
    row = conn.execute(
        "SELECT id, slug, display_name, youtube_channel_id FROM channels WHERE id = ?",
        (channel_id,),
    ).fetchone()
    assert row is not None
    return row


def update_channel_display_name(
    conn: sqlite3.Connection,
    *,
    slug: str,
    display_name: str,
) -> Optional[Dict[str, Any]]:
    cols = _table_columns(conn, "channels")
    if "updated_at" in cols:
        conn.execute(
            "UPDATE channels SET display_name = ?, updated_at = ? WHERE slug = ?",
            (display_name, now_ts(), slug),
        )
    else:
        conn.execute(
            "UPDATE channels SET display_name = ? WHERE slug = ?",
            (display_name, slug),
        )
    return conn.execute(
        "SELECT id, slug, display_name FROM channels WHERE slug = ?",
        (slug,),
    ).fetchone()


def channel_has_jobs(conn: sqlite3.Connection, channel_id: int) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM jobs j
        JOIN releases r ON r.id = j.release_id
        WHERE r.channel_id = ?
        LIMIT 1
        """,
        (channel_id,),
    ).fetchone()
    return row is not None


def delete_channel_by_slug(conn: sqlite3.Connection, slug: str) -> int:
    cur = conn.execute("DELETE FROM channels WHERE slug = ?", (slug,))
    return int(cur.rowcount or 0)


def enable_track_catalog_for_channel(conn: sqlite3.Connection, channel_slug: str) -> None:
    conn.execute("INSERT OR IGNORE INTO canon_channels(value) VALUES(?)", (channel_slug,))
    conn.execute("INSERT OR IGNORE INTO canon_thresholds(value) VALUES(?)", (channel_slug,))


def disable_track_catalog_for_channel(conn: sqlite3.Connection, channel_slug: str) -> None:
    conn.execute("DELETE FROM canon_channels WHERE value = ?", (channel_slug,))
    conn.execute("DELETE FROM canon_thresholds WHERE value = ?", (channel_slug,))


def list_jobs(conn: sqlite3.Connection, state: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    if state:
        cur = conn.execute(
            """
            SELECT j.*, r.title AS release_title, c.slug AS channel_slug, c.display_name AS channel_name
            FROM jobs j
            JOIN releases r ON r.id = j.release_id
            JOIN channels c ON c.id = r.channel_id
            WHERE j.state = ?
            ORDER BY j.priority DESC, j.created_at ASC
            LIMIT ?
            """,
            (state, limit),
        )
    else:
        cur = conn.execute(
            """
            SELECT j.*, r.title AS release_title, c.slug AS channel_slug, c.display_name AS channel_name
            FROM jobs j
            JOIN releases r ON r.id = j.release_id
            JOIN channels c ON c.id = r.channel_id
            ORDER BY j.updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
    return cur.fetchall()


def list_jobs_state_domain(conn: sqlite3.Connection) -> list[str]:
    """Return ordered Jobs-page status domain.

    Starts from canonical project states and appends unknown states currently
    present in DB for backward compatibility with existing rows.
    """

    ordered: list[str] = list(UI_JOB_STATES)
    known = set(ordered)
    rows = conn.execute("SELECT DISTINCT state FROM jobs ORDER BY state ASC").fetchall()
    for row in rows:
        state = str(row.get("state") or "").strip()
        if state and state not in known:
            ordered.append(state)
            known.add(state)
    return ordered


def get_job(conn: sqlite3.Connection, job_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT j.*, r.title AS release_title, r.description AS release_description, r.tags_json AS release_tags_json,
               r.channel_id AS channel_id,
               c.slug AS channel_slug, c.display_name AS channel_name, c.kind AS channel_kind, c.autopublish_enabled
        FROM jobs j
        JOIN releases r ON r.id = j.release_id
        JOIN channels c ON c.id = r.channel_id
        WHERE j.id = ?
        """,
        (job_id,),
    )
    return cur.fetchone()


def get_ui_job_draft(conn: sqlite3.Connection, job_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT d.*, c.display_name AS channel_name
        FROM ui_job_drafts d
        JOIN channels c ON c.id = d.channel_id
        WHERE d.job_id = ?
        """,
        (job_id,),
    )
    return cur.fetchone()


def get_playlist_builder_channel_settings(conn: sqlite3.Connection, channel_slug: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM playlist_builder_channel_settings
        WHERE channel_slug = ?
        """,
        (channel_slug,),
    )
    return cur.fetchone()


def upsert_playlist_builder_channel_settings(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    default_generation_mode: str,
    min_duration_min: int,
    max_duration_min: int,
    tolerance_min: int,
    preferred_month_batch: Optional[str],
    preferred_batch_ratio: int,
    allow_cross_channel: bool,
    novelty_target_min: float,
    novelty_target_max: float,
    position_memory_window: int,
    strictness_mode: str,
    vocal_policy: str,
    reuse_policy: str = "avoid_recent",
) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO playlist_builder_channel_settings(
            channel_slug, default_generation_mode, min_duration_min, max_duration_min,
            tolerance_min, preferred_month_batch, preferred_batch_ratio, allow_cross_channel,
            novelty_target_min, novelty_target_max, position_memory_window,
            strictness_mode, vocal_policy, reuse_policy, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_slug) DO UPDATE SET
            default_generation_mode = excluded.default_generation_mode,
            min_duration_min = excluded.min_duration_min,
            max_duration_min = excluded.max_duration_min,
            tolerance_min = excluded.tolerance_min,
            preferred_month_batch = excluded.preferred_month_batch,
            preferred_batch_ratio = excluded.preferred_batch_ratio,
            allow_cross_channel = excluded.allow_cross_channel,
            novelty_target_min = excluded.novelty_target_min,
            novelty_target_max = excluded.novelty_target_max,
            position_memory_window = excluded.position_memory_window,
            strictness_mode = excluded.strictness_mode,
            vocal_policy = excluded.vocal_policy,
            reuse_policy = excluded.reuse_policy,
            updated_at = excluded.updated_at
        """,
        (
            channel_slug,
            default_generation_mode,
            min_duration_min,
            max_duration_min,
            tolerance_min,
            preferred_month_batch,
            preferred_batch_ratio,
            int(allow_cross_channel),
            novelty_target_min,
            novelty_target_max,
            position_memory_window,
            strictness_mode,
            vocal_policy,
            reuse_policy,
            ts,
            ts,
        ),
    )


def update_ui_job_playlist_builder_override_json(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    playlist_builder_override_json: Optional[str],
) -> bool:
    ts = now_ts()
    cur = conn.execute(
        """
        UPDATE ui_job_drafts
        SET playlist_builder_override_json = ?, updated_at = ?
        WHERE job_id = ?
        """,
        (playlist_builder_override_json, ts, job_id),
    )
    return int(cur.rowcount or 0) > 0


def create_title_template(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    template_name: str,
    template_body: str,
    status: str,
    is_default: bool,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    created_at: str,
    updated_at: str,
    archived_at: str | None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO title_templates(
            channel_slug, template_name, template_body, status, is_default,
            validation_status, validation_errors_json, last_validated_at,
            created_at, updated_at, archived_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            channel_slug,
            template_name,
            template_body,
            status,
            int(is_default),
            validation_status,
            validation_errors_json,
            last_validated_at,
            created_at,
            updated_at,
            archived_at,
        ),
    )
    return int(cur.lastrowid)


def unset_active_default_title_template(conn: sqlite3.Connection, *, channel_slug: str) -> None:
    conn.execute(
        """
        UPDATE title_templates
        SET is_default = 0
        WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1
        """,
        (channel_slug,),
    )


def get_title_template_by_id(conn: sqlite3.Connection, template_id: int) -> Optional[Dict[str, Any]]:
    return conn.execute("SELECT * FROM title_templates WHERE id = ?", (template_id,)).fetchone()


def list_title_templates(
    conn: sqlite3.Connection,
    *,
    channel_slug: str | None,
    status: str | None,
    q: str | None,
) -> List[Dict[str, Any]]:
    where: list[str] = []
    args: list[Any] = []
    if channel_slug:
        where.append("channel_slug = ?")
        args.append(channel_slug)
    if status == "ACTIVE":
        where.append("status = 'ACTIVE'")
    elif status == "ARCHIVED":
        where.append("status = 'ARCHIVED'")
    if q:
        where.append("template_name LIKE ?")
        args.append(f"%{q}%")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    return conn.execute(
        f"""
        SELECT *
        FROM title_templates
        {where_clause}
        ORDER BY updated_at DESC, id DESC
        """,
        tuple(args),
    ).fetchall()


def update_title_template_fields(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    template_name: str,
    template_body: str,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE title_templates
        SET template_name = ?,
            template_body = ?,
            validation_status = ?,
            validation_errors_json = ?,
            last_validated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            template_name,
            template_body,
            validation_status,
            validation_errors_json,
            last_validated_at,
            updated_at,
            template_id,
        ),
    )
    return int(cur.rowcount or 0) > 0


def set_title_template_default_flag(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    is_default: bool,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE title_templates
        SET is_default = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (int(is_default), updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def archive_title_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    updated_at: str,
    archived_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE title_templates
        SET status = 'ARCHIVED',
            is_default = 0,
            archived_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (archived_at, updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def activate_title_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE title_templates
        SET status = 'ACTIVE',
            archived_at = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def create_description_template(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    template_name: str,
    template_body: str,
    status: str,
    is_default: bool,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    created_at: str,
    updated_at: str,
    archived_at: str | None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO description_templates(
            channel_slug, template_name, template_body, status, is_default,
            validation_status, validation_errors_json, last_validated_at,
            created_at, updated_at, archived_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            channel_slug,
            template_name,
            template_body,
            status,
            int(is_default),
            validation_status,
            validation_errors_json,
            last_validated_at,
            created_at,
            updated_at,
            archived_at,
        ),
    )
    return int(cur.lastrowid)


def unset_active_default_description_template(conn: sqlite3.Connection, *, channel_slug: str) -> None:
    conn.execute(
        """
        UPDATE description_templates
        SET is_default = 0
        WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1
        """,
        (channel_slug,),
    )


def get_description_template_by_id(conn: sqlite3.Connection, template_id: int) -> Optional[Dict[str, Any]]:
    return conn.execute("SELECT * FROM description_templates WHERE id = ?", (template_id,)).fetchone()


def list_description_templates(
    conn: sqlite3.Connection,
    *,
    channel_slug: str | None,
    status: str | None,
    q: str | None,
) -> List[Dict[str, Any]]:
    where: list[str] = []
    args: list[Any] = []
    if channel_slug:
        where.append("channel_slug = ?")
        args.append(channel_slug)
    if status == "ACTIVE":
        where.append("status = 'ACTIVE'")
    elif status == "ARCHIVED":
        where.append("status = 'ARCHIVED'")
    if q:
        where.append("template_name LIKE ?")
        args.append(f"%{q}%")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    return conn.execute(
        f"""
        SELECT *
        FROM description_templates
        {where_clause}
        ORDER BY updated_at DESC, id DESC
        """,
        tuple(args),
    ).fetchall()


def update_description_template_fields(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    template_name: str,
    template_body: str,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE description_templates
        SET template_name = ?,
            template_body = ?,
            validation_status = ?,
            validation_errors_json = ?,
            last_validated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            template_name,
            template_body,
            validation_status,
            validation_errors_json,
            last_validated_at,
            updated_at,
            template_id,
        ),
    )
    return int(cur.rowcount or 0) > 0


def set_description_template_default_flag(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    is_default: bool,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE description_templates
        SET is_default = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (int(is_default), updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def archive_description_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    updated_at: str,
    archived_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE description_templates
        SET status = 'ARCHIVED',
            is_default = 0,
            archived_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (archived_at, updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def activate_description_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE description_templates
        SET status = 'ACTIVE',
            archived_at = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def create_video_tag_preset(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    preset_name: str,
    preset_body_json: str,
    status: str,
    is_default: bool,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    created_at: str,
    updated_at: str,
    archived_at: str | None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO video_tag_presets(
            channel_slug, preset_name, preset_body_json, status, is_default,
            validation_status, validation_errors_json, last_validated_at,
            created_at, updated_at, archived_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            channel_slug,
            preset_name,
            preset_body_json,
            status,
            int(is_default),
            validation_status,
            validation_errors_json,
            last_validated_at,
            created_at,
            updated_at,
            archived_at,
        ),
    )
    return int(cur.lastrowid)


def unset_active_default_video_tag_preset(conn: sqlite3.Connection, *, channel_slug: str) -> None:
    conn.execute(
        """
        UPDATE video_tag_presets
        SET is_default = 0
        WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1
        """,
        (channel_slug,),
    )


def get_video_tag_preset_by_id(conn: sqlite3.Connection, preset_id: int) -> Optional[Dict[str, Any]]:
    return conn.execute("SELECT * FROM video_tag_presets WHERE id = ?", (preset_id,)).fetchone()


def get_channel_metadata_defaults(conn: sqlite3.Connection, *, channel_slug: str) -> Optional[Dict[str, Any]]:
    return conn.execute("SELECT * FROM channel_metadata_defaults WHERE channel_slug = ?", (channel_slug,)).fetchone()


def upsert_channel_metadata_defaults(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    default_title_template_id: int | None,
    default_description_template_id: int | None,
    default_video_tag_preset_id: int | None,
    created_at: str,
    updated_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO channel_metadata_defaults(
            channel_slug,
            default_title_template_id,
            default_description_template_id,
            default_video_tag_preset_id,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_slug) DO UPDATE SET
            default_title_template_id = excluded.default_title_template_id,
            default_description_template_id = excluded.default_description_template_id,
            default_video_tag_preset_id = excluded.default_video_tag_preset_id,
            updated_at = excluded.updated_at
        """,
        (
            channel_slug,
            default_title_template_id,
            default_description_template_id,
            default_video_tag_preset_id,
            created_at,
            updated_at,
        ),
    )


def list_video_tag_presets(
    conn: sqlite3.Connection,
    *,
    channel_slug: str | None,
    status: str | None,
    q: str | None,
) -> List[Dict[str, Any]]:
    where: list[str] = []
    args: list[Any] = []
    if channel_slug:
        where.append("channel_slug = ?")
        args.append(channel_slug)
    if status == "ACTIVE":
        where.append("status = 'ACTIVE'")
    elif status == "ARCHIVED":
        where.append("status = 'ARCHIVED'")
    if q:
        where.append("preset_name LIKE ?")
        args.append(f"%{q}%")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    return conn.execute(
        f"""
        SELECT *
        FROM video_tag_presets
        {where_clause}
        ORDER BY updated_at DESC, id DESC
        """,
        tuple(args),
    ).fetchall()


def update_video_tag_preset_fields(
    conn: sqlite3.Connection,
    *,
    preset_id: int,
    preset_name: str,
    preset_body_json: str,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE video_tag_presets
        SET preset_name = ?,
            preset_body_json = ?,
            validation_status = ?,
            validation_errors_json = ?,
            last_validated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            preset_name,
            preset_body_json,
            validation_status,
            validation_errors_json,
            last_validated_at,
            updated_at,
            preset_id,
        ),
    )
    return int(cur.rowcount or 0) > 0


def set_video_tag_preset_default_flag(
    conn: sqlite3.Connection,
    *,
    preset_id: int,
    is_default: bool,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE video_tag_presets
        SET is_default = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (int(is_default), updated_at, preset_id),
    )
    return int(cur.rowcount or 0) > 0


def archive_video_tag_preset(
    conn: sqlite3.Connection,
    *,
    preset_id: int,
    updated_at: str,
    archived_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE video_tag_presets
        SET status = 'ARCHIVED',
            is_default = 0,
            archived_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (archived_at, updated_at, preset_id),
    )
    return int(cur.rowcount or 0) > 0


def activate_video_tag_preset(
    conn: sqlite3.Connection,
    *,
    preset_id: int,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE video_tag_presets
        SET status = 'ACTIVE',
            archived_at = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (updated_at, preset_id),
    )
    return int(cur.rowcount or 0) > 0


def create_channel_visual_style_template(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    template_name: str,
    template_payload_json: str,
    status: str,
    is_default: bool,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    created_at: str,
    updated_at: str,
    archived_at: str | None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO channel_visual_style_templates(
            channel_slug, template_name, template_payload_json, status, is_default,
            validation_status, validation_errors_json, last_validated_at,
            created_at, updated_at, archived_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            channel_slug,
            template_name,
            template_payload_json,
            status,
            int(is_default),
            validation_status,
            validation_errors_json,
            last_validated_at,
            created_at,
            updated_at,
            archived_at,
        ),
    )
    return int(cur.lastrowid)


def get_channel_visual_style_template_by_id(conn: sqlite3.Connection, template_id: int) -> Optional[Dict[str, Any]]:
    return conn.execute("SELECT * FROM channel_visual_style_templates WHERE id = ?", (template_id,)).fetchone()


def list_channel_visual_style_templates(
    conn: sqlite3.Connection,
    *,
    channel_slug: str | None,
    status: str | None,
    q: str | None,
) -> List[Dict[str, Any]]:
    where: list[str] = []
    args: list[Any] = []
    if channel_slug:
        where.append("channel_slug = ?")
        args.append(channel_slug)
    if status == "ACTIVE":
        where.append("status = 'ACTIVE'")
    elif status == "ARCHIVED":
        where.append("status = 'ARCHIVED'")
    if q:
        where.append("template_name LIKE ?")
        args.append(f"%{q}%")
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    return conn.execute(
        f"""
        SELECT *
        FROM channel_visual_style_templates
        {where_clause}
        ORDER BY updated_at DESC, id DESC
        """,
        tuple(args),
    ).fetchall()


def update_channel_visual_style_template_fields(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    template_name: str,
    template_payload_json: str,
    validation_status: str,
    validation_errors_json: str | None,
    last_validated_at: str,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE channel_visual_style_templates
        SET template_name = ?,
            template_payload_json = ?,
            validation_status = ?,
            validation_errors_json = ?,
            last_validated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            template_name,
            template_payload_json,
            validation_status,
            validation_errors_json,
            last_validated_at,
            updated_at,
            template_id,
        ),
    )
    return int(cur.rowcount or 0) > 0


def set_channel_visual_style_template_default_flag(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    is_default: bool,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE channel_visual_style_templates
        SET is_default = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (int(is_default), updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def unset_active_default_channel_visual_style_template(conn: sqlite3.Connection, *, channel_slug: str) -> None:
    conn.execute(
        """
        UPDATE channel_visual_style_templates
        SET is_default = 0
        WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1
        """,
        (channel_slug,),
    )


def archive_channel_visual_style_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    updated_at: str,
    archived_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE channel_visual_style_templates
        SET status = 'ARCHIVED',
            is_default = 0,
            archived_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (archived_at, updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def activate_channel_visual_style_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    updated_at: str,
) -> bool:
    cur = conn.execute(
        """
        UPDATE channel_visual_style_templates
        SET status = 'ACTIVE',
            archived_at = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (updated_at, template_id),
    )
    return int(cur.rowcount or 0) > 0


def _next_job_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM jobs").fetchone()
    assert row is not None
    return int(row["next_id"])


def insert_job_with_lineage_defaults(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    job_type: str,
    state: str,
    stage: str,
    priority: int,
    attempt: int,
    created_at: float,
    updated_at: float,
) -> int:
    job_id = _next_job_id(conn)
    conn.execute(
        """
        INSERT INTO jobs(
            id, release_id, job_type, state, stage, priority, attempt,
            retry_of_job_id, root_job_id, attempt_no, force_refetch_inputs,
            created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?, 1, 0, ?, ?)
        """,
        (job_id, release_id, job_type, state, stage, priority, attempt, job_id, created_at, updated_at),
    )
    return job_id


def create_ui_job_draft(
    conn: sqlite3.Connection,
    *,
    channel_id: int,
    title: str,
    description: str,
    tags_csv: str,
    cover_name: Optional[str],
    cover_ext: Optional[str],
    background_name: str,
    background_ext: str,
    audio_ids_text: str,
    job_type: str = "UI",
) -> int:
    ts = now_ts()
    tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
    cur = conn.execute(
        """
        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
        VALUES(?, ?, ?, ?, NULL, NULL, NULL, ?)
        """,
        (channel_id, title, description, json_dumps(tags), ts),
    )
    release_id = int(cur.lastrowid)
    job_id = insert_job_with_lineage_defaults(
        conn,
        release_id=release_id,
        job_type=job_type,
        state="DRAFT",
        stage="DRAFT",
        priority=0,
        attempt=0,
        created_at=ts,
        updated_at=ts,
    )
    conn.execute(
        """
        INSERT INTO ui_job_drafts(
            job_id, channel_id, title, description, tags_csv,
            cover_name, cover_ext, background_name, background_ext,
            audio_ids_text, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            channel_id,
            title,
            description,
            tags_csv,
            cover_name,
            cover_ext,
            background_name,
            background_ext,
            audio_ids_text,
            ts,
            ts,
        ),
    )
    return job_id


def update_ui_job_draft(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    title: str,
    description: str,
    tags_csv: str,
    cover_name: Optional[str],
    cover_ext: Optional[str],
    background_name: str,
    background_ext: str,
    audio_ids_text: str,
) -> None:
    ts = now_ts()
    tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
    conn.execute(
        """
        UPDATE ui_job_drafts
        SET title=?, description=?, tags_csv=?, cover_name=?, cover_ext=?,
            background_name=?, background_ext=?, audio_ids_text=?, updated_at=?
        WHERE job_id = ?
        """,
        (title, description, tags_csv, cover_name, cover_ext, background_name, background_ext, audio_ids_text, ts, job_id),
    )
    conn.execute(
        """
        UPDATE releases
        SET title=?, description=?, tags_json=?
        WHERE id = (SELECT release_id FROM jobs WHERE id = ?)
        """,
        (title, description, json_dumps(tags), job_id),
    )


def claim_job(
    conn: sqlite3.Connection,
    *,
    want_state: str,
    worker_id: str,
    lock_ttl_sec: int,
) -> Optional[int]:
    """Claim one job atomically.

    Rules:
      - only jobs in want_state
      - skip jobs scheduled for retry in the future (retry_at)
      - reclaim expired locks (locked_at older than lock_ttl_sec)
    """

    ts = now_ts()
    expiry = ts - float(lock_ttl_sec)

    conn.execute("BEGIN IMMEDIATE;")

    # Release expired locks in this state.
    conn.execute(
        """
        UPDATE jobs
        SET locked_by = NULL, locked_at = NULL, updated_at = ?
        WHERE state = ?
          AND locked_by IS NOT NULL
          AND locked_at IS NOT NULL
          AND locked_at < ?
        """,
        (ts, want_state, expiry),
    )

    row = conn.execute(
        """
        SELECT id FROM jobs
        WHERE state = ?
          AND locked_by IS NULL
          AND (retry_at IS NULL OR retry_at <= ?)
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        """,
        (want_state, ts),
    ).fetchone()
    if not row:
        conn.execute("COMMIT;")
        return None

    job_id = int(row["id"])
    cur = conn.execute(
        """
        UPDATE jobs
        SET locked_by = ?, locked_at = ?, updated_at = ?
        WHERE id = ? AND locked_by IS NULL
        """,
        (worker_id, ts, ts, job_id),
    )
    conn.execute("COMMIT;")
    if cur.rowcount != 1:
        return None
    return job_id


def touch_worker(
    conn: sqlite3.Connection,
    *,
    worker_id: str,
    role: str,
    pid: int,
    hostname: str,
    details: Dict[str, Any],
) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO worker_heartbeats(worker_id, role, pid, hostname, details_json, last_seen)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(worker_id) DO UPDATE SET
            role=excluded.role,
            pid=excluded.pid,
            hostname=excluded.hostname,
            details_json=excluded.details_json,
            last_seen=excluded.last_seen
        """,
        (worker_id, role, pid, hostname, json_dumps(details), ts),
    )


def list_workers(conn: sqlite3.Connection, limit: int = 200) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT worker_id, role, pid, hostname, details_json, last_seen
        FROM worker_heartbeats
        ORDER BY last_seen DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def increment_attempt(conn: sqlite3.Connection, job_id: int) -> int:
    ts = now_ts()
    conn.execute("UPDATE jobs SET attempt = attempt + 1, updated_at = ? WHERE id = ?", (ts, job_id))
    row = conn.execute("SELECT attempt FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return int(row["attempt"]) if row else 0


def schedule_retry(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    next_state: str,
    stage: str,
    error_reason: str,
    backoff_sec: int,
) -> None:
    ts = now_ts()
    retry_at = ts + float(backoff_sec)
    conn.execute(
        """
        UPDATE jobs
        SET state = ?, stage = ?, error_reason = ?, retry_at = ?, updated_at = ?, locked_by = NULL, locked_at = NULL
        WHERE id = ? AND state != 'CANCELLED'
        """,
        (next_state, stage, error_reason, retry_at, ts, job_id),
    )


def clear_retry(conn: sqlite3.Connection, job_id: int) -> None:
    ts = now_ts()
    conn.execute("UPDATE jobs SET retry_at = NULL, updated_at = ? WHERE id = ?", (ts, job_id))


def reclaim_stale_render_jobs(
    conn: sqlite3.Connection,
    *,
    lock_ttl_sec: int,
    backoff_sec: int,
    max_attempts: int,
) -> int:
    """Recover jobs that were in-progress inside orchestrator (FETCHING_INPUTS/RENDERING)
    and got stuck due to worker crash.

    Returns number of reclaimed jobs.
    """

    ts = now_ts()
    expiry = ts - float(lock_ttl_sec)
    rows = conn.execute(
        """
        SELECT id, state, locked_by FROM jobs
        WHERE state IN ('FETCHING_INPUTS','RENDERING')
          AND locked_by IS NOT NULL
          AND locked_at IS NOT NULL
          AND locked_at < ?
        """,
        (expiry,),
    ).fetchall()

    reclaimed = 0
    for r in rows:
        job_id = int(r["id"])
        prev_state = str(r.get("state") or "")
        attempt = increment_attempt(conn, job_id)
        reason = f"reclaimed stale lock from {prev_state}"
        if attempt < max_attempts:
            schedule_retry(
                conn,
                job_id,
                next_state="READY_FOR_RENDER",
                stage="FETCH",
                error_reason=f"attempt={attempt} retry: {reason}",
                backoff_sec=backoff_sec,
            )
        else:
            update_job_state(
                conn,
                job_id,
                state="RENDER_FAILED",
                stage="RENDER",
                error_reason=f"attempt={attempt} terminal: {reason}",
            )
            clear_retry(conn, job_id)
            conn.execute(
                "UPDATE jobs SET locked_by=NULL, locked_at=NULL, updated_at=? WHERE id=?",
                (now_ts(), job_id),
            )
        reclaimed += 1

    return reclaimed




def force_unlock(conn: sqlite3.Connection, job_id: int) -> None:
    # Force unlock a job regardless of who holds the lock (admin action).
    ts = now_ts()
    conn.execute(
        "UPDATE jobs SET locked_by=NULL, locked_at=NULL, updated_at=? WHERE id=?",
        (ts, job_id),
    )


def cancel_job(conn: sqlite3.Connection, job_id: int, *, reason: str = 'cancelled by user') -> None:
    # Mark job as CANCELLED and clear lock/retry. Safe to call multiple times.
    ts = now_ts()
    conn.execute(
        '''
        UPDATE jobs
        SET state='CANCELLED', stage='CANCELLED',
            progress_text='cancelled',
            error_reason=?,
            retry_at=NULL,
            locked_by=NULL,
            locked_at=NULL,
            updated_at=?
        WHERE id=?
        ''',
        (reason, ts, job_id),
    )
def release_lock(conn: sqlite3.Connection, job_id: int, worker_id: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        UPDATE jobs SET locked_by = NULL, locked_at = NULL, updated_at = ?
        WHERE id = ? AND locked_by = ?
        """,
        (ts, job_id, worker_id),
    )


def update_job_state(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    state: str,
    stage: Optional[str] = None,
    error_reason: Optional[str] = None,
    progress_pct: Optional[float] = None,
    progress_text: Optional[str] = None,
    approval_notified_at: Optional[float] = None,
    published_at: Optional[float] = None,
    delete_mp4_at: Optional[float] = None,
) -> None:
    ts = now_ts()
    fields: List[str] = ["state = ?", "updated_at = ?"]
    vals: List[Any] = [state, ts]

    if stage is not None:
        fields.append("stage = ?")
        vals.append(stage)
    if error_reason is not None:
        fields.append("error_reason = ?")
        vals.append(error_reason)
    if progress_pct is not None:
        fields.append("progress_pct = ?")
        vals.append(progress_pct)
        fields.append("progress_updated_at = ?")
        vals.append(ts)
    if progress_text is not None:
        fields.append("progress_text = ?")
        vals.append(progress_text)
    if approval_notified_at is not None:
        fields.append("approval_notified_at = ?")
        vals.append(approval_notified_at)
    if published_at is not None:
        fields.append("published_at = ?")
        vals.append(published_at)
    if delete_mp4_at is not None:
        fields.append("delete_mp4_at = ?")
        vals.append(delete_mp4_at)

    where = " WHERE id = ?"
    if state != 'CANCELLED':
        where = " WHERE id = ? AND state != 'CANCELLED'"
    q = "UPDATE jobs SET " + ", ".join(fields) + where
    vals.append(job_id)
    conn.execute(q, tuple(vals))


def create_asset(
    conn: sqlite3.Connection,
    *,
    channel_id: int,
    kind: str,
    origin: str,
    origin_id: Optional[str],
    name: Optional[str],
    path: Optional[str],
) -> int:
    ts = now_ts()
    cur = conn.execute(
        """
        INSERT INTO assets(channel_id, kind, origin, origin_id, name, path, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (channel_id, kind, origin, origin_id, name, path, ts),
    )
    return int(cur.lastrowid)


def link_job_input(conn: sqlite3.Connection, job_id: int, asset_id: int, role: str, order_index: int) -> None:
    conn.execute(
        "INSERT INTO job_inputs(job_id, asset_id, role, order_index) VALUES(?, ?, ?, ?)",
        (job_id, asset_id, role, order_index),
    )


def link_job_output(conn: sqlite3.Connection, job_id: int, asset_id: int, role: str) -> None:
    conn.execute(
        "INSERT INTO job_outputs(job_id, asset_id, role) VALUES(?, ?, ?)",
        (job_id, asset_id, role),
    )


def set_qa_report(conn: sqlite3.Connection, job_id: int, report: Dict[str, Any]) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO qa_reports(
            job_id, hard_ok, warnings_json, info_json, duration_expected, duration_actual,
            vcodec, acodec, fps, width, height, sr, ch, mean_volume_db, max_volume_db, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            hard_ok=excluded.hard_ok,
            warnings_json=excluded.warnings_json,
            info_json=excluded.info_json,
            duration_expected=excluded.duration_expected,
            duration_actual=excluded.duration_actual,
            vcodec=excluded.vcodec,
            acodec=excluded.acodec,
            fps=excluded.fps,
            width=excluded.width,
            height=excluded.height,
            sr=excluded.sr,
            ch=excluded.ch,
            mean_volume_db=excluded.mean_volume_db,
            max_volume_db=excluded.max_volume_db,
            created_at=excluded.created_at
        """,
        (
            job_id,
            1 if report.get("hard_ok") else 0,
            json_dumps(report.get("warnings", [])),
            json_dumps(report.get("info", [])),
            report.get("duration_expected"),
            report.get("duration_actual"),
            report.get("vcodec"),
            report.get("acodec"),
            report.get("fps"),
            report.get("width"),
            report.get("height"),
            report.get("sr"),
            report.get("ch"),
            report.get("mean_volume_db"),
            report.get("max_volume_db"),
            ts,
        ),
    )


def set_approval(conn: sqlite3.Connection, job_id: int, decision: str, comment: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO approvals(job_id, decision, comment, decided_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            decision=excluded.decision,
            comment=excluded.comment,
            decided_at=excluded.decided_at
        """,
        (job_id, decision, comment, ts),
    )


def set_youtube_upload(conn: sqlite3.Connection, job_id: int, *, video_id: str, url: str, studio_url: str, privacy: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO youtube_uploads(job_id, video_id, url, studio_url, privacy, uploaded_at)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            video_id=excluded.video_id,
            url=excluded.url,
            studio_url=excluded.studio_url,
            privacy=excluded.privacy,
            uploaded_at=excluded.uploaded_at,
            error=NULL
        """,
        (job_id, video_id, url, studio_url, privacy, ts),
    )


def set_youtube_error(conn: sqlite3.Connection, job_id: int, error: str) -> None:
    conn.execute(
        """
        INSERT INTO youtube_uploads(job_id, video_id, url, studio_url, privacy, uploaded_at, error)
        VALUES(?, '', '', '', '', ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET error=excluded.error
        """,
        (job_id, now_ts(), error),
    )


def upsert_tg_message(conn: sqlite3.Connection, job_id: int, chat_id: int, message_id: int) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO tg_messages(job_id, chat_id, message_id, created_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            chat_id=excluded.chat_id,
            message_id=excluded.message_id,
            created_at=excluded.created_at
        """,
        (job_id, chat_id, message_id, ts),
    )


def set_pending_reply(conn: sqlite3.Connection, user_id: int, job_id: int, kind: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO tg_pending(user_id, job_id, kind, created_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            job_id=excluded.job_id,
            kind=excluded.kind,
            created_at=excluded.created_at
        """,
        (user_id, job_id, kind, ts),
    )


def pop_pending_reply(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM tg_pending WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        return None
    conn.execute("DELETE FROM tg_pending WHERE user_id = ?", (user_id,))
    return row


def insert_metadata_preview_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    release_id: int,
    channel_slug: str,
    session_status: str,
    requested_fields_json: str,
    current_bundle_json: str,
    proposed_bundle_json: str,
    sources_json: str,
    field_statuses_json: str,
    dependency_fingerprints_json: str,
    warnings_json: str,
    errors_json: str,
    fields_snapshot_json: str,
    effective_source_selection_json: str,
    effective_source_provenance_json: str,
    created_by: str | None,
    created_at: str,
    expires_at: str,
    applied_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO metadata_preview_sessions(
            id, release_id, channel_slug, session_status, requested_fields_json,
            current_bundle_json, proposed_bundle_json, sources_json, field_statuses_json,
            dependency_fingerprints_json, warnings_json, errors_json, fields_snapshot_json,
            effective_source_selection_json, effective_source_provenance_json,
            created_by, created_at, expires_at, applied_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            release_id,
            channel_slug,
            session_status,
            requested_fields_json,
            current_bundle_json,
            proposed_bundle_json,
            sources_json,
            field_statuses_json,
            dependency_fingerprints_json,
            warnings_json,
            errors_json,
            fields_snapshot_json,
            effective_source_selection_json,
            effective_source_provenance_json,
            created_by,
            created_at,
            expires_at,
            applied_at,
        ),
    )


def insert_metadata_bulk_preview_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    planner_context_json: str,
    selected_item_ids_json: str,
    requested_fields_json: str,
    selected_channels_json: str,
    session_status: str,
    aggregate_summary_json: str,
    item_states_json: str,
    created_by: str | None,
    created_at: str,
    expires_at: str,
    applied_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO metadata_bulk_preview_sessions(
            id, planner_context_json, selected_item_ids_json, requested_fields_json, selected_channels_json,
            session_status, aggregate_summary_json, item_states_json, created_by, created_at, expires_at, applied_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            planner_context_json,
            selected_item_ids_json,
            requested_fields_json,
            selected_channels_json,
            session_status,
            aggregate_summary_json,
            item_states_json,
            created_by,
            created_at,
            expires_at,
            applied_at,
        ),
    )


def insert_planner_mass_action_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    action_type: str,
    planner_scope_fingerprint: str,
    selected_item_ids_json: str,
    preview_status: str,
    aggregate_preview_json: str,
    item_preview_json: str,
    created_by: str | None,
    created_at: str,
    expires_at: str,
    executed_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO planner_mass_action_sessions(
            id, action_type, planner_scope_fingerprint, selected_item_ids_json, preview_status,
            aggregate_preview_json, item_preview_json, created_by, created_at, expires_at, executed_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            action_type,
            planner_scope_fingerprint,
            selected_item_ids_json,
            preview_status,
            aggregate_preview_json,
            item_preview_json,
            created_by,
            created_at,
            expires_at,
            executed_at,
        ),
    )


def insert_publish_bulk_action_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    action_type: str,
    action_payload_json: str,
    selection_fingerprint: str,
    selected_job_ids_json: str,
    preview_status: str,
    aggregate_preview_json: str,
    item_preview_json: str,
    invalidation_reason_code: str | None,
    created_by: str | None,
    created_at: str,
    expires_at: str,
    executed_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO publish_bulk_action_sessions(
            id, action_type, action_payload_json, selection_fingerprint, selected_job_ids_json, preview_status,
            aggregate_preview_json, item_preview_json, invalidation_reason_code,
            created_by, created_at, expires_at, executed_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            action_type,
            action_payload_json,
            selection_fingerprint,
            selected_job_ids_json,
            preview_status,
            aggregate_preview_json,
            item_preview_json,
            invalidation_reason_code,
            created_by,
            created_at,
            expires_at,
            executed_at,
        ),
    )
