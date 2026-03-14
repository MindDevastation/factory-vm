from __future__ import annotations

from enum import Enum
from typing import Dict, FrozenSet


class ArtifactCategory(str, Enum):
    TEMP_PREVIEWS = "temporary_previews"
    TEMP_EXPORTS = "temporary_exported_files"
    TRANSIENT_REPORTS = "transient_reports_intermediate_files"
    TERMINAL_WORKSPACES = "terminal_abandoned_job_workspaces"
    STALE_SCRATCH_DIRS = "stale_temp_scratch_dirs"

    CURRENT_SQLITE_DB = "current_sqlite_db"
    BACKUPS_SNAPSHOTS_QUARANTINE = "backups_snapshots_quarantine"
    ENV_CONFIG_TEMPLATES_POLICIES = "env_config_templates_policies"
    CANONICAL_SEEDS_TAXONOMY_POLICIES = "canonical_seed_taxonomy_policy_artifacts"
    MEDIA_SOURCE_AUDIO_LIBRARY = "media_source_audio_library"
    FINAL_OUTPUT_VIDEO_LIBRARY = "final_output_video_library"
    ACTIVE_JOB_WORKSPACES = "active_job_workspaces"
    FEATURE_SOURCE_OF_TRUTH = "feature_marked_source_of_truth"
    OUTSIDE_ALLOWLIST_SCOPE = "outside_allowlist_scope"


class ArtifactDisposition(str, Enum):
    DISPOSABLE = "disposable"
    PROTECTED = "protected"


DISPOSABLE_ARTIFACT_CATEGORIES: FrozenSet[ArtifactCategory] = frozenset(
    {
        ArtifactCategory.TEMP_PREVIEWS,
        ArtifactCategory.TEMP_EXPORTS,
        ArtifactCategory.TRANSIENT_REPORTS,
        ArtifactCategory.TERMINAL_WORKSPACES,
        ArtifactCategory.STALE_SCRATCH_DIRS,
    }
)

PROTECTED_ARTIFACT_CATEGORIES: FrozenSet[ArtifactCategory] = frozenset(
    {
        ArtifactCategory.CURRENT_SQLITE_DB,
        ArtifactCategory.BACKUPS_SNAPSHOTS_QUARANTINE,
        ArtifactCategory.ENV_CONFIG_TEMPLATES_POLICIES,
        ArtifactCategory.CANONICAL_SEEDS_TAXONOMY_POLICIES,
        ArtifactCategory.MEDIA_SOURCE_AUDIO_LIBRARY,
        ArtifactCategory.FINAL_OUTPUT_VIDEO_LIBRARY,
        ArtifactCategory.ACTIVE_JOB_WORKSPACES,
        ArtifactCategory.FEATURE_SOURCE_OF_TRUTH,
        ArtifactCategory.OUTSIDE_ALLOWLIST_SCOPE,
    }
)


def artifact_disposition_map() -> Dict[ArtifactCategory, ArtifactDisposition]:
    out: Dict[ArtifactCategory, ArtifactDisposition] = {}
    for category in DISPOSABLE_ARTIFACT_CATEGORIES:
        out[category] = ArtifactDisposition.DISPOSABLE
    for category in PROTECTED_ARTIFACT_CATEGORIES:
        out[category] = ArtifactDisposition.PROTECTED
    return out
