from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


GenerationMode = Literal["safe", "smart", "curated"]
StrictnessMode = Literal["strict", "balanced", "flexible"]
VocalPolicy = Literal[
    "allow_any",
    "prefer_instrumental",
    "require_instrumental",
    "prefer_lyrical",
    "require_lyrical",
    "exclude_speech",
]
ReusePolicy = Literal["avoid_recent", "penalty_only", "allow_recent"]


class RelaxationItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    constraint_name: str
    target_value: Any = None
    achieved_value: Any = None
    relaxation_applied: str
    reason: str


class PlaylistBrief(BaseModel):
    model_config = ConfigDict(extra="forbid")

    channel_slug: str
    job_id: Optional[int] = None
    content_type: Optional[str] = None
    generation_mode: GenerationMode = "smart"
    strictness_mode: StrictnessMode = "balanced"
    min_duration_min: int = 30
    max_duration_min: int = 60
    tolerance_min: int = 5
    allow_cross_channel: bool = False
    preferred_month_batch: Optional[str] = None
    preferred_batch_ratio: int = 70
    novelty_target_min: float = 0.50
    novelty_target_max: float = 0.80
    position_memory_window: int = 20
    vocal_policy: VocalPolicy = "allow_any"
    required_tags: list[str] = Field(default_factory=list)
    excluded_tags: list[str] = Field(default_factory=list)
    notes: Optional[str] = None
    random_seed: Optional[int] = None
    candidate_limit: Optional[int] = None
    preferred_track_count_min: Optional[int] = None
    preferred_track_count_max: Optional[int] = None
    reuse_policy: ReusePolicy = "avoid_recent"

    @property
    def target_duration_min(self) -> float:
        return (self.min_duration_min + self.max_duration_min) / 2

    @model_validator(mode="after")
    def _validate_ranges(self) -> "PlaylistBrief":
        if self.min_duration_min <= 0:
            raise ValueError("min_duration_min must be > 0")
        if self.max_duration_min < self.min_duration_min:
            raise ValueError("max_duration_min must be >= min_duration_min")
        if self.tolerance_min < 0:
            raise ValueError("tolerance_min must be >= 0")
        if self.preferred_batch_ratio < 0 or self.preferred_batch_ratio > 100:
            raise ValueError("preferred_batch_ratio must be in [0,100]")
        if self.novelty_target_min < 0 or self.novelty_target_min > 1:
            raise ValueError("novelty_target_min must be in [0,1]")
        if self.novelty_target_max < 0 or self.novelty_target_max > 1:
            raise ValueError("novelty_target_max must be in [0,1]")
        if self.novelty_target_min > self.novelty_target_max:
            raise ValueError("novelty_target_min must be <= novelty_target_max")
        if self.position_memory_window < 1:
            raise ValueError("position_memory_window must be >= 1")
        return self

    def to_api_dict(self) -> dict:
        payload = self.model_dump()
        payload["target_duration_min"] = self.target_duration_min
        return payload


@dataclass(frozen=True)
class TrackCandidate:
    track_pk: int
    track_id: str
    channel_slug: str
    duration_sec: float
    month_batch: Optional[str]
    tags: frozenset[str]
    voice_flag: Optional[bool]
    speech_flag: Optional[bool]
    dominant_texture: Optional[str]
    dsp_score: Optional[float]


@dataclass(frozen=True)
class PlaylistHistoryEntry:
    history_id: int
    job_id: Optional[int]
    history_stage: str
    tracks: tuple[int, ...]
    month_batches: tuple[Optional[str], ...] = ()


@dataclass
class CandidateScore:
    track_pk: int
    hard_eligible: bool
    context_fit: float
    novelty_contribution: float
    batch_ratio_contribution: float
    voice_policy_fit: float
    required_tags_fit: float
    low_reuse_penalty_inverse: float
    duration_fit_micro: float
    base_fit: float
    fit_note: str = ""


class PlaylistPreviewResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: GenerationMode
    status: Literal["ok", "not_implemented", "empty"]
    warnings: list[str] = Field(default_factory=list)
    relaxations: list[str] = Field(default_factory=list)
    relaxations_structured: list[RelaxationItem] = Field(default_factory=list)
    selected_track_pks: list[int] = Field(default_factory=list)
    ordered_track_pks: list[int] = Field(default_factory=list)
    achieved_duration_sec: float = 0.0
    achieved_duration_min: float = 0.0
    achieved_novelty: float = 0.0
    achieved_batch_ratio: float = 0.0
    per_track_fit_notes: list[dict[str, Any]] = Field(default_factory=list)
    ordering_rationale: str = ""
    candidate_pool_size: int = 0


class PlaylistBriefOverrides(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content_type: Optional[str] = None
    generation_mode: Optional[GenerationMode] = None
    strictness_mode: Optional[StrictnessMode] = None
    min_duration_min: Optional[int] = None
    max_duration_min: Optional[int] = None
    tolerance_min: Optional[int] = None
    allow_cross_channel: Optional[bool] = None
    preferred_month_batch: Optional[str] = None
    preferred_batch_ratio: Optional[int] = None
    novelty_target_min: Optional[float] = None
    novelty_target_max: Optional[float] = None
    position_memory_window: Optional[int] = None
    vocal_policy: Optional[VocalPolicy] = None
    required_tags: Optional[list[str]] = None
    excluded_tags: Optional[list[str]] = None
    notes: Optional[str] = None
    random_seed: Optional[int] = None
    candidate_limit: Optional[int] = None
    preferred_track_count_min: Optional[int] = None
    preferred_track_count_max: Optional[int] = None
    reuse_policy: Optional[ReusePolicy] = None

    def as_patch_dict(self) -> dict:
        return self.model_dump(exclude_none=True)


class PlaylistChannelSettingsPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generation_mode: Optional[GenerationMode] = None
    strictness_mode: Optional[StrictnessMode] = None
    min_duration_min: Optional[int] = None
    max_duration_min: Optional[int] = None
    tolerance_min: Optional[int] = None
    preferred_month_batch: Optional[str] = None
    preferred_batch_ratio: Optional[int] = None
    allow_cross_channel: Optional[bool] = None
    novelty_target_min: Optional[float] = None
    novelty_target_max: Optional[float] = None
    position_memory_window: Optional[int] = None
    vocal_policy: Optional[VocalPolicy] = None
    reuse_policy: Optional[ReusePolicy] = None

    def as_patch_dict(self) -> dict:
        return self.model_dump(exclude_none=True)
