from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


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


class PlaylistBrief(BaseModel):
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


class PlaylistBriefOverrides(BaseModel):
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

    def as_patch_dict(self) -> dict:
        return self.model_dump(exclude_none=True)


class PlaylistChannelSettingsPatch(BaseModel):
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

    def as_patch_dict(self) -> dict:
        return self.model_dump(exclude_none=True)
