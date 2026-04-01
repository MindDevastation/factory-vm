from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class YouTubeAnalyticsProvider:
    """External provider boundary for YouTube analytics ingestion.

    Runtime path depends on this adapter boundary. Tests may inject fake providers.
    """

    transport: Callable[..., dict[str, Any]]

    def fetch_channel_metrics(
        self,
        *,
        channel_slug: str,
        metric_families: tuple[str, ...],
        observed_from: float | None,
        observed_to: float | None,
    ) -> dict[str, Any]:
        return self.transport(
            scope_type="CHANNEL",
            channel_slug=channel_slug,
            youtube_video_id=None,
            metric_families=metric_families,
            observed_from=observed_from,
            observed_to=observed_to,
        )

    def fetch_video_metrics(
        self,
        *,
        channel_slug: str,
        youtube_video_id: str,
        metric_families: tuple[str, ...],
        observed_from: float | None,
        observed_to: float | None,
    ) -> dict[str, Any]:
        return self.transport(
            scope_type="RELEASE_VIDEO",
            channel_slug=channel_slug,
            youtube_video_id=youtube_video_id,
            metric_families=metric_families,
            observed_from=observed_from,
            observed_to=observed_to,
        )
