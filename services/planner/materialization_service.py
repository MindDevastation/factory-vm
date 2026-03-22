from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import sqlite3
from typing import Any, Dict


class PlannerMaterializationError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class MaterializationResult:
    planner_item_id: int
    release_id: int
    planner_status: str
    materialization_status: str


class PlannerMaterializationService:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def materialize_or_get(self, *, planner_item_id: int, created_by: str | None) -> MaterializationResult:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            planner = self._conn.execute(
                "SELECT * FROM planned_releases WHERE id = ?",
                (planner_item_id,),
            ).fetchone()
            if planner is None:
                raise PlannerMaterializationError(code="PLM_NOT_FOUND", message="planner item not found")

            binding = self._conn.execute(
                "SELECT release_id FROM planner_release_links WHERE planned_release_id = ?",
                (planner_item_id,),
            ).fetchone()

            status = str(planner["status"])
            if binding is not None:
                if status != "LOCKED":
                    raise PlannerMaterializationError(
                        code="PLM_INCONSISTENT_STATE",
                        message="existing binding requires planner status LOCKED",
                    )
                release_id = int(binding["release_id"])
                release = self._conn.execute("SELECT id FROM releases WHERE id = ?", (release_id,)).fetchone()
                if release is None:
                    raise PlannerMaterializationError(
                        code="PLM_INCONSISTENT_STATE",
                        message="binding points to missing release",
                    )
                self._conn.execute("COMMIT")
                return MaterializationResult(
                    planner_item_id=planner_item_id,
                    release_id=release_id,
                    planner_status="LOCKED",
                    materialization_status="EXISTING_BINDING",
                )

            if status == "LOCKED":
                raise PlannerMaterializationError(
                    code="PLM_INCONSISTENT_STATE",
                    message="locked planner item has no binding",
                )
            if status != "PLANNED":
                raise PlannerMaterializationError(
                    code="PLM_INVALID_STATUS",
                    message="planner item status must be PLANNED for first materialization",
                )

            channel = self._conn.execute("SELECT id FROM channels WHERE slug = ?", (planner["channel_slug"],)).fetchone()
            if channel is None:
                raise PlannerMaterializationError(code="PLM_INTERNAL", message="planner channel not found")

            now_ts = datetime.now(timezone.utc).timestamp()
            title = str(planner.get("title") or "").strip() or f"Planned #{planner_item_id}"
            description = str(planner.get("notes") or "")
            planned_at = planner.get("publish_at")
            origin_meta_file_id = f"planner-item-{planner_item_id}"

            cur = self._conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, ?, ?, '[]', ?, NULL, ?, ?)
                """,
                (int(channel["id"]), title, description, planned_at, origin_meta_file_id, now_ts),
            )
            release_id = int(cur.lastrowid)

            try:
                self._conn.execute(
                    """
                    INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by)
                    VALUES(?, ?, ?, ?)
                    """,
                    (planner_item_id, release_id, self._now_iso(), created_by),
                )
            except sqlite3.IntegrityError as exc:
                raise PlannerMaterializationError(code="PLM_BINDING_CONFLICT", message="binding conflict") from exc

            self._conn.execute(
                "UPDATE planned_releases SET status = 'LOCKED', updated_at = ? WHERE id = ?",
                (self._now_iso(), planner_item_id),
            )

            self._conn.execute("COMMIT")
            return MaterializationResult(
                planner_item_id=planner_item_id,
                release_id=release_id,
                planner_status="LOCKED",
                materialization_status="CREATED",
            )
        except PlannerMaterializationError:
            self._conn.execute("ROLLBACK")
            raise
        except Exception as exc:
            self._conn.execute("ROLLBACK")
            raise PlannerMaterializationError(code="PLM_INTERNAL", message="materialization failed") from exc

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
