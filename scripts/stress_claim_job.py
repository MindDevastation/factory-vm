from __future__ import annotations

import threading

from services.common.env import Env
from services.common import db as dbm
from services.common.config import load_channels


def main() -> None:
    env = Env.load()

    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        # Ensure channels exist (minimal seed).
        for c in load_channels("configs/channels.yaml"):
            conn.execute(
                """
                INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    display_name=excluded.display_name,
                    kind=excluded.kind,
                    weight=excluded.weight,
                    render_profile=excluded.render_profile,
                    autopublish_enabled=excluded.autopublish_enabled
                """,
                (c.slug, c.display_name, c.kind, c.weight, c.render_profile, 1 if c.autopublish_enabled else 0),
            )
        conn.execute("DELETE FROM jobs WHERE state IN ('READY_FOR_RENDER','CLAIMED_FOR_TEST')")

        ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        if not ch:
            raise RuntimeError("channel 'darkwood-reverie' not found")

        ts = dbm.now_ts()
        cur = conn.execute(
            """INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (int(ch["id"]), "Stress Claim", "d", "[]", None, None, f"stress_meta_{int(ts)}", ts),
        )
        rid = int(cur.lastrowid)

        total = 500
        for _ in range(total):
            conn.execute(
                """INSERT INTO jobs(release_id, job_type, state, stage, priority, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)""",
                (rid, "RENDER_LONG", "READY_FOR_RENDER", "FETCH", 1, ts, ts),
            )
    finally:
        conn.close()

    claimed: set[int] = set()
    lock = threading.Lock()

    def worker(tid: int) -> None:
        while True:
            c = dbm.connect(env)
            try:
                jid = dbm.claim_job(
                    c,
                    want_state="READY_FOR_RENDER",
                    worker_id=f"stress:{tid}",
                    lock_ttl_sec=env.job_lock_ttl_sec,
                )
                if not jid:
                    return
                dbm.update_job_state(c, jid, state="CLAIMED_FOR_TEST", stage="TEST")
                dbm.release_lock(c, jid, f"stress:{tid}")
            finally:
                c.close()

            with lock:
                if jid in claimed:
                    raise RuntimeError(f"duplicate claim detected: {jid}")
                claimed.add(jid)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"OK: unique claims = {len(claimed)}")


if __name__ == "__main__":
    # Run as: PYTHONPATH=. FACTORY_DB_PATH=... FACTORY_STORAGE_ROOT=... python scripts/stress_claim_job.py
    main()
