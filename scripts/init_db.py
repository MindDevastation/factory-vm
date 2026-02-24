from __future__ import annotations

import os
from services.common.profile import load_profile_env

from services.common.env import Env
from services.common import db as dbm


def main() -> None:
    load_profile_env()
    env = Env.load()
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        print(f"DB initialized at: {env.db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
