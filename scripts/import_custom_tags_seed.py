from __future__ import annotations

from services.common import db as dbm
from services.common.env import Env
from services.common.profile import load_profile_env
from services.custom_tags import catalog_service


def main() -> None:
    load_profile_env()
    env = Env.load()
    conn = dbm.connect(env)
    try:
        result = catalog_service.import_catalog(conn, seed_dir=env.custom_tags_seed_dir)
    finally:
        conn.close()
    print(f"custom tags seed imported: {result}")


if __name__ == "__main__":
    main()
