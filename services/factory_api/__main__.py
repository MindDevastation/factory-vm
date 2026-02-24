from __future__ import annotations

import os

from dotenv import load_dotenv
from services.common.profile import load_profile_env
import uvicorn

from services.common.env import Env
from services.common.logging_setup import setup_logging, get_logger


def main() -> None:
    # load deploy/env if present
    load_profile_env()
    env = Env.load()
    setup_logging(env, service="factory_api")
    log = get_logger("factory_api")
    if env.basic_pass == "change_me":
        raise RuntimeError("FACTORY_BASIC_AUTH_PASS is not set (default 'change_me' is insecure).")
    log.info("starting api bind=%s port=%s", env.bind, env.port)
    uvicorn.run("services.factory_api.app:app", host=env.bind, port=env.port, reload=False)


if __name__ == "__main__":
    main()
