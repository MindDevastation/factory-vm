from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv


def load_profile_env() -> str:
    """Load env file based on FACTORY_PROFILE.

    Order:
    - deploy/env.<profile> if exists
    - deploy/env if exists
    """
    profile = os.environ.get("FACTORY_PROFILE", "").strip() or "prod"
    cand = Path("deploy") / f"env.{profile}"
    if cand.exists():
        load_dotenv(str(cand), override=False)
        return str(cand)
    fallback = Path("deploy") / "env"
    if fallback.exists():
        load_dotenv(str(fallback), override=False)
        return str(fallback)
    return ""
