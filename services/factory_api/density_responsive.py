from __future__ import annotations


def density_profile_contract() -> dict[str, object]:
    return {
        "density_mode": "DESKTOP_COMPACT",
        "row_density": "COMPACT",
        "table_padding_px": 10,
        "anti_showcase": True,
    }


def responsive_fallback_contract() -> dict[str, object]:
    return {
        "tablet_breakpoint_px": 1024,
        "mobile_breakpoint_px": 768,
        "fallback_mode": "READABLE_STACKED",
        "preserve_operator_density_on_desktop": True,
    }


def density_responsive_catalog() -> dict[str, object]:
    return {
        "density": density_profile_contract(),
        "responsive_fallback": responsive_fallback_contract(),
    }
