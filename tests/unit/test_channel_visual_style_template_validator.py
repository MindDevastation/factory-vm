from __future__ import annotations

import unittest

from services.metadata.channel_visual_style_template_validator import validate_template_payload


class TestChannelVisualStyleTemplateValidator(unittest.TestCase):
    def test_valid_payload(self) -> None:
        result = validate_template_payload(_valid_payload())
        self.assertTrue(result.is_valid)
        self.assertEqual(result.errors, [])
        assert result.normalized_payload is not None
        self.assertEqual(result.normalized_payload["allowed_motifs"], ["forest", "mist"])
        self.assertEqual(result.normalized_payload["default_background_asset_id"], 42)

    def test_missing_required_key(self) -> None:
        payload = _valid_payload()
        payload.pop("palette_guidance")
        result = validate_template_payload(payload)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.errors[0]["code"], "CVST_PAYLOAD_REQUIRED_KEY")
        self.assertIn("palette_guidance", result.errors[0]["message"])

    def test_wrong_top_level_type(self) -> None:
        result = validate_template_payload(["not", "an", "object"])
        self.assertFalse(result.is_valid)
        self.assertEqual(result.errors, [{"code": "CVST_PAYLOAD_TYPE", "message": "template_payload must be an object"}])

    def test_blank_string_rejected(self) -> None:
        payload = _valid_payload()
        payload["branding_rules"] = "   "
        result = validate_template_payload(payload)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.errors[0]["code"], "CVST_PAYLOAD_STRING_EMPTY")
        self.assertIn("branding_rules", result.errors[0]["message"])

    def test_invalid_list_item_rejected(self) -> None:
        payload = _valid_payload()
        payload["allowed_motifs"] = ["forest", " "]
        result = validate_template_payload(payload)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.errors[0]["code"], "CVST_PAYLOAD_LIST_ITEM_EMPTY")
        self.assertEqual(result.errors[0]["message"], "template_payload.allowed_motifs[1] must be non-empty")

    def test_default_background_asset_id_rejects_non_integer_like(self) -> None:
        payload = _valid_payload()
        payload["default_background_asset_id"] = "abc"
        result = validate_template_payload(payload)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.errors[0]["code"], "CVST_PAYLOAD_DEFAULT_BACKGROUND_ASSET_ID_TYPE")

    def test_default_background_asset_id_rejects_zero_and_negative(self) -> None:
        for raw in (0, -1):
            payload = _valid_payload()
            payload["default_background_asset_id"] = raw
            result = validate_template_payload(payload)
            self.assertFalse(result.is_valid)
            self.assertEqual(result.errors[0]["code"], "CVST_PAYLOAD_DEFAULT_BACKGROUND_ASSET_ID_RANGE")


def _valid_payload() -> dict[str, object]:
    return {
        "palette_guidance": "Muted earth tones",
        "typography_rules": "Use clean sans serif titles",
        "text_layout_rules": "Center align title block",
        "composition_framing_rules": "Subject centered with margin",
        "allowed_motifs": ["forest", "mist"],
        "banned_motifs": ["neon"],
        "branding_rules": "Keep logo in lower right",
        "output_profile_guidance": "16:9 high contrast",
        "background_compatibility_guidance": "Works on dark backgrounds",
        "cover_composition_guidance": "Leave top third for text",
        "default_background_asset_id": 42,
    }


if __name__ == "__main__":
    unittest.main()
