from __future__ import annotations

import unittest

from services.factory_api.context_continuity import (
    build_context_envelope,
    decode_context_token,
    encode_context_token,
    resolve_incoming_context,
)


class TestE6Mf1S3ContextContinuity(unittest.TestCase):
    def test_envelope_preserves_allowed_filters_only(self) -> None:
        envelope = build_context_envelope(
            current_path="/ui/publish/queue",
            parent_path="/",
            raw_query={"statuses": "FAILED", "channel_slug": "alpha", "unknown": "x"},
        )
        self.assertEqual(envelope.filters, {"statuses": "FAILED", "channel_slug": "alpha"})

    def test_token_roundtrip(self) -> None:
        envelope = build_context_envelope(
            current_path="/ui/planner",
            parent_path="/",
            raw_query={"time_window": "30d", "severity": "HIGH"},
        )
        token = encode_context_token(envelope)
        restored = decode_context_token(token)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.current_path, "/ui/planner")
        self.assertEqual(restored.parent_path, "/")
        self.assertEqual(restored.filters, {"time_window": "30d", "severity": "HIGH"})

    def test_invalid_token_is_safely_discarded(self) -> None:
        self.assertIsNone(decode_context_token("not-a-token"))

    def test_resolve_incoming_context_requires_known_paths(self) -> None:
        envelope = build_context_envelope(
            current_path="/ui/publish/queue",
            parent_path="/",
            raw_query={"statuses": "FAILED"},
        )
        token = encode_context_token(envelope)
        allowed = {"/", "/ui/publish/queue"}
        self.assertIsNotNone(resolve_incoming_context(token=token, known_paths=allowed))
        denied = {"/", "/ui/planner"}
        self.assertIsNone(resolve_incoming_context(token=token, known_paths=denied))


if __name__ == "__main__":
    unittest.main()
