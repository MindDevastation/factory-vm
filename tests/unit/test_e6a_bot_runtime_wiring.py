from __future__ import annotations

import unittest

from services.bot.handlers import router


class TestE6ABotRuntimeWiring(unittest.TestCase):
    def test_router_includes_e6a_callback_handlers(self) -> None:
        callback_names = {h.callback.__name__ for h in router.callback_query.handlers}
        self.assertIn("cb_e6a_publish_action", callback_names)
        self.assertIn("cb_e6a_ops_action", callback_names)

    def test_router_includes_e6a_operator_commands(self) -> None:
        message_names = {h.callback.__name__ for h in router.message.handlers}
        self.assertIn("cmd_whoami", message_names)
        self.assertIn("cmd_overview", message_names)


if __name__ == "__main__":
    unittest.main()
