import unittest
from unittest import mock

import sentienta_bridge as bridge


class FsBrowserCompatibilityTests(unittest.TestCase):
    def call(self, tool):
        return bridge.BridgeCall(
            msg_id="test-message",
            bridge_id="desktop_fs",
            tool=tool,
            args={"url": "https://example.com"},
            raw={},
        )

    def test_translates_allowed_preparation_action(self):
        translated = bridge.translate_fs_browser_compat_call(self.call("fs.browser.open"))
        self.assertEqual(translated.tool, "browser.open")
        self.assertEqual(translated.raw["compatibility_alias"], "fs.browser.open")
        self.assertEqual(translated.raw["actual_service"], "native_browser")
        self.assertTrue(translated.raw["temporary_shim"])

    def test_submit_is_deliberately_rejected(self):
        with self.assertRaisesRegex(bridge.BridgeError, "Unsupported temporary browser"):
            bridge.translate_fs_browser_compat_call(self.call("fs.browser.submit"))

    def test_execution_reports_actual_service_and_alias(self):
        with mock.patch.object(
            bridge,
            "execute_native_browser_call",
            return_value={"status": "completed", "tool": "browser.open"},
        ) as execute:
            result = bridge.execute_call(
                self.call("fs.browser.open"),
                [],
                {},
                ["local_fs", "native_browser"],
                1,
                1,
                1,
                1,
            )
        self.assertEqual(execute.call_args.args[0].tool, "browser.open")
        self.assertEqual(result["requested_tool"], "fs.browser.open")
        self.assertEqual(result["service_family"], "native_browser")
        self.assertEqual(result["compatibility_shim"], "fs.browser")

    def test_execution_requires_native_browser_service(self):
        with self.assertRaisesRegex(bridge.BridgeError, "Service disabled: native_browser"):
            bridge.execute_call(
                self.call("fs.browser.open"),
                [],
                {},
                ["local_fs"],
                1,
                1,
                1,
                1,
            )


if __name__ == "__main__":
    unittest.main()
