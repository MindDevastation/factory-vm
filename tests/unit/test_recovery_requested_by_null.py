import importlib
import unittest
from unittest import mock

from starlette.requests import Request

from tests._helpers import temp_env


class RecoveryRequestedByNullTests(unittest.TestCase):
    def _request(self, authorization: str | None = None) -> Request:
        headers = []
        if authorization is not None:
            headers.append((b"authorization", authorization.encode("utf-8")))
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/v1/ops/recovery/jobs/1/actions/retry_failed/execute",
            "headers": headers,
        }
        return Request(scope)

    def test_recovery_requested_by_returns_none_when_auth_is_missing_or_invalid(self) -> None:
        with temp_env() as (_, _):
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)

            self.assertIsNone(mod._recovery_requested_by(self._request()))
            self.assertIsNone(mod._recovery_requested_by(self._request("Basic not-base64")))

    def test_execute_inserts_audit_with_null_requested_by_when_auth_header_invalid(self) -> None:
        with temp_env() as (_, _):
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)

            fake_conn = mock.Mock()
            fake_conn.close = mock.Mock()

            payload = mod.RecoveryActionExecutePayload(confirm=True)
            request = self._request("Basic not-base64")

            with mock.patch.object(mod.dbm, "connect", return_value=fake_conn), mock.patch.object(
                mod.dbm,
                "get_job",
                return_value={"id": 1, "state": "FAILED"},
            ), mock.patch.object(
                mod,
                "recovery_preview_action",
                return_value={"allowed": False},
            ), mock.patch.object(
                mod,
                "insert_recovery_audit",
                return_value=777,
            ) as audit_mock:
                with self.assertRaises(mod.HTTPException) as exc_info:
                    mod.api_ops_recovery_action_execute(
                        job_id=1,
                        action="retry_failed",
                        payload=payload,
                        request=request,
                        _=True,
                    )

            self.assertEqual(exc_info.exception.status_code, 409)
            self.assertEqual(exc_info.exception.detail["code"], "ORC_ACTION_NOT_ALLOWED")
            self.assertEqual(exc_info.exception.detail["audit_id"], 777)
            self.assertEqual(audit_mock.call_args.kwargs["requested_by"], None)
            fake_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
