from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest import mock


def _mod(name: str) -> types.ModuleType:
    m = types.ModuleType(name)
    return m


class TestOptionalGoogleImports(unittest.TestCase):
    def test_gdrive_import_try_block_and_auth_not_configured(self) -> None:
        # Install fake google modules so the optional import try-block executes.
        saved = dict(sys.modules)
        try:
            google = _mod("google")
            oauth2 = _mod("google.oauth2")
            oauth2_sa = _mod("google.oauth2.service_account")
            oauth2_creds = _mod("google.oauth2.credentials")
            auth = _mod("google.auth")
            auth_transport = _mod("google.auth.transport")
            auth_transport_requests = _mod("google.auth.transport.requests")
            ga = _mod("googleapiclient")
            ga_discovery = _mod("googleapiclient.discovery")
            ga_http = _mod("googleapiclient.http")
            ga_flow = _mod("google_auth_oauthlib")
            ga_flow_flow = _mod("google_auth_oauthlib.flow")

            # Minimal stubs referenced by the integration modules
            oauth2_sa.Credentials = types.SimpleNamespace(from_service_account_file=lambda *a, **k: object())
            oauth2_creds.Credentials = types.SimpleNamespace(from_authorized_user_file=lambda *a, **k: object())
            ga_discovery.build = lambda *a, **k: object()
            ga_http.MediaIoBaseDownload = object
            ga_flow_flow.InstalledAppFlow = types.SimpleNamespace(from_client_secrets_file=lambda *a, **k: object())
            auth_transport_requests.Request = object

            sys.modules.update(
                {
                    "google": google,
                    "google.oauth2": oauth2,
                    "google.oauth2.service_account": oauth2_sa,
                    "google.oauth2.credentials": oauth2_creds,
                    "google.auth": auth,
                    "google.auth.transport": auth_transport,
                    "google.auth.transport.requests": auth_transport_requests,
                    "googleapiclient": ga,
                    "googleapiclient.discovery": ga_discovery,
                    "googleapiclient.http": ga_http,
                    "google_auth_oauthlib": ga_flow,
                    "google_auth_oauthlib.flow": ga_flow_flow,
                }
            )

            mod = importlib.import_module("services.integrations.gdrive")
            importlib.reload(mod)
            self.assertIsNone(getattr(mod, "_GOOGLE_IMPORT_ERROR"))

            # With deps available but without auth configured, it must raise the config error.
            with self.assertRaises(RuntimeError):
                mod.DriveClient(service_account_json="", oauth_client_json="", oauth_token_json="")
        finally:
            sys.modules.clear()
            sys.modules.update(saved)

    def test_youtube_import_try_block_executes(self) -> None:
        saved = dict(sys.modules)
        try:
            ga = _mod("googleapiclient")
            ga_discovery = _mod("googleapiclient.discovery")
            ga_http = _mod("googleapiclient.http")
            ga_flow = _mod("google_auth_oauthlib")
            ga_flow_flow = _mod("google_auth_oauthlib.flow")
            google = _mod("google")
            auth = _mod("google.auth")
            auth_transport = _mod("google.auth.transport")
            auth_transport_requests = _mod("google.auth.transport.requests")
            oauth2 = _mod("google.oauth2")
            oauth2_creds = _mod("google.oauth2.credentials")

            ga_discovery.build = lambda *a, **k: object()
            ga_http.MediaFileUpload = object
            ga_flow_flow.InstalledAppFlow = types.SimpleNamespace(from_client_secrets_file=lambda *a, **k: object())
            auth_transport_requests.Request = object
            oauth2_creds.Credentials = types.SimpleNamespace(from_authorized_user_file=lambda *a, **k: types.SimpleNamespace(valid=True))

            sys.modules.update(
                {
                    "googleapiclient": ga,
                    "googleapiclient.discovery": ga_discovery,
                    "googleapiclient.http": ga_http,
                    "google_auth_oauthlib": ga_flow,
                    "google_auth_oauthlib.flow": ga_flow_flow,
                    "google": google,
                    "google.auth": auth,
                    "google.auth.transport": auth_transport,
                    "google.auth.transport.requests": auth_transport_requests,
                    "google.oauth2": oauth2,
                    "google.oauth2.credentials": oauth2_creds,
                }
            )

            mod = importlib.import_module("services.integrations.youtube")
            importlib.reload(mod)
            self.assertIsNone(getattr(mod, "_GOOGLE_IMPORT_ERROR"))
        finally:
            sys.modules.clear()
            sys.modules.update(saved)
