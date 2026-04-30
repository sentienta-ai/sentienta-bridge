#!/usr/bin/env python3
"""
Sentienta desktop bridge (Phase 1).

Polls Sentienta retrieve endpoint for bridge_call messages, executes local tools,
and posts bridge_result back via query endpoint.

This script is intentionally conservative:
- supports OpenClaw tools
- de-duplicates call_id values per run
"""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, parse_qsl, quote, unquote, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen


DEFAULT_QUERY_ENDPOINT = "https://v75rxsd18a.execute-api.us-west-2.amazonaws.com/PROD/query"
DEFAULT_RETRIEVE_ENDPOINT = "https://w1e0ns2550.execute-api.us-west-2.amazonaws.com/PROD/read"
DEFAULT_BRIDGE_ID = "desktop"
LOCAL_BRIDGE_DEBUG_VERSION = "local-bridge-debug-2026-04-06-v2"
SUPPORTED_SERVICES = ("openclaw_exec",)
BRIDGE_ID_SERVICE_POLICY: Dict[str, Tuple[str, ...]] = {
    "desktop_openclaw": ("openclaw_exec",),
}
SERVICE_TO_BRIDGE_ID: Dict[str, str] = {
    "openclaw_exec": "desktop_openclaw",
}
MEDIA_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "pdf"}
DEFAULT_PAIRING_FILE = Path.home() / ".sentienta-bridge" / "pairing-code.json"
DEFAULT_OC_TASK_CACHE_FILE = Path.home() / ".sentienta-bridge" / "openclaw-task-cache.json"
PAIR_ATTEMPT_WINDOW_SECS = 5 * 60
PAIR_ATTEMPT_MAX_FAILURES = 8


@dataclass
class BridgeCall:
    msg_id: str
    bridge_id: str
    tool: str
    args: Dict[str, object]
    raw: Dict[str, object]


class BridgeError(Exception):
    pass


def persist_pairing_code_file(
    *,
    bridge_id: str,
    pairing_state: Dict[str, object],
    path: Optional[Path] = None,
) -> Optional[Path]:
    """
    Persist current bridge pairing code details to local disk for admin recovery.
    """
    try:
        out_path = (path or DEFAULT_PAIRING_FILE).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        now_epoch = int(time.time())
        payload = {
        "bridge_id": str(bridge_id or "").strip() or "desktop",
        "passcode": str(pairing_state.get("passcode", "") or "").strip(),
        "expires_at": int(pairing_state.get("passcode_expires_at", 0) or 0),
        "generated_at": now_epoch,
        "listen_port": int(pairing_state.get("listen_port", 0) or 0),
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return out_path
    except Exception as e:
        print(f"[bridge][pair] failed writing pairing file: {e}", flush=True)
        return None


@dataclass
class ActiveQuery:
    team_name: str
    query_id: str
    user_id: str = ""
    dialog_index: int = -1
    last_seen_ts: float = 0.0
    poll_errors: int = 0
    auth_headers: Optional[Dict[str, str]] = None
    last_body: str = ""
    unchanged_polls: int = 0
    openclaw_terminal_ts: float = 0.0
    openclaw_terminal_task_id: str = ""
    current_openclaw_task_id: str = ""
    show_partial_results: bool = False


def _bridge_debug_events(pairing_state: Dict[str, object]) -> List[Dict[str, object]]:
    events = pairing_state.get("debug_events")
    if isinstance(events, list):
        return events
    events = []
    pairing_state["debug_events"] = events
    return events


def record_bridge_debug_event(
    pairing_state: Dict[str, object],
    stage: str,
    *,
    team_name: str = "",
    query_id: str = "",
    user_id: str = "",
    payload: Optional[Dict[str, object]] = None,
) -> None:
    try:
        events = _bridge_debug_events(pairing_state)
        next_seq = int(pairing_state.get("debug_seq", 0) or 0) + 1
        pairing_state["debug_seq"] = next_seq
        query_key = f"{str(team_name or '').strip()}::{str(query_id or '').strip()}"
        per_query = pairing_state.get("debug_query_seq")
        if not isinstance(per_query, dict):
            per_query = {}
            pairing_state["debug_query_seq"] = per_query
        next_query_seq = int(per_query.get(query_key, 0) or 0) + 1
        per_query[query_key] = next_query_seq
        item: Dict[str, object] = {
            "seq": next_seq,
            "query_seq": next_query_seq,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "stage": str(stage or "").strip(),
            "teamName": str(team_name or "").strip(),
            "queryID": str(query_id or "").strip(),
            "userID": str(user_id or "").strip(),
        }
        if isinstance(payload, dict):
            item["payload"] = payload
        events.append(item)
        if len(events) > 250:
            del events[:-250]
    except Exception:
        pass


def parse_args() -> argparse.Namespace:
    help_epilog = (
        "Examples:\n"
        "  # Public OpenClaw bridge profile (minimal service surface)\n"
        "  python sentienta_bridge.py --service openclaw_exec\n"
        "\n"
        "  # Provide auth token at startup (alternative to /register-query)\n"
        "  python sentienta_bridge.py --id-token <token> --service openclaw_exec\n"
        "\n"
        "Notes:\n"
        "  - If --service is omitted, all supported services are selected (subject to bridge_id policy).\n"
        "  - For public use, pass only --service openclaw_exec.\n"
        "  - Start OpenClaw first when using --service openclaw_exec."
    )
    p = argparse.ArgumentParser(
        description="Sentienta desktop bridge (Phase 1)",
        epilog=help_epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--bridge-id",
        default=DEFAULT_BRIDGE_ID,
        help=f"Optional bridge identifier (default: {DEFAULT_BRIDGE_ID}).",
    )
    p.add_argument("--user-id", default="", help="Optional Sentienta userID/username for outbox polling")
    p.add_argument("--team-name", default="", help="Optional initial team name for retrieve/query APIs")
    p.add_argument("--query-id", default="", help="Optional initial queryID to monitor")
    p.add_argument("--id-token", default=os.getenv("SENTIENTA_ID_TOKEN", ""), help="Auth token (or env SENTIENTA_ID_TOKEN)")
    p.add_argument("--owner-key", default=os.getenv("SENTIENTA_OWNER_KEY", ""), help="Owner key (or env SENTIENTA_OWNER_KEY)")
    p.add_argument(
        "--owner-key-header",
        default=os.getenv("SENTIENTA_OWNER_KEY_HEADER", "x-owner-key"),
        help="HTTP header name used for owner key auth (default: x-owner-key)",
    )
    p.add_argument(
        "--auth-mode",
        choices=["auto", "id-token", "owner-key"],
        default="auto",
        help="Auth mode: auto prefers id-token then owner-key",
    )
    p.add_argument("--query-endpoint", default=DEFAULT_QUERY_ENDPOINT)
    p.add_argument("--retrieve-endpoint", default=DEFAULT_RETRIEVE_ENDPOINT)
    p.add_argument("--listen-port", type=int, default=8765, help="Local bridge registration HTTP port")
    p.add_argument(
        "--pair-passcode-ttl-secs",
        type=int,
        default=int(os.getenv("SENTIENTA_BRIDGE_PASSCODE_TTL_SECS", "43200")),
        help="Pairing passcode TTL in seconds (default: 43200 / 12 hours). Use 0 for no expiry.",
    )
    p.add_argument("--poll-ms", type=int, default=250)
    p.add_argument("--max-chars-default", type=int, default=40000)
    p.add_argument("--max-chars-hard", type=int, default=500000)
    p.add_argument("--max-find-results-default", type=int, default=20)
    p.add_argument("--max-find-results-hard", type=int, default=200)
    p.add_argument(
        "--service",
        action="append",
        choices=SUPPORTED_SERVICES,
        default=[],
        help="Enable only the specified service(s). Repeat to enable multiple. Public OpenClaw profile: --service openclaw_exec.",
    )
    p.add_argument(
        "--openclaw-cli",
        default=os.getenv("SENTIENTA_OPENCLAW_CLI", "openclaw"),
        help="OpenClaw CLI executable path/name",
    )
    p.add_argument(
        "--openclaw-default-agent",
        default=os.getenv("SENTIENTA_OPENCLAW_DEFAULT_AGENT", "main"),
        help="Default OpenClaw agent id for run_task",
    )
    p.add_argument(
        "--openclaw-default-timeout-ms",
        type=int,
        default=int(os.getenv("SENTIENTA_OPENCLAW_DEFAULT_TIMEOUT_MS", "210000")),
        help="Default timeout for openclaw.run_task in milliseconds",
    )
    p.add_argument(
        "--openclaw-execution-mode",
        choices=["cli"],
        default=os.getenv("SENTIENTA_OPENCLAW_EXECUTION_MODE", "cli"),
        help="OpenClaw execution mode for public bridge builds. Only cli is supported.",
    )
    p.add_argument("--exit-on-eod", action="store_true", help="Stop polling when EOD appears")
    p.add_argument(
        "--enable-legacy-dialog-calls",
        action="store_true",
        help="Also parse bridge_call JSON from dialog body (legacy fallback). Default is off.",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def log(msg: str, *, verbose: bool = True) -> None:
    if verbose:
        print(msg, flush=True)


def _supports_ansi_color() -> bool:
    try:
        return bool(sys.stdout and getattr(sys.stdout, "isatty", lambda: False)())
    except Exception:
        return False


def _ansi_wrap(text: str, code: str) -> str:
    if not _supports_ansi_color():
        return text
    return f"\033[{code}m{text}\033[0m"


def _format_pairing_expiry_for_banner(expires_at: object) -> str:
    try:
        exp = float(expires_at or 0)
    except Exception:
        exp = 0.0
    if exp <= 0:
        return "Never"
    try:
        dt = datetime.fromtimestamp(exp, tz=timezone.utc).astimezone()
        return dt.strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        return str(expires_at)


def _print_pairing_banner(passcode: str, expires_at: object, pairing_file: Optional[Path]) -> None:
    line = "=" * 50
    print("", flush=True)
    print(_ansi_wrap(line, "94;1"), flush=True)
    print(_ansi_wrap(f"PAIRING PASSCODE: {str(passcode or '').strip()}", "94;1"), flush=True)
    print(_ansi_wrap(f"Expires: {_format_pairing_expiry_for_banner(expires_at)}", "94"), flush=True)
    if pairing_file is not None:
        print(_ansi_wrap(f"Saved to: {pairing_file}", "94"), flush=True)
    print(_ansi_wrap("Use this in Sentienta > Desktop Automation", "94"), flush=True)
    print(_ansi_wrap(line, "94;1"), flush=True)
    print("", flush=True)


def auth_headers(
    *,
    auth_mode: str,
    id_token: str,
    owner_key: str,
    owner_key_header: str,
) -> Tuple[Dict[str, str], str]:
    headers = {"Content-Type": "application/json"}
    mode = (auth_mode or "auto").strip().lower()

    if mode == "id-token":
        if not id_token:
            raise BridgeError("Auth mode id-token selected but token missing. Pass --id-token or set SENTIENTA_ID_TOKEN.")
        headers["Authorization"] = f"Bearer {id_token}"
        return headers, "id-token"

    if mode == "owner-key":
        if not owner_key:
            raise BridgeError("Auth mode owner-key selected but key missing. Pass --owner-key or set SENTIENTA_OWNER_KEY.")
        header_name = (owner_key_header or "x-owner-key").strip()
        if not header_name:
            raise BridgeError("Owner-key header name is empty. Set --owner-key-header.")
        headers[header_name] = owner_key
        return headers, "owner-key"

    # auto mode: prefer id-token, then owner-key
    if id_token:
        headers["Authorization"] = f"Bearer {id_token}"
        return headers, "id-token"
    if owner_key:
        header_name = (owner_key_header or "x-owner-key").strip()
        if not header_name:
            raise BridgeError("Owner-key header name is empty. Set --owner-key-header.")
        headers[header_name] = owner_key
        return headers, "owner-key"
    return headers, "none"


def redact_secret(value: str) -> str:
    s = str(value or "")
    if len(s) <= 8:
        return "*" * len(s)
    return f"{s[:4]}...{s[-4:]}"


def print_auth_diagnostics(headers: Dict[str, str], resolved_mode: str) -> None:
    print(f"[bridge][auth] mode={resolved_mode}", flush=True)
    if "Authorization" in headers:
        token = str(headers.get("Authorization", ""))
        if token.lower().startswith("bearer "):
            token = token[7:]
        print(f"[bridge][auth] header=Authorization (Bearer {redact_secret(token)})", flush=True)
        return
    key_headers = [k for k in headers.keys() if k.lower() not in {"content-type"}]
    if key_headers:
        k = key_headers[0]
        print(f"[bridge][auth] header={k} ({redact_secret(str(headers.get(k, '')))})", flush=True)
    else:
        print("[bridge][auth] warning=no credential header detected", flush=True)


def has_auth_credential(headers: Optional[Dict[str, str]]) -> bool:
    if not headers:
        return False
    for k in headers.keys():
        if str(k).lower() not in {"content-type"}:
            return True
    return False


def load_json_file(path: Path) -> Dict[str, object]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json_file(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_selected_services(raw_services: List[str], bridge_id: str) -> List[str]:
    bridge_key = str(bridge_id or "").strip().lower()
    policy_services = list(BRIDGE_ID_SERVICE_POLICY.get(bridge_key, ()))
    if not raw_services:
        return policy_services if policy_services else list(SUPPORTED_SERVICES)
    deduped: List[str] = []
    for svc in raw_services:
        s = str(svc or "").strip()
        if s and s not in deduped:
            deduped.append(s)
    if policy_services:
        deduped = [s for s in deduped if s in policy_services]
    return deduped


def resolve_accepted_bridge_ids(primary_bridge_id: str, selected_services: List[str]) -> List[str]:
    ids: List[str] = []
    primary = str(primary_bridge_id or "").strip()
    if primary:
        ids.append(primary)
    for svc in selected_services:
        alias = SERVICE_TO_BRIDGE_ID.get(str(svc or "").strip())
        if alias and alias not in ids:
            ids.append(alias)
    return ids


def _prune_pair_failures(pairing_state: Dict[str, object], now: Optional[int] = None) -> List[int]:
    ts_now = int(now if now is not None else time.time())
    raw = pairing_state.get("pair_failures")
    kept: List[int] = []
    if isinstance(raw, list):
        for item in raw:
            try:
                ts = int(item)
            except Exception:
                continue
            if ts_now - ts <= PAIR_ATTEMPT_WINDOW_SECS:
                kept.append(ts)
    pairing_state["pair_failures"] = kept
    return kept


def _record_pair_failure(pairing_state: Dict[str, object], now: Optional[int] = None) -> int:
    ts_now = int(now if now is not None else time.time())
    kept = _prune_pair_failures(pairing_state, now=ts_now)
    kept.append(ts_now)
    pairing_state["pair_failures"] = kept
    return len(kept)


def compute_available_services(selected_services: List[str]) -> List[str]:
    services: List[str] = []
    if "openclaw_exec" in selected_services:
        services.append("openclaw_exec")
    return services


def append_query_param(url: str, key: str, value: str) -> str:
    try:
        parsed = urlparse(url)
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
        q[str(key)] = str(value)
        new_query = urlencode(q)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        return url


def run_auth_probe(
    *,
    retrieve_endpoint: str,
    headers: Dict[str, str],
    team_name: str,
    query_id: str,
) -> None:
    print("[bridge][auth] probing retrieve endpoint...", flush=True)
    try:
        resp = poll_dialog(
            retrieve_endpoint=retrieve_endpoint,
            headers=headers,
            team_name=team_name,
            query_id=query_id,
            dialog_index=-1,
        )
        keys = sorted(list(resp.keys()))
        print(f"[bridge][auth] probe OK (response keys: {keys})", flush=True)
    except BridgeError as e:
        print(f"[bridge][auth] probe FAILED: {e}", flush=True)
        raise


def make_registration_handler(
    bridge_id: str,
    active_queries: Dict[Tuple[str, str], ActiveQuery],
    lock: threading.Lock,
    default_auth_headers: Dict[str, str],
    pairing_state: Dict[str, object],
    roots: List[Path],
    selected_services: List[str],
    accepted_bridge_ids: List[str],
    max_chars_default: int,
    max_chars_hard: int,
    max_find_results_default: int,
    max_find_results_hard: int,
):
    class RegistrationHandler(BaseHTTPRequestHandler):
        def _json_response(self, code: int, payload: Dict[str, object]) -> None:
            raw = json.dumps(payload).encode("utf-8")
            try:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Bridge-Secret")
                self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
                self.end_headers()
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                return

        def do_OPTIONS(self) -> None:  # noqa: N802
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Bridge-Secret")
            self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path or ""
            if path.startswith("/openclaw-media/browser/"):
                filename = Path(unquote(path[len("/openclaw-media/browser/") :].strip())).name
                if not filename:
                    self._json_response(404, {"ok": False, "error": "media_not_found"})
                    return
                media_root = Path.home() / ".openclaw" / "media" / "browser"
                p = (media_root / filename).resolve()
                try:
                    p.relative_to(media_root.resolve())
                except Exception:
                    self._json_response(403, {"ok": False, "error": "media_forbidden"})
                    return
                if not p.exists() or not p.is_file():
                    self._json_response(404, {"ok": False, "error": "media_missing"})
                    return
                try:
                    raw = p.read_bytes()
                except Exception:
                    self._json_response(500, {"ok": False, "error": "media_read_failed"})
                    return
                ctype = mimetypes.guess_type(str(p))[0] or "application/octet-stream"
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Cache-Control", "private, max-age=600")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(raw)
                return
            if path.startswith("/media/"):
                _cleanup_expired_media_tokens(pairing_state, max_remove=400)
                rest = path[len("/media/") :].strip()
                token = rest.split("/", 1)[0].strip()
                media_tokens = pairing_state.get("media_tokens")
                if not token or not isinstance(media_tokens, dict):
                    self._json_response(404, {"ok": False, "error": "media_not_found"})
                    return
                item = media_tokens.get(token)
                if not isinstance(item, dict):
                    self._json_response(404, {"ok": False, "error": "media_not_found"})
                    return
                expires_at = int(item.get("expires_at", 0) or 0)
                if expires_at and int(time.time()) > expires_at:
                    media_tokens.pop(token, None)
                    self._json_response(410, {"ok": False, "error": "media_expired"})
                    return
                file_path = str(item.get("path", "") or "").strip()
                p = Path(file_path)
                if not file_path or (not p.exists()) or (not p.is_file()):
                    self._json_response(404, {"ok": False, "error": "media_missing"})
                    return
                try:
                    raw = p.read_bytes()
                except Exception:
                    self._json_response(500, {"ok": False, "error": "media_read_failed"})
                    return
                ctype = str(item.get("content_type", "") or "").strip() or "application/octet-stream"
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Cache-Control", "private, max-age=600")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(raw)
                return
            if path == "/debug-state":
                params = parse_qs(parsed.query or "")
                team_filter = str((params.get("teamName") or [""])[0] or "").strip()
                query_filter = str((params.get("queryID") or [""])[0] or "").strip()
                user_filter = str((params.get("userID") or [""])[0] or "").strip()
                with lock:
                    items = [
                        {
                            "teamName": q.team_name,
                            "queryID": q.query_id,
                            "userID": q.user_id,
                            "dialogIndex": q.dialog_index,
                            "pollErrors": q.poll_errors,
                            "lastSeenTs": q.last_seen_ts,
                            "showPartialResults": bool(q.show_partial_results),
                            "lastBodyPreview": str(q.last_body or "")[:180],
                        }
                        for q in active_queries.values()
                        if (not team_filter or q.team_name == team_filter)
                        and (not query_filter or q.query_id == query_filter)
                        and (not user_filter or q.user_id == user_filter)
                    ]
                events = []
                for event in _bridge_debug_events(pairing_state):
                    if not isinstance(event, dict):
                        continue
                    if team_filter and str(event.get("teamName") or "").strip() != team_filter:
                        continue
                    if query_filter and str(event.get("queryID") or "").strip() != query_filter:
                        continue
                    if user_filter and str(event.get("userID") or "").strip() != user_filter:
                        continue
                    events.append(event)
                self._json_response(
                    200,
                    {
                        "ok": True,
                        "bridgeId": bridge_id,
                        "localBridgeDebugVersion": LOCAL_BRIDGE_DEBUG_VERSION,
                        "activeQueries": items,
                        "events": events[-80:],
                    },
                )
                return
            if path != "/health":
                self._json_response(404, {"ok": False, "error": "not_found"})
                return
            print("[bridge][health] request", flush=True)
            with lock:
                items = [
                    {"teamName": q.team_name, "queryID": q.query_id, "dialogIndex": q.dialog_index}
                    for q in active_queries.values()
                ]
            available_services = compute_available_services(selected_services)
            self._json_response(
                200,
                {
                    "ok": True,
                    "bridgeId": bridge_id,
                    "acceptedBridgeIds": list(accepted_bridge_ids),
                    "activeQueries": items,
                    "paired": bool(pairing_state.get("bridge_secret")),
                    "bridgeSecretExpiresAt": int(pairing_state.get("bridge_secret_expires_at", 0) or 0),
                    "availableServices": list(available_services),
                },
            )

        def do_POST(self) -> None:  # noqa: N802
            if self.path not in {"/register-query", "/pair", "/unpair", "/run-tool", "/cancel-query"}:
                self._json_response(404, {"ok": False, "error": "not_found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            body = self.rfile.read(max(0, length)).decode("utf-8", errors="replace")
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                self._json_response(400, {"ok": False, "error": "invalid_json"})
                return

            if not isinstance(payload, dict):
                self._json_response(400, {"ok": False, "error": "payload_must_be_object"})
                return

            if self.path == "/pair":
                req_bridge_id = str(payload.get("bridgeId", "") or bridge_id).strip()
                if req_bridge_id and req_bridge_id not in accepted_bridge_ids:
                    self._json_response(
                        409,
                        {
                            "ok": False,
                            "error": "bridge_id_mismatch",
                            "expectedBridgeId": bridge_id,
                            "acceptedBridgeIds": list(accepted_bridge_ids),
                            "receivedBridgeId": req_bridge_id,
                        },
                    )
                    return

                passcode = str(payload.get("passcode", "")).strip()
                expected_code = str(pairing_state.get("passcode", ""))
                expires_at = int(pairing_state.get("passcode_expires_at", 0) or 0)
                now = int(time.time())
                recent_failures = _prune_pair_failures(pairing_state, now=now)
                if len(recent_failures) >= PAIR_ATTEMPT_MAX_FAILURES:
                    retry_after = max(1, PAIR_ATTEMPT_WINDOW_SECS - (now - recent_failures[0]))
                    self._json_response(
                        429,
                        {
                            "ok": False,
                            "error": "pair_rate_limited",
                            "detail": f"Too many failed pairing attempts. Try again in {retry_after} seconds.",
                            "retryAfterSecs": retry_after,
                        },
                    )
                    return
                if not passcode or passcode != expected_code or (expires_at and now > expires_at):
                    failure_count = _record_pair_failure(pairing_state, now=now)
                    self._json_response(401, {"ok": False, "error": "invalid_or_expired_passcode"})
                    if failure_count >= PAIR_ATTEMPT_MAX_FAILURES:
                        print("[bridge][pair] pairing temporarily rate limited after repeated failures", flush=True)
                    return

                requested_caps = payload.get("capabilities") if isinstance(payload.get("capabilities"), list) else []
                requested_caps = [str(c).strip() for c in requested_caps if str(c).strip()]
                cap_to_service = {
                    "openclaw": "openclaw_exec",
                    "openclaw_exec": "openclaw_exec",
                }
                requested_services = [cap_to_service[c] for c in requested_caps if c in cap_to_service]
                enabled_bridge_ids: List[str] = []
                effective_services = requested_services if requested_services else list(selected_services)
                for svc in effective_services:
                    bid = SERVICE_TO_BRIDGE_ID.get(svc)
                    if bid and bid in accepted_bridge_ids and bid not in enabled_bridge_ids:
                        enabled_bridge_ids.append(bid)
                if bridge_id not in enabled_bridge_ids:
                    enabled_bridge_ids.append(bridge_id)

                bridge_secret = secrets.token_urlsafe(24)
                secret_expires_at = now + 8 * 60 * 60
                pairing_state["bridge_secret"] = bridge_secret
                pairing_state["bridge_secret_expires_at"] = secret_expires_at
                pairing_state["paired_at"] = now
                pairing_state["pair_failures"] = []

                available_services = compute_available_services(selected_services)
                self._json_response(
                    200,
                    {
                        "ok": True,
                        "bridgeId": bridge_id,
                        "acceptedBridgeIds": list(accepted_bridge_ids),
                        "bridgeSecret": bridge_secret,
                        "enabledBridgeIds": enabled_bridge_ids,
                        "availableServices": list(available_services),
                        "expiresAt": secret_expires_at,
                    },
                )
                _ = persist_pairing_code_file(bridge_id=bridge_id, pairing_state=pairing_state)
                return

            if self.path == "/unpair":
                header_secret = str(self.headers.get("X-Bridge-Secret", "")).strip()
                payload_secret = str(payload.get("bridgeSecret", "")).strip()
                expected_secret = str(pairing_state.get("bridge_secret", "") or "").strip()
                if expected_secret:
                    provided_secret = header_secret or payload_secret
                    if not provided_secret:
                        self._json_response(401, {"ok": False, "error": "bridge_secret_required"})
                        return
                    if provided_secret != expected_secret:
                        self._json_response(401, {"ok": False, "error": "invalid_bridge_secret"})
                        return

                pairing_state["bridge_secret"] = ""
                pairing_state["bridge_secret_expires_at"] = 0
                pairing_state["paired_at"] = 0
                with lock:
                    active_queries.clear()
                _ = persist_pairing_code_file(bridge_id=bridge_id, pairing_state=pairing_state)
                self._json_response(
                    200,
                    {
                        "ok": True,
                        "bridgeId": bridge_id,
                        "unpaired": True,
                    },
                )
                return

            if self.path == "/run-tool":
                req_bridge_id = str(payload.get("bridgeId", "") or bridge_id).strip()
                if req_bridge_id and req_bridge_id not in accepted_bridge_ids:
                    self._json_response(
                        409,
                        {
                            "ok": False,
                            "error": "bridge_id_mismatch",
                            "expectedBridgeId": bridge_id,
                            "acceptedBridgeIds": list(accepted_bridge_ids),
                            "receivedBridgeId": req_bridge_id,
                        },
                    )
                    return

                header_secret = str(self.headers.get("X-Bridge-Secret", "")).strip()
                payload_secret = str(payload.get("bridgeSecret", "")).strip()
                expected_secret = str(pairing_state.get("bridge_secret", "") or "").strip()
                expected_secret_expires_at = int(pairing_state.get("bridge_secret_expires_at", 0) or 0)
                now = int(time.time())
                if expected_secret and expected_secret_expires_at and now > expected_secret_expires_at:
                    pairing_state["bridge_secret"] = ""
                    pairing_state["bridge_secret_expires_at"] = 0
                    expected_secret = ""

                if expected_secret:
                    provided_secret = header_secret or payload_secret
                    if not provided_secret:
                        self._json_response(401, {"ok": False, "error": "bridge_secret_required"})
                        return
                    if provided_secret != expected_secret:
                        self._json_response(401, {"ok": False, "error": "invalid_bridge_secret"})
                        return

                tool = str(payload.get("tool", "") or "").strip()
                if not tool:
                    self._json_response(400, {"ok": False, "error": "tool_required"})
                    return
                raw_args = payload.get("args", {})
                args_obj = raw_args if isinstance(raw_args, dict) else {}
                try:
                    if tool.startswith("openclaw."):
                        print(
                            f"[bridge][run-tool] request bridge_id={req_bridge_id or bridge_id} tool={tool} args={json.dumps(args_obj, ensure_ascii=False)}",
                            flush=True,
                        )
                except Exception:
                    pass

                msg_id = f"ui-{int(time.time() * 1000)}"
                call = BridgeCall(
                    msg_id=msg_id,
                    bridge_id=req_bridge_id or bridge_id,
                    tool=tool,
                    args=args_obj,
                    raw={
                        "type": "bridge_call",
                        "msg_id": msg_id,
                        "bridge_id": req_bridge_id or bridge_id,
                        "tool": tool,
                        "args": args_obj,
                        "source": "UI",
                    },
                )

                try:
                    result = execute_call(
                        call,
                        roots,
                        pairing_state,
                        selected_services,
                        max_chars_default,
                        max_chars_hard,
                        max_find_results_default,
                        max_find_results_hard,
                    )
                except BridgeError as e:
                    try:
                        if tool.startswith("openclaw."):
                            print(
                                f"[bridge][run-tool] failed tool={tool} detail={str(e)}",
                                flush=True,
                            )
                    except Exception:
                        pass
                    self._json_response(400, {"ok": False, "error": "tool_failed", "detail": str(e)})
                    return
                try:
                    if tool.startswith("openclaw."):
                        result_keys = list(result.keys()) if isinstance(result, dict) else []
                        print(
                            f"[bridge][run-tool] ok tool={tool} result_keys={result_keys}",
                            flush=True,
                        )
                    pass
                except Exception:
                    pass

                self._json_response(
                    200,
                    {
                        "ok": True,
                        "bridgeId": bridge_id,
                        "tool": tool,
                        "result": result,
                    },
                )
                return

            if self.path == "/cancel-query":
                req_bridge_id = str(payload.get("bridgeId", "") or bridge_id).strip()
                if req_bridge_id and req_bridge_id not in accepted_bridge_ids:
                    self._json_response(
                        409,
                        {
                            "ok": False,
                            "error": "bridge_id_mismatch",
                            "expectedBridgeId": bridge_id,
                            "acceptedBridgeIds": list(accepted_bridge_ids),
                            "receivedBridgeId": req_bridge_id,
                        },
                    )
                    return

                header_secret = str(self.headers.get("X-Bridge-Secret", "")).strip()
                payload_secret = str(payload.get("bridgeSecret", "")).strip()
                expected_secret = str(pairing_state.get("bridge_secret", "") or "").strip()
                expected_secret_expires_at = int(pairing_state.get("bridge_secret_expires_at", 0) or 0)
                now = int(time.time())
                if expected_secret and expected_secret_expires_at and now > expected_secret_expires_at:
                    pairing_state["bridge_secret"] = ""
                    pairing_state["bridge_secret_expires_at"] = 0
                    expected_secret = ""

                if expected_secret:
                    provided_secret = header_secret or payload_secret
                    if not provided_secret:
                        self._json_response(401, {"ok": False, "error": "bridge_secret_required"})
                        return
                    if provided_secret != expected_secret:
                        self._json_response(401, {"ok": False, "error": "invalid_bridge_secret"})
                        return

                team_name = str(payload.get("teamName", "") or "").strip()
                query_id = str(payload.get("queryID", "") or "").strip()
                if not team_name or not query_id:
                    self._json_response(400, {"ok": False, "error": "teamName_and_queryID_required"})
                    return

                print(
                    f"[bridge][cancel-query] request bridge_id={req_bridge_id or bridge_id} team={team_name} query={query_id}",
                    flush=True,
                )
                try:
                    result = cancel_openclaw_tasks_for_query(
                        team_name=team_name,
                        query_id=query_id,
                        active_queries=active_queries,
                        pairing_state=pairing_state,
                    )
                except BridgeError as e:
                    print(
                        f"[bridge][cancel-query] failed team={team_name} query={query_id} detail={str(e)}",
                        flush=True,
                    )
                    self._json_response(400, {"ok": False, "error": "cancel_query_failed", "detail": str(e)})
                    return

                print(
                    f"[bridge][cancel-query] ok team={team_name} query={query_id} canceled={len(result.get('canceled', []))}",
                    flush=True,
                )
                self._json_response(
                    200,
                    {
                        "ok": True,
                        "bridgeId": bridge_id,
                        "result": result,
                    },
                )
                return

            register_started = time.perf_counter()
            team_name = str(payload.get("teamName", "")).strip()
            query_id = str(payload.get("queryID", "")).strip()
            user_id = str(payload.get("userID") or payload.get("username") or "").strip()
            show_partial_results = bool(payload.get("showPartialResults", False))
            req_bridge_id = str(payload.get("bridgeId", "")).strip()
            if req_bridge_id and req_bridge_id not in accepted_bridge_ids:
                self._json_response(
                    409,
                    {
                        "ok": False,
                        "error": "bridge_id_mismatch",
                        "expectedBridgeId": bridge_id,
                        "acceptedBridgeIds": list(accepted_bridge_ids),
                        "receivedBridgeId": req_bridge_id,
                    },
                )
                return
            if not team_name or not query_id:
                self._json_response(400, {"ok": False, "error": "teamName_and_queryID_required"})
                return

            header_secret = str(self.headers.get("X-Bridge-Secret", "")).strip()
            payload_secret = str(payload.get("bridgeSecret", "")).strip()
            expected_secret = str(pairing_state.get("bridge_secret", "") or "").strip()
            expected_secret_expires_at = int(pairing_state.get("bridge_secret_expires_at", 0) or 0)
            now = int(time.time())
            if expected_secret and expected_secret_expires_at and now > expected_secret_expires_at:
                pairing_state["bridge_secret"] = ""
                pairing_state["bridge_secret_expires_at"] = 0
                expected_secret = ""

            if header_secret or payload_secret:
                print(
                    f"[bridge][auth] register-query bridgeSecret present header={bool(header_secret)} payload={bool(payload_secret)}",
                    flush=True,
                )
            provided_secret = header_secret or payload_secret
            if not expected_secret:
                self._json_response(401, {"ok": False, "error": "bridge_not_paired"})
                return
            if not provided_secret:
                self._json_response(401, {"ok": False, "error": "bridge_secret_required"})
                return
            if provided_secret != expected_secret:
                self._json_response(401, {"ok": False, "error": "invalid_bridge_secret"})
                return

            id_token = str(payload.get("idToken", "") or "").strip()
            owner_key = str(payload.get("ownerKey", "") or "").strip()
            owner_key_header = str(payload.get("ownerKeyHeader", "") or "x-owner-key").strip()
            auth_mode = str(payload.get("authMode", "auto") or "auto").strip().lower()
            if id_token:
                print(
                    f"[bridge][auth] register-query received idToken={redact_secret(id_token)} len={len(id_token)} team={team_name} query={query_id}",
                    flush=True,
                )
            else:
                print(
                    f"[bridge][auth] register-query missing idToken team={team_name} query={query_id}",
                    flush=True,
                )
            try:
                req_headers, req_mode = auth_headers(
                    auth_mode=auth_mode,
                    id_token=id_token,
                    owner_key=owner_key,
                    owner_key_header=owner_key_header,
                )
            except BridgeError as e:
                self._json_response(400, {"ok": False, "error": "invalid_auth", "detail": str(e)})
                return
            merged_headers = req_headers if has_auth_credential(req_headers) else dict(default_auth_headers)
            record_bridge_debug_event(
                pairing_state,
                "register_query",
                team_name=team_name,
                query_id=query_id,
                user_id=user_id,
                payload={
                    "authMode": req_mode if has_auth_credential(req_headers) else "default",
                    "hasAuthCredential": bool(has_auth_credential(merged_headers)),
                    "showPartialResults": bool(show_partial_results),
                    "requestMs": int(round((time.perf_counter() - register_started) * 1000.0)),
                },
            )

            key = (team_name, query_id)
            with lock:
                q = active_queries.get(key)
                if q is None:
                    active_queries[key] = ActiveQuery(
                        team_name=team_name,
                        query_id=query_id,
                        user_id=user_id,
                        dialog_index=-1,
                        last_seen_ts=time.time(),
                        auth_headers=merged_headers,
                        show_partial_results=show_partial_results,
                    )
                else:
                    q.last_seen_ts = time.time()
                    q.poll_errors = 0
                    q.auth_headers = merged_headers
                    if user_id:
                        q.user_id = user_id
                    q.show_partial_results = show_partial_results
            record_bridge_debug_event(
                pairing_state,
                "active_query_upserted",
                team_name=team_name,
                query_id=query_id,
                user_id=user_id,
                payload={
                    "activeCount": len(active_queries),
                    "requestMs": int(round((time.perf_counter() - register_started) * 1000.0)),
                },
            )
            self._json_response(
                200,
                {
                    "ok": True,
                    "bridgeId": bridge_id,
                    "teamName": team_name,
                    "queryID": query_id,
                    "showPartialResults": bool(show_partial_results),
                    "authMode": req_mode if has_auth_credential(req_headers) else "default",
                },
            )

        def log_message(self, fmt: str, *args) -> None:
            return

    return RegistrationHandler


def start_registration_server(
    *,
    port: int,
    bridge_id: str,
    active_queries: Dict[Tuple[str, str], ActiveQuery],
    lock: threading.Lock,
    default_auth_headers: Dict[str, str],
    pairing_state: Dict[str, object],
    roots: List[Path],
    selected_services: List[str],
    accepted_bridge_ids: List[str],
    max_chars_default: int,
    max_chars_hard: int,
    max_find_results_default: int,
    max_find_results_hard: int,
) -> ThreadingHTTPServer:
    handler = make_registration_handler(
        bridge_id,
        active_queries,
        lock,
        default_auth_headers,
        pairing_state,
        roots,
        selected_services,
        accepted_bridge_ids,
        max_chars_default,
        max_chars_hard,
        max_find_results_default,
        max_find_results_hard,
    )
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def http_post_json(url: str, payload: Dict[str, object], headers: Dict[str, str]) -> Dict[str, object]:
    body = json.dumps(payload).encode("utf-8")
    req = Request(url=url, data=body, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        raise BridgeError(f"HTTP {e.code} from {url}: {e.reason}") from e
    except URLError as e:
        raise BridgeError(f"Network error calling {url}: {e}") from e

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def poll_dialog(retrieve_endpoint: str, headers: Dict[str, str], team_name: str, query_id: str, dialog_index: int) -> Dict[str, object]:
    payload = {
        "dialogIndex": dialog_index,
        "queryID": query_id,
        "teamName": team_name,
        "queryType": "getResponses",
    }
    return http_post_json(retrieve_endpoint, payload, headers)


def poll_bridge_messages(
    query_endpoint: str,
    headers: Dict[str, str],
    team_name: str,
    query_id: str,
    bridge_id: str,
    user_id: str = "",
) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "type": "getBridgeMessages",
        "queryID": query_id,
        "teamName": team_name,
        "bridgeID": bridge_id,
        "debugBridgeMessages": True,
    }
    if user_id:
        payload["userID"] = user_id
        payload["username"] = user_id
    return http_post_json(query_endpoint, payload, headers)


def extract_bridge_calls_from_messages(messages: object) -> List[BridgeCall]:
    calls: List[BridgeCall] = []
    seen: set[str] = set()
    if not isinstance(messages, list):
        return calls

    for m in messages:
        obj: Optional[Dict[str, object]] = None
        envelope_source = ""
        envelope_meta: Dict[str, object] = {}
        if isinstance(m, dict):
            envelope_source = str(m.get("source", "") or "").strip()
            envelope_meta = {
                "created_at": m.get("created_at", m.get("createdAt")),
                "claimedAt": m.get("claimedAt"),
                "processedAt": m.get("processedAt"),
                "queryID": m.get("queryID"),
                "teamName": m.get("teamName"),
                "runKey": m.get("run_key", m.get("runKey")),
                "status": m.get("status"),
            }
            if isinstance(m.get("payload"), dict):
                obj = dict(m.get("payload") or {})  # outbox item shape
                if envelope_source and not str(obj.get("source", "")).strip():
                    obj["source"] = envelope_source
            else:
                obj = dict(m)
                if envelope_source and not str(obj.get("source", "")).strip():
                    obj["source"] = envelope_source
        elif isinstance(m, str):
            obj = try_load_json(m.strip())

        if not isinstance(obj, dict):
            continue
        if str(obj.get("type", "")).strip() != "bridge_call":
            continue

        if envelope_meta:
            sentienta_meta = obj.get("_sentienta")
            if not isinstance(sentienta_meta, dict):
                sentienta_meta = {}
            for key, value in envelope_meta.items():
                if value is None:
                    continue
                sentienta_meta[key] = value
            obj["_sentienta"] = sentienta_meta

        msg_id = str(obj.get("msg_id") or obj.get("call_id") or "").strip()
        if not msg_id or msg_id in seen:
            continue
        seen.add(msg_id)

        calls.append(
            BridgeCall(
                msg_id=msg_id,
                bridge_id=str(obj.get("bridge_id", "")).strip(),
                tool=str(obj.get("tool", "")).strip(),
                args=obj.get("args", {}) if isinstance(obj.get("args", {}), dict) else {},
                raw=obj,
            )
        )
    return calls


def _bridge_call_source_name(raw_call: Dict[str, object]) -> str:
    """
    Resolve display source for a bridge result.
    Prefer the originating agent/source attached to the bridge call payload.
    """
    if not isinstance(raw_call, dict):
        return "DesktopBridge"
    src = str(raw_call.get("source", "") or "").strip()
    if not src:
        return "DesktopBridge"
    # Normalize common transport prefixes.
    if ":" in src:
        src = src.split(":", 1)[0].strip()
    return src or "DesktopBridge"


def parse_json_object_from_index(text: str, start_index: int) -> Optional[Tuple[str, int]]:
    depth = 0
    in_string = False
    escape = False
    first = -1
    for i in range(start_index, len(text)):
        ch = text[i]
        if first < 0:
            if ch == "{":
                first = i
                depth = 1
            continue

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[first : i + 1], i + 1
    return None


def try_load_json(s: str) -> Optional[Dict[str, object]]:
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        return None
    if isinstance(obj, dict):
        return obj
    return None


def extract_bridge_calls(text: str) -> List[BridgeCall]:
    calls: List[BridgeCall] = []
    seen: set[str] = set()
    decoded_text = html.unescape(str(text or ""))

    # 1) Explicit tag envelope
    for m in re.finditer(r"<bridge_call>\s*(\{.*?\})\s*</bridge_call>", decoded_text, flags=re.IGNORECASE | re.DOTALL):
        obj = try_load_json(m.group(1).strip())
        if not obj:
            continue
        if str(obj.get("type", "")).strip() != "bridge_call":
            continue
        msg_id = str(obj.get("msg_id") or obj.get("call_id") or "").strip()
        if not msg_id or msg_id in seen:
            continue
        seen.add(msg_id)
        calls.append(
            BridgeCall(
                msg_id=msg_id,
                bridge_id=str(obj.get("bridge_id", "")).strip(),
                tool=str(obj.get("tool", "")).strip(),
                args=obj.get("args", {}) if isinstance(obj.get("args", {}), dict) else {},
                raw=obj,
            )
        )

    # 2) Raw JSON object containing "type":"bridge_call"
    idx = 0
    while idx < len(decoded_text):
        pos = decoded_text.find('"type"', idx)
        if pos < 0:
            break
        candidate_start = decoded_text.rfind("{", 0, pos)
        if candidate_start < 0:
            idx = pos + 6
            continue
        parsed = parse_json_object_from_index(decoded_text, candidate_start)
        if not parsed:
            idx = pos + 6
            continue
        blob, end_pos = parsed
        idx = end_pos
        obj = try_load_json(blob)
        if not obj or str(obj.get("type", "")).strip() != "bridge_call":
            continue
        msg_id = str(obj.get("msg_id") or obj.get("call_id") or "").strip()
        if not msg_id or msg_id in seen:
            continue
        seen.add(msg_id)
        calls.append(
            BridgeCall(
                msg_id=msg_id,
                bridge_id=str(obj.get("bridge_id", "")).strip(),
                tool=str(obj.get("tool", "")).strip(),
                args=obj.get("args", {}) if isinstance(obj.get("args", {}), dict) else {},
                raw=obj,
            )
        )

    return calls


def is_actionable_bridge_call(call: BridgeCall, accepted_bridge_ids: List[str]) -> bool:
    """
    Final execution guard:
    - must be type=bridge_call
    - must target this bridge_id
    - must include required fields
    """
    raw = call.raw if isinstance(call.raw, dict) else {}
    if str(raw.get("type", "")).strip() != "bridge_call":
        return False
    provided_bridge_id = str(call.bridge_id or "").strip().lower()
    accepted = {str(x or "").strip().lower() for x in accepted_bridge_ids}
    if provided_bridge_id not in accepted:
        return False
    if not str(call.msg_id or "").strip():
        return False
    if not str(call.tool or "").strip():
        return False
    if not isinstance(call.args, dict):
        return False
    return True


def _ensure_openclaw_runtime(pairing_state: Dict[str, object]) -> Dict[str, object]:
    runtime = pairing_state.get("openclaw_runtime")
    if isinstance(runtime, dict):
        if not isinstance(runtime.get("tasks"), dict):
            runtime["tasks"] = {}
        return runtime
    runtime = {"tasks": {}}
    pairing_state["openclaw_runtime"] = runtime
    return runtime


def _oc_task_cache_path(pairing_state: Dict[str, object]) -> Path:
    cfg = pairing_state.get("openclaw_config", {})
    if isinstance(cfg, dict):
        p = str(cfg.get("task_cache_path", "") or "").strip()
        if p:
            return Path(p).expanduser().resolve()
    return DEFAULT_OC_TASK_CACHE_FILE


def _oc_load_task_cache(pairing_state: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    cache = pairing_state.get("_oc_task_cache")
    if isinstance(cache, dict):
        return cache
    path = _oc_task_cache_path(pairing_state)
    loaded: Dict[str, Dict[str, object]] = {}
    try:
        if path.exists():
            obj = try_load_json(path.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, dict):
                        loaded[str(k)] = v
    except Exception as e:
        print(f"[bridge][oc] failed loading task cache: {e}", flush=True)
    pairing_state["_oc_task_cache"] = loaded
    return loaded


def _oc_save_task_cache(pairing_state: Dict[str, object], cache: Dict[str, Dict[str, object]]) -> None:
    try:
        path = _oc_task_cache_path(pairing_state)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Keep latest 300 entries by updated_at_ms.
        items = sorted(
            cache.items(),
            key=lambda kv: int((kv[1] or {}).get("updated_at_ms", 0) or 0),
            reverse=True,
        )[:300]
        trimmed = {k: v for k, v in items}
        path.write_text(json.dumps(trimmed, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        pairing_state["_oc_task_cache"] = trimmed
    except Exception as e:
        print(f"[bridge][oc] failed saving task cache: {e}", flush=True)


def _oc_compact_result_for_cache(result_obj: object) -> Dict[str, object]:
    try:
        if not isinstance(result_obj, dict):
            return {}
        out = json.loads(json.dumps(result_obj, ensure_ascii=False))
        if not isinstance(out, dict):
            return {}
        inner = out.get("result")
        if not isinstance(inner, dict):
            return out
        payloads = inner.get("payloads")
        if not isinstance(payloads, list):
            return out

        compact_payloads: List[Dict[str, object]] = []
        for payload in payloads[:8]:
            if not isinstance(payload, dict):
                continue
            entry = dict(payload)
            text = str(entry.get("text", "") or "")
            if len(text) > 2000:
                entry["text"] = text[:2000] + "..."
            compact_payloads.append(entry)
        inner["payloads"] = compact_payloads
        return out
    except Exception:
        return result_obj if isinstance(result_obj, dict) else {}


def _oc_compact_recent_events_for_cache(events: object) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    try:
        for event in (events if isinstance(events, list) else [])[-10:]:
            if not isinstance(event, dict):
                continue
            item = dict(event)
            text = str(item.get("text", "") or "")
            if len(text) > 500:
                item["text"] = text[:500] + "..."
            out.append(item)
    except Exception:
        return out
    return out


def _oc_persist_task_snapshot(pairing_state: Dict[str, object], rec: Dict[str, object]) -> None:
    try:
        task_id = str(rec.get("task_id", "") or "").strip()
        if not task_id:
            return
        snap = {
            "task_id": task_id,
            "status": str(rec.get("status", "unknown") or "unknown"),
            "created_at": int(rec.get("created_at", int(time.time())) or int(time.time())),
            "created_at_ms": int(rec.get("created_at_ms", int(time.time() * 1000)) or int(time.time() * 1000)),
            "updated_at": int(rec.get("updated_at", int(time.time())) or int(time.time())),
            "updated_at_ms": int(rec.get("updated_at_ms", int(time.time() * 1000)) or int(time.time() * 1000)),
            "elapsed_ms": int(rec.get("elapsed_ms", 0) or 0),
            "timeout_ms": int(rec.get("timeout_ms", 210000) or 210000),
            "poll_count": int(rec.get("poll_count", 0) or 0),
            "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
            "agent": str(rec.get("agent", "") or ""),
            "execution_mode": str(rec.get("execution_mode", "") or ""),
            "session_key": str(rec.get("session_key", "") or ""),
            "run_id": str(rec.get("run_id", "") or ""),
            "summary": str(rec.get("summary", "") or "").strip(),
            "error": str(rec.get("error", "") or "").strip(),
            "result": _oc_compact_result_for_cache(rec.get("result", {})),
            "stdout_tail": _oc_snapshot_tail(rec, "stdout_tail", n=5),
            "stderr_tail": _oc_snapshot_tail(rec, "stderr_tail", n=5),
            "last_stdout_line": str(rec.get("last_stdout_line", "") or ""),
            "last_stderr_line": str(rec.get("last_stderr_line", "") or ""),
            "latest_event": str(rec.get("latest_event", "") or "").strip(),
            "recent_events": _oc_compact_recent_events_for_cache(rec.get("recent_events", [])),
            "session_id": str(rec.get("session_id", "") or "").strip(),
            "session_jsonl_path": str(rec.get("session_jsonl_path", "") or "").strip(),
            "session_read_pos": int(rec.get("session_read_pos", 0) or 0),
        }
        cache = _oc_load_task_cache(pairing_state)
        cache[task_id] = snap
        _oc_save_task_cache(pairing_state, cache)
    except Exception as e:
        print(f"[bridge][oc] failed persisting task snapshot: {e}", flush=True)


def _oc_get_cached_task(
    pairing_state: Dict[str, object],
    task_id: str,
    requested_agent_id: str = "",
) -> Optional[Dict[str, object]]:
    try:
        cache = _oc_load_task_cache(pairing_state)
        rec = cache.get(str(task_id or "").strip())
        if not isinstance(rec, dict):
            return None
        req = str(requested_agent_id or "").strip()
        have = str(rec.get("agent", "") or "").strip()
        if req and have and req != have:
            return None
        return rec
    except Exception as e:
        print(f"[bridge][oc] failed reading cached task: {e}", flush=True)
        return None


def _oc_is_meaningful_event_line(text: str) -> bool:
    s = str(text or "").strip()
    if not s:
        return False
    if s in {"{", "}", "[", "]", ",", "```", "```json"}:
        return False
    if re.fullmatch(r"[\{\}\[\],:]+", s):
        return False
    # Drop noisy boilerplate that is not user-meaningful progress.
    low = s.lower()
    noisy_prefixes = (
        "security notice:",
        "source: web fetch",
    )
    if any(low.startswith(p) for p in noisy_prefixes):
        return False
    return True


def _oc_sanitize_event_text(text: str, max_len: int = 500) -> str:
    """
    Normalize noisy stream text into a compact single-line progress fragment.
    """
    try:
        s = str(text or "")
        if not s:
            return ""
        # Strip ANSI color/control sequences.
        s = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", s)
        s = s.replace("\r", " ").replace("\n", " ").strip()
        s = re.sub(r"\s+", " ", s)
        if not s:
            return ""
        if len(s) > max_len:
            s = s[: max_len - 3].rstrip() + "..."
        return s
    except Exception:
        return ""


def _oc_make_fallback_progress_line(rec: Dict[str, object]) -> str:
    """
    Produce a low-noise fallback when no meaningful event line is captured.
    """
    try:
        last_out = _oc_sanitize_event_text(rec.get("last_stdout_line", ""))
        last_err = _oc_sanitize_event_text(rec.get("last_stderr_line", ""))

        # Prefer stderr if present (often carries explicit progress text).
        base = last_err or last_out
        if not base:
            tail = _oc_snapshot_tail(rec, "stdout_tail", n=3)
            for ln in reversed(tail):
                cleaned = _oc_sanitize_event_text(ln)
                if cleaned:
                    base = cleaned
                    break
        if not base:
            return ""

        if len(base) < 16:
            base = f"Progress update: {base}"
        return base
    except Exception:
        return ""


def _oc_agents_root_from_state(pairing_state: Dict[str, object]) -> Path:
    try:
        cfg = pairing_state.get("openclaw_config", {})
        if isinstance(cfg, dict):
            p = str(cfg.get("agents_root", "") or "").strip()
            if p:
                return Path(p).expanduser().resolve()
    except Exception:
        pass
    return (Path.home() / ".openclaw" / "agents").resolve()


def _oc_config_get_json(cli_path: str, key: str, timeout_sec: int = 60) -> object:
    try:
        rc, out, err = _oc_cli_run(cli_path, ["config", "get", str(key or "").strip()], timeout_sec=timeout_sec)
    except Exception as e:
        raise BridgeError(f"OpenClaw config get failed: {e}") from e
    if rc != 0:
        raise BridgeError(f"OpenClaw config get failed: {str(err or out or '').strip() or f'exit {rc}'}")
    txt = str(out or "").strip()
    if not txt:
        return None
    try:
        return json.loads(txt)
    except Exception as e:
        raise BridgeError(f"OpenClaw config output was not valid JSON for {key}: {e}") from e


def _oc_parse_agents_list_text(cli_path: str) -> Dict[str, Dict[str, object]]:
    out_map: Dict[str, Dict[str, object]] = {}
    try:
        rc, out, err = _oc_cli_run(cli_path, ["agents", "list"], timeout_sec=60)
        if rc != 0:
            return out_map
        current: Optional[Dict[str, object]] = None
        for raw_line in str(out or "").splitlines():
            line = str(raw_line or "").rstrip()
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("- "):
                head = stripped[2:].strip()
                default_flag = False
                if head.endswith("(default)"):
                    head = head[: -len("(default)")].rstrip()
                    default_flag = True
                agent_id = head
                current = {
                    "agent_id": agent_id,
                    "name": agent_id,
                    "default": default_flag,
                }
                out_map[agent_id] = current
                continue
            if current is None or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            key_low = key.strip().lower()
            val = value.strip()
            if key_low == "identity":
                current["identity_display"] = val
            elif key_low == "workspace":
                current["workspace_path"] = val
            elif key_low == "agent dir":
                current["agent_dir"] = val
            elif key_low == "model":
                current["model"] = val
            elif key_low == "routing rules":
                current["routing_rules"] = val
            elif key_low == "routing":
                current["routing"] = val
    except Exception as e:
        print("Failed in _oc_parse_agents_list_text", e)
    return out_map


def _oc_read_persona_files(workspace_path: Path) -> Tuple[Dict[str, str], str]:
    files: Dict[str, str] = {}
    ordered_names = ["SOUL.md", "IDENTITY.md", "AGENTS.md", "USER.md", "TOOLS.md"]
    summary_parts: List[str] = []
    try:
        for name in ordered_names:
            p = workspace_path / name
            if not p.exists() or not p.is_file():
                continue
            txt = p.read_text(encoding="utf-8", errors="replace").strip()
            if not txt:
                continue
            files[name] = txt
            if len(summary_parts) < 3:
                cleaned = re.sub(r"\s+", " ", txt).strip()
                if cleaned:
                    summary_parts.append(cleaned[:280])
    except Exception as e:
        print("Failed in _oc_read_persona_files", e)
    summary = "\n\n".join(summary_parts).strip()
    return files, summary


def _oc_build_agent_record_from_sources(
    agent_obj: Dict[str, object],
    text_details: Dict[str, object],
    pairing_state: Dict[str, object],
) -> Dict[str, object]:
    obj = agent_obj if isinstance(agent_obj, dict) else {}
    text = text_details if isinstance(text_details, dict) else {}
    agent_id = str(obj.get("id") or obj.get("agent_id") or text.get("agent_id") or "").strip()
    if not agent_id:
        raise BridgeError("OpenClaw agent record missing id")
    workspace_raw = str(obj.get("workspace") or text.get("workspace_path") or "").strip()
    if not workspace_raw:
        workspace_raw = str((_oc_agents_root_from_state(pairing_state) / agent_id).resolve())
    workspace_path = Path(workspace_raw).expanduser().resolve()
    persona_files, persona_summary = _oc_read_persona_files(workspace_path)
    identity_obj = obj.get("identity", {}) if isinstance(obj.get("identity"), dict) else {}
    identity_name = str(identity_obj.get("name") or "").strip()
    identity_theme = str(identity_obj.get("theme") or "").strip()
    identity_emoji = str(identity_obj.get("emoji") or "").strip()
    if not identity_name:
        ident_md = persona_files.get("IDENTITY.md", "")
        first_line = next((ln.strip("# ").strip() for ln in ident_md.splitlines() if ln.strip()), "")
        identity_name = first_line
    model = str(obj.get("model") or text.get("model") or "").strip()
    tools = obj.get("tools", [])
    if not isinstance(tools, list):
        tools = []
    runtime = obj.get("runtime")
    sandbox = obj.get("sandbox")
    name = str(obj.get("name") or text.get("name") or agent_id or identity_name).strip() or agent_id
    return {
        "agent_id": agent_id,
        "name": name,
        "workspace_path": str(workspace_path),
        "agent_dir": str(obj.get("agentDir") or text.get("agent_dir") or ""),
        "model": model,
        "runtime": runtime if runtime is not None else "",
        "sandbox": sandbox if sandbox is not None else "",
        "tools": tools,
        "subagents": obj.get("subagents", []) if isinstance(obj.get("subagents"), list) else [],
        "identity": {
            "name": identity_name,
            "theme": identity_theme,
            "emoji": identity_emoji,
            "avatar": str(identity_obj.get("avatar") or "").strip(),
            "display": str(text.get("identity_display") or "").strip(),
        },
        "persona_files": persona_files,
        "persona_summary": persona_summary,
        "default": bool(obj.get("default") or text.get("default")),
    }


def execute_openclaw_agents_list(call: BridgeCall, pairing_state: Dict[str, object]) -> Dict[str, object]:
    cfg = pairing_state.get("openclaw_config", {})
    if not isinstance(cfg, dict):
        cfg = {}
    cli_hint = str(cfg.get("cli_path", "openclaw") or "openclaw").strip() or "openclaw"
    cli_path = _resolve_openclaw_cli(cli_hint)
    raw_agents = _oc_config_get_json(cli_path, "agents.list", timeout_sec=60)
    if not isinstance(raw_agents, list):
        raise BridgeError("OpenClaw returned non-list value for agents.list")
    text_map = _oc_parse_agents_list_text(cli_path)
    agents: List[Dict[str, object]] = []
    details_cache: Dict[str, Dict[str, object]] = pairing_state.setdefault("_oc_agent_details_cache", {})
    for item in raw_agents:
        if not isinstance(item, dict):
            continue
        aid = str(item.get("id") or item.get("agent_id") or "").strip()
        if not aid:
            continue
        display_name = str(item.get("name") or "").strip() or str(item.get("identity", {}).get("name") if isinstance(item.get("identity"), dict) else "").strip() or aid
        row = {
            "agent_id": aid,
            "name": display_name,
            "model": str(item.get("model") or text_map.get(aid, {}).get("model") or "").strip(),
            "workspace_path": str(item.get("workspace") or text_map.get(aid, {}).get("workspace_path") or ""),
            "default": bool(item.get("default") or text_map.get(aid, {}).get("default")),
        }
        agents.append(row)
        try:
            details_cache[aid] = _oc_build_agent_record_from_sources(item, text_map.get(aid, {}), pairing_state)
        except Exception:
            pass
    return {
        "tool": "openclaw.agents.list",
        "agents": agents,
    }


def execute_openclaw_agents_get(call: BridgeCall, pairing_state: Dict[str, object]) -> Dict[str, object]:
    args = call.args if isinstance(call.args, dict) else {}
    agent_id = str(args.get("agent_id") or args.get("id") or "").strip()
    if not agent_id:
        raise BridgeError("args.agent_id is required")
    cache = pairing_state.setdefault("_oc_agent_details_cache", {})
    if isinstance(cache, dict):
        cached = cache.get(agent_id)
        if isinstance(cached, dict):
            return {
                "tool": "openclaw.agents.get",
                "agent": cached,
            }
    cfg = pairing_state.get("openclaw_config", {})
    if not isinstance(cfg, dict):
        cfg = {}
    cli_hint = str(cfg.get("cli_path", "openclaw") or "openclaw").strip() or "openclaw"
    cli_path = _resolve_openclaw_cli(cli_hint)
    raw_agents = _oc_config_get_json(cli_path, "agents.list", timeout_sec=60)
    if not isinstance(raw_agents, list):
        raise BridgeError("OpenClaw returned non-list value for agents.list")
    text_map = _oc_parse_agents_list_text(cli_path)
    for item in raw_agents:
        if not isinstance(item, dict):
            continue
        aid = str(item.get("id") or item.get("agent_id") or "").strip()
        if aid != agent_id:
            continue
        record = _oc_build_agent_record_from_sources(item, text_map.get(aid, {}), pairing_state)
        if isinstance(cache, dict):
            cache[aid] = record
        return {
            "tool": "openclaw.agents.get",
            "agent": record,
        }
    raise BridgeError(f'OpenClaw agent "{agent_id}" not found')


def execute_openclaw_healthcheck(call: BridgeCall, pairing_state: Dict[str, object]) -> Dict[str, object]:
    cfg = pairing_state.get("openclaw_config", {})
    if not isinstance(cfg, dict):
        cfg = {}
    args = call.args if isinstance(call.args, dict) else {}
    scope = str(args.get("scope") or "all").strip().lower() or "all"

    cli_hint = str(cfg.get("cli_path", "openclaw") or "openclaw").strip() or "openclaw"
    result: Dict[str, object] = {
        "tool": "openclaw.healthcheck",
        "scope": scope,
        "cli": {"ok": False, "path": "", "detail": ""},
        "runtime": {"ok": False, "detail": "", "raw": ""},
        "agents": {"ok": False, "count": 0, "detail": ""},
        "agent_rows": [],
        "template_agent": {"ok": False, "agent_id": "main", "detail": ""},
        "warnings": [],
    }

    try:
        cli_path = _resolve_openclaw_cli(cli_hint)
        result["cli"] = {"ok": True, "path": cli_path, "detail": "OpenClaw CLI resolved."}
    except Exception as e:
        result["cli"] = {"ok": False, "path": "", "detail": str(e)}
        return result

    if scope == "cli":
        return result

    cli_path = str(result["cli"].get("path") or "").strip()
    warnings: List[str] = []

    if scope in {"all", "gateway", "runtime", "template"}:
        try:
            rc, out, err = _oc_gateway_call(
                cli_path,
                "health",
                {},
                timeout_ms=5000,
                expect_final=False,
            )
            raw = str(out or err or "").strip()
            if rc == 0:
                result["runtime"] = {
                    "ok": True,
                    "detail": "Runtime responded.",
                    "raw": raw[:1000],
                }
            else:
                result["runtime"] = {
                    "ok": False,
                    "detail": raw or f"runtime health probe exited with code {rc}",
                    "raw": raw[:1000],
                }
            if "brave" in raw.lower() and "api key" in raw.lower():
                warnings.append("Brave Search API key appears to be missing.")
        except Exception as e:
            result["runtime"] = {"ok": False, "detail": str(e), "raw": ""}
        if scope in {"gateway", "runtime"}:
            result["warnings"] = warnings
            return result

    runtime = _ensure_openclaw_runtime(pairing_state)
    if scope in {"all", "agents"}:
        try:
            list_result = execute_openclaw_agents_list(call, pairing_state)
            rows = list_result.get("agents") if isinstance(list_result, dict) else []
            if isinstance(rows, list):
                result["agents"] = {
                    "ok": True,
                    "count": len([x for x in rows if isinstance(x, dict)]),
                    "detail": "Agent inventory loaded.",
                }
                result["agent_rows"] = rows
            else:
                result["agents"] = {"ok": False, "count": 0, "detail": "OpenClaw returned non-list agents.list output."}
        except Exception as e:
            result["agents"] = {"ok": False, "count": 0, "detail": str(e)}
        if scope == "agents":
            result["warnings"] = warnings
            return result

    template_agent_id = str(cfg.get("default_agent", "main") or "main").strip() or "main"
    if scope in {"all", "template"}:
        runtime_ok = bool(result.get("runtime", {}).get("ok")) if isinstance(result.get("runtime"), dict) else False
        if not runtime_ok:
            result["template_agent"] = {
                "ok": False,
                "agent_id": template_agent_id,
                "detail": "Runtime is unavailable, so the template agent was not tested.",
            }
        else:
            try:
                _oc_smoke_test_template_agent(cli_path, runtime, template_agent_id, use_cache=False)
                result["template_agent"] = {
                    "ok": True,
                    "agent_id": template_agent_id,
                    "detail": f'Template agent "{template_agent_id}" responded to test query.',
                }
            except Exception as e:
                detail = str(e)
                result["template_agent"] = {
                    "ok": False,
                    "agent_id": template_agent_id,
                    "detail": detail,
                }
                if _oc_is_stale_auth_error(detail) or "stale oauth" in detail.lower():
                    warnings.append(f'Template agent "{template_agent_id}" has stale OAuth/auth state.')

    result["warnings"] = warnings
    return result


def _oc_resolve_agent_session_jsonl(
    pairing_state: Dict[str, object],
    agent_id: str,
) -> Tuple[str, Optional[Path]]:
    """
    Resolve active session JSONL path for an agent from sessions/sessions.json.
    """
    try:
        aid = str(agent_id or "").strip()
        if not aid:
            return "", None
        agent_dir = _oc_agents_root_from_state(pairing_state) / aid
        sessions_meta = agent_dir / "sessions" / "sessions.json"
        if not sessions_meta.exists():
            return "", None
        obj = try_load_json(sessions_meta.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return "", None

        best_sid = ""
        best_updated = -1
        key_prefix = f"agent:{aid}:"
        for k, v in obj.items():
            if not isinstance(v, dict):
                continue
            if not str(k or "").startswith(key_prefix):
                continue
            sid = str(v.get("sessionId", "") or "").strip()
            upd = int(v.get("updatedAt", 0) or 0)
            if sid and upd >= best_updated:
                best_sid = sid
                best_updated = upd
        if not best_sid:
            return "", None
        p = agent_dir / "sessions" / f"{best_sid}.jsonl"
        return best_sid, p
    except Exception as e:
        print("Failed in _oc_resolve_agent_session_jsonl", e)
        return "", None


def _oc_extract_event_lines_from_jsonl_obj(obj: Dict[str, object]) -> List[Dict[str, str]]:
    """
    Extract concise user-meaningful lines from one JSONL event object.
    """
    out: List[Dict[str, str]] = []
    try:
        if not isinstance(obj, dict):
            return out
        typ = str(obj.get("type", "") or "").strip().lower()
        if typ != "message":
            return out
        msg = obj.get("message")
        if not isinstance(msg, dict):
            return out
        author = msg.get("author")
        if not isinstance(author, dict):
            author = {}
        raw_role = (
            msg.get("role")
            or msg.get("authorRole")
            or author.get("role")
            or author.get("type")
            or ""
        )
        raw_name = (
            msg.get("name")
            or author.get("name")
            or author.get("label")
            or author.get("displayName")
            or ""
        )
        raw_role_txt = str(raw_role or "").strip().lower()
        raw_name_txt = str(raw_name or "").strip().lower()
        is_tool_message = raw_role_txt in {"tool", "toolresult"} or raw_name_txt in {"tool", "tools"}
        role = "toolresult" if is_tool_message else "assistant"
        content = msg.get("content")
        if not isinstance(content, list):
            return out

        noisy_key_re = re.compile(
            r'^\s*"?('
            r'fetchedAt|timestamp|type|id|status|extractMode|extractor|'
            r'contentType|title|url|finalUrl|length|rawLength|wrappedLength|'
            r'tookMs|api|provider|model'
            r')"?\s*:',
            flags=re.IGNORECASE,
        )

        def _collect_from_text_blob(blob: str) -> None:
            txt = str(blob or "")
            if not txt:
                return

            # If the blob is JSON-serialized tool payload, try to unwrap the inner "text".
            j = try_load_json(txt)
            if isinstance(j, dict):
                inner_txt = str(j.get("text", "") or "")
                if inner_txt:
                    txt = inner_txt

            for ln in txt.splitlines():
                clean = _oc_sanitize_event_text(ln, max_len=500)
                if not clean:
                    continue
                low = clean.lower()
                if "security notice:" in low:
                    continue
                if "<<<external_untrusted_content>>>" in low or "<<<end_external_untrusted_content>>>" in low:
                    continue
                if noisy_key_re.match(clean):
                    continue
                # Prefer clearly user-meaningful lines.
                keep = False
                if clean.startswith("http://") or clean.startswith("https://"):
                    keep = True
                elif "127.0.0.1" in clean:
                    keep = True
                elif low.startswith("successfully wrote"):
                    keep = True
                elif low.startswith("latest "):
                    keep = True
                elif low.startswith("saved ") or " saved to " in low:
                    keep = True
                elif role == "assistant":
                    # For assistant replies, keep concise final text too.
                    keep = True
                elif len(clean) >= 40 and _oc_is_meaningful_event_line(clean):
                    keep = True
                if keep:
                    out.append({"text": clean, "role": role, "stream": "session"})

        for part in content:
            if not isinstance(part, dict):
                continue
            ptype = str(part.get("type", "") or "").strip().lower()
            if ptype == "text":
                _collect_from_text_blob(str(part.get("text", "") or ""))
    except Exception as e:
        print("Failed in _oc_extract_event_lines_from_jsonl_obj", e)
    return out


def _oc_capture_session_jsonl_events(rec: Dict[str, object], pairing_state: Dict[str, object]) -> None:
    """
    Tail the OpenClaw session JSONL and merge new event lines into recent_events/latest_event.
    """
    try:
        if not isinstance(rec, dict):
            return
        dbg_count = int(rec.get("session_debug_poll_count", 0) or 0) + 1
        rec["session_debug_poll_count"] = dbg_count
        agent_id = str(rec.get("agent", "") or "").strip()
        if not agent_id:
            return

        sid = str(rec.get("session_id", "") or "").strip()
        ptxt = str(rec.get("session_jsonl_path", "") or "").strip()
        jsonl_path = Path(ptxt).expanduser().resolve() if ptxt else None

        if not sid or not jsonl_path or not jsonl_path.exists():
            rsid, rpath = _oc_resolve_agent_session_jsonl(pairing_state, agent_id)
            if not rsid or not rpath:
                try:
                    if dbg_count <= 3 or (dbg_count % 10 == 0):
                        log(
                            f"[bridge][oc][session] task_id={str(rec.get('task_id','') or '')} "
                            f"agent={agent_id} unresolved_session=True",
                            verbose=True,
                        )
                except Exception:
                    pass
                return
            sid = rsid
            jsonl_path = rpath
            rec["session_id"] = sid
            rec["session_jsonl_path"] = str(jsonl_path)
            rec["session_read_pos"] = 0

        if not jsonl_path.exists():
            try:
                if dbg_count <= 3 or (dbg_count % 10 == 0):
                    log(
                        f"[bridge][oc][session] task_id={str(rec.get('task_id','') or '')} "
                        f"session_id={sid} jsonl_missing=True path={str(jsonl_path)}",
                        verbose=True,
                    )
            except Exception:
                pass
            return

        size = int(jsonl_path.stat().st_size)
        pos = int(rec.get("session_read_pos", 0) or 0)
        if pos < 0 or pos > size:
            pos = 0

        if size <= pos:
            try:
                if dbg_count <= 3 or (dbg_count % 10 == 0):
                    log(
                        f"[bridge][oc][session] task_id={str(rec.get('task_id','') or '')} "
                        f"session_id={sid} no_new_bytes=True pos={pos} size={size}",
                        verbose=True,
                    )
            except Exception:
                pass
            return

        blob = b""
        with open(jsonl_path, "rb") as fh:
            fh.seek(pos)
            blob = fh.read()
            rec["session_read_pos"] = int(fh.tell())

        txt = blob.decode("utf-8", errors="replace")
        lines = [ln for ln in txt.splitlines() if ln and ln.strip()]
        if not lines:
            try:
                if dbg_count <= 3 or (dbg_count % 10 == 0):
                    log(
                        f"[bridge][oc][session] task_id={str(rec.get('task_id','') or '')} "
                        f"session_id={sid} bytes_read={len(blob)} nonempty_lines=0",
                        verbose=True,
                    )
            except Exception:
                pass
            return

        recent = rec.get("recent_events")
        if not isinstance(recent, list):
            recent = []
        seen = set()
        for ev in recent:
            if isinstance(ev, dict):
                t = str(ev.get("text", "") or "").strip()
                if t:
                    seen.add(t)

        added = 0
        for ln in lines:
            obj = try_load_json(ln)
            if not isinstance(obj, dict):
                continue
            extracted = _oc_extract_event_lines_from_jsonl_obj(obj)
            for item in extracted:
                if not isinstance(item, dict):
                    continue
                t = str(item.get("text", "") or "").strip()
                if t in seen:
                    continue
                recent.append({
                    "ts": int(time.time()),
                    "stream": str(item.get("stream", "session") or "session"),
                    "role": str(item.get("role", "") or "").strip(),
                    "text": t,
                })
                seen.add(t)
                rec["latest_event"] = t
                added += 1

        if len(recent) > 40:
            recent = recent[-40:]
        rec["recent_events"] = recent

        if added > 0:
            if str(rec.get("status", "") or "").strip().lower() == "canceled":
                rec["post_cancel_event_count"] = int(rec.get("post_cancel_event_count", 0) or 0) + added
                try:
                    log(
                        f"[bridge][oc][session-after-cancel] task_id={str(rec.get('task_id','') or '')} "
                        f"session_id={sid} added={added} total_after_cancel={int(rec.get('post_cancel_event_count', 0) or 0)} "
                        f"latest_len={len(str(rec.get('latest_event','') or ''))}",
                        verbose=True,
                    )
                except Exception:
                    pass
            try:
                log(
                    f"[bridge][oc][session] task_id={str(rec.get('task_id','') or '')} "
                    f"session_id={sid} added={added} recent_n={len(recent)} "
                    f"read_pos={int(rec.get('session_read_pos',0) or 0)}",
                    verbose=True,
                )
            except Exception:
                pass
        else:
            try:
                if dbg_count <= 3 or (dbg_count % 10 == 0):
                    log(
                        f"[bridge][oc][session] task_id={str(rec.get('task_id','') or '')} "
                        f"session_id={sid} lines_read={len(lines)} extracted=0",
                        verbose=True,
                    )
            except Exception:
                pass
    except Exception as e:
        print("Failed in _oc_capture_session_jsonl_events", e)


def _oc_capture_intermediate_events(rec: Dict[str, object]) -> None:
    """
    Capture meaningful incremental lines from stdout/stderr into rec.recent_events.
    """
    try:
        candidates: List[Tuple[str, str]] = []
        ls = _oc_sanitize_event_text(rec.get("last_stdout_line", ""))
        le = _oc_sanitize_event_text(rec.get("last_stderr_line", ""))
        if ls:
            candidates.append(("stdout", ls))
        if le:
            candidates.append(("stderr", le))
        for ln in _oc_snapshot_tail(rec, "stdout_tail", n=40):
            t = _oc_sanitize_event_text(ln)
            if t:
                candidates.append(("stdout", t))
        for ln in _oc_snapshot_tail(rec, "stderr_tail", n=40):
            t = _oc_sanitize_event_text(ln)
            if t:
                candidates.append(("stderr", t))

        if not candidates:
            fallback = _oc_make_fallback_progress_line(rec)
            if fallback:
                rec["latest_event"] = fallback
            try:
                log(
                    f"[bridge][oc][partial] task_id={str(rec.get('task_id','') or '')} "
                    f"candidates=0 fallback_set={bool(fallback)} "
                    f"stdout_tail_n={len(_oc_snapshot_tail(rec, 'stdout_tail', n=5))} "
                    f"stderr_tail_n={len(_oc_snapshot_tail(rec, 'stderr_tail', n=5))} "
                    f"last_stdout_set={bool(str(rec.get('last_stdout_line','') or '').strip())} "
                    f"last_stderr_set={bool(str(rec.get('last_stderr_line','') or '').strip())}",
                    verbose=True,
                )
            except Exception:
                pass
            return

        recent = rec.get("recent_events")
        if not isinstance(recent, list):
            recent = []

        seen = set()
        for item in recent:
            if isinstance(item, dict):
                txt = str(item.get("text", "") or "").strip()
                if txt:
                    seen.add(txt)

        added = 0
        for stream, line in candidates:
            if not _oc_is_meaningful_event_line(line):
                continue
            if line in seen:
                continue
            evt = {
                "ts": int(time.time()),
                "stream": stream,
                "text": line,
            }
            recent.append(evt)
            seen.add(line)
            rec["latest_event"] = line
            added += 1

        # If everything was filtered/deduped, still keep one concise fallback
        # so orchestrator can surface partial progress text.
        if added == 0:
            fallback = _oc_make_fallback_progress_line(rec)
            if fallback:
                rec["latest_event"] = fallback
                if fallback not in seen:
                    recent.append({
                        "ts": int(time.time()),
                        "stream": "stdout",
                        "text": fallback,
                    })
        try:
            latest_dbg = str(rec.get("latest_event", "") or "").strip()
            log(
                f"[bridge][oc][partial] task_id={str(rec.get('task_id','') or '')} "
                f"candidates={len(candidates)} added={added} "
                f"latest_set={bool(latest_dbg)} latest_len={len(latest_dbg)} "
                f"recent_n={len(recent)}",
                verbose=True,
            )
        except Exception:
            pass

        if len(recent) > 25:
            recent = recent[-25:]
        rec["recent_events"] = recent
    except Exception:
        pass


def _try_parse_openclaw_json_output(stdout_text: str) -> Dict[str, object]:
    s = str(stdout_text or "").strip()
    if not s:
        return {}
    # Try whole output first.
    obj = try_load_json(s)
    if isinstance(obj, dict):
        return obj
    # Fallback: parse last JSON object-looking line.
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    for ln in reversed(lines):
        maybe = try_load_json(ln)
        if isinstance(maybe, dict):
            return maybe
    # Fallback: scan for balanced JSON object substrings in mixed output.
    in_str = False
    esc = False
    depth = 0
    start = -1
    candidates: List[str] = []
    for i, ch in enumerate(s):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "\"":
                in_str = False
            continue
        if ch == "\"":
            in_str = True
            continue
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
            continue
        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                candidates.append(s[start : i + 1])
                start = -1
    for blob in reversed(candidates):
        maybe = try_load_json(blob)
        if isinstance(maybe, dict):
            return maybe
    return {}


def _oc_summary_from_text(text: str) -> str:
    """
    Best-effort one-line summary for non-JSON OpenClaw output.
    """
    try:
        s = str(text or "").strip()
        if not s:
            return ""
        lines = [ln.strip() for ln in s.splitlines() if ln and ln.strip()]
        for ln in reversed(lines):
            clean = _oc_sanitize_event_text(ln, max_len=240)
            if not clean:
                continue
            if clean in {"{", "}", "[", "]", ",", "```", "```json"}:
                continue
            return clean
    except Exception:
        pass
    return ""


def _oc_has_user_payload_shape(obj: Dict[str, object]) -> bool:
    """
    True when parsed JSON looks like a user-facing result envelope.
    """
    try:
        if not isinstance(obj, dict) or not obj:
            return False
        if any(k in obj for k in ("reply", "message", "summary", "payloads", "media")):
            return True
        inner = obj.get("result")
        if isinstance(inner, dict):
            if any(k in inner for k in ("reply", "message", "summary", "payloads", "media")):
                return True
        return False
    except Exception:
        return False


def _oc_is_gateway_start_ack(obj: Dict[str, object]) -> bool:
    try:
        if not isinstance(obj, dict) or not obj:
            return False
        status = str(obj.get("status", "") or "").strip().lower()
        run_id = str(obj.get("runId", "") or obj.get("run_id", "") or "").strip()
        return status == "started" and bool(run_id)
    except Exception:
        return False


def _oc_collect_recent_event_texts(rec: Dict[str, object], allowed_roles: Optional[set] = None) -> List[str]:
    out: List[str] = []
    try:
        recent = rec.get("recent_events")
        if not isinstance(recent, list):
            return out
        seen = set()
        for ev in recent:
            if not isinstance(ev, dict):
                continue
            role = str(ev.get("role", "") or "").strip().lower()
            if allowed_roles is not None and role not in allowed_roles:
                continue
            txt = str(ev.get("text", "") or "").strip()
            if not txt or txt in seen:
                continue
            low = txt.lower()
            if low in {"{", "}", "[", "]"}:
                continue
            if txt in {'"', '",', '"}', '"status": "started"', '"runId":'}:
                continue
            if '"status": "started"' in low or '"runid":' in low:
                continue
            seen.add(txt)
            out.append(txt)
    except Exception:
        return out
    return out


def _oc_ensure_payload_text(result_obj: Dict[str, object], text: str, *, role: str = "", source: str = "") -> Dict[str, object]:
    """
    Ensure canonical nested shape:
      { ..., "result": { ..., "payloads": [ {"text": "..."} ] } }
    """
    try:
        out = result_obj if isinstance(result_obj, dict) else {}
        if not isinstance(out.get("result"), dict):
            out["result"] = {}
        inner = out.get("result")
        if not isinstance(inner, dict):
            inner = {}
            out["result"] = inner
        payloads = inner.get("payloads")
        if not isinstance(payloads, list):
            payloads = []
            inner["payloads"] = payloads

        txt = str(text or "").strip()
        if not txt:
            return out

        seen = set()
        for p in payloads:
            if isinstance(p, dict):
                t = str(p.get("text", "") or "").strip()
                if t:
                    seen.add(t)
        if txt not in seen:
            entry: Dict[str, object] = {"text": txt}
            if str(role or "").strip():
                entry["role"] = str(role).strip()
            if str(source or "").strip():
                entry["source"] = str(source).strip()
            payloads.append(entry)
        return out
    except Exception:
        return result_obj if isinstance(result_obj, dict) else {}


def _iter_strings(obj: object) -> Iterable[str]:
    if isinstance(obj, str):
        yield obj
        return
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _iter_strings(v)
        return
    if isinstance(obj, list):
        for v in obj:
            yield from _iter_strings(v)


def _extract_local_media_paths_from_text(text: str) -> List[str]:
    src = str(text or "")
    out: List[str] = []
    if not src:
        return out
    ext_re = "|".join(sorted(MEDIA_EXTENSIONS))
    win_re = re.compile(rf"([A-Za-z]:\\[^\r\n<>\"|?*]+?\.(?:{ext_re}))", flags=re.IGNORECASE)
    nix_re = re.compile(rf"(/[^ \t\r\n\"'<>]+?\.(?:{ext_re}))", flags=re.IGNORECASE)
    for m in win_re.finditer(src):
        p = str(m.group(1) or "").strip()
        if p and p not in out:
            out.append(p)
    for m in nix_re.finditer(src):
        p = str(m.group(1) or "").strip()
        if p.startswith("//"):
            continue
        if p and p not in out:
            out.append(p)
    return out


def _collect_local_media_paths_from_openclaw_payload(payload: Dict[str, object]) -> List[str]:
    out: List[str] = []
    for s in _iter_strings(payload):
        for p in _extract_local_media_paths_from_text(s):
            if p not in out:
                out.append(p)
    return out


def _extract_summary_media_path(summary: object) -> str:
    src = str(summary or "").strip()
    if not src:
        return ""
    m = re.search(r"^MEDIA:(.+)$", src, flags=re.IGNORECASE)
    if not m:
        return ""
    return str(m.group(1) or "").strip()


def _cleanup_expired_media_tokens(pairing_state: Dict[str, object], max_remove: int = 1000) -> int:
    try:
        media_tokens = pairing_state.get("media_tokens")
        if not isinstance(media_tokens, dict) or not media_tokens:
            return 0
        now = int(time.time())
        removed = 0
        for token in list(media_tokens.keys()):
            item = media_tokens.get(token)
            if not isinstance(item, dict):
                media_tokens.pop(token, None)
                removed += 1
            else:
                expires_at = int(item.get("expires_at", 0) or 0)
                if expires_at and now > expires_at:
                    media_tokens.pop(token, None)
                    removed += 1
            if removed >= max_remove:
                break
        return removed
    except Exception:
        return 0


def _stage_local_media_file(path_str: str, pairing_state: Dict[str, object]) -> Optional[Dict[str, object]]:
    started = time.perf_counter()
    try:
        raw = str(path_str or "").strip()
        lowered = raw.lower()
        if lowered.startswith(("http://", "https://", "//", "\\\\")):
            return None
        p = Path(str(path_str or "")).expanduser()
        if not p.exists() or not p.is_file():
            return None
        ext = p.suffix.lower().lstrip(".")
        if ext not in MEDIA_EXTENSIONS:
            return None
        size = int(p.stat().st_size)
        if size > 25 * 1024 * 1024:
            return None

        media_tokens = pairing_state.get("media_tokens")
        if not isinstance(media_tokens, dict):
            media_tokens = {}
            pairing_state["media_tokens"] = media_tokens
        _cleanup_expired_media_tokens(pairing_state, max_remove=200)

        token = secrets.token_urlsafe(18)
        now = int(time.time())
        expires_at = now + 60 * 60
        ctype = mimetypes.guess_type(str(p))[0] or "application/octet-stream"
        media_tokens[token] = {
            "path": str(p.resolve()),
            "content_type": ctype,
            "name": p.name,
            "size_bytes": size,
            "expires_at": expires_at,
            "created_at": now,
        }
        listen_port = int(pairing_state.get("listen_port", 8765) or 8765)
        media_url = f"http://127.0.0.1:{listen_port}/media/{token}/{quote(p.name)}"
        return {
            "path": str(p),
            "name": p.name,
            "content_type": ctype,
            "size_bytes": size,
            "expires_at": expires_at,
            "mediaUrl": media_url,
        }
    except Exception:
        return None
    finally:
        try:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if elapsed_ms >= 250:
                log(
                    f"[bridge][oc][media-stage] path={str(path_str or '')[:220]} elapsed_ms={elapsed_ms}",
                    verbose=True,
                )
        except Exception:
            pass


def _augment_openclaw_result_with_media(payload: Dict[str, object], pairing_state: Dict[str, object]) -> Dict[str, object]:
    started = time.perf_counter()
    try:
        if not isinstance(payload, dict):
            return payload
        if str(payload.get("tool", "") or "").strip() != "openclaw.get_status":
            return payload
        if str(payload.get("status", "") or "").strip().lower() != "completed":
            return payload
        existing = payload.get("media")
        if isinstance(existing, list) and existing:
            return payload

        # For completed task replay, only stage task-final media. Scanning all
        # historical text (tails/events) causes prior/intermediate artifacts to
        # leak into the final payload.
        candidates: List[str] = []
        summary_media_path = _extract_summary_media_path(payload.get("summary"))
        if summary_media_path:
            candidates = [summary_media_path]
        else:
            nested = payload.get("result")
            if isinstance(nested, dict):
                inner = nested.get("result")
                if isinstance(inner, dict):
                    payloads = inner.get("payloads")
                    if isinstance(payloads, list):
                        for p in payloads:
                            if not isinstance(p, dict):
                                continue
                            media_url = str(p.get("mediaUrl", "") or "").strip()
                            if media_url:
                                continue
                            txt = str(p.get("text", "") or "").strip()
                            for path in _extract_local_media_paths_from_text(txt):
                                if path not in candidates:
                                    candidates.append(path)
        if not candidates:
            return payload
        try:
            preview = [str(c)[:180] for c in candidates[:5]]
            log(
                f"[bridge][oc][media-augment] candidates={len(candidates)} preview={json.dumps(preview, ensure_ascii=False)}",
                verbose=True,
            )
        except Exception:
            pass
        staged: List[Dict[str, object]] = []
        for c in candidates:
            item = _stage_local_media_file(c, pairing_state)
            if item:
                staged.append(item)
        if staged:
            payload["media"] = staged
            nested = payload.get("result")
            if isinstance(nested, dict):
                inner = nested.get("result")
                if isinstance(inner, dict):
                    payloads = inner.get("payloads")
                    if not isinstance(payloads, list):
                        payloads = []
                        inner["payloads"] = payloads
                    existing_urls = set()
                    for p in payloads:
                        if isinstance(p, dict):
                            u = str(p.get("mediaUrl", "") or "").strip()
                            t = str(p.get("text", "") or "").strip()
                            if u:
                                existing_urls.add(u)
                            if t.startswith("http://") or t.startswith("https://"):
                                existing_urls.add(t)
                    for m in staged:
                        u = str(m.get("mediaUrl", "") or "").strip()
                        if not u or u in existing_urls:
                            continue
                        payloads.insert(0, {"text": u, "mediaUrl": u})
                        existing_urls.add(u)
        return payload
    except Exception:
        return payload
    finally:
        try:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if elapsed_ms >= 250:
                media_count = len(payload.get("media", [])) if isinstance(payload, dict) and isinstance(payload.get("media"), list) else 0
                log(
                    f"[bridge][oc][media-augment] elapsed_ms={elapsed_ms} media_count={media_count}",
                    verbose=True,
                )
        except Exception:
            pass


def _oc_tail_append(rec: Dict[str, object], key: str, line: str, limit: int = 80) -> None:
    lock = rec.get("stream_lock")
    if not hasattr(lock, "acquire") or not hasattr(lock, "release"):
        lock = None
    if lock:
        lock.acquire()
    try:
        arr = rec.get(key)
        if not isinstance(arr, list):
            arr = []
        arr.append(str(line or ""))
        if len(arr) > limit:
            arr = arr[-limit:]
        rec[key] = arr
    finally:
        if lock:
            lock.release()


def _oc_read_stream_to_tail(stream, rec: Dict[str, object], tail_key: str, last_key: str, full_key: str) -> None:
    try:
        for raw in iter(stream.readline, ""):
            line = str(raw or "").rstrip("\r\n")
            if not line:
                continue
            _oc_tail_append(rec, tail_key, line)
            rec[last_key] = line
            try:
                cnt_key = f"{tail_key}_count"
                cnt = int(rec.get(cnt_key, 0) or 0) + 1
                rec[cnt_key] = cnt
                if cnt in {1, 3, 10} or (cnt % 25 == 0):
                    preview = _oc_sanitize_event_text(line, max_len=140)
                    log(
                        f"[bridge][oc][stream] task_id={str(rec.get('task_id','') or '')} "
                        f"stream={tail_key} count={cnt} line_len={len(str(line or ''))} "
                        f"preview={preview}",
                        verbose=True,
                    )
            except Exception:
                pass
            lock = rec.get("stream_lock")
            if hasattr(lock, "acquire") and hasattr(lock, "release"):
                lock.acquire()
            try:
                prev = str(rec.get(full_key, "") or "")
                rec[full_key] = f"{prev}{line}\n"
            finally:
                if hasattr(lock, "acquire") and hasattr(lock, "release"):
                    lock.release()
    except Exception:
        pass
    finally:
        try:
            stream.close()
        except Exception:
            pass


def _oc_start_stream_readers(proc: subprocess.Popen, rec: Dict[str, object]) -> None:
    rec["stream_lock"] = threading.Lock()
    rec["stdout_tail"] = []
    rec["stderr_tail"] = []
    rec["stdout_full"] = ""
    rec["stderr_full"] = ""
    rec["last_stdout_line"] = ""
    rec["last_stderr_line"] = ""
    readers: List[threading.Thread] = []
    if proc.stdout is not None:
        t_out = threading.Thread(
            target=_oc_read_stream_to_tail,
            args=(proc.stdout, rec, "stdout_tail", "last_stdout_line", "stdout_full"),
            daemon=True,
        )
        t_out.start()
        readers.append(t_out)
    if proc.stderr is not None:
        t_err = threading.Thread(
            target=_oc_read_stream_to_tail,
            args=(proc.stderr, rec, "stderr_tail", "last_stderr_line", "stderr_full"),
            daemon=True,
        )
        t_err.start()
        readers.append(t_err)
    rec["stream_readers"] = readers


def _oc_join_stream_readers(rec: Dict[str, object], per_thread_timeout: float = 0.6) -> None:
    try:
        readers = rec.get("stream_readers")
        if not isinstance(readers, list):
            return
        for th in readers:
            try:
                if isinstance(th, threading.Thread) and th.is_alive():
                    th.join(timeout=per_thread_timeout)
            except Exception:
                pass
    except Exception:
        pass


def _oc_snapshot_tail(rec: Dict[str, object], key: str, n: int = 5) -> List[str]:
    lock = rec.get("stream_lock")
    if not hasattr(lock, "acquire") or not hasattr(lock, "release"):
        lock = None
    if lock:
        lock.acquire()
    try:
        arr = rec.get(key)
        if not isinstance(arr, list):
            return []
        return [str(x or "") for x in arr[-n:]]
    finally:
        if lock:
            lock.release()


def _oc_snapshot_full(rec: Dict[str, object], key: str) -> str:
    lock = rec.get("stream_lock")
    if not hasattr(lock, "acquire") or not hasattr(lock, "release"):
        lock = None
    if lock:
        lock.acquire()
    try:
        return str(rec.get(key, "") or "")
    finally:
        if lock:
            lock.release()


def _oc_is_terminal_payload(obj: Dict[str, object]) -> bool:
    if not isinstance(obj, dict) or not obj:
        return False
    status = str(obj.get("status", "") or "").strip().lower()
    if status in {"completed", "complete", "succeeded", "success", "done"}:
        return True
    for k in ("reply", "message", "summary", "result", "output", "final"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return True
        if isinstance(v, (dict, list)) and len(v) > 0:
            return True
    return False


def _oc_try_early_completion(rec: Dict[str, object]) -> Tuple[bool, Dict[str, object], str]:
    """
    Try to infer task completion from streamed stdout before process exit.
    Returns (is_complete, result_obj, summary).
    """
    try:
        full = _oc_snapshot_full(rec, "stdout_full")
        parsed = _try_parse_openclaw_json_output(full)
        if _oc_is_terminal_payload(parsed):
            summary = str(parsed.get("reply") or parsed.get("message") or parsed.get("summary") or "").strip()
            return True, parsed, summary
        tail = _oc_snapshot_tail(rec, "stdout_tail", n=20)
        for ln in reversed(tail):
            text = str(ln or "").strip()
            if not text:
                continue
            if "OC_SMOKE_OK" in text or "OPENCLAW_SMOKE_OK" in text:
                return True, {"message": text}, text
        if "OC_SMOKE_OK" in full or "OPENCLAW_SMOKE_OK" in full:
            token = "OC_SMOKE_OK" if "OC_SMOKE_OK" in full else "OPENCLAW_SMOKE_OK"
            return True, {"message": token}, token
    except Exception:
        pass
    return False, {}, ""


def _oc_stop_process(rec: Dict[str, object], *, force_tree: bool = False) -> None:
    proc = rec.get("process")
    if proc is None:
        return
    try:
        if proc.poll() is not None:
            return
    except Exception:
        pass

    pid = int(getattr(proc, "pid", 0) or 0)
    used_taskkill = False
    if pid > 0 and os.name == "nt":
        cmd = ["taskkill", "/PID", str(pid), "/T"]
        if force_tree:
            cmd.append("/F")
        try:
            subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=5,
            )
            used_taskkill = True
        except Exception:
            used_taskkill = False

    if not used_taskkill:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except Exception:
                proc.kill()
        except Exception:
            pass

    try:
        proc.wait(timeout=3)
    except Exception:
        pass


def _oc_send_stdin_command(rec: Dict[str, object], command: str, *, log_label: str = "") -> bool:
    proc = rec.get("process")
    if proc is None:
        return False
    try:
        if proc.poll() is not None:
            return False
    except Exception:
        pass
    stream = getattr(proc, "stdin", None)
    if stream is None:
        return False
    try:
        cmd = str(command or "").strip()
        if not cmd:
            return False
        stream.write(cmd + "\n")
        stream.flush()
        if log_label:
            log(
                f"[bridge][oc] {log_label} task_id={str(rec.get('task_id', '') or '')}",
                verbose=True,
            )
        return True
    except Exception:
        return False


def _oc_request_abort_via_stdin(rec: Dict[str, object]) -> bool:
    ok = False
    if _oc_send_stdin_command(rec, "/reset", log_label="reset requested via stdin"):
        ok = True
        rec["resetRequestedAt"] = int(time.time() * 1000)
        time.sleep(0.2)
    if _oc_send_stdin_command(rec, "/exit", log_label="exit requested via stdin"):
        ok = True
        rec["exitRequestedAt"] = int(time.time() * 1000)
    return ok


def _resolve_openclaw_cli(cli_hint: str) -> str:
    hint = str(cli_hint or "").strip()
    if hint:
        hint_path = Path(hint)
        if hint_path.is_absolute():
            if hint_path.exists():
                return str(hint_path)
            raise BridgeError(
                f'Configured OpenClaw CLI was not found at "{hint}". '
                "Update --openclaw-cli to the correct full path."
            )
    candidates: List[str] = []
    if hint:
        candidates.append(hint)
    # Common names
    candidates.extend(["openclaw.cmd", "openclaw"])
    # Common npm global path on Windows
    user_profile = str(os.environ.get("USERPROFILE", "") or "").strip()
    if user_profile:
        candidates.append(str(Path(user_profile) / "AppData" / "Roaming" / "npm" / "openclaw.cmd"))

    for c in candidates:
        if not c:
            continue
        p = Path(c)
        if p.is_absolute() and p.exists():
            return str(p)
        resolved = shutil.which(c)
        if resolved:
            return resolved
    raise BridgeError(
        "OpenClaw CLI not found. Set --openclaw-cli to full path (for example "
        r"C:\Users\<you>\AppData\Roaming\npm\openclaw.cmd)."
    )


def _oc_cli_run(cli_path: str, subargs: List[str], timeout_sec: int = 25) -> Tuple[int, str, str]:
    try:
        cmd = [str(cli_path or "").strip()] + [str(x or "").strip() for x in (subargs or []) if str(x or "").strip()]
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(1, int(timeout_sec or 25)),
        )
        return int(proc.returncode), str(proc.stdout or ""), str(proc.stderr or "")
    except Exception as e:
        return 999, "", f"{e}"


def _oc_gateway_call(
    cli_path: str,
    method: str,
    params: Dict[str, object],
    *,
    timeout_ms: int = 10000,
    expect_final: bool = False,
) -> Tuple[int, str, str]:
    subargs = [
        "gateway",
        "call",
        str(method or "").strip(),
        "--params",
        json.dumps(params or {}, ensure_ascii=False),
        "--timeout",
        str(max(1, int(timeout_ms or 10000))),
        "--json",
    ]
    if expect_final:
        subargs.append("--expect-final")
    return _oc_cli_run(cli_path, subargs, timeout_sec=max(5, int(timeout_ms / 1000) + 10))


def _extract_agent_ids_from_any(obj: object, out: set) -> None:
    try:
        if isinstance(obj, dict):
            for k, v in obj.items():
                lk = str(k or "").strip().lower()
                if lk in {"id", "agent", "agent_id", "name"} and isinstance(v, str):
                    vv = str(v or "").strip()
                    if vv:
                        out.add(vv)
                _extract_agent_ids_from_any(v, out)
        elif isinstance(obj, list):
            for el in obj:
                _extract_agent_ids_from_any(el, out)
    except Exception:
        return


def _oc_parse_agent_ids(stdout_text: str) -> set:
    ids = set()
    src = str(stdout_text or "")
    if not src.strip():
        return ids

    # Try full JSON first.
    obj = try_load_json(src.strip())
    if obj is not None:
        _extract_agent_ids_from_any(obj, ids)

    # Try JSON-per-line fallback.
    if not ids:
        for ln in src.splitlines():
            blob = str(ln or "").strip()
            if not blob or ("{" not in blob and "[" not in blob):
                continue
            parsed = try_load_json(blob)
            if parsed is not None:
                _extract_agent_ids_from_any(parsed, ids)

    # Text fallback: table/list where first token is agent id/name.
    if not ids:
        for ln in src.splitlines():
            s = str(ln or "").strip()
            if not s:
                continue
            low = s.lower()
            if any(h in low for h in ["agent id", "agent_id", "name", "status"]):
                continue
            # remove bullets/numbering
            s = re.sub(r"^[\-\*\d\.\)\s]+", "", s)
            tok = s.split()[0] if s.split() else ""
            if re.match(r"^[A-Za-z0-9][A-Za-z0-9_\-]{1,120}$", tok or ""):
                ids.add(tok)

    return ids


def _oc_list_agent_ids(cli_path: str, runtime: Dict[str, object], force: bool = False) -> set:
    """
    Return known OpenClaw agent ids. Uses short TTL cache to avoid frequent shell calls.
    """
    try:
        now = int(time.time())
        cache_ids = runtime.get("known_agent_ids")
        cache_ts = int(runtime.get("known_agent_ids_ts", 0) or 0)
        cache_age = max(0, now - cache_ts) if cache_ts else 0
        cache_ttl = int(runtime.get("known_agent_ids_ttl_sec", 20) or 20)
        cache_source = str(runtime.get("known_agent_ids_source", "") or "").strip() or "runtime"
        if (not force) and isinstance(cache_ids, set) and cache_ids and cache_age < cache_ttl:
            log(
                f"[bridge][oc] agents inventory cache-hit source={cache_source} age_sec={cache_age} "
                f"ttl_sec={cache_ttl} count={len(cache_ids)}",
                verbose=True,
            )
            return set(cache_ids)
        if (not force) and isinstance(cache_ids, set) and cache_ids:
            log(
                f"[bridge][oc] agents inventory cache-stale source={cache_source} age_sec={cache_age} "
                f"ttl_sec={cache_ttl} count={len(cache_ids)}",
                verbose=True,
            )

        candidates = [
            ["agents", "list", "--json"],
            ["agents", "list"],
        ]
        ids = set()
        for subargs in candidates:
            started = time.perf_counter()
            rc, out, err = _oc_cli_run(cli_path, subargs, timeout_sec=20)
            elapsed_ms = int(round((time.perf_counter() - started) * 1000.0))
            if rc == 0:
                parsed_ids = _oc_parse_agent_ids(out)
                log(
                    f"[bridge][oc] agents inventory cli cmd={subargs} rc={rc} elapsed_ms={elapsed_ms} "
                    f"parsed_count={len(parsed_ids)}",
                    verbose=True,
                )
                if parsed_ids:
                    ids |= parsed_ids
                    break
            else:
                log(
                    f"[bridge][oc] agents list failed cmd={subargs} rc={rc} elapsed_ms={elapsed_ms} "
                    f"err={str(err or '')[:180]}",
                    verbose=True,
                )

        if ids:
            runtime["known_agent_ids"] = set(ids)
            runtime["known_agent_ids_ts"] = now
            runtime["known_agent_ids_source"] = "cli_agents_list"
        return ids
    except Exception as e:
        print("Failed in _oc_list_agent_ids", e)
        return set()


def _oc_try_create_agent(cli_path: str, agent_id: str, cfg: Optional[Dict[str, object]] = None) -> bool:
    """
    Best-effort agent provisioning across likely OpenClaw CLI variants.
    """
    try:
        aid = str(agent_id or "").strip()
        if not aid:
            return False
        cfg = cfg if isinstance(cfg, dict) else {}
        root_hint = str(cfg.get("agents_root", "") or "").strip()
        if root_hint:
            agents_root = Path(root_hint).expanduser().resolve()
        else:
            user_profile = str(os.environ.get("USERPROFILE", "") or "").strip()
            if user_profile:
                agents_root = (Path(user_profile) / ".openclaw" / "agents").resolve()
            else:
                agents_root = (Path.home() / ".openclaw" / "agents").resolve()
        try:
            agents_root.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        ws_dir = (agents_root / aid).resolve()
        try:
            ws_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        attempts = [
            ["agents", "add", aid, "--non-interactive", "--workspace", str(ws_dir), "--json"],
            ["agents", "add", aid, "--non-interactive", "--workspace", str(ws_dir)],
            ["agents", "add", aid, "--workspace", str(ws_dir), "--json"],
            ["agents", "add", aid, "--workspace", str(ws_dir)],
            ["agents", "add", aid, "--json"],
            ["agents", "add", aid],
        ]
        for subargs in attempts:
            rc, out, err = _oc_cli_run(cli_path, subargs, timeout_sec=25)
            if rc == 0:
                log(f"[bridge][oc] created agent_id={aid} via cmd={subargs}", verbose=True)
                return True
            log(
                f"[bridge][oc] create attempt failed agent_id={aid} cmd={subargs} rc={rc} err={str(err or '')[:180]}",
                verbose=True,
            )
        return False
    except Exception as e:
        print("Failed in _oc_try_create_agent", e)
        return False


def _oc_is_stale_auth_error(text: object) -> bool:
    try:
        src = str(text or "").lower()
        if not src:
            return False
        return any(
            needle in src
            for needle in (
                "oauth token refresh failed",
                "refresh_token_reused",
                "failed to refresh oauth token",
                "openai-codex",
            )
        )
    except Exception:
        return False


def _oc_is_auth_store_error(text: object) -> bool:
    try:
        src = str(text or "").lower()
        if not src:
            return False
        return (
            "device-auth.json" in src
            and ("eperm" in src or "operation not permitted" in src or "access is denied" in src)
        )
    except Exception:
        return False


def _oc_smoke_test_template_agent(
    cli_path: str,
    runtime: Dict[str, object],
    template_agent_id: str,
    *,
    use_cache: bool = True,
) -> None:
    try:
        aid = str(template_agent_id or "").strip() or "main"
        now = int(time.time())
        cache_key = f"template_smoke_ok_ts:{aid}"
        cache_ts = int(runtime.get(cache_key, 0) or 0)
        if use_cache and cache_ts and (now - cache_ts) < 1800:
            return

        rc, out, err = _oc_cli_run(
            cli_path,
            [
                "agent",
                "--agent",
                aid,
                "--message",
                'Echo this exact message and nothing else: "SENTIENTA_OPENCLAW_TEMPLATE_OK"',
                "--timeout",
                "30",
                "--json",
            ],
            timeout_sec=45,
        )
        combined = "\n".join([str(out or ""), str(err or "")]).strip()
        if rc == 0:
            runtime[cache_key] = now
            log(f"[bridge][oc] template smoke passed agent={aid}", verbose=True)
            return
        runtime.pop(cache_key, None)
        if _oc_is_auth_store_error(combined):
            raise BridgeError(
                'OpenClaw could not access its auth store at '
                '"C:\\Users\\Chris\\.openclaw\\identity\\device-auth.json" '
                "(EPERM / access denied). Close or fix the process/file lock and verify the "
                'OpenClaw CLI can run `openclaw agent --agent main --message "test" --json`.'
            )
        if _oc_is_stale_auth_error(combined):
            raise BridgeError(
                'OpenClaw main agent has stale OAuth/auth state. Refresh the OpenClaw auth for agent "main", '
                'verify that "main" can answer a simple prompt, then try Create New OpenClaw Agent again.\n\n'
                'Alternatively, bind this Sentienta agent to another working existing OpenClaw agent.'
            )
        raise BridgeError(
            f'OpenClaw template agent "{aid}" failed smoke test before managed agent creation. '
            "Verify it can answer a simple prompt in OpenClaw, then try again."
        )
    except Exception as e:
        try:
            runtime.pop(f"template_smoke_ok_ts:{str(template_agent_id or '').strip() or 'main'}", None)
        except Exception:
            pass
        if isinstance(e, BridgeError):
            raise
        raise BridgeError(f"OpenClaw template smoke test failed: {e}") from e


def _oc_ensure_agent_exists(
    cli_path: str,
    runtime: Dict[str, object],
    agent_id: str,
    cfg: Optional[Dict[str, object]] = None,
) -> None:
    """
    Ensure requested OpenClaw agent id exists. Provision when missing.
    """
    try:
        aid = str(agent_id or "").strip()
        if not aid:
            raise BridgeError("OpenClaw agent_id is empty")

        cache_ids = runtime.get("known_agent_ids")
        cache_ts = int(runtime.get("known_agent_ids_ts", 0) or 0)
        cache_source = str(runtime.get("known_agent_ids_source", "") or "").strip() or "runtime"
        if isinstance(cache_ids, set) and aid in cache_ids:
            cache_age = max(0, int(time.time()) - cache_ts) if cache_ts else 0
            log(
                f"[bridge][oc] trusted cached agent_id={aid} source={cache_source} age_sec={cache_age}",
                verbose=True,
            )
            return

        template_agent_id = str((cfg or {}).get("template_agent_id", "main") or "main").strip() or "main"
        default_agent_id = str((cfg or {}).get("default_agent", "main") or "main").strip() or "main"
        known = _oc_list_agent_ids(cli_path, runtime, force=False)
        if aid in known:
            return

        # Some OpenClaw installs answer direct agent prompts fine while
        # `agents list` intermittently stalls. If the requested agent is the
        # configured template/default agent, trust a direct smoke test instead
        # of hard-failing on inventory enumeration.
        if aid in {template_agent_id, default_agent_id}:
            _oc_smoke_test_template_agent(cli_path, runtime, aid)
            runtime["known_agent_ids"] = set(known) | {aid}
            runtime["known_agent_ids_ts"] = int(time.time())
            return

        if aid != template_agent_id:
            _oc_smoke_test_template_agent(cli_path, runtime, template_agent_id)

        created = _oc_try_create_agent(cli_path, aid, cfg=cfg)
        refreshed = _oc_list_agent_ids(cli_path, runtime, force=True)
        if created and (aid in refreshed):
            return

        raise BridgeError(
            f'OpenClaw agent_id "{aid}" is not configured and auto-provisioning failed. '
            f'Please verify CLI support for agent creation.'
        )
    except Exception as e:
        if isinstance(e, BridgeError):
            raise
        print("Failed in _oc_ensure_agent_exists", e)
        raise BridgeError(f"Failed ensuring OpenClaw agent exists: {e}") from e


def execute_openclaw_run_task(call: BridgeCall, pairing_state: Dict[str, object]) -> Dict[str, object]:
    launch_started = time.perf_counter()
    launch_timing: Dict[str, int] = {}

    def _mark_launch_timing(name: str, start_perf: float) -> None:
        try:
            launch_timing[name] = int(round((time.perf_counter() - start_perf) * 1000.0))
        except Exception:
            pass

    runtime = _ensure_openclaw_runtime(pairing_state)
    tasks = runtime["tasks"]
    cfg = pairing_state.get("openclaw_config", {})
    if not isinstance(cfg, dict):
        cfg = {}

    args = call.args if isinstance(call.args, dict) else {}
    objective = str(args.get("objective", "") or "").strip()
    if not objective:
        raise BridgeError("args.objective is required")
    context = args.get("context", {})
    timeout_ms = args.get("timeout_ms", cfg.get("default_timeout_ms", 210000))
    try:
        timeout_ms = int(timeout_ms)
    except Exception as e:
        raise BridgeError("args.timeout_ms must be integer") from e
    timeout_ms = max(1000, min(timeout_ms, 20 * 60 * 1000))
    timeout_sec = max(1, int(round(timeout_ms / 1000.0)))
    agent_id = str(
        args.get("agent_id")
        or args.get("agent")
        or cfg.get("default_agent", "main")
        or "main"
    ).strip() or "main"
    execution_mode = "cli"
    if not (args.get("agent_id") or args.get("agent")):
        log(
            f"[bridge][oc] run_task msg_id={call.msg_id} missing agent_id/agent in args; using default agent={agent_id}",
            verbose=True,
        )

    # Keep the adapter thin: pass objective and optional structured context.
    message = objective
    if isinstance(context, dict) and context:
        message += "\n\nContext JSON:\n" + json.dumps(context, ensure_ascii=False, indent=2)
    elif isinstance(context, str) and context.strip():
        message += "\n\nContext:\n" + context.strip()

    cli_hint = str(cfg.get("cli_path", "openclaw") or "openclaw").strip() or "openclaw"
    cli_start = time.perf_counter()
    cli_path = _resolve_openclaw_cli(cli_hint)
    _mark_launch_timing("resolve_cli_ms", cli_start)
    ensure_agent_start = time.perf_counter()
    _oc_ensure_agent_exists(cli_path, runtime, agent_id, cfg=cfg)
    _mark_launch_timing("ensure_agent_ms", ensure_agent_start)
    session_key = ""
    run_id = ""
    effective_execution_mode = "cli"
    use_json_mode = bool(cfg.get("run_task_json_mode", False))
    cmd = [
        cli_path,
        "agent",
        "--agent",
        agent_id,
        "--message",
        message,
        "--verbose",
        "on",
        "--timeout",
        str(timeout_sec),
    ]
    if use_json_mode:
        cmd.append("--json")
    env = os.environ.copy()
    # Encourage child Python process trees to flush incremental output.
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        try:
            log(
                f"[bridge][oc] launch task msg_id={call.msg_id} agent={agent_id} "
                f"mode={effective_execution_mode} timeout_sec={timeout_sec} "
                f"json={'on' if use_json_mode else 'off'} session_key={session_key or '<none>'} "
                f"py_unbuffered={env.get('PYTHONUNBUFFERED','')}",
                verbose=True,
            )
        except Exception:
            pass
        popen_start = time.perf_counter()
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        _mark_launch_timing("popen_ms", popen_start)
    except Exception as e:  # noqa: BLE001
        raise BridgeError(f"Failed to start OpenClaw task: {e}") from e

    task_id = f"oc_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    now = int(time.time())
    now_ms = int(time.time() * 1000)
    tasks[task_id] = {
        "task_id": task_id,
        "status": "running",
        "created_at": now,
        "created_at_ms": now_ms,
        "updated_at": now,
        "updated_at_ms": now_ms,
        "objective": objective,
        "agent": agent_id,
        "execution_mode": effective_execution_mode,
        "session_key": session_key,
        "run_id": run_id,
        "process": proc,
        "timeout_ms": timeout_ms,
        "poll_count": 0,
        "heartbeat_seq": 0,
        "bridge_msg_id": call.msg_id,
        "session_id": "",
        "session_jsonl_path": "",
        "session_read_pos": 0,
    }
    try:
        session_bind_start = time.perf_counter()
        sid, spath = _oc_resolve_agent_session_jsonl(pairing_state, agent_id)
        if sid and spath:
            tasks[task_id]["session_id"] = sid
            tasks[task_id]["session_jsonl_path"] = str(spath)
            tasks[task_id]["session_read_pos"] = int(spath.stat().st_size) if spath.exists() else 0
            log(
                f"[bridge][oc][session] bound task_id={task_id} session_id={sid} "
                f"start_pos={tasks[task_id]['session_read_pos']}",
                verbose=True,
            )
        _mark_launch_timing("session_bind_ms", session_bind_start)
    except Exception as e:
        print("Failed binding session jsonl on run_task", e)
    persist_start = time.perf_counter()
    _oc_persist_task_snapshot(pairing_state, tasks[task_id])
    _mark_launch_timing("persist_snapshot_ms", persist_start)
    stream_start = time.perf_counter()
    _oc_start_stream_readers(proc, tasks[task_id])
    _mark_launch_timing("start_stream_readers_ms", stream_start)
    launch_timing["total_launch_ms"] = int(round((time.perf_counter() - launch_started) * 1000.0))
    tasks[task_id]["launch_timing"] = dict(launch_timing)
    try:
        log(
            f"[bridge][oc] run_task launch-summary task_id={task_id} "
            f"agent={agent_id} timing={json.dumps(launch_timing, ensure_ascii=False, sort_keys=True)}",
            verbose=True,
        )
    except Exception:
        pass

    return {
        "tool": "openclaw.run_task",
        "task_id": task_id,
        "status": "running",
        "agent": agent_id,
        "agent_id": agent_id,
        "execution_mode": effective_execution_mode,
        "session_key": session_key,
        "run_id": run_id,
        "objective": objective,
        "launch_timing": dict(launch_timing),
    }


def execute_openclaw_get_status(call: BridgeCall, pairing_state: Dict[str, object]) -> Dict[str, object]:
    runtime = _ensure_openclaw_runtime(pairing_state)
    tasks = runtime["tasks"]
    args = call.args if isinstance(call.args, dict) else {}
    task_id = str(args.get("task_id", "") or "").strip()
    requested_agent_id = str(args.get("agent_id") or args.get("agent") or "").strip()
    if not task_id:
        raise BridgeError("args.task_id is required")
    log(
        f"[bridge][oc][status-poll] start msg_id={str(call.msg_id or '').strip()} "
        f"task_id={task_id} requested_agent={requested_agent_id or '<none>'} "
        f"in_runtime={bool(isinstance(tasks.get(task_id), dict))}",
        verbose=True,
    )
    record_bridge_debug_event(
        pairing_state,
        "openclaw_get_status_start",
        payload={
            "msgID": str(call.msg_id or "").strip(),
            "taskID": task_id,
            "requestedAgentID": requested_agent_id,
            "taskInRuntime": bool(isinstance(tasks.get(task_id), dict)),
        },
    )
    complete_timing: Dict[str, int] = {}

    def _timed_ms(label: str, fn):
        started = time.perf_counter()
        result = fn()
        complete_timing[label] = int((time.perf_counter() - started) * 1000)
        return result

    def _log_complete_timing(stage: str) -> None:
        if not complete_timing:
            return
        try:
            total_ms = sum(int(v or 0) for v in complete_timing.values())
            details = " ".join(
                f"{key}={int(value)}ms" for key, value in complete_timing.items()
            )
            log(
                f"[bridge][oc][complete-timing] task_id={task_id} stage={stage} "
                f"total_ms={total_ms} {details}",
                verbose=True,
            )
        except Exception:
            pass

    def _with_media(payload_obj: Dict[str, object]) -> Dict[str, object]:
        if str(payload_obj.get("status", "") or "").strip().lower() == "completed":
            return _timed_ms(
                "augment_media_ms",
                lambda: _augment_openclaw_result_with_media(payload_obj, pairing_state),
            )
        return _augment_openclaw_result_with_media(payload_obj, pairing_state)

    rec = tasks.get(task_id)
    if not isinstance(rec, dict):
        cached = _oc_get_cached_task(pairing_state, task_id, requested_agent_id=requested_agent_id)
        if not isinstance(cached, dict):
            raise BridgeError("OpenClaw task not found")
        status = str(cached.get("status", "unknown") or "unknown")
        summary = str(cached.get("summary", "") or "").strip()
        if not summary and status == "completed":
            # Fallback for historical completed tasks that did not store structured summary.
            tails = cached.get("stdout_tail", [])
            if not (isinstance(tails, list) and tails):
                tails = cached.get("stderr_tail", [])
            if isinstance(tails, list):
                nonempty = [str(x).strip() for x in tails if str(x).strip()]
                if nonempty:
                    summary = nonempty[-1]

        cached_result = cached.get("result", {}) if isinstance(cached.get("result"), dict) else {}
        try:
            # Backfill canonical payload shape for older cached records that only
            # preserved summary/tails but not nested result.payloads.
            if status == "completed":
                out = dict(cached_result) if isinstance(cached_result, dict) else {}
                if not _oc_has_user_payload_shape(out):
                    out = {"status": "ok", "summary": summary or ""}

                summary_media_path = _extract_summary_media_path(summary)
                if summary_media_path:
                    out = _oc_ensure_payload_text(out, f"MEDIA:{summary_media_path}")
                    cached_result = out
                    raise StopIteration

                tail_lines: List[str] = []
                st = cached.get("stdout_tail")
                if isinstance(st, list):
                    tail_lines.extend([str(x).strip() for x in st if str(x).strip()])
                se = cached.get("stderr_tail")
                if isinstance(se, list):
                    tail_lines.extend([str(x).strip() for x in se if str(x).strip()])

                # Keep important lines; de-dup while preserving order.
                dedup = []
                seen = set()
                for ln in tail_lines:
                    k = ln.strip()
                    if not k or k in seen:
                        continue
                    seen.add(k)
                    dedup.append(k)

                if dedup:
                    out = _oc_ensure_payload_text(out, "\n".join(dedup))

                latest = str(cached.get("latest_event", "") or "").strip()
                if latest:
                    out = _oc_ensure_payload_text(out, latest)

                rv = cached.get("recent_events")
                if isinstance(rv, list):
                    for ev in rv:
                        if isinstance(ev, dict):
                            txt = str(ev.get("text", "") or "").strip()
                            if txt:
                                out = _oc_ensure_payload_text(
                                    out,
                                    txt,
                                    role=str(ev.get("role", "") or "").strip(),
                                    source=str(ev.get("stream", "") or "").strip(),
                                )

                if summary:
                    out = _oc_ensure_payload_text(out, summary)

                cached_result = out
        except StopIteration:
            pass
        except Exception:
            pass

        payload = {
            "tool": "openclaw.get_status",
            "task_id": task_id,
            "agent_id": str(cached.get("agent", "") or ""),
            "execution_mode": str(cached.get("execution_mode", "") or ""),
            "session_key": str(cached.get("session_key", "") or ""),
            "status": status,
            "progress": 100 if status in {"completed", "failed", "canceled"} else 0,
            "last_update": int(cached.get("updated_at", int(time.time()))),
            "last_update_ms": int(cached.get("updated_at_ms", int(time.time() * 1000))),
            "elapsed_ms": int(cached.get("elapsed_ms", 0) or 0),
            "timeout_ms": int(cached.get("timeout_ms", 210000) or 210000),
            "poll_count": int(cached.get("poll_count", 0) or 0),
            "heartbeat_seq": int(cached.get("heartbeat_seq", 0) or 0),
            "summary": summary,
            "result": cached_result,
            "error": str(cached.get("error", "") or "").strip(),
            "stdout_tail": cached.get("stdout_tail", []),
            "stderr_tail": cached.get("stderr_tail", []),
            "last_stdout_line": str(cached.get("last_stdout_line", "") or ""),
            "last_stderr_line": str(cached.get("last_stderr_line", "") or ""),
            "latest_event": str(cached.get("latest_event", "") or "").strip(),
            "recent_events": cached.get("recent_events", []) if isinstance(cached.get("recent_events"), list) else [],
            "cached": True,
        }
        return _with_media(payload)

    status = str(rec.get("status", "unknown"))
    try:
        # Pull incremental events from OpenClaw session JSONL (dashboard-like source).
        _oc_capture_session_jsonl_events(rec, pairing_state)
    except Exception:
        pass
    try:
        # Capture any newly-arrived stream lines regardless of current status.
        _oc_capture_intermediate_events(rec)
    except Exception:
        pass

    def _return(payload_obj: Dict[str, object]) -> Dict[str, object]:
        payload_obj.setdefault("execution_mode", str(rec.get("execution_mode", "") or ""))
        payload_obj.setdefault("session_key", str(rec.get("session_key", "") or ""))
        started = time.perf_counter()
        if str(payload_obj.get("status", "") or "").strip().lower() == "completed":
            _timed_ms("persist_task_snapshot_ms", lambda: _oc_persist_task_snapshot(pairing_state, rec))
            out = _with_media(payload_obj)
            complete_timing["return_finalize_ms"] = int((time.perf_counter() - started) * 1000)
            _log_complete_timing("return")
            return out
        _oc_persist_task_snapshot(pairing_state, rec)
        return _with_media(payload_obj)
    proc = rec.get("process")
    if status == "running" and proc is not None:
        try:
            rc = proc.poll()
        except Exception:
            rc = None
        if rc is None:
            # Check timeout.
            created_at = int(rec.get("created_at", int(time.time())) or int(time.time()))
            created_at_ms = int(rec.get("created_at_ms", created_at * 1000) or (created_at * 1000))
            timeout_ms = int(rec.get("timeout_ms", 210000) or 210000)
            now_ms = int(time.time() * 1000)
            elapsed_ms = max(0, now_ms - created_at_ms)
            early_done, early_result, early_summary = _oc_try_early_completion(rec)
            if early_done:
                rec["status"] = "completed"
                rec["result"] = early_result
                rec["summary"] = early_summary
                rec["updated_at"] = int(time.time())
                rec["updated_at_ms"] = now_ms
                rec["elapsed_ms"] = elapsed_ms
                _oc_stop_process(rec)
                _oc_join_stream_readers(rec, per_thread_timeout=0.4)
                return _return({
                    "tool": "openclaw.get_status",
                    "task_id": task_id,
                    "agent_id": str(rec.get("agent", "") or ""),
                    "status": "completed",
                    "progress": 100,
                    "last_update": rec["updated_at"],
                    "last_update_ms": rec["updated_at_ms"],
                    "elapsed_ms": elapsed_ms,
                    "poll_count": int(rec.get("poll_count", 0) or 0),
                    "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
                    "summary": str(rec.get("summary", "") or "").strip(),
                    "result": rec.get("result", {}),
                    "stdout_tail": _oc_snapshot_tail(rec, "stdout_tail", n=5),
                    "stderr_tail": _oc_snapshot_tail(rec, "stderr_tail", n=5),
                })
            if elapsed_ms > timeout_ms:
                # One final read-before-fail check to avoid missing late output.
                late_done, late_result, late_summary = _oc_try_early_completion(rec)
                if late_done:
                    rec["status"] = "completed"
                    rec["result"] = late_result
                    rec["summary"] = late_summary
                    rec["updated_at"] = int(time.time())
                    rec["updated_at_ms"] = now_ms
                    rec["elapsed_ms"] = elapsed_ms
                    return _return({
                        "tool": "openclaw.get_status",
                        "task_id": task_id,
                        "agent_id": str(rec.get("agent", "") or ""),
                        "status": "completed",
                        "progress": 100,
                        "last_update": rec["updated_at"],
                        "last_update_ms": rec["updated_at_ms"],
                        "elapsed_ms": elapsed_ms,
                        "poll_count": int(rec.get("poll_count", 0) or 0),
                        "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
                        "summary": str(rec.get("summary", "") or "").strip(),
                        "result": rec.get("result", {}),
                        "stdout_tail": _oc_snapshot_tail(rec, "stdout_tail", n=5),
                        "stderr_tail": _oc_snapshot_tail(rec, "stderr_tail", n=5),
                    })
                _oc_stop_process(rec, force_tree=True)
                _oc_join_stream_readers(rec, per_thread_timeout=0.4)
                rec["status"] = "failed"
                rec["error"] = f"OpenClaw task timed out after {timeout_ms}ms"
                rec["updated_at"] = int(time.time())
                rec["updated_at_ms"] = now_ms
                rec["elapsed_ms"] = elapsed_ms
            else:
                rec["updated_at"] = int(time.time())
                rec["updated_at_ms"] = now_ms
                rec["elapsed_ms"] = elapsed_ms
                rec["poll_count"] = int(rec.get("poll_count", 0) or 0) + 1
                rec["heartbeat_seq"] = int(rec.get("heartbeat_seq", 0) or 0) + 1
                try:
                    _oc_capture_session_jsonl_events(rec, pairing_state)
                except Exception:
                    pass
                _oc_capture_intermediate_events(rec)
                progress = int(min(95, max(1, (elapsed_ms * 95) / max(1, timeout_ms))))
                stdout_tail = _oc_snapshot_tail(rec, "stdout_tail", n=5)
                stderr_tail = _oc_snapshot_tail(rec, "stderr_tail", n=5)
                log(
                    f"[bridge][oc] status task_id={task_id} status=running progress={progress} "
                    f"poll_count={int(rec.get('poll_count', 0) or 0)} heartbeat_seq={int(rec.get('heartbeat_seq', 0) or 0)} "
                    f"elapsed_ms={elapsed_ms} remaining_ms={max(0, timeout_ms - elapsed_ms)} process_alive=True",
                    verbose=True,
                )
                try:
                    latest_dbg = str(rec.get("latest_event", "") or "").strip()
                    recent_dbg = rec.get("recent_events", [])
                    recent_n = len(recent_dbg) if isinstance(recent_dbg, list) else 0
                    log(
                        f"[bridge][oc][partial] payload task_id={task_id} "
                        f"latest_set={bool(latest_dbg)} latest_len={len(latest_dbg)} "
                        f"recent_n={recent_n} "
                        f"stdout_tail_n={len(stdout_tail)} stderr_tail_n={len(stderr_tail)} "
                        f"last_stdout_set={bool(str(rec.get('last_stdout_line','') or '').strip())} "
                        f"last_stderr_set={bool(str(rec.get('last_stderr_line','') or '').strip())}",
                        verbose=True,
                    )
                except Exception:
                    pass
                return _return({
                    "tool": "openclaw.get_status",
                    "task_id": task_id,
                    "agent_id": str(rec.get("agent", "") or ""),
                    "status": "running",
                    "progress": progress,
                    "progress_model": "elapsed_timeout_ratio",
                    "last_update": rec["updated_at"],
                    "last_update_ms": rec["updated_at_ms"],
                    "elapsed_ms": elapsed_ms,
                    "timeout_ms": timeout_ms,
                    "remaining_ms": max(0, timeout_ms - elapsed_ms),
                    "poll_count": int(rec.get("poll_count", 0) or 0),
                    "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
                    "process_alive": True,
                    "stdout_tail": stdout_tail,
                    "stderr_tail": stderr_tail,
                    "last_stdout_line": str(rec.get("last_stdout_line", "") or ""),
                    "last_stderr_line": str(rec.get("last_stderr_line", "") or ""),
                    "latest_event": str(rec.get("latest_event", "") or "").strip(),
                    "recent_events": rec.get("recent_events", []) if isinstance(rec.get("recent_events"), list) else [],
                })
        else:
            # Stream readers already consume stdout/stderr; avoid communicate() here
            # because it can start competing internal reader threads.
            _timed_ms("join_stream_readers_ms", lambda: _oc_join_stream_readers(rec, per_thread_timeout=0.8))
            try:
                _timed_ms("capture_session_jsonl_ms", lambda: _oc_capture_session_jsonl_events(rec, pairing_state))
            except Exception:
                pass
            try:
                # Ensure late-arriving lines are captured into latest_event/recent_events
                # before final completed/failed payload is produced.
                _timed_ms("capture_intermediate_events_ms", lambda: _oc_capture_intermediate_events(rec))
            except Exception:
                pass
            streamed_out = _timed_ms("snapshot_stdout_full_ms", lambda: _oc_snapshot_full(rec, "stdout_full"))
            streamed_err = _timed_ms("snapshot_stderr_full_ms", lambda: _oc_snapshot_full(rec, "stderr_full"))
            rec["exit_code"] = int(rc)
            rec["stdout"] = str(streamed_out or "").strip()
            rec["stderr"] = str(streamed_err or "").strip()
            now_ms = int(time.time() * 1000)
            rec["updated_at"] = int(time.time())
            rec["updated_at_ms"] = now_ms
            created_at = int(rec.get("created_at", rec["updated_at"]) or rec["updated_at"])
            created_at_ms = int(rec.get("created_at_ms", created_at * 1000) or (created_at * 1000))
            rec["elapsed_ms"] = max(0, now_ms - created_at_ms)
            if rc == 0:
                assemble_started = time.perf_counter()
                rec["status"] = "completed"
                out_txt = str(rec.get("stdout", "") or "").strip()
                parsed = _try_parse_openclaw_json_output(rec["stdout"])
                parsed_ok = (
                    isinstance(parsed, dict)
                    and parsed
                    and _oc_has_user_payload_shape(parsed)
                    and not _oc_is_gateway_start_ack(parsed)
                )
                if parsed_ok:
                    rec_result = dict(parsed)
                else:
                    rec_result = {"status": "ok", "summary": ""}

                assistant_texts = _oc_collect_recent_event_texts(rec, allowed_roles={"assistant"})
                recent_texts = assistant_texts or _oc_collect_recent_event_texts(rec)
                if recent_texts:
                    for txt in recent_texts:
                        rec_result = _oc_ensure_payload_text(
                            rec_result,
                            txt,
                            role="assistant" if txt in assistant_texts else "",
                            source="session",
                        )
                # Always carry raw stdout text as a canonical payload block so
                # downstream formatter can preserve full multiline results even
                # when session-event extraction only yields partial fragments.
                if out_txt and not _oc_is_gateway_start_ack(parsed):
                    rec_result = _oc_ensure_payload_text(rec_result, out_txt, role="assistant", source="stdout")
                rec["result"] = rec_result

                if recent_texts:
                    rec["summary"] = str(recent_texts[-1] or "").strip()
                else:
                    rec["summary"] = str(
                        rec_result.get("reply")
                        or rec_result.get("message")
                        or rec_result.get("summary")
                        or _oc_summary_from_text(out_txt)
                        or ""
                    ).strip()
                complete_timing["assemble_completed_result_ms"] = int((time.perf_counter() - assemble_started) * 1000)
                log(
                    f"[bridge][oc] status task_id={task_id} status=completed progress=100 "
                    f"poll_count={int(rec.get('poll_count', 0) or 0)} heartbeat_seq={int(rec.get('heartbeat_seq', 0) or 0)} "
                    f"elapsed_ms={int(rec.get('elapsed_ms', 0) or 0)} process_alive=False",
                    verbose=True,
                )
            else:
                rec["status"] = "failed"
                rec["error"] = str(rec.get("stderr", "") or "").strip() or f"openclaw exited with code {rc}"
                log(
                    f"[bridge][oc] status task_id={task_id} status=failed progress=100 "
                    f"poll_count={int(rec.get('poll_count', 0) or 0)} heartbeat_seq={int(rec.get('heartbeat_seq', 0) or 0)} "
                    f"elapsed_ms={int(rec.get('elapsed_ms', 0) or 0)} process_alive=False error={str(rec.get('error', '') or '')[:140]}",
                    verbose=True,
                )

    if rec.get("status") == "completed":
        log(
            f"[bridge][oc] status task_id={task_id} status=completed progress=100 "
            f"poll_count={int(rec.get('poll_count', 0) or 0)} heartbeat_seq={int(rec.get('heartbeat_seq', 0) or 0)} "
            f"elapsed_ms={int(rec.get('elapsed_ms', 0) or 0)} process_alive=False",
            verbose=True,
        )
        return _return({
            "tool": "openclaw.get_status",
            "task_id": task_id,
            "agent_id": str(rec.get("agent", "") or ""),
            "status": "completed",
            "progress": 100,
            "last_update": int(rec.get("updated_at", int(time.time()))),
            "last_update_ms": int(rec.get("updated_at_ms", int(time.time() * 1000))),
            "elapsed_ms": int(rec.get("elapsed_ms", 0) or 0),
            "poll_count": int(rec.get("poll_count", 0) or 0),
            "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
            "summary": str(rec.get("summary", "") or "").strip(),
            "result": rec.get("result", {}),
            "stdout_tail": _oc_snapshot_tail(rec, "stdout_tail", n=5),
            "stderr_tail": _oc_snapshot_tail(rec, "stderr_tail", n=5),
            "latest_event": str(rec.get("latest_event", "") or "").strip(),
            "recent_events": rec.get("recent_events", []) if isinstance(rec.get("recent_events"), list) else [],
        })
    if rec.get("status") == "canceled":
        log(
            f"[bridge][oc] status task_id={task_id} status=canceled progress=100 "
            f"poll_count={int(rec.get('poll_count', 0) or 0)} heartbeat_seq={int(rec.get('heartbeat_seq', 0) or 0)} "
            f"elapsed_ms={int(rec.get('elapsed_ms', 0) or 0)} process_alive=False",
            verbose=True,
        )
        return _return({
            "tool": "openclaw.get_status",
            "task_id": task_id,
            "agent_id": str(rec.get("agent", "") or ""),
            "status": "canceled",
            "progress": 100,
            "last_update": int(rec.get("updated_at", int(time.time()))),
            "last_update_ms": int(rec.get("updated_at_ms", int(time.time() * 1000))),
            "elapsed_ms": int(rec.get("elapsed_ms", 0) or 0),
            "poll_count": int(rec.get("poll_count", 0) or 0),
            "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
            "latest_event": str(rec.get("latest_event", "") or "").strip(),
            "recent_events": rec.get("recent_events", []) if isinstance(rec.get("recent_events"), list) else [],
        })
    if rec.get("status") == "failed":
        log(
            f"[bridge][oc] status task_id={task_id} status=failed progress=100 "
            f"poll_count={int(rec.get('poll_count', 0) or 0)} heartbeat_seq={int(rec.get('heartbeat_seq', 0) or 0)} "
            f"elapsed_ms={int(rec.get('elapsed_ms', 0) or 0)} process_alive=False error={str(rec.get('error', '') or '')[:140]}",
            verbose=True,
        )
        return _return({
            "tool": "openclaw.get_status",
            "task_id": task_id,
            "agent_id": str(rec.get("agent", "") or ""),
            "status": "failed",
            "progress": 100,
            "last_update": int(rec.get("updated_at", int(time.time()))),
            "last_update_ms": int(rec.get("updated_at_ms", int(time.time() * 1000))),
            "elapsed_ms": int(rec.get("elapsed_ms", 0) or 0),
            "poll_count": int(rec.get("poll_count", 0) or 0),
            "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
            "error": str(rec.get("error", "") or "OpenClaw task failed"),
            "stdout_tail": _oc_snapshot_tail(rec, "stdout_tail", n=5),
            "stderr_tail": _oc_snapshot_tail(rec, "stderr_tail", n=5),
            "latest_event": str(rec.get("latest_event", "") or "").strip(),
            "recent_events": rec.get("recent_events", []) if isinstance(rec.get("recent_events"), list) else [],
        })
    log(
        f"[bridge][oc] status task_id={task_id} status={str(rec.get('status', 'unknown'))} progress=0 "
        f"poll_count={int(rec.get('poll_count', 0) or 0)} heartbeat_seq={int(rec.get('heartbeat_seq', 0) or 0)} "
        f"elapsed_ms={int(rec.get('elapsed_ms', 0) or 0)} process_alive={(proc is not None and status == 'running')}",
        verbose=True,
    )
    return _return({
        "tool": "openclaw.get_status",
        "task_id": task_id,
        "agent_id": str(rec.get("agent", "") or ""),
        "status": str(rec.get("status", "unknown")),
        "progress": 0,
        "last_update": int(rec.get("updated_at", int(time.time()))),
        "last_update_ms": int(rec.get("updated_at_ms", int(time.time() * 1000))),
        "elapsed_ms": int(rec.get("elapsed_ms", 0) or 0),
        "poll_count": int(rec.get("poll_count", 0) or 0),
        "heartbeat_seq": int(rec.get("heartbeat_seq", 0) or 0),
        "latest_event": str(rec.get("latest_event", "") or "").strip(),
        "recent_events": rec.get("recent_events", []) if isinstance(rec.get("recent_events"), list) else [],
    })


def execute_openclaw_cancel_task(call: BridgeCall, pairing_state: Dict[str, object]) -> Dict[str, object]:
    runtime = _ensure_openclaw_runtime(pairing_state)
    tasks = runtime["tasks"]
    cfg = pairing_state.get("openclaw_config", {})
    if not isinstance(cfg, dict):
        cfg = {}
    args = call.args if isinstance(call.args, dict) else {}
    task_id = str(args.get("task_id", "") or "").strip()
    if not task_id:
        raise BridgeError("args.task_id is required")
    rec = tasks.get(task_id)
    if not isinstance(rec, dict):
        raise BridgeError("OpenClaw task not found")

    if str(rec.get("status", "")) in {"completed", "failed", "canceled"}:
        return {
            "tool": "openclaw.cancel_task",
            "task_id": task_id,
            "status": str(rec.get("status", "")),
        }

    proc = rec.get("process")
    if proc is not None:
        _oc_request_abort_via_stdin(rec)
        try:
            proc.wait(timeout=1.5)
        except Exception:
            pass
        _oc_stop_process(rec, force_tree=True)
    _oc_join_stream_readers(rec, per_thread_timeout=0.4)
    rec["status"] = "canceled"
    rec["updated_at"] = int(time.time())
    rec["updated_at_ms"] = int(time.time() * 1000)
    rec["canceled_session_id"] = str(rec.get("session_id", "") or "").strip()
    rec["canceled_session_read_pos"] = int(rec.get("session_read_pos", 0) or 0)
    rec["post_cancel_event_count"] = int(rec.get("post_cancel_event_count", 0) or 0)
    return {
        "tool": "openclaw.cancel_task",
        "task_id": task_id,
        "status": "canceled",
    }


def cancel_openclaw_tasks_for_query(
    *,
    team_name: str,
    query_id: str,
    active_queries: Dict[Tuple[str, str], ActiveQuery],
    pairing_state: Dict[str, object],
) -> Dict[str, object]:
    team_txt = str(team_name or "").strip()
    query_txt = str(query_id or "").strip()
    if not team_txt or not query_txt:
        raise BridgeError("teamName and queryID are required")

    q = active_queries.get((team_txt, query_txt))
    runtime = _ensure_openclaw_runtime(pairing_state)
    tasks = runtime["tasks"]
    canceled = []
    already_terminal = []

    target_ids = []
    if q and str(q.current_openclaw_task_id or "").strip():
        target_ids.append(str(q.current_openclaw_task_id).strip())

    for task_id, rec in list(tasks.items()):
        if not isinstance(rec, dict):
            continue
        if str(rec.get("team_name", "") or "").strip() != team_txt:
            continue
        if str(rec.get("query_id", "") or "").strip() != query_txt:
            continue
        task_id_txt = str(task_id or "").strip()
        if task_id_txt and task_id_txt not in target_ids:
            target_ids.append(task_id_txt)

    for task_id in target_ids:
        rec = tasks.get(task_id)
        if not isinstance(rec, dict):
            continue
        status = str(rec.get("status", "") or "").strip().lower()
        if status in {"completed", "failed", "canceled"}:
            already_terminal.append({"task_id": task_id, "status": status})
            continue
        execute_openclaw_cancel_task(
            BridgeCall(
                msg_id=f"ui-cancel-{int(time.time() * 1000)}",
                bridge_id="desktop_openclaw",
                tool="openclaw.cancel_task",
                args={
                    "task_id": task_id,
                    "agent_id": str(rec.get("agent", "") or "main"),
                },
                raw={},
            ),
            pairing_state,
        )
        canceled.append({"task_id": task_id, "status": "canceled"})

    if q:
        q.current_openclaw_task_id = ""

    return {
        "tool": "openclaw.cancel_query",
        "team_name": team_txt,
        "query_id": query_txt,
        "canceled": canceled,
        "already_terminal": already_terminal,
    }


def execute_call(
    call: BridgeCall,
    roots: List[Path],
    pairing_state: Dict[str, object],
    selected_services: List[str],
    max_chars_default: int,
    max_chars_hard: int,
    max_find_results_default: int,
    max_find_results_hard: int,
) -> Dict[str, object]:
    if call.tool == "openclaw.run_task":
        if "openclaw_exec" not in selected_services:
            raise BridgeError("Service disabled: openclaw_exec")
        return execute_openclaw_run_task(call, pairing_state)
    if call.tool == "openclaw.agents.list":
        if "openclaw_exec" not in selected_services:
            raise BridgeError("Service disabled: openclaw_exec")
        return execute_openclaw_agents_list(call, pairing_state)
    if call.tool == "openclaw.agents.get":
        if "openclaw_exec" not in selected_services:
            raise BridgeError("Service disabled: openclaw_exec")
        return execute_openclaw_agents_get(call, pairing_state)
    if call.tool == "openclaw.healthcheck":
        if "openclaw_exec" not in selected_services:
            raise BridgeError("Service disabled: openclaw_exec")
        return execute_openclaw_healthcheck(call, pairing_state)
    if call.tool == "openclaw.get_status":
        if "openclaw_exec" not in selected_services:
            raise BridgeError("Service disabled: openclaw_exec")
        return execute_openclaw_get_status(call, pairing_state)
    if call.tool == "openclaw.cancel_task":
        if "openclaw_exec" not in selected_services:
            raise BridgeError("Service disabled: openclaw_exec")
        return execute_openclaw_cancel_task(call, pairing_state)
    raise BridgeError(f"Unsupported tool: {call.tool}")


def build_result(call: BridgeCall, bridge_id: str, ok: bool, result: Optional[Dict[str, object]] = None, error: str = "") -> Dict[str, object]:
    obj: Dict[str, object] = {
        "type": "bridge_result",
        "v": 1,
        "msg_id": call.msg_id,
        # Backward compatibility for any server code still expecting call_id.
        "call_id": call.msg_id,
        "bridge_id": bridge_id,
        "ok": ok,
    }
    if ok:
        obj["result"] = result or {}
    else:
        obj["error"] = error
    return obj


def post_bridge_result(
    query_endpoint: str,
    headers: Dict[str, str],
    team_name: str,
    query_id: str,
    bridge_id: str,
    source: str,
    original_call: Dict[str, object],
    result_obj: Dict[str, object],
    user_id: str = "",
) -> Dict[str, object]:
    # Preferred path: structured bridge ingress.
    preferred_payload: Dict[str, object] = {
        "type": "putBridgeMessages",
        "clientType": "SentientaBridge",
        "queryID": query_id,
        "teamName": team_name,
        "bridgeID": bridge_id,
        "source": source,
        "messages": [result_obj],
        # Keep context for debugging/compatibility.
        "query": json.dumps(original_call, ensure_ascii=False),
    }
    if user_id:
        preferred_payload["userID"] = user_id
        preferred_payload["username"] = user_id

    try:
        print(
            f"[bridge][post] sending type=putBridgeMessages team={team_name} query={query_id} "
            f"bridge_id={bridge_id} msg_id={str(result_obj.get('msg_id') or result_obj.get('call_id') or '').strip()} "
            f"user_id={user_id or '<none>'}",
            flush=True,
        )
        return http_post_json(query_endpoint, preferred_payload, headers)
    except BridgeError as e:
        print(
            f"[bridge][post] putBridgeMessages failed team={team_name} query={query_id} "
            f"msg_id={str(result_obj.get('msg_id') or result_obj.get('call_id') or '').strip()} error={e}",
            flush=True,
        )
        # Fallback path: legacy appendDialogEvent.
        payload = {
            "type": "appendDialogEvent",
            "clientType": "SentientaBridge",
            "queryID": query_id,
            "teamName": team_name,
            "source": source,
            "message": json.dumps(result_obj, ensure_ascii=False),
            "query": json.dumps(original_call, ensure_ascii=False),
        }
        print(
            f"[bridge][post] falling back type=appendDialogEvent team={team_name} query={query_id} "
            f"msg_id={str(result_obj.get('msg_id') or result_obj.get('call_id') or '').strip()}",
            flush=True,
        )
        return http_post_json(query_endpoint, payload, headers)


def main() -> int:
    args = parse_args()
    roots: List[Path] = []
    selected_services = resolve_selected_services(args.service, args.bridge_id)
    accepted_bridge_ids = resolve_accepted_bridge_ids(args.bridge_id, selected_services)
    if args.service:
        requested = []
        for s in args.service:
            ss = str(s or "").strip()
            if ss and ss not in requested:
                requested.append(ss)
        dropped = [s for s in requested if s not in selected_services]
        if dropped:
            print(
                f"[bridge] requested services ignored by bridge_id policy ({args.bridge_id}): {', '.join(dropped)}",
                flush=True,
            )
    headers, resolved_mode = auth_headers(
        auth_mode=args.auth_mode,
        id_token=args.id_token,
        owner_key=args.owner_key,
        owner_key_header=args.owner_key_header,
    )
    print_auth_diagnostics(headers, resolved_mode)
    if not args.id_token:
        print("[bridge][auth] waiting for idToken via /register-query from UI client", flush=True)
    if args.team_name and args.query_id and has_auth_credential(headers):
        run_auth_probe(
            retrieve_endpoint=args.retrieve_endpoint,
            headers=headers,
            team_name=args.team_name,
            query_id=args.query_id,
        )
    else:
        print("[bridge][auth] probe skipped (missing seed query or credential)", flush=True)

    active_queries: Dict[Tuple[str, str], ActiveQuery] = {}
    active_lock = threading.Lock()
    now_epoch = int(time.time())
    passcode_ttl_secs = max(0, int(args.pair_passcode_ttl_secs or 0))
    pairing_state: Dict[str, object] = {
        "passcode": f"{secrets.randbelow(1000000):06d}",
        "passcode_expires_at": (now_epoch + passcode_ttl_secs) if passcode_ttl_secs > 0 else 0,
        "bridge_secret": "",
        "bridge_secret_expires_at": 0,
        "paired_at": 0,
        "listen_port": int(args.listen_port),
        "media_tokens": {},
        "openclaw_config": {
            "cli_path": str(args.openclaw_cli or "openclaw").strip() or "openclaw",
            "default_agent": str(args.openclaw_default_agent or "main").strip() or "main",
            "default_timeout_ms": int(args.openclaw_default_timeout_ms or 210000),
            "execution_mode": "cli",
            "agents_root": str((Path.home() / ".openclaw" / "agents").resolve()),
        },
        "openclaw_runtime": {
            "tasks": {},
        },
    }
    pairing_file = persist_pairing_code_file(bridge_id=args.bridge_id, pairing_state=pairing_state)
    if pairing_file is not None:
        print(f"[bridge][pair] pairing code file: {pairing_file}", flush=True)
    if args.team_name and args.query_id:
        seed = ActiveQuery(
            team_name=args.team_name,
            query_id=args.query_id,
            user_id=str(args.user_id or "").strip(),
            dialog_index=-1,
            last_seen_ts=time.time(),
            auth_headers=headers if has_auth_credential(headers) else None,
        )
        active_queries[(seed.team_name, seed.query_id)] = seed

    server = start_registration_server(
        port=args.listen_port,
        bridge_id=args.bridge_id,
        active_queries=active_queries,
        lock=active_lock,
        default_auth_headers=headers,
        pairing_state=pairing_state,
        roots=roots,
        selected_services=selected_services,
        accepted_bridge_ids=accepted_bridge_ids,
        max_chars_default=args.max_chars_default,
        max_chars_hard=args.max_chars_hard,
        max_find_results_default=args.max_find_results_default,
        max_find_results_hard=args.max_find_results_hard,
    )
    print(f"[bridge] registration endpoint listening on http://127.0.0.1:{args.listen_port}/register-query", flush=True)

    # De-dupe per query thread, not globally by msg_id.
    # Some routers reuse short ids like "msg_0001" across different queries.
    seen_call_ids: set[tuple[str, str, str]] = set()

    log(f"[bridge] started bridge_id={args.bridge_id}", verbose=True)
    log("[bridge] accepted_bridge_ids=" + ", ".join(accepted_bridge_ids), verbose=True)
    log("[bridge] selected_services=" + ", ".join(selected_services), verbose=True)
    _print_pairing_banner(
        str(pairing_state.get("passcode", "") or ""),
        pairing_state.get("passcode_expires_at"),
        pairing_file,
    )

    try:
        while True:
            with active_lock:
                snapshot = list(active_queries.values())
            loop_sleep = max(0.1, args.poll_ms / 1000.0)

            for q in snapshot:
                poll_headers = q.auth_headers if has_auth_credential(q.auth_headers) else headers
                if not has_auth_credential(poll_headers):
                    record_bridge_debug_event(
                        pairing_state,
                        "poll_skipped_no_auth",
                        team_name=q.team_name,
                        query_id=q.query_id,
                        user_id=q.user_id,
                    )
                    continue
                bridge_calls: List[BridgeCall] = []
                try:
                    # New path: poll Core outbox channel for bridge-targeted messages.
                    outbox_messages: List[object] = []
                    for poll_bridge_id in accepted_bridge_ids:
                        debug_meta: Dict[str, object] = {}
                        backend_debug_version = ""
                        backend_cfg_version = ""
                        outbox_resp = poll_bridge_messages(
                            query_endpoint=args.query_endpoint,
                            headers=poll_headers,
                            team_name=q.team_name,
                            query_id=q.query_id,
                            bridge_id=poll_bridge_id,
                            user_id=q.user_id,
                        )
                        current_messages: object = []
                        parsed_body: Dict[str, object] = {}

                        raw_body = outbox_resp.get("body")
                        if isinstance(raw_body, str):
                            parsed = try_load_json(raw_body)
                            if isinstance(parsed, dict):
                                parsed_body = parsed
                        elif isinstance(raw_body, dict):
                            parsed_body = raw_body

                        if isinstance(outbox_resp.get("messages"), list):
                            current_messages = outbox_resp.get("messages")
                        elif isinstance(parsed_body.get("messages"), list):
                            current_messages = parsed_body.get("messages")

                        if isinstance(outbox_resp.get("debug"), dict):
                            debug_meta = outbox_resp.get("debug")
                        elif isinstance(parsed_body.get("debug"), dict):
                            debug_meta = parsed_body.get("debug")

                        backend_debug_version = str(
                            outbox_resp.get("backendDebugVersion")
                            or parsed_body.get("backendDebugVersion")
                            or ""
                        ).strip()
                        backend_cfg_version = str(
                            outbox_resp.get("backendCfgVersion")
                            or parsed_body.get("backendCfgVersion")
                            or ""
                        ).strip()
                        record_bridge_debug_event(
                            pairing_state,
                            "poll_outbox_result",
                            team_name=q.team_name,
                            query_id=q.query_id,
                            user_id=q.user_id,
                            payload={
                                "bridgeID": poll_bridge_id,
                                "fetchedCount": len(current_messages) if isinstance(current_messages, list) else 0,
                                "messages": [
                                    {
                                        "msgID": str((m or {}).get("msg_id") or (m or {}).get("call_id") or "").strip(),
                                        "tool": str((m or {}).get("tool") or "").strip(),
                                        "queryID": str((m or {}).get("_sentienta", {}).get("queryID") or "").strip() if isinstance((m or {}).get("_sentienta"), dict) else "",
                                        "teamName": str((m or {}).get("_sentienta", {}).get("teamName") or "").strip() if isinstance((m or {}).get("_sentienta"), dict) else "",
                                    }
                                    for m in (current_messages[:8] if isinstance(current_messages, list) else [])
                                    if isinstance(m, dict)
                                ],
                                "debug": debug_meta,
                                "backendDebugVersion": backend_debug_version,
                                "backendCfgVersion": backend_cfg_version,
                                "localBridgeDebugVersion": LOCAL_BRIDGE_DEBUG_VERSION,
                            },
                        )
                        if isinstance(current_messages, list) and current_messages:
                            outbox_messages.extend(current_messages)
                    if args.verbose:
                        log(
                            f"[bridge] fetch team={q.team_name} query={q.query_id} "
                            f"accepted_bridge_ids={sorted(list(accepted_bridge_ids))} "
                            f"fetched_count={len(outbox_messages)}",
                            verbose=True,
                        )
                    if args.verbose:
                        preview = str(outbox_messages)
                        if len(preview) > 320:
                            preview = preview[:317] + "..."
                        log(f"[bridge] msgs team={q.team_name} query={q.query_id}: {preview}", verbose=True)
                    bridge_calls.extend(extract_bridge_calls_from_messages(outbox_messages))

                    # Legacy path: still poll dialog retrieve endpoint for compatibility.
                    resp = poll_dialog(
                        retrieve_endpoint=args.retrieve_endpoint,
                        headers=poll_headers,
                        team_name=q.team_name,
                        query_id=q.query_id,
                        dialog_index=q.dialog_index,
                    )
                    q.poll_errors = 0
                except BridgeError as e:
                    q.poll_errors += 1
                    record_bridge_debug_event(
                        pairing_state,
                        "poll_error",
                        team_name=q.team_name,
                        query_id=q.query_id,
                        user_id=q.user_id,
                        payload={
                            "error": str(e),
                            "pollErrors": q.poll_errors,
                        },
                    )
                    log(f"[bridge] poll error team={q.team_name} query={q.query_id}: {e}", verbose=True)
                    if q.poll_errors >= 5:
                        with active_lock:
                            active_queries.pop((q.team_name, q.query_id), None)
                        record_bridge_debug_event(
                            pairing_state,
                            "query_dropped_after_poll_errors",
                            team_name=q.team_name,
                            query_id=q.query_id,
                            user_id=q.user_id,
                            payload={"pollErrors": q.poll_errors},
                        )
                        log(f"[bridge] dropped query after repeated poll errors: team={q.team_name} query={q.query_id}", verbose=True)
                    continue

                if isinstance(resp.get("dialogIndex"), int):
                    q.dialog_index = int(resp["dialogIndex"])
                q.last_seen_ts = time.time()

                body = str(resp.get("body", ""))
                body_norm = body.strip()
                same_as_last = body_norm == q.last_body
                if same_as_last:
                    q.unchanged_polls += 1
                else:
                    q.unchanged_polls = 0
                    q.last_body = body_norm

                # Suppress repetitive status-only spam while keeping periodic visibility.
                status_only = body_norm.startswith("<status>") and body_norm.endswith("</status>")
                should_log_body = bool(args.verbose and body)
                if should_log_body and status_only and same_as_last:
                    # Show every 20 repeats instead of every poll.
                    should_log_body = (q.unchanged_polls % 20 == 0)
                if should_log_body:
                    preview = body.replace("\n", " ")
                    if len(preview) > 180:
                        preview = preview[:177] + "..."
                    suffix = f" (repeat x{q.unchanged_polls + 1})" if same_as_last and status_only else ""
                    log(f"[bridge] body team={q.team_name} query={q.query_id}: {preview}{suffix}", verbose=True)

                # If server appears stuck on same status message, apply only a light backoff
                # so bridge status handling remains responsive.
                if status_only and same_as_last and q.unchanged_polls >= 10:
                    loop_sleep = max(loop_sleep, 0.35)

                # Always keep legacy parser as a fallback until outbox delivery is fully reliable.
                calls = list(bridge_calls)
                calls.extend(extract_bridge_calls(body))
                # Only log parser miss if body appears to contain a true bridge_call type payload.
                bridge_type_marker = '"type"' in body and ("bridge_call" in body or "&quot;bridge_call&quot;" in body)
                if args.verbose and bridge_type_marker and not calls:
                    preview = html.unescape(body).replace("\n", " ")
                    if len(preview) > 320:
                        preview = preview[:317] + "..."
                    log(f"[bridge] found bridge_call marker but parsed 0 calls. body_preview={preview}", verbose=True)
                for call in calls:
                    if not is_actionable_bridge_call(call, accepted_bridge_ids):
                        if args.verbose:
                            ctype = str((call.raw or {}).get("type", "")) if isinstance(call.raw, dict) else ""
                            log(
                                f"[bridge] ignoring non-actionable call msg_id={call.msg_id or '<none>'} type={ctype or '<none>'} bridge_id={call.bridge_id or '<none>'}",
                                verbose=True,
                            )
                        continue
                    dedupe_key = (q.team_name, q.query_id, call.msg_id)
                    if dedupe_key in seen_call_ids:
                        continue
                    seen_call_ids.add(dedupe_key)
                    sentienta_meta = call.raw.get("_sentienta") if isinstance(call.raw, dict) and isinstance(call.raw.get("_sentienta"), dict) else {}
                    created_at = 0
                    claimed_at = 0
                    try:
                        created_at = int(
                            sentienta_meta.get("created_at")
                            or call.raw.get("created_at")
                            or call.raw.get("createdAt")
                            or 0
                        )
                    except Exception:
                        created_at = 0
                    try:
                        claimed_at = int(
                            sentienta_meta.get("claimedAt")
                            or call.raw.get("claimedAt")
                            or 0
                        )
                    except Exception:
                        claimed_at = 0
                    now_epoch = int(time.time())
                    queue_wait_s = max(0, now_epoch - created_at) if created_at else 0
                    claim_delay_s = max(0, claimed_at - created_at) if (created_at and claimed_at) else 0
                    post_claim_delay_s = max(0, now_epoch - claimed_at) if claimed_at else 0
                    record_bridge_debug_event(
                        pairing_state,
                        "handle_call",
                        team_name=q.team_name,
                        query_id=q.query_id,
                        user_id=q.user_id,
                        payload={
                            "msgID": call.msg_id,
                            "tool": call.tool,
                            "bridgeID": call.bridge_id,
                            "queueWaitS": queue_wait_s,
                            "claimDelayS": claim_delay_s,
                            "postClaimDelayS": post_claim_delay_s,
                            "createdAt": created_at,
                            "claimedAt": claimed_at,
                        },
                    )
                    log(
                        f"[bridge] handling msg_id={call.msg_id} tool={call.tool} "
                        f"bridge_id={call.bridge_id} team={q.team_name} query={q.query_id} "
                        f"queue_wait_s={queue_wait_s} claim_delay_s={claim_delay_s} post_claim_delay_s={post_claim_delay_s}",
                        verbose=True,
                    )
                    response_bridge_id = str(call.bridge_id or "").strip() or str(args.bridge_id)

                    try:
                        if not str(pairing_state.get("bridge_secret", "") or "").strip():
                            raise BridgeError("Services are not currently enabled for this request. Please enable desktop automation.")
                        result = execute_call(
                            call,
                            roots,
                            pairing_state,
                            selected_services,
                            args.max_chars_default,
                            args.max_chars_hard,
                            args.max_find_results_default,
                            args.max_find_results_hard,
                        )
                        # Per-query toggle from UI: suppress partial-event fields while task is running.
                        if (
                            call.tool == "openclaw.get_status"
                            and isinstance(result, dict)
                            and not bool(q.show_partial_results)
                        ):
                            status_val = str(result.get("status", "") or "").strip().lower()
                            if status_val == "running":
                                result["latest_event"] = ""
                                result["recent_events"] = []
                                result["last_stdout_line"] = ""
                                result["last_stderr_line"] = ""
                                result["stdout_tail"] = []
                                result["stderr_tail"] = []
                        if args.verbose:
                            result_status = ""
                            if isinstance(result, dict):
                                result_status = str(result.get("status", "") or "")
                            log(
                                f"[bridge] result msg_id={call.msg_id} ok=True status={result_status} "
                                f"result_keys={list(result.keys()) if isinstance(result, dict) else []}",
                                verbose=True,
                            )
                        result_obj = build_result(call, response_bridge_id, ok=True, result=result)
                        # Remember terminal OpenClaw completion so we can retire noisy post-completion chatter.
                        if call.tool == "openclaw.get_status" and isinstance(result, dict):
                            status_val = str(result.get("status", "") or "").strip().lower()
                            if status_val in {"completed", "failed", "canceled"}:
                                q.openclaw_terminal_ts = time.time()
                                q.openclaw_terminal_task_id = str(result.get("task_id", "") or "").strip()
                                if args.verbose:
                                    log(
                                        f"[bridge][oc] terminal status captured team={q.team_name} query={q.query_id} "
                                        f"task_id={q.openclaw_terminal_task_id or '<unknown>'} status={status_val}",
                                        verbose=True,
                                    )
                        elif call.tool == "openclaw.run_task":
                            # New run clears prior terminal marker for this query.
                            q.openclaw_terminal_ts = 0.0
                            q.openclaw_terminal_task_id = ""
                            q.current_openclaw_task_id = str(result.get("task_id", "") or "").strip()
                            runtime = _ensure_openclaw_runtime(pairing_state)
                            tasks = runtime["tasks"]
                            task_ref = tasks.get(q.current_openclaw_task_id)
                            if isinstance(task_ref, dict):
                                task_ref["team_name"] = q.team_name
                                task_ref["query_id"] = q.query_id
                        result_status = str(result.get("status", "") or "").strip() if isinstance(result, dict) else ""
                        result_launch_timing = (
                            dict(result.get("launch_timing") or {})
                            if isinstance(result, dict) and isinstance(result.get("launch_timing"), dict)
                            else {}
                        )
                        record_bridge_debug_event(
                            pairing_state,
                            "execute_call_result",
                            team_name=q.team_name,
                            query_id=q.query_id,
                            user_id=q.user_id,
                            payload={
                                "msgID": call.msg_id,
                                "tool": call.tool,
                                "ok": True,
                                "status": result_status,
                                "launchTiming": result_launch_timing,
                            },
                        )
                    except BridgeError as e:
                        record_bridge_debug_event(
                            pairing_state,
                            "execute_call_result",
                            team_name=q.team_name,
                            query_id=q.query_id,
                            user_id=q.user_id,
                            payload={
                                "msgID": call.msg_id,
                                "tool": call.tool,
                                "ok": False,
                                "error": str(e),
                            },
                        )
                        if args.verbose:
                            log(
                                f"[bridge] result msg_id={call.msg_id} ok=False error={str(e)}",
                                verbose=True,
                            )
                        result_obj = build_result(call, response_bridge_id, ok=False, error=str(e))

                    try:
                        result_source = _bridge_call_source_name(call.raw)
                        post_resp = post_bridge_result(
                            query_endpoint=args.query_endpoint,
                            headers=poll_headers,
                            team_name=q.team_name,
                            query_id=q.query_id,
                            bridge_id=response_bridge_id,
                            source=result_source,
                            original_call=call.raw,
                            result_obj=result_obj,
                            user_id=q.user_id,
                        )
                        if args.verbose:
                            log(
                                f"[bridge] post msg_id={call.msg_id} queued={post_resp.get('queued')} "
                                f"total={post_resp.get('total')} errors={post_resp.get('errors')}",
                                verbose=True,
                            )
                            log(f"[bridge] posted bridge_result msg_id={call.msg_id} resp={str(post_resp)[:160]}", verbose=True)
                        record_bridge_debug_event(
                            pairing_state,
                            "post_bridge_result",
                            team_name=q.team_name,
                            query_id=q.query_id,
                            user_id=q.user_id,
                            payload={
                                "msgID": call.msg_id,
                                "queued": post_resp.get("queued"),
                                "total": post_resp.get("total"),
                                "errors": post_resp.get("errors"),
                            },
                        )
                    except BridgeError as e:
                        record_bridge_debug_event(
                            pairing_state,
                            "post_bridge_result_error",
                            team_name=q.team_name,
                            query_id=q.query_id,
                            user_id=q.user_id,
                            payload={
                                "msgID": call.msg_id,
                                "error": str(e),
                            },
                        )
                        log(f"[bridge] failed posting bridge_result for {call.msg_id}: {e}", verbose=True)

                if "EOD" in body:
                    with active_lock:
                        active_queries.pop((q.team_name, q.query_id), None)
                    log(f"[bridge] EOD reached, retired query team={q.team_name} query={q.query_id}", verbose=True)
                    if args.exit_on_eod:
                        with active_lock:
                            if not active_queries:
                                log("[bridge] no active queries remain, exiting.", verbose=True)
                                return 0
                elif q.openclaw_terminal_ts > 0.0:
                    # Do not retire solely on terminal OpenClaw status.
                    # The server may still need to post final dialog content and EOD.
                    pass

            time.sleep(loop_sleep)
    finally:
        server.shutdown()
        server.server_close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
