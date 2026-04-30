import argparse
import base64
import json
import os
import platform
import socket
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sentienta_bridge import (
    DEFAULT_QUERY_ENDPOINT,
    BridgeCall,
    BridgeError,
    build_result,
    execute_openclaw_agents_list,
    execute_call,
    post_bridge_result,
    resolve_selected_services,
)


DEFAULT_WORKER_HOME = Path.home() / ".sentienta-bridge"
DEFAULT_WORKER_SESSION_FILE = DEFAULT_WORKER_HOME / "enterprise-session.json"
DEFAULT_WORKER_CONFIG_FILE = DEFAULT_WORKER_HOME / "enterprise-worker.json"


def parse_args():
    help_epilog = (
        "Examples:\n"
        "  # Minimal enterprise worker startup\n"
        "  python sentienta_bridge_enterprise.py --worker-username worker@example.com --owner-user-id user_123 --service openclaw_exec\n"
        "\n"
        "  # Provide worker password via environment variable\n"
        "  set SENTIENTA_WORKER_PASSWORD=secret\n"
        "  python sentienta_bridge_enterprise.py --worker-username worker@example.com --owner-user-id user_123 --verbose\n"
        "\n"
        "Notes:\n"
        "  - Provide --worker-username and either --worker-password or the env var named by --worker-password-env.\n"
        "  - If --service is omitted, the worker uses the bridge service policy or the currently supported service set.\n"
        "  - Start OpenClaw first when using --service openclaw_exec."
    )
    p = argparse.ArgumentParser(
        description="Headless enterprise Sentienta bridge worker for OpenClaw-backed async agents.",
        epilog=help_epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--query-endpoint",
        default=DEFAULT_QUERY_ENDPOINT,
        help=f"Sentienta query API endpoint (default: {DEFAULT_QUERY_ENDPOINT}).",
    )
    p.add_argument("--bridge-id", default="", help="Optional explicit enterprise bridge identifier.")
    p.add_argument(
        "--worker-config-file",
        default=str(DEFAULT_WORKER_CONFIG_FILE),
        help=f"Path for persisted worker config JSON (default: {DEFAULT_WORKER_CONFIG_FILE}).",
    )
    p.add_argument(
        "--worker-session-file",
        default=str(DEFAULT_WORKER_SESSION_FILE),
        help=f"Path for persisted Cognito session JSON (default: {DEFAULT_WORKER_SESSION_FILE}).",
    )
    p.add_argument(
        "--worker-username",
        default=os.getenv("SENTIENTA_WORKER_USERNAME", ""),
        help="Enterprise worker username/email (or env SENTIENTA_WORKER_USERNAME).",
    )
    p.add_argument(
        "--worker-password",
        default=os.getenv("SENTIENTA_WORKER_PASSWORD", ""),
        help="Enterprise worker password (or env SENTIENTA_WORKER_PASSWORD).",
    )
    p.add_argument(
        "--worker-password-env",
        default="SENTIENTA_WORKER_PASSWORD",
        help="Environment variable name to read the worker password from when --worker-password is omitted.",
    )
    p.add_argument(
        "--owner-user-id",
        default=os.getenv("SENTIENTA_WORKER_OWNER_USER_ID", ""),
        help="Sentienta owner mailbox userID for this worker (or env SENTIENTA_WORKER_OWNER_USER_ID).",
    )
    p.add_argument(
        "--cognito-region",
        default=os.getenv("SENTIENTA_COGNITO_REGION", "us-west-2"),
        help="AWS Cognito region (default: us-west-2).",
    )
    p.add_argument(
        "--cognito-client-id",
        default=os.getenv("SENTIENTA_COGNITO_CLIENT_ID", "7r0mn4q7thj015er50ncauq7i6"),
        help="AWS Cognito app client id for worker authentication.",
    )
    p.add_argument(
        "--poll-interval-secs",
        type=float,
        default=float(os.getenv("SENTIENTA_WORKER_POLL_INTERVAL_SECS", "1")),
        help="Idle delay between job polls in seconds (default: 1).",
    )
    p.add_argument(
        "--heartbeat-interval-secs",
        type=float,
        default=float(os.getenv("SENTIENTA_WORKER_HEARTBEAT_INTERVAL_SECS", "15")),
        help="Heartbeat interval in seconds (default: 15).",
    )
    p.add_argument("--limit", type=int, default=10, help="Maximum number of queued jobs to fetch per poll (default: 10).")
    p.add_argument("--once", action="store_true", help="Run one poll cycle and then exit.")
    p.add_argument("--verbose", action="store_true", help="Enable verbose worker logging.")
    p.add_argument(
        "--service",
        action="append",
        choices=["openclaw_exec"],
        default=[],
        help="Enable only the specified service(s). Repeat to enable multiple. Current supported service: openclaw_exec.",
    )
    p.add_argument(
        "--openclaw-cli",
        default=os.getenv("SENTIENTA_OPENCLAW_CLI", "openclaw"),
        help="OpenClaw CLI executable path/name.",
    )
    p.add_argument(
        "--openclaw-default-agent",
        default=os.getenv("SENTIENTA_OPENCLAW_DEFAULT_AGENT", "main"),
        help="Default OpenClaw agent id for task execution.",
    )
    p.add_argument(
        "--openclaw-default-timeout-ms",
        type=int,
        default=int(os.getenv("SENTIENTA_OPENCLAW_DEFAULT_TIMEOUT_MS", "210000")),
        help="Default OpenClaw task timeout in milliseconds (default: 210000).",
    )
    p.add_argument("--max-chars-default", type=int, default=40000, help="Default response character limit for tool results.")
    p.add_argument("--max-chars-hard", type=int, default=500000, help="Hard maximum response character limit for tool results.")
    p.add_argument("--max-find-results-default", type=int, default=20, help="Default result limit for find/search-style operations.")
    p.add_argument("--max-find-results-hard", type=int, default=200, help="Hard maximum result limit for find/search-style operations.")
    return p.parse_args()


def log(msg: str, verbose: bool = True):
    if verbose:
        print(msg, flush=True)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _result_summary(result: object) -> Dict[str, object]:
    if not isinstance(result, dict):
        return {}
    nested = result.get("result")
    payload_block = nested.get("result") if isinstance(nested, dict) else {}
    payloads = payload_block.get("payloads") if isinstance(payload_block, dict) else None
    return {
        "status": str(result.get("status") or "").strip(),
        "task_id": str(result.get("task_id") or "").strip(),
        "agent_id": str(result.get("agent_id") or result.get("agent") or "").strip(),
        "poll_count": int(result.get("poll_count") or 0),
        "heartbeat_seq": int(result.get("heartbeat_seq") or 0),
        "elapsed_ms": int(result.get("elapsed_ms") or 0),
        "latest_event_set": bool(str(result.get("latest_event") or "").strip()),
        "summary_preview": str(result.get("summary") or "").strip()[:180],
        "payload_count": len(payloads) if isinstance(payloads, list) else 0,
    }


def _extract_agent_ids(agents: object) -> List[str]:
    ids: List[str] = []
    seen = set()
    for item in agents if isinstance(agents, list) else []:
        if not isinstance(item, dict):
            continue
        candidate = str(item.get("agent_id") or item.get("name") or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        ids.append(candidate)
    return ids


def _sync_runtime_agent_inventory(
    pairing_state: Dict[str, object],
    agents: object,
    refreshed_at: Optional[float] = None,
) -> None:
    try:
        runtime = pairing_state.get("openclaw_runtime")
        if not isinstance(runtime, dict):
            return
        ids = _extract_agent_ids(agents)
        if not ids:
            return
        runtime["known_agent_ids"] = set(ids)
        runtime["known_agent_ids_ts"] = int(refreshed_at or time.time())
        runtime["known_agent_ids_ttl_sec"] = 75
        runtime["known_agent_ids_source"] = "enterprise_inventory"
    except Exception:
        return


def _ensure_parent(path_str: str) -> Path:
    path = Path(path_str).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_json_file(path: Path) -> Dict[str, object]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Failed to load json file {path}: {e}", flush=True)
    return {}


def _save_json_file(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _jwt_exp(token: str) -> int:
    try:
        parts = str(token or "").split(".")
        if len(parts) < 2:
            return 0
        padded = parts[1] + ("=" * (-len(parts[1]) % 4))
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8"))
        return int(payload.get("exp") or 0)
    except Exception:
        return 0


def _cognito_username(raw: str) -> str:
    txt = str(raw or "").strip()
    if "@" in txt:
        return txt.replace("@", "-at-")
    return txt


class CognitoWorkerSession:
    def __init__(self, region: str, client_id: str, session_file: Path, username: str, password: str):
        self.region = str(region or "").strip() or "us-west-2"
        self.client_id = str(client_id or "").strip()
        self.session_file = session_file
        self.username = str(username or "").strip()
        self.password = str(password or "").strip()
        self.client = boto3.client("cognito-idp", region_name=self.region)
        self.state = _load_json_file(session_file)

    def _store_auth_result(self, auth: Dict[str, object], refresh_token_fallback: str = ""):
        id_token = str(auth.get("IdToken") or "").strip()
        refresh_token = str(auth.get("RefreshToken") or refresh_token_fallback or "").strip()
        expires_in = int(auth.get("ExpiresIn") or 3600)
        expires_at = max(int(time.time()) + expires_in - 120, _jwt_exp(id_token) - 120)
        self.state.update({
            "username": self.username,
            "idToken": id_token,
            "refreshToken": refresh_token,
            "expiresAt": expires_at,
            "updatedAt": int(time.time()),
        })
        _save_json_file(self.session_file, self.state)

    def _login_password(self):
        if not self.client_id:
            raise BridgeError("Missing Cognito client id.")
        if not self.username or not self.password:
            raise BridgeError("Missing worker username/password.")
        resp = self.client.initiate_auth(
            ClientId=self.client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": _cognito_username(self.username),
                "PASSWORD": self.password,
            },
        )
        auth = resp.get("AuthenticationResult") or {}
        if not auth.get("IdToken"):
            raise BridgeError("Cognito login did not return an id token.")
        self._store_auth_result(auth)

    def _refresh(self):
        refresh_token = str(self.state.get("refreshToken") or "").strip()
        if not refresh_token:
            self._login_password()
            return
        resp = self.client.initiate_auth(
            ClientId=self.client_id,
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh_token},
        )
        auth = resp.get("AuthenticationResult") or {}
        if not auth.get("IdToken"):
            raise BridgeError("Cognito refresh did not return an id token.")
        self._store_auth_result(auth, refresh_token_fallback=refresh_token)

    def ensure_id_token(self) -> str:
        token = str(self.state.get("idToken") or "").strip()
        expires_at = int(self.state.get("expiresAt") or 0)
        now = int(time.time())
        if token and expires_at and now < expires_at:
            return token
        try:
            self._refresh()
        except Exception:
            self._login_password()
        token = str(self.state.get("idToken") or "").strip()
        if not token:
            raise BridgeError("Unable to obtain Cognito id token for enterprise worker.")
        return token


class EnterpriseBridgeWorker:
    def __init__(self, args):
        self.args = args
        self.config_path = _ensure_parent(args.worker_config_file)
        self.session_path = _ensure_parent(args.worker_session_file)
        self.config = _load_json_file(self.config_path)
        self.bridge_id = self._resolve_bridge_id()
        self.owner_user_id = str(args.owner_user_id or self.config.get("ownerUserId") or "").strip()
        password = str(args.worker_password or os.getenv(args.worker_password_env or "", "") or "").strip()
        self.session = CognitoWorkerSession(
            region=args.cognito_region,
            client_id=args.cognito_client_id,
            session_file=self.session_path,
            username=str(args.worker_username or self.config.get("workerUsername") or "").strip(),
            password=password,
        )
        self.selected_services = resolve_selected_services(args.service, self.bridge_id)
        self.cached_openclaw_agents = []
        self.last_openclaw_agents_refresh = 0.0
        self.pairing_state = {
            "openclaw_config": {
                "cli_path": str(args.openclaw_cli or "openclaw").strip() or "openclaw",
                "default_agent": str(args.openclaw_default_agent or "main").strip() or "main",
                "default_timeout_ms": int(args.openclaw_default_timeout_ms or 210000),
                "execution_mode": "cli",
                "agents_root": str((Path.home() / ".openclaw" / "agents").resolve()),
            },
            "openclaw_runtime": {"tasks": {}},
        }

    def _cached_openclaw_agents_snapshot(self) -> List[Dict[str, object]]:
        cached = list(self.cached_openclaw_agents or [])
        if cached:
            _sync_runtime_agent_inventory(
                self.pairing_state,
                cached,
                refreshed_at=time.time(),
            )
        return cached

    def _ensure_cached_openclaw_agent(self, agent_id: str, name: str = "") -> None:
        aid = str(agent_id or "").strip()
        if not aid:
            return
        cached = list(self.cached_openclaw_agents or [])
        for item in cached:
            if not isinstance(item, dict):
                continue
            if str(item.get("agent_id") or "").strip() == aid:
                return
        cached.append({
            "agent_id": aid,
            "name": str(name or aid).strip() or aid,
        })
        self.cached_openclaw_agents = cached
        if not self.last_openclaw_agents_refresh:
            self.last_openclaw_agents_refresh = time.time()
        _sync_runtime_agent_inventory(
            self.pairing_state,
            self.cached_openclaw_agents,
            refreshed_at=(self.last_openclaw_agents_refresh or time.time()),
        )
        if bool(self.args.verbose):
            log(
                f"[enterprise-bridge][inventory] on-demand-add bridge_id={self.bridge_id} agent_id={aid} "
                f"count={len(self.cached_openclaw_agents)}",
                True,
            )

    def _refresh_openclaw_agents_cache(self, force: bool = False) -> List[Dict[str, object]]:
        now = time.time()
        if not force and (now - float(self.last_openclaw_agents_refresh or 0.0)) < 60.0:
            cached = list(self.cached_openclaw_agents or [])
            _sync_runtime_agent_inventory(self.pairing_state, cached, refreshed_at=now)
            if bool(self.args.verbose):
                sample = [
                    {
                        "agent_id": str(item.get("agent_id") or "").strip(),
                        "name": str(item.get("name") or "").strip(),
                    }
                    for item in cached[:8]
                    if isinstance(item, dict)
                ]
                log(
                    f"[enterprise-bridge][inventory] cache-hit bridge_id={self.bridge_id} age_sec={int(now - float(self.last_openclaw_agents_refresh or 0.0))} "
                    f"count={len(cached)} sample={json.dumps(sample, ensure_ascii=False)}",
                    True,
                )
            return cached
        try:
            if bool(self.args.verbose):
                log(
                    f"[enterprise-bridge][inventory] refresh-start bridge_id={self.bridge_id} force={str(bool(force)).lower()}",
                    True,
                )
            result = execute_openclaw_agents_list(
                BridgeCall(
                    msg_id="enterprise_bridge_inventory",
                    bridge_id=self.bridge_id,
                    tool="openclaw.agents.list",
                    args={},
                    raw={},
                ),
                self.pairing_state,
            )
            agents = result.get("agents") if isinstance(result, dict) else []
            self.cached_openclaw_agents = agents if isinstance(agents, list) else []
            self.last_openclaw_agents_refresh = now
            _sync_runtime_agent_inventory(
                self.pairing_state,
                self.cached_openclaw_agents,
                refreshed_at=self.last_openclaw_agents_refresh,
            )
            if bool(self.args.verbose):
                sample = [
                    {
                        "agent_id": str(item.get("agent_id") or "").strip(),
                        "name": str(item.get("name") or "").strip(),
                    }
                    for item in self.cached_openclaw_agents[:8]
                    if isinstance(item, dict)
                ]
                log(
                    f"[enterprise-bridge][inventory] refresh-done bridge_id={self.bridge_id} count={len(self.cached_openclaw_agents)} "
                    f"sample={json.dumps(sample, ensure_ascii=False)}",
                    True,
                )
        except Exception as e:
            log(f"[enterprise-bridge][inventory] refresh failed err={str(e)}", bool(self.args.verbose))
        return list(self.cached_openclaw_agents or [])

    def _resolve_bridge_id(self) -> str:
        configured = str(self.args.bridge_id or self.config.get("bridgeId") or "").strip().lower()
        if configured:
            bridge_id = configured
        else:
            bridge_id = f"bridge_{platform.node().lower().replace(' ', '_')}_{uuid.uuid4().hex[:6]}"
        self.config["bridgeId"] = bridge_id
        if self.args.worker_username:
            self.config["workerUsername"] = str(self.args.worker_username).strip()
        if self.args.owner_user_id:
            self.config["ownerUserId"] = str(self.args.owner_user_id).strip()
        _save_json_file(self.config_path, self.config)
        return bridge_id

    def _auth_headers(self) -> Dict[str, str]:
        token = self.session.ensure_id_token()
        return {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        }

    def _post_query_json(self, payload: Dict[str, object]) -> Dict[str, object]:
        body = json.dumps(payload).encode("utf-8")
        req = Request(url=self.args.query_endpoint, data=body, headers=self._auth_headers(), method="POST")
        try:
            with urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except HTTPError as e:
            err_body = ""
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            detail = f"HTTP {e.code} from {self.args.query_endpoint}: {e.reason}"
            if err_body:
                detail += f" body={err_body[:1200]}"
            raise BridgeError(detail) from e
        except URLError as e:
            raise BridgeError(f"Network error calling {self.args.query_endpoint}: {e}") from e

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}

    def _query(self, payload: Dict[str, object]) -> Dict[str, object]:
        response = self._post_query_json(payload)
        body = response.get("body")
        if isinstance(body, str):
            try:
                return json.loads(body)
            except Exception:
                return {"raw": body}
        if isinstance(body, dict):
            return body
        return response if isinstance(response, dict) else {}

    def register(self):
        openclaw_agents = self._refresh_openclaw_agents_cache(force=True)
        if bool(self.args.verbose):
            sample = [
                {
                    "agent_id": str(item.get("agent_id") or "").strip(),
                    "name": str(item.get("name") or "").strip(),
                }
                for item in openclaw_agents[:8]
                if isinstance(item, dict)
            ]
            log(
                f"[enterprise-bridge][inventory] register bridge_id={self.bridge_id} count={len(openclaw_agents)} "
                f"sample={json.dumps(sample, ensure_ascii=False)}",
                True,
            )
        payload = {
            "type": "registerBridgeWorker",
            "bridgeId": self.bridge_id,
            "ownerUserId": self.owner_user_id,
            "bridgeType": "enterprise",
            "status": "online",
            "capabilities": self.selected_services,
            "openclawAgents": openclaw_agents,
            "openclawAgentsUpdatedAt": int(time.time()),
            "clientVersion": "bridge-worker-phase1",
            "hostLabel": socket.gethostname(),
        }
        return self._query(payload)

    def heartbeat(self):
        openclaw_agents = self._cached_openclaw_agents_snapshot()
        if not openclaw_agents:
            openclaw_agents = self._refresh_openclaw_agents_cache(force=True)
        if bool(self.args.verbose):
            sample = [
                {
                    "agent_id": str(item.get("agent_id") or "").strip(),
                    "name": str(item.get("name") or "").strip(),
                }
                for item in openclaw_agents[:8]
                if isinstance(item, dict)
            ]
            log(
                f"[enterprise-bridge][inventory] heartbeat bridge_id={self.bridge_id} count={len(openclaw_agents)} "
                f"sample={json.dumps(sample, ensure_ascii=False)}",
                True,
            )
        payload = {
            "type": "heartbeatBridgeWorker",
            "bridgeId": self.bridge_id,
            "ownerUserId": self.owner_user_id,
            "status": "online",
            "capabilities": self.selected_services,
            "openclawAgents": openclaw_agents,
            "openclawAgentsUpdatedAt": int(self.last_openclaw_agents_refresh or time.time()),
        }
        return self._query(payload)

    def poll_jobs(self) -> List[Dict[str, object]]:
        payload = {
            "type": "getBridgeWorkerMessages",
            "bridgeId": self.bridge_id,
            "limit": int(self.args.limit or 10),
        }
        body = self._query(payload)
        msgs = body.get("messages")
        debug = body.get("debug") if isinstance(body.get("debug"), dict) else {}
        if bool(self.args.verbose):
            sample = []
            for msg in (msgs if isinstance(msgs, list) else [])[:8]:
                if not isinstance(msg, dict):
                    continue
                ctx = msg.get("_sentienta") if isinstance(msg.get("_sentienta"), dict) else {}
                created_at = _safe_int(msg.get("created_at") or msg.get("createdAt") or 0)
                claimed_at = _safe_int(msg.get("claimedAt") or 0)
                now_epoch = int(time.time())
                sample.append({
                    "msg_id": str(msg.get("msg_id") or msg.get("call_id") or "").strip(),
                    "tool": str(msg.get("tool") or "").strip(),
                    "query_id": str(ctx.get("queryID") or "").strip(),
                    "team_name": str(ctx.get("teamName") or "").strip(),
                    "run_key": str(ctx.get("runKey") or "").strip(),
                    "created_at": created_at,
                    "claimed_at": claimed_at,
                    "queue_wait_s": max(0, now_epoch - created_at) if created_at else 0,
                    "claim_delay_s": max(0, claimed_at - created_at) if (created_at and claimed_at) else 0,
                })
            log(
                f"[enterprise-bridge][poll] bridge_id={self.bridge_id} fetched={len(msgs) if isinstance(msgs, list) else 0} sample={json.dumps(sample, ensure_ascii=False)}",
                True,
            )
            if debug:
                log(
                    f"[enterprise-bridge][poll-debug] bridge_id={self.bridge_id} debug={json.dumps(debug, ensure_ascii=False)}",
                    True,
                )
        return msgs if isinstance(msgs, list) else []

    def _execute_message(self, msg: Dict[str, object]) -> None:
        worker_started = time.perf_counter()
        ctx = msg.get("_sentienta") if isinstance(msg.get("_sentienta"), dict) else {}
        team_name = str(ctx.get("teamName") or "").strip()
        query_id = str(ctx.get("queryID") or "").strip()
        run_key = str(ctx.get("runKey") or "").strip()
        msg_id = str(msg.get("msg_id") or msg.get("call_id") or "").strip()
        tool_name = str(msg.get("tool") or "").strip()
        created_at = _safe_int(msg.get("created_at") or msg.get("createdAt") or 0)
        claimed_at = _safe_int(msg.get("claimedAt") or 0)
        now_epoch = int(time.time())
        queue_wait_s = max(0, now_epoch - created_at) if created_at else 0
        claim_delay_s = max(0, claimed_at - created_at) if (created_at and claimed_at) else 0
        post_claim_delay_s = max(0, now_epoch - claimed_at) if claimed_at else 0
        user_id = str(
            msg.get("userID")
            or msg.get("username")
            or ctx.get("userID")
            or ctx.get("username")
            or ctx.get("ownerUserId")
            or ctx.get("ownerUserID")
            or ""
        ).strip()
        if not user_id and run_key:
            team_suffix = f"_{team_name}" if team_name else ""
            if team_suffix and run_key.endswith(team_suffix):
                user_id = run_key[: -len(team_suffix)].strip()
            elif "_" in run_key:
                user_id = run_key.rsplit("_", 1)[0].strip()
        call = BridgeCall(
            msg_id=msg_id or f"invalid_message_{int(time.time() * 1000)}",
            bridge_id=str(msg.get("bridge_id") or self.bridge_id).strip(),
            tool=tool_name or "unknown",
            args=msg.get("args") if isinstance(msg.get("args"), dict) else {},
            raw=msg,
        )
        validation_errors = []
        if not team_name:
            validation_errors.append("team")
        if not query_id:
            validation_errors.append("query")
        if not msg_id:
            validation_errors.append("msg_id")
        if not tool_name:
            validation_errors.append("tool")
        if validation_errors:
            error_text = "Worker message missing required context: " + ", ".join(validation_errors)
            if team_name and query_id:
                result_obj = build_result(call, self.bridge_id, False, error=error_text)
                post_bridge_result(
                    query_endpoint=self.args.query_endpoint,
                    headers=self._auth_headers(),
                    team_name=team_name,
                    query_id=query_id,
                    bridge_id=self.bridge_id,
                    source=f"EnterpriseBridgeWorker:{self.bridge_id}",
                    original_call=msg,
                    result_obj=result_obj,
                    user_id=user_id,
                )
            raise BridgeError(error_text)
        log(
            f"[enterprise-bridge][exec] start msg_id={call.msg_id} tool={call.tool} team={team_name} query={query_id} user_id={user_id or '<none>'} "
            f"queue_wait_s={queue_wait_s} claim_delay_s={claim_delay_s} post_claim_delay_s={post_claim_delay_s}",
            bool(self.args.verbose),
        )
        exec_started = time.perf_counter()
        try:
            result = execute_call(
                call=call,
                roots=[],
                pairing_state=self.pairing_state,
                selected_services=self.selected_services,
                max_chars_default=int(self.args.max_chars_default),
                max_chars_hard=int(self.args.max_chars_hard),
                max_find_results_default=int(self.args.max_find_results_default),
                max_find_results_hard=int(self.args.max_find_results_hard),
            )
            if call.tool == "openclaw.get_status":
                summary = _result_summary(result)
                log(
                    f"[enterprise-bridge][status] msg_id={call.msg_id} status={summary.get('status') or '<none>'} "
                    f"task_id={summary.get('task_id') or '<none>'} agent_id={summary.get('agent_id') or '<none>'} "
                    f"poll_count={summary.get('poll_count', 0)} heartbeat_seq={summary.get('heartbeat_seq', 0)} "
                    f"elapsed_ms={summary.get('elapsed_ms', 0)} payload_count={summary.get('payload_count', 0)} "
                    f"latest_event_set={summary.get('latest_event_set', False)} summary={summary.get('summary_preview') or '<none>'}",
                    bool(self.args.verbose),
                )
            launch_timing = result.get("launch_timing") if isinstance(result, dict) and isinstance(result.get("launch_timing"), dict) else {}
            if call.tool == "openclaw.run_task" and launch_timing and bool(self.args.verbose):
                log(
                    f"[enterprise-bridge][launch] msg_id={call.msg_id} task_id={str(result.get('task_id') or '').strip() or '<none>'} "
                    f"timing={json.dumps(launch_timing, ensure_ascii=False, sort_keys=True)}",
                    True,
                )
            if call.tool == "openclaw.run_task":
                args_agent_id = str(
                    call.args.get("agent_id")
                    or call.args.get("agent")
                    or ""
                ).strip() if isinstance(call.args, dict) else ""
                result_agent_id = str(
                    result.get("agent_id")
                    or result.get("agent")
                    or args_agent_id
                    or ""
                ).strip()
                self._ensure_cached_openclaw_agent(result_agent_id)
            result_obj = build_result(call, self.bridge_id, True, result=result)
        except Exception as e:
            result_obj = build_result(call, self.bridge_id, False, error=str(e))
            log(
                f"[enterprise-bridge][exec] error msg_id={call.msg_id} tool={call.tool} err={str(e)}",
                True,
            )
        exec_duration_ms = int(round((time.perf_counter() - exec_started) * 1000.0))

        post_summary = _result_summary(result_obj.get("result")) if isinstance(result_obj.get("result"), dict) else {}
        log(
            f"[enterprise-bridge][post] prepare msg_id={call.msg_id} tool={call.tool} ok={bool(result_obj.get('ok'))} "
            f"status={post_summary.get('status') or '<none>'} task_id={post_summary.get('task_id') or '<none>'} "
            f"exec_ms={exec_duration_ms} "
            f"team={team_name} query={query_id}",
            bool(self.args.verbose),
        )
        post_started = time.perf_counter()
        post_bridge_result(
            query_endpoint=self.args.query_endpoint,
            headers=self._auth_headers(),
            team_name=team_name,
            query_id=query_id,
            bridge_id=self.bridge_id,
            source=f"EnterpriseBridgeWorker:{self.bridge_id}",
            original_call=msg,
            result_obj=result_obj,
            user_id=user_id,
        )
        post_duration_ms = int(round((time.perf_counter() - post_started) * 1000.0))
        total_worker_ms = int(round((time.perf_counter() - worker_started) * 1000.0))
        log(
            f"[enterprise-bridge][post] sent msg_id={call.msg_id} tool={call.tool} ok={bool(result_obj.get('ok'))} "
            f"post_ms={post_duration_ms} total_worker_ms={total_worker_ms}",
            bool(self.args.verbose),
        )

    def run(self):
        verbose = bool(self.args.verbose)
        log(f"[enterprise-bridge] bridge_id={self.bridge_id} services={','.join(self.selected_services)}", verbose)
        reg = self.register()
        log(f"[enterprise-bridge] register={json.dumps(reg, ensure_ascii=False)}", verbose)
        last_heartbeat = 0.0
        while True:
            now = time.time()
            try:
                if now - last_heartbeat >= float(self.args.heartbeat_interval_secs):
                    hb = self.heartbeat()
                    log(f"[enterprise-bridge] heartbeat={json.dumps(hb, ensure_ascii=False)}", verbose)
                    last_heartbeat = now
                messages = self.poll_jobs()
                if messages:
                    msg_ids = [str((m or {}).get("msg_id") or (m or {}).get("call_id") or "").strip() for m in messages]
                    log(f"[enterprise-bridge] polled {len(messages)} job(s) msg_ids={msg_ids}", verbose)
                for msg in messages:
                    try:
                        self._execute_message(msg)
                    except Exception as e:
                        print(f"[enterprise-bridge] execute failed msg_id={str((msg or {}).get('msg_id') or '').strip()} err={e}", flush=True)
            except Exception as e:
                print(f"[enterprise-bridge] loop error: {e}", flush=True)
            if self.args.once:
                break
            time.sleep(max(0.5, float(self.args.poll_interval_secs)))


def main() -> int:
    args = parse_args()
    worker = EnterpriseBridgeWorker(args)
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
