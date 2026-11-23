"""Keep the Bridge signed in and find v3 work without an open Sentienta page.

When the owner pairs from the v3 page, the page also hands over a Cognito refresh
token. The Bridge then renews its own short-lived sign-in token, keeps every work
queue's credentials current, and asks v3 which queues have pending work (the same
question the page asks), so the page does not need to stay open.

The refresh token is held in memory only: it is never written to disk or logged,
and a Bridge restart requires pairing again.
"""
from __future__ import annotations

import base64
import json
import threading
import time
from typing import Dict, List, Optional

import requests

REFRESH_MARGIN_SECONDS = 300      # renew this long before the sign-in token expires
DISCOVERY_SECONDS = 15
RETIRE_AFTER_SECONDS = 180
REQUEST_TIMEOUT = 15


def _token_expiry(id_token: str) -> float:
    try:
        payload = id_token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload.encode()).decode()).get("exp") or 0)
    except Exception:
        return 0.0


class OwnerSession:
    def __init__(self, *, user_id: str, refresh_token: str, client_id: str, region: str, v3_endpoint: str,
                 id_token: str = "", post=None):
        self.user_id = user_id
        self._refresh_token = refresh_token
        self.client_id = client_id
        self.region = region
        self.v3_endpoint = v3_endpoint
        self.id_token = id_token
        self.expires_at = _token_expiry(id_token) if id_token else 0.0
        self.next_discovery = 0.0
        self.last_error = ""
        self.added: Dict[tuple, float] = {}   # queues this session registered, and when
        self._post = post or requests.post

    def current_id_token(self, now: Optional[float] = None) -> str:
        now = time.time() if now is None else now
        if self.id_token and now < self.expires_at - REFRESH_MARGIN_SECONDS:
            return self.id_token
        response = self._post(
            f"https://cognito-idp.{self.region}.amazonaws.com/",
            headers={"Content-Type": "application/x-amz-json-1.1",
                     "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth"},
            data=json.dumps({"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": self.client_id,
                             "AuthParameters": {"REFRESH_TOKEN": self._refresh_token}}),
            timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise RuntimeError(f"Sign-in renewal was refused ({response.status_code}); pair the Bridge again.")
        token = str(((response.json() or {}).get("AuthenticationResult") or {}).get("IdToken") or "")
        if not token:
            raise RuntimeError("Sign-in renewal returned no token; pair the Bridge again.")
        self.id_token, self.expires_at = token, _token_expiry(token)
        return token

    def pending_queues(self, id_token: str) -> List[Dict[str, str]]:
        response = self._post(self.v3_endpoint,
                              headers={"Content-Type": "application/json", "Authorization": f"Bearer {id_token}"},
                              data=json.dumps({"type": "v3GetPurposeBridgeRegistration", "includePursuit": True}),
                              timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise RuntimeError(f"v3 work discovery failed ({response.status_code}).")
        body = response.json() or {}
        message = body.get("message", body)
        if isinstance(message, str):
            try:
                message = json.loads(message)
            except ValueError:
                message = {}
        return [q for q in (message or {}).get("queues") or [] if isinstance(q, dict)]


_lock = threading.Lock()
_session: Optional[OwnerSession] = None


def install(session: Optional[OwnerSession]) -> None:
    global _session
    with _lock:
        _session = session


def current() -> Optional[OwnerSession]:
    with _lock:
        return _session


def maintain(active_queries: dict, active_lock, make_query, auth_headers, log=print, now: Optional[float] = None) -> int:
    """Renew sign-in, refresh queue credentials, and register newly pending v3 queues.

    make_query(team_name, query_id, user_id, headers) builds a queue entry like the
    page's registration does. Returns the number of queues added.
    """
    session = current()
    now = time.time() if now is None else now
    if session is None or now < session.next_discovery:
        return 0
    session.next_discovery = now + DISCOVERY_SECONDS
    try:
        token = session.current_id_token(now)
        headers = auth_headers(token)
        queues = session.pending_queues(token)
    except Exception as error:
        message = str(error)[:200]
        if message != session.last_error:
            log(f"[bridge][session] {message}")
        session.last_error = message
        return 0
    session.last_error = ""
    added = 0
    pending = {(str(i.get("teamName") or ""), str(i.get("queryID") or "")) for i in queues}
    with active_lock:
        # Queues this session added retire once v3 no longer lists them (their work is
        # done or abandoned), after a grace period for the result to be posted.
        for key, started in list(session.added.items()):
            if key not in pending and now - started > RETIRE_AFTER_SECONDS:
                active_queries.pop(key, None)
                session.added.pop(key, None)
        for q in list(active_queries.values()):
            if not q.user_id or q.user_id == session.user_id:
                q.auth_headers = dict(headers)
        for item in queues:
            key = (str(item.get("teamName") or ""), str(item.get("queryID") or ""))
            if not all(key):
                continue
            if key in active_queries:
                active_queries[key].auth_headers = dict(headers)
                continue
            active_queries[key] = make_query(key[0], key[1], session.user_id, dict(headers))
            session.added[key] = now
            added += 1
    if added:
        log(f"[bridge][session] registered {added} pending v3 work queue(s) without the page")
    return added
