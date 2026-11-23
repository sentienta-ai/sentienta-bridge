#!/usr/bin/env python3
"""Isolated native-browser worker for the Sentienta Bridge prototype.

The public surface is deliberately narrow. Callers provide structured tool
requests; they cannot provide JavaScript, CSS selectors, shell commands, or
browser credentials. Chrome runs in a dedicated profile and remains visible by
default so the user can interrupt or complete authentication directly.
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import os
import queue
import re
import shutil
import socket
import subprocess
import threading
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse

import requests
from websockets.sync.client import connect


SUPPORTED_BROWSER_TOOLS = {
    "browser.open",
    "browser.inspect_form",
    "browser.activate_action",
    "browser.begin_auth_handoff",
    "browser.end_auth_handoff",
    "browser.fill_form",
    "browser.attach_file",
    "browser.wait_for_page_change",
    "browser.screenshot",
    "browser.submit",
    "browser.cancel",
    "browser.close",
}
PROHIBITED_FIELD_KINDS = {"password", "file"}
SAFE_FIELD_KINDS = {"text", "email", "url", "tel", "number", "date", "textarea", "select", "checkbox", "radio", "contenteditable"}


class BrowserWorkerError(RuntimeError):
    """A safe, user-readable browser worker failure."""


class StalePageRevisionError(BrowserWorkerError):
    """The verified page changed before the selected action could execute."""


def _sha256_json(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _find_chrome() -> Path:
    configured = os.getenv("SENTIENTA_BROWSER_EXECUTABLE", "").strip()
    candidates = [
        Path(configured) if configured else None,
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
    ]
    for candidate in candidates:
        if candidate and candidate.is_file():
            return candidate
    raise BrowserWorkerError("A supported Chrome or Edge installation was not found.")


def _reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _validate_url(raw_url: object, *, allow_localhost: bool = False) -> str:
    value = str(raw_url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise BrowserWorkerError("Only complete HTTP or HTTPS URLs are allowed.")
    hostname = parsed.hostname.lower().rstrip(".")
    if hostname in {"localhost", "127.0.0.1", "::1"} and not allow_localhost:
        raise BrowserWorkerError("Local-network browser destinations are disabled.")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address and (address.is_private or address.is_loopback or address.is_link_local or address.is_reserved) and not allow_localhost:
        raise BrowserWorkerError("Private and local-network browser destinations are disabled.")
    if parsed.username or parsed.password:
        raise BrowserWorkerError("Credentials must not be included in a browser URL.")
    return value


def _origin_for_url(raw_url: object) -> str:
    parsed = urlparse(str(raw_url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise BrowserWorkerError("A valid HTTP or HTTPS origin is required.")
    hostname = parsed.hostname.lower().rstrip(".")
    host = f"[{hostname}]" if ":" in hostname else hostname
    port = parsed.port
    default_port = 80 if parsed.scheme == "http" else 443
    return f"{parsed.scheme}://{host}" + (f":{port}" if port and port != default_port else "")


class _CdpConnection:
    def __init__(self, ws_url: str):
        self._socket = connect(ws_url, open_timeout=10, max_size=16 * 1024 * 1024, ping_interval=None)
        self._next_id = 0
        self._pending: Dict[int, queue.Queue[dict]] = {}
        self._events: queue.Queue[dict] = queue.Queue()
        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self) -> None:
        try:
            for raw in self._socket:
                message = json.loads(raw)
                msg_id = message.get("id")
                if isinstance(msg_id, int) and msg_id in self._pending:
                    self._pending[msg_id].put(message)
                else:
                    self._events.put(message)
        except Exception as exc:  # pragma: no cover - exercised by disconnect integration
            if not self._closed:
                self._events.put({"method": "Sentienta.connectionClosed", "params": {"error": str(exc)}})

    def call(self, method: str, params: Optional[dict] = None, *, timeout: float = 15.0) -> dict:
        if self._closed:
            raise BrowserWorkerError("The browser session is closed.")
        self._next_id += 1
        call_id = self._next_id
        response_queue: queue.Queue[dict] = queue.Queue(maxsize=1)
        self._pending[call_id] = response_queue
        try:
            self._socket.send(json.dumps({"id": call_id, "method": method, "params": params or {}}))
            try:
                response = response_queue.get(timeout=timeout)
            except queue.Empty as exc:
                raise BrowserWorkerError(f"Browser operation timed out while running {method}.") from exc
            if "error" in response:
                detail = str((response.get("error") or {}).get("message") or "Browser operation failed.")
                raise BrowserWorkerError(detail)
            return dict(response.get("result") or {})
        finally:
            self._pending.pop(call_id, None)

    def close(self) -> None:
        self._closed = True
        try:
            self._socket.close()
        except Exception:
            pass

    def drain_events(self) -> list[dict]:
        events = []
        while True:
            try:
                events.append(self._events.get_nowait())
            except queue.Empty:
                return events


_INSPECT_SCRIPT = r"""
(() => {
  const roots = [];
  const visitRoot = (root) => {
    roots.push(root);
    for (const el of root.querySelectorAll('*')) if (el.shadowRoot) visitRoot(el.shadowRoot);
  };
  visitRoot(document);
  const all = (selector) => roots.flatMap(root => Array.from(root.querySelectorAll(selector)));
  const byId = (id) => roots.map(root => root.getElementById?.(id)).find(Boolean);
  const visible = (el) => {
    const s = window.getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return s.visibility !== 'hidden' && s.display !== 'none' && r.width > 0 && r.height > 0;
  };
  const clean = (v, n=240) => String(v || '').replace(/\s+/g, ' ').trim().slice(0, n);
  const labelFor = (el) => {
    if (el.labels && el.labels.length) return clean(Array.from(el.labels).map(x => x.innerText).join(' '));
    const aria = el.getAttribute('aria-label');
    if (aria) return clean(aria);
    const labelled = el.getAttribute('aria-labelledby');
    if (labelled) return clean(labelled.split(/\s+/).map(id => byId(id)?.innerText || '').join(' '));
    return clean(el.getAttribute('placeholder') || el.name || '');
  };
  const controls = all('input, textarea, select, [contenteditable]:not([contenteditable="false"])').filter(el => {
    const type = clean(el.getAttribute('type') || '').toLowerCase();
    return visible(el) && !el.disabled && type !== 'hidden' && type !== 'submit' && type !== 'button' && type !== 'reset';
  });
  const fields = controls.map((el, index) => {
    const tag = el.tagName.toLowerCase();
    const rawType = clean(el.getAttribute('type') || '').toLowerCase();
    const kind = el.isContentEditable ? 'contenteditable' : tag === 'textarea' ? 'textarea' : tag === 'select' ? 'select' : (rawType || 'text');
    const semanticRole = rawType === 'search' || el.getAttribute('role') === 'searchbox' || !!el.closest('[role="search"]') ? 'search' : (el.isContentEditable ? 'editor' : 'form');
    const label = labelFor(el);
    const signature = [index, kind, label, clean(el.name), clean(el.id)].join('|');
    const ref = 'field-' + index + '-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    el.setAttribute('data-sentienta-field-ref', ref);
    return {
      index,
      ref,
      signature,
      kind,
      semantic_role: semanticRole,
      label,
      required: !!el.required,
      constraints: {
        max_length: Number.isFinite(el.maxLength) && el.maxLength > 0 ? el.maxLength : null,
        min: clean(el.getAttribute('min')) || null,
        max: clean(el.getAttribute('max')) || null,
        options: tag === 'select' ? Array.from(el.options).slice(0, 100).map(o => ({label: clean(o.text), value: clean(o.value)})) : null
      }
    };
  });
  const isSubmit = (el) => el.matches('input[type="submit"]') || (el.tagName.toLowerCase() === 'button' && ((el.getAttribute('type') || '').toLowerCase() === 'submit' || (!(el.getAttribute('type') || '') && !!el.closest('form'))));
  const submits = all('button, input[type="submit"]').filter(el => visible(el) && isSubmit(el)).map((el, index) => {
    const ref = 'submit-' + index + '-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    el.setAttribute('data-sentienta-submit-ref', ref);
    return {index, ref, label: clean(el.innerText || el.value || el.getAttribute('aria-label') || 'Submit'), disabled: !!el.disabled};
  });
  const navigation = all('a[href], button, [role="button"]').filter(el => visible(el) && !el.disabled && !isSubmit(el)).map((el, index) => {
    const href = el.tagName.toLowerCase() === 'a' ? String(el.href || '') : '';
    let destinationOrigin = '';
    try { destinationOrigin = href ? new URL(href, location.href).origin : ''; } catch (_) {}
    const ref = 'nav-' + index + '-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    el.setAttribute('data-sentienta-nav-ref', ref);
    return {
      index,
      ref,
      label: clean(el.innerText || el.getAttribute('aria-label') || el.title || 'Action'),
      kind: el.tagName.toLowerCase() === 'a' ? 'link' : 'button',
      element_tag: el.tagName.toLowerCase(),
      role: clean(el.getAttribute('role') || ''),
      input_type: clean(el.getAttribute('type') || ''),
      destination_origin: destinationOrigin,
      same_origin: !destinationOrigin || destinationOrigin === location.origin
    };
  });
  const frames = all('iframe, frame').filter(visible).map((el, index) => ({
    index,
    src: clean(el.src || el.getAttribute('src') || 'about:blank', 1000),
    title: clean(el.title || el.getAttribute('aria-label') || '', 240),
    same_origin: (() => { try { return !!el.contentDocument; } catch (_) { return false; } })()
  }));
  const busy = all('[aria-busy="true"], [role="progressbar"]').some(visible);
  return {url: location.href, origin: location.origin, title: clean(document.title, 300), busy, fields, submits, navigation, frames};
})()
"""


_FILL_SCRIPT = r"""
((payload) => {
  const roots = [];
  const visitRoot = (root) => {
    roots.push(root);
    for (const el of root.querySelectorAll('*')) if (el.shadowRoot) visitRoot(el.shadowRoot);
  };
  visitRoot(document);
  const findRef = (attribute, ref) => roots.map(root => root.querySelector('[' + attribute + '="' + CSS.escape(ref) + '"]')).find(Boolean);
  const visible = (el) => {
    const s = window.getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return s.visibility !== 'hidden' && s.display !== 'none' && r.width > 0 && r.height > 0;
  };
  const updated = [];
  for (const item of payload.fields) {
    const el = findRef('data-sentienta-field-ref', item.ref);
    if (!el) throw new Error('field_missing');
    const type = String(el.getAttribute('type') || '').toLowerCase();
    if (type === 'password' || type === 'file') throw new Error('prohibited_field');
    if (el.isContentEditable) {
      el.textContent = String(item.value ?? '');
    } else if (type === 'checkbox' || type === 'radio') {
      el.checked = !!item.value;
    } else {
      const next = String(item.value ?? '');
      const setter = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(el), 'value')?.set;
      if (setter) setter.call(el, next); else el.value = next;
    }
    el.dispatchEvent(new Event('input', {bubbles:true}));
    el.dispatchEvent(new Event('change', {bubbles:true}));
    updated.push({ref:item.ref, value:type === 'password' ? '[REDACTED]' : (el.isContentEditable ? String(el.textContent || '') : (type === 'checkbox' || type === 'radio' ? !!el.checked : String(el.value)))});
  }
  return updated;
})(__PAYLOAD__)
"""


_SUBMIT_SCRIPT = r"""
((payload) => {
  const roots = [];
  const visitRoot = (root) => {
    roots.push(root);
    for (const el of root.querySelectorAll('*')) if (el.shadowRoot) visitRoot(el.shadowRoot);
  };
  visitRoot(document);
  const findRef = (attribute, ref) => roots.map(root => root.querySelector('[' + attribute + '="' + CSS.escape(ref) + '"]')).find(Boolean);
  const visible = (el) => {
    const s = window.getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return s.visibility !== 'hidden' && s.display !== 'none' && r.width > 0 && r.height > 0;
  };
  const isSubmit = (el) => el.matches('input[type="submit"]') || (el.tagName.toLowerCase() === 'button' && ((el.getAttribute('type') || '').toLowerCase() === 'submit' || (!(el.getAttribute('type') || '') && !!el.closest('form'))));
  const action = findRef('data-sentienta-submit-ref', payload.ref);
  if (!action || action.disabled) throw new Error('submit_action_unavailable');
  action.click();
  return true;
})(__PAYLOAD__)
"""


_ACTIVATE_SCRIPT = r"""
((payload) => {
  const roots = [];
  const visitRoot = (root) => {
    roots.push(root);
    for (const el of root.querySelectorAll('*')) if (el.shadowRoot) visitRoot(el.shadowRoot);
  };
  visitRoot(document);
  const findRef = (attribute, ref) => roots.map(root => root.querySelector('[' + attribute + '="' + CSS.escape(ref) + '"]')).find(Boolean);
  const visible = (el) => {
    const s = window.getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return s.visibility !== 'hidden' && s.display !== 'none' && r.width > 0 && r.height > 0;
  };
  const isSubmit = (el) => el.matches('input[type="submit"]') || (el.tagName.toLowerCase() === 'button' && ((el.getAttribute('type') || '').toLowerCase() === 'submit' || (!(el.getAttribute('type') || '') && !!el.closest('form'))));
  const action = findRef('data-sentienta-nav-ref', payload.ref);
  if (!action) throw new Error('navigation_action_unavailable');
  action.scrollIntoView({block:'center', inline:'center'});
  action.focus();
  action.click();
  return true;
})(__PAYLOAD__)
"""


@dataclass
class BrowserSession:
    session_id: str
    delegation_id: str
    profile_dir: Path
    process: subprocess.Popen
    connection: _CdpConnection
    browser_connection: _CdpConnection
    target_id: str
    allow_localhost: bool = False
    allowed_origins: set[str] = field(default_factory=set)
    allowed_target_ids: set[str] = field(default_factory=set)
    schema: Optional[dict] = None
    field_lookup: Dict[str, dict] = field(default_factory=dict)
    action_lookup: Dict[str, dict] = field(default_factory=dict)
    navigation_lookup: Dict[str, dict] = field(default_factory=dict)
    prepared_digest: str = ""
    filled_field_ids: list[str] = field(default_factory=list)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    persistent_profile: bool = False
    auth_handoff_until: float = 0.0
    auth_popup_target_id: str = ""


class BrowserWorker:
    def __init__(self, *, profile_root: Optional[Path] = None, screenshot_root: Optional[Path] = None, attachment_roots: Optional[Iterable[Path]] = None, headless: bool = False):
        base = Path.home() / ".sentienta-bridge" / "browser-prototype"
        self.profile_root = (profile_root or base / "profiles").expanduser().resolve()
        self.screenshot_root = (screenshot_root or base / "screenshots").expanduser().resolve()
        self.profile_root.mkdir(parents=True, exist_ok=True)
        self.screenshot_root.mkdir(parents=True, exist_ok=True)
        self.headless = bool(headless)
        self.attachment_roots = [Path(root).expanduser().resolve() for root in (attachment_roots or [])]
        self.sessions: Dict[str, BrowserSession] = {}
        self.request_cache: Dict[str, Dict[str, object]] = {}

    def dispatch(self, tool: str, args: Dict[str, object], *, request_id: str = "") -> Dict[str, object]:
        if tool not in SUPPORTED_BROWSER_TOOLS:
            raise BrowserWorkerError(f"Unsupported browser tool: {tool}")
        normalized_request_id = str(request_id or "").strip()
        request_digest = _sha256_json({"tool": tool, "args": args})
        if normalized_request_id:
            cached = self.request_cache.get(normalized_request_id)
            if cached:
                if cached.get("digest") != request_digest:
                    raise BrowserWorkerError("A browser request ID cannot be reused with different arguments.")
                return deepcopy(dict(cached.get("result") or {}))
        handler = getattr(self, tool.replace(".", "_"))
        result = handler(dict(args or {}))
        if normalized_request_id:
            self.request_cache[normalized_request_id] = {"digest": request_digest, "result": deepcopy(result)}
            if len(self.request_cache) > 500:
                oldest = next(iter(self.request_cache))
                self.request_cache.pop(oldest, None)
        return result

    def browser_open(self, args: Dict[str, object]) -> Dict[str, object]:
        delegation_id = str(args.get("delegation_id") or "").strip()
        if not delegation_id:
            raise BrowserWorkerError("delegation_id is required.")
        allow_localhost = bool(args.get("allow_localhost", False))
        url = _validate_url(args.get("url"), allow_localhost=allow_localhost)
        allowed_origins = {_origin_for_url(url)}
        raw_allowed_origins = args.get("allowed_origins") or []
        if not isinstance(raw_allowed_origins, list):
            raise BrowserWorkerError("allowed_origins must be a list of reviewed HTTP or HTTPS origins.")
        for candidate in raw_allowed_origins:
            reviewed_url = _validate_url(candidate, allow_localhost=allow_localhost)
            allowed_origins.add(_origin_for_url(reviewed_url))
        session_id = "brs_" + uuid.uuid4().hex
        profile_key = str(args.get("profile_key") or "").strip()
        if profile_key and not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", profile_key):
            raise BrowserWorkerError("profile_key contains unsupported characters.")
        persistent_profile = bool(profile_key)
        profile_name = "persistent_" + hashlib.sha256(profile_key.encode("utf-8")).hexdigest()[:24] if persistent_profile else session_id
        profile_dir = (self.profile_root / profile_name).resolve()
        profile_dir.mkdir(parents=True, exist_ok=persistent_profile)
        port = _reserve_port()
        executable = _find_chrome()
        command = [
            str(executable),
            f"--remote-debugging-port={port}",
            f"--user-data-dir={profile_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-session-crashed-bubble",
            "--hide-crash-restore-bubble",
            "--disable-sync",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-features=Translate,OptimizationHints,MediaRouter",
            "about:blank",
        ]
        if self.headless:
            command.insert(-1, "--headless=new")
        browser_connection = None
        page_connection = None
        try:
            version_url = f"http://127.0.0.1:{port}/json/version"
            version = None
            process = None
            for launch_attempt in range(2 if persistent_profile else 1):
                process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                deadline = time.time() + 15
                while time.time() < deadline:
                    if process.poll() is not None:
                        break
                    try:
                        response = requests.get(version_url, timeout=1)
                        if response.ok:
                            version = response.json()
                            break
                    except requests.RequestException:
                        time.sleep(0.1)
                if version:
                    break
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=3)
                if launch_attempt == 0 and persistent_profile:
                    time.sleep(2.0)
            if process is None or process.poll() is not None and not version:
                raise BrowserWorkerError("The dedicated browser exited before it was ready.")
            if not version:
                raise BrowserWorkerError("The dedicated browser did not become ready.")
            browser_connection = _CdpConnection(str(version["webSocketDebuggerUrl"]))
            browser_connection.call("Browser.setDownloadBehavior", {"behavior": "deny", "eventsEnabled": True})
            created = browser_connection.call("Target.createTarget", {"url": url})
            target_id = str(created["targetId"])
            targets = browser_connection.call("Target.getTargets").get("targetInfos", [])
            target = next((item for item in targets if item.get("targetId") == target_id), None)
            if not target or not target.get("webSocketDebuggerUrl"):
                target_list = requests.get(f"http://127.0.0.1:{port}/json/list", timeout=3).json()
                target = next((item for item in target_list if item.get("id") == target_id), None)
            if not target or not target.get("webSocketDebuggerUrl"):
                browser_connection.close()
                raise BrowserWorkerError("The new browser page could not be attached.")
            page_connection = _CdpConnection(str(target["webSocketDebuggerUrl"]))
            page_connection.call("Page.enable")
            page_connection.call("Runtime.enable")
            allowed_target_ids = {
                str(item.get("targetId") or "")
                for item in browser_connection.call("Target.getTargets").get("targetInfos", [])
                if item.get("type") == "page"
            }
            session = BrowserSession(
                session_id=session_id,
                delegation_id=delegation_id,
                profile_dir=profile_dir,
                process=process,
                connection=page_connection,
                browser_connection=browser_connection,
                target_id=target_id,
                allow_localhost=allow_localhost,
                allowed_origins=allowed_origins,
                allowed_target_ids=allowed_target_ids,
                persistent_profile=persistent_profile,
            )
            self.sessions[session_id] = session
            self._wait_ready(session)
            inspected = self._inspect(session)
            return {"ok": True, "browser_session_id": session_id, "state": "opened", "url": inspected["url"], "origin": inspected["origin"], "title": inspected["title"]}
        except Exception:
            self.sessions.pop(session_id, None)
            if page_connection is not None:
                page_connection.close()
            if browser_connection is not None:
                browser_connection.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except Exception:
                process.kill()
            if not persistent_profile:
                shutil.rmtree(profile_dir, ignore_errors=True)
            raise

    def _session(self, args: Dict[str, object]) -> BrowserSession:
        session_id = str(args.get("browser_session_id") or "").strip()
        session = self.sessions.get(session_id)
        if not session:
            raise BrowserWorkerError("The browser session was not found or has expired.")
        delegation_id = str(args.get("delegation_id") or "").strip()
        if delegation_id and delegation_id != session.delegation_id:
            raise BrowserWorkerError("The delegation does not own this browser session.")
        return session

    def _evaluate(self, session: BrowserSession, expression: str, *, user_gesture: bool = False) -> object:
        result = session.connection.call("Runtime.evaluate", {"expression": expression, "returnByValue": True, "awaitPromise": True, "userGesture": bool(user_gesture)})
        remote = dict(result.get("result") or {})
        if remote.get("subtype") == "error":
            raise BrowserWorkerError(str(remote.get("description") or "Browser evaluation failed."))
        return remote.get("value")

    def _wait_ready(self, session: BrowserSession, timeout: float = 15.0, expected_url: str = "") -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            self._enforce_download_policy(session)
            try:
                state = self._evaluate(session, "document.readyState")
                current_url = str(self._evaluate(session, "location.href") or "")
            except BrowserWorkerError:
                time.sleep(0.1)
                continue
            try:
                current_origin = _origin_for_url(current_url)
            except BrowserWorkerError:
                if current_url in {"", "about:blank"}:
                    time.sleep(0.1)
                    continue
                raise
            if current_origin not in session.allowed_origins:
                raise BrowserWorkerError(f"The browser redirected to an unauthorized origin: {current_origin}")
            if state in {"interactive", "complete"}:
                return
            time.sleep(0.1)
        raise BrowserWorkerError("The browser page did not finish loading.")

    def _inspect(self, session: BrowserSession) -> dict:
        self._enforce_target_policy(session)
        self._enforce_download_policy(session)
        raw = self._evaluate(session, _INSPECT_SCRIPT)
        if not isinstance(raw, dict):
            raise BrowserWorkerError("The browser returned an invalid form description.")
        current_origin = _origin_for_url(raw.get("url"))
        if current_origin not in session.allowed_origins:
            raise BrowserWorkerError(f"The browser navigated to an unauthorized origin: {current_origin}")
        visible_frames = [item for item in raw.get("frames") or [] if isinstance(item, dict)]
        embedded_frames = []
        for item in visible_frames:
            try:
                frame_origin = _origin_for_url(item.get("src"))
            except BrowserWorkerError:
                frame_origin = ""
            signature = f"{int(item.get('index', -1))}|{frame_origin}|{str(item.get('title') or '')}"
            embedded_frames.append({
                "frame_id": "frame_" + hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16],
                "origin": frame_origin,
                "title": str(item.get("title") or "")[:240],
                "same_origin": bool(frame_origin and frame_origin == current_origin),
                "inspected": False,
                "interactive": False,
            })
        safe_fields = []
        field_lookup: Dict[str, dict] = {}
        for item in raw.get("fields") or []:
            kind = str(item.get("kind") or "text").lower()
            signature = str(item.get("signature") or "")
            field_id = "field_" + hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]
            public_item = {
                "field_id": field_id,
                "kind": kind,
                "element_tag": str(item.get("element_tag") or "")[:40],
                "role": str(item.get("role") or "")[:80],
                "input_type": str(item.get("input_type") or "")[:40],
                "semantic_role": str(item.get("semantic_role") or "form")[:40],
                "label": str(item.get("label") or "")[:240],
                "required": bool(item.get("required")),
                "constraints": dict(item.get("constraints") or {}),
                "fillable": kind in SAFE_FIELD_KINDS and kind not in PROHIBITED_FIELD_KINDS,
            }
            safe_fields.append(public_item)
            field_lookup[field_id] = {**public_item, "index": int(item.get("index", -1)), "ref": str(item.get("ref") or "")}
        actions = []
        action_lookup: Dict[str, dict] = {}
        for item in raw.get("submits") or []:
            label = str(item.get("label") or "Submit")[:160]
            signature = f"{int(item.get('index', -1))}|{label}"
            action_id = "submit_" + hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]
            public_item = {"action_id": action_id, "label": label, "effect": "external_write", "disabled": bool(item.get("disabled"))}
            actions.append(public_item)
            action_lookup[action_id] = {**public_item, "index": int(item.get("index", -1)), "ref": str(item.get("ref") or "")}
        navigation_actions = []
        navigation_lookup: Dict[str, dict] = {}
        for item in raw.get("navigation") or []:
            label = str(item.get("label") or "Action")[:240]
            kind = str(item.get("kind") or "button")
            destination_origin = str(item.get("destination_origin") or "")
            signature = f"{int(item.get('index', -1))}|{kind}|{label}|{destination_origin}"
            action_id = "action_" + hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]
            public_item = {
                "action_id": action_id,
                "label": label,
                "kind": kind,
                "element_tag": str(item.get("element_tag") or "")[:40],
                "role": str(item.get("role") or "")[:80],
                "input_type": str(item.get("input_type") or "")[:40],
                "effect": "navigation",
                "destination_origin": destination_origin,
                "same_origin": bool(item.get("same_origin")),
            }
            navigation_actions.append(public_item)
            navigation_lookup[action_id] = {**public_item, "index": int(item.get("index", -1)), "ref": str(item.get("ref") or "")}
        schema_basis = {
            "url": str(raw.get("url") or ""),
            "origin": str(raw.get("origin") or ""),
            "title": str(raw.get("title") or ""),
            "busy": bool(raw.get("busy")),
            "fields": safe_fields,
            "embedded_frames": embedded_frames,
            "navigation_actions": navigation_actions,
            "submit_actions": actions,
        }
        schema = {**schema_basis, "page_revision": _sha256_json(schema_basis), "browser_session_id": session.session_id}
        session.schema = schema
        session.field_lookup = field_lookup
        session.action_lookup = action_lookup
        session.navigation_lookup = navigation_lookup
        return schema

    def _enforce_target_policy(self, session: BrowserSession) -> None:
        targets = session.browser_connection.call("Target.getTargets").get("targetInfos", [])
        current_page_ids = {
            str(item.get("targetId") or "")
            for item in targets
            if item.get("type") == "page"
        }
        if session.auth_popup_target_id and time.time() > session.auth_handoff_until:
            session.allowed_target_ids.discard(session.auth_popup_target_id)
            session.auth_popup_target_id = ""
        if session.auth_popup_target_id and session.auth_popup_target_id not in current_page_ids:
            session.allowed_target_ids.discard(session.auth_popup_target_id)
            session.auth_popup_target_id = ""
        unexpected = [
            str(item.get("targetId") or "")
            for item in targets
            if item.get("type") == "page" and str(item.get("targetId") or "") not in session.allowed_target_ids
        ]
        if not unexpected:
            return
        if time.time() <= session.auth_handoff_until and not session.auth_popup_target_id:
            accepted = unexpected.pop(0)
            session.auth_popup_target_id = accepted
            session.allowed_target_ids.add(accepted)
            if not unexpected:
                return
        for target_id in unexpected:
            try:
                session.browser_connection.call("Target.closeTarget", {"targetId": target_id})
            except BrowserWorkerError:
                pass
        if session.auth_popup_target_id and time.time() <= session.auth_handoff_until:
            raise BrowserWorkerError("An additional popup opened during authentication and was closed.")
        raise BrowserWorkerError("The page attempted to open an unauthorized popup; it was closed.")

    def _enforce_download_policy(self, session: BrowserSession) -> None:
        events = session.browser_connection.drain_events() + session.connection.drain_events()
        if any(str(item.get("method") or "").endswith("downloadWillBegin") for item in events):
            raise BrowserWorkerError("Browser downloads are disabled for this delegation.")

    def browser_inspect_form(self, args: Dict[str, object]) -> Dict[str, object]:
        return {"ok": True, **self._inspect(self._session(args))}

    def browser_begin_auth_handoff(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        timeout_seconds = min(max(float(args.get("timeout_seconds", 300) or 300), 10.0), 300.0)
        session.auth_handoff_until = time.time() + timeout_seconds
        session.auth_popup_target_id = ""
        return {
            "ok": True,
            "state": "waiting_for_sign_in",
            "browser_session_id": session.session_id,
            "expires_at": session.auth_handoff_until,
            "popup_limit": 1,
        }

    def browser_end_auth_handoff(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        popup_target_id = session.auth_popup_target_id
        if popup_target_id:
            try:
                targets = session.browser_connection.call("Target.getTargets").get("targetInfos", [])
                if any(str(item.get("targetId") or "") == popup_target_id for item in targets):
                    session.browser_connection.call("Target.closeTarget", {"targetId": popup_target_id})
            except BrowserWorkerError:
                pass
            session.allowed_target_ids.discard(popup_target_id)
        session.auth_popup_target_id = ""
        session.auth_handoff_until = 0.0
        return {"ok": True, "state": "authentication_handoff_closed", "browser_session_id": session.session_id}

    def _activate_navigation_control(self, session: BrowserSession, action: dict) -> None:
        ref_literal = json.dumps(str(action["ref"]))
        hit_target = self._evaluate(session, f"""
(() => {{
  const roots = [];
  const visitRoot = (root) => {{
    roots.push(root);
    for (const el of root.querySelectorAll('*')) if (el.shadowRoot) visitRoot(el.shadowRoot);
  }};
  visitRoot(document);
  const action = roots.map(root => root.querySelector('[data-sentienta-nav-ref=' + CSS.escape({ref_literal}) + ']')).find(Boolean);
  if (!action) return null;
  action.scrollIntoView({{block: 'center', inline: 'center'}});
  action.focus();
  const rect = action.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) return null;
  return {{x: rect.left + rect.width / 2, y: rect.top + rect.height / 2}};
}})()
""")
        if not isinstance(hit_target, dict):
            raise BrowserWorkerError("The verified navigation control could not be activated.")
        if str(action.get("role") or "").lower() == "button" and str(action.get("element_tag") or "").lower() != "button":
            session.connection.call("Input.dispatchKeyEvent", {
                "type": "rawKeyDown", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13,
            })
            session.connection.call("Input.dispatchKeyEvent", {
                "type": "keyUp", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13,
            })
        else:
            point = {"x": float(hit_target["x"]), "y": float(hit_target["y"])}
            session.connection.call("Input.dispatchMouseEvent", {"type": "mouseMoved", **point})
            session.connection.call("Input.dispatchMouseEvent", {
                "type": "mousePressed", "button": "left", "clickCount": 1, **point,
            })
            session.connection.call("Input.dispatchMouseEvent", {
                "type": "mouseReleased", "button": "left", "clickCount": 1, **point,
            })

    def browser_activate_action(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        before = self._require_revision(session, args)
        action_id = str(args.get("action_id") or "").strip()
        action = session.navigation_lookup.get(action_id)
        if not action:
            raise BrowserWorkerError("The requested navigation action is not available on the current page.")
        destination_origin = str(action.get("destination_origin") or "")
        if destination_origin and destination_origin not in session.allowed_origins:
            raise BrowserWorkerError(f"The navigation action targets an unauthorized origin: {destination_origin}")
        self._activate_navigation_control(session, action)
        self._wait_ready(session, timeout=10)
        deadline = time.time() + 3.0
        after = self._inspect(session)
        while after["page_revision"] == before["page_revision"] and time.time() < deadline:
            time.sleep(0.2)
            after = self._inspect(session)
        return {
            "ok": True,
            "state": "navigation_completed",
            "browser_session_id": session.session_id,
            "action_id": action_id,
            "before_page_revision": before["page_revision"],
            "page_revision": after["page_revision"],
            "url": after["url"],
            "origin": after["origin"],
            "title": after["title"],
        }

    def _require_revision(self, session: BrowserSession, args: Dict[str, object]) -> dict:
        current = self._inspect(session)
        expected = str(args.get("page_revision") or "").strip()
        if not expected or expected != current["page_revision"]:
            raise StalePageRevisionError("The page changed after inspection; inspect it again before continuing.")
        return current

    def browser_fill_form(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        self._require_revision(session, args)
        raw_fields = args.get("fields")
        if not isinstance(raw_fields, list) or not raw_fields:
            raise BrowserWorkerError("At least one validated field value is required.")
        internal_fields = []
        digest_fields = []
        for raw in raw_fields:
            if not isinstance(raw, dict):
                raise BrowserWorkerError("Each field value must be a structured object.")
            field_id = str(raw.get("field_id") or "").strip()
            definition = session.field_lookup.get(field_id)
            if not definition or not definition.get("fillable"):
                raise BrowserWorkerError(f"Field {field_id or '[missing]'} is unknown or prohibited.")
            value = raw.get("value")
            max_length = (definition.get("constraints") or {}).get("max_length")
            if isinstance(max_length, int) and max_length > 0 and len(str(value or "")) > max_length:
                raise BrowserWorkerError(f"The value for {field_id} exceeds its maximum length.")
            internal_fields.append({"ref": definition["ref"], "value": value})
            digest_fields.append({"field_id": field_id, "value": value})
        expression = _FILL_SCRIPT.replace("__PAYLOAD__", json.dumps({"fields": internal_fields}, ensure_ascii=False))
        updated = self._evaluate(session, expression)
        session.prepared_digest = _sha256_json({"origin": session.schema["origin"], "page_revision": session.schema["page_revision"], "fields": digest_fields})
        session.filled_field_ids = [x["field_id"] for x in digest_fields]
        return {"ok": True, "state": "prepared", "browser_session_id": session.session_id, "page_revision": session.schema["page_revision"], "filled_field_ids": [x["field_id"] for x in digest_fields], "prepared_digest": session.prepared_digest, "verified_count": len(updated or [])}

    def browser_attach_file(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        before = self._require_revision(session, args)
        action_id = str(args.get("action_id") or "").strip()
        action = session.navigation_lookup.get(action_id)
        if not action:
            raise BrowserWorkerError("The requested file attachment action is not available on the current page.")
        raw_path = str(args.get("file_path") or "").strip()
        if not raw_path:
            raise BrowserWorkerError("file_path is required for a governed attachment.")
        path = Path(raw_path).expanduser().resolve()
        if not self.attachment_roots or not any(path == root or root in path.parents for root in self.attachment_roots):
            raise BrowserWorkerError("The attachment is outside the Bridge's approved file roots.")
        if not path.is_file():
            raise BrowserWorkerError("The approved attachment file does not exist.")
        if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp", ".gif"}:
            raise BrowserWorkerError("This browser prototype only permits reviewed image attachments.")
        size_bytes = path.stat().st_size
        if size_bytes <= 0 or size_bytes > 20 * 1024 * 1024:
            raise BrowserWorkerError("The image attachment must be between 1 byte and 20 MB.")
        session.connection.call("Page.setInterceptFileChooserDialog", {"enabled": True})
        session.connection.drain_events()
        self._activate_navigation_control(session, action)
        chooser = None
        deadline = time.time() + 10
        while time.time() < deadline and chooser is None:
            for event in session.connection.drain_events():
                if event.get("method") == "Page.fileChooserOpened":
                    chooser = event.get("params") or {}
                    break
            time.sleep(0.1)
        backend_node_id = (chooser or {}).get("backendNodeId")
        if not backend_node_id:
            raise BrowserWorkerError("The verified action did not open a file chooser.")
        session.connection.call("DOM.setFileInputFiles", {
            "files": [str(path)],
            "backendNodeId": backend_node_id,
        })
        digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        time.sleep(0.5)
        after = self._inspect(session)
        return {
            "ok": True,
            "state": "attachment_prepared",
            "browser_session_id": session.session_id,
            "action_id": action_id,
            "file_name": path.name,
            "size_bytes": size_bytes,
            "sha256": digest,
            "source_path": str(path),
            "before_page_revision": before["page_revision"],
            "page_revision": after["page_revision"],
            "url": after["url"],
            "title": after["title"],
        }

    def browser_wait_for_page_change(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        previous_revision = str(args.get("page_revision") or "").strip()
        if not previous_revision:
            raise BrowserWorkerError("page_revision is required while waiting for a page change.")
        timeout_seconds = min(max(float(args.get("timeout_seconds", 120) or 120), 1.0), 300.0)
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if session.cancel_event.is_set():
                raise BrowserWorkerError("The browser delegation was canceled.")
            try:
                current = self._inspect(session)
            except BrowserWorkerError as exc:
                if session.cancel_event.is_set():
                    raise BrowserWorkerError("The browser delegation was canceled.") from exc
                if session.process.poll() is not None:
                    raise BrowserWorkerError("The user closed the dedicated browser while sign-in was pending.") from exc
                raise
            if current["page_revision"] != previous_revision:
                return {"ok": True, "state": "page_changed", **current}
            if session.process.poll() is not None:
                raise BrowserWorkerError("The user closed the dedicated browser while sign-in was pending.")
            time.sleep(0.5)
        raise BrowserWorkerError("Sign-in was not completed before the local browser wait expired.")

    def browser_cancel(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        session.cancel_event.set()
        self.sessions.pop(session.session_id, None)
        self._shutdown_session(session)
        if bool(args.get("delete_profile", False)):
            shutil.rmtree(session.profile_dir, ignore_errors=True)
        return {"ok": True, "state": "canceled", "browser_session_id": session.session_id}

    def browser_screenshot(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        requested = args.get("redact_field_ids")
        redact_ids = session.filled_field_ids if requested is None else [str(item) for item in requested] if isinstance(requested, list) else []
        indexes = []
        for field_id in redact_ids:
            definition = session.field_lookup.get(field_id)
            if not definition:
                raise BrowserWorkerError(f"Unknown screenshot redaction field: {field_id}")
            indexes.append(int(definition["index"]))
        token = "redact_" + uuid.uuid4().hex
        if indexes:
            expression = r"""((payload) => {
              const visible = (el) => { const s=getComputedStyle(el), r=el.getBoundingClientRect(); return s.visibility!=='hidden' && s.display!=='none' && r.width>0 && r.height>0; };
              const controls=Array.from(document.querySelectorAll('input, textarea, select')).filter(el=>{const t=String(el.getAttribute('type')||'').toLowerCase(); return visible(el)&&!el.disabled&&t!=='hidden'&&t!=='submit'&&t!=='button'&&t!=='reset';});
              for(const index of payload.indexes){const el=controls[index]; if(!el) continue; el.dataset.sentientaRedactionToken=payload.token; el.dataset.sentientaOldColor=el.style.color||''; el.dataset.sentientaOldBackground=el.style.backgroundColor||''; el.style.color='transparent'; el.style.backgroundColor='#111';}
              return true;
            })(__PAYLOAD__)""".replace("__PAYLOAD__", json.dumps({"indexes": indexes, "token": token}))
            self._evaluate(session, expression)
        try:
            captured = session.connection.call("Page.captureScreenshot", {"format": "png", "fromSurface": True, "captureBeyondViewport": False})
        finally:
            if indexes:
                restore = r"""((token) => { for(const el of document.querySelectorAll('[data-sentienta-redaction-token="'+token+'"]')){el.style.color=el.dataset.sentientaOldColor||''; el.style.backgroundColor=el.dataset.sentientaOldBackground||''; delete el.dataset.sentientaRedactionToken; delete el.dataset.sentientaOldColor; delete el.dataset.sentientaOldBackground;} return true;})(__TOKEN__)""".replace("__TOKEN__", json.dumps(token))
                self._evaluate(session, restore)
        encoded = str(captured.get("data") or "")
        if not encoded:
            raise BrowserWorkerError("The browser did not return a screenshot.")
        output = self.screenshot_root / f"{session.session_id}-{int(time.time())}.png"
        output.write_bytes(base64.b64decode(encoded))
        return {"ok": True, "browser_session_id": session.session_id, "screenshot_path": str(output), "redacted_field_ids": redact_ids}

    def browser_submit(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        self._require_revision(session, args)
        action_id = str(args.get("action_id") or "").strip()
        action = session.action_lookup.get(action_id)
        if not action or action.get("disabled"):
            raise BrowserWorkerError("The approved submit action is not currently available.")
        approved_digest = str(args.get("approved_digest") or "").strip()
        expected_approval_digest = _sha256_json({
            "delegation_id": session.delegation_id,
            "origin": session.schema.get("origin"),
            "page_revision": session.schema.get("page_revision"),
            "prepared_digest": session.prepared_digest,
            "action_id": action_id,
        })
        if not session.prepared_digest or approved_digest != expected_approval_digest:
            raise BrowserWorkerError("Submission requires an approval bound to the currently prepared values.")
        before_url = str(session.schema.get("url") or "")
        expression = _SUBMIT_SCRIPT.replace("__PAYLOAD__", json.dumps({"ref": action["ref"]}))
        self._evaluate(session, expression)
        time.sleep(0.4)
        self._wait_ready(session, timeout=10)
        after = self._inspect(session)
        session.prepared_digest = ""
        session.filled_field_ids = []
        return {"ok": True, "state": "submitted", "browser_session_id": session.session_id, "action_id": action_id, "before_url": before_url, "after_url": after["url"], "verified": True}

    def browser_close(self, args: Dict[str, object]) -> Dict[str, object]:
        session = self._session(args)
        self.sessions.pop(session.session_id, None)
        self._shutdown_session(session)
        if bool(args.get("delete_profile", False)):
            shutil.rmtree(session.profile_dir, ignore_errors=True)
        return {"ok": True, "state": "closed", "browser_session_id": session.session_id}

    @staticmethod
    def _shutdown_session(session: BrowserSession) -> None:
        if session.process.poll() is None:
            try:
                session.browser_connection.call("Browser.close", timeout=2)
            except BrowserWorkerError:
                pass
        session.connection.close()
        session.browser_connection.close()
        if session.process.poll() is None:
            try:
                session.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                session.process.terminate()
                try:
                    session.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    session.process.kill()
        if session.persistent_profile:
            # Chromium may release its profile singleton a fraction after the
            # controlling process exits.  A Role can legitimately resume the
            # same dedicated profile immediately, so wait briefly for that
            # local teardown instead of misreporting the next launch as dead.
            time.sleep(1.5)

    def close_all(self, *, delete_profiles: bool = False) -> None:
        for session_id in list(self.sessions):
            try:
                self.browser_close({"browser_session_id": session_id, "delete_profile": delete_profiles})
            except Exception:
                pass


def main() -> int:
    """JSON-lines developer entry point; not registered with production Bridge."""
    worker = BrowserWorker(headless=os.getenv("SENTIENTA_BROWSER_HEADLESS") == "1")
    try:
        for line in iter(input, ""):
            try:
                payload = json.loads(line)
                result = worker.dispatch(
                    str(payload.get("tool") or ""),
                    dict(payload.get("args") or {}),
                    request_id=str(payload.get("request_id") or ""),
                )
                print(json.dumps(result, ensure_ascii=False), flush=True)
            except (BrowserWorkerError, ValueError, TypeError) as exc:
                print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), flush=True)
    except EOFError:
        pass
    finally:
        worker.close_all()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
