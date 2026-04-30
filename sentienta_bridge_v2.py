#!/usr/bin/env python3
"""
Sentienta desktop bridge v2.

This entrypoint keeps the legacy bridge available while adopting a safer local
inventory model inspired by the enterprise worker.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import html
import json
import secrets
import threading
import time

import sentienta_bridge as bridge


LOCAL_V2_DEBUG_VERSION = "2026-04-17-v1"
LOCAL_V2_AGENT_CACHE_TTL_SEC = 75


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
    *,
    refreshed_at: Optional[float] = None,
    source: str = "local_v2_inventory",
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
        runtime["known_agent_ids_ttl_sec"] = LOCAL_V2_AGENT_CACHE_TTL_SEC
        runtime["known_agent_ids_source"] = source
    except Exception:
        return


def _ensure_cached_openclaw_agent(pairing_state: Dict[str, object], agent_id: str) -> None:
    try:
        aid = str(agent_id or "").strip()
        if not aid:
            return
        runtime = bridge._ensure_openclaw_runtime(pairing_state)
        current = runtime.get("known_agent_ids")
        ids = set(current) if isinstance(current, set) else set()
        if aid in ids:
            return
        ids.add(aid)
        runtime["known_agent_ids"] = ids
        runtime["known_agent_ids_ts"] = int(time.time())
        runtime["known_agent_ids_ttl_sec"] = LOCAL_V2_AGENT_CACHE_TTL_SEC
        runtime["known_agent_ids_source"] = "local_v2_result"
    except Exception:
        return


def _refresh_local_openclaw_inventory(
    pairing_state: Dict[str, object],
    *,
    force: bool = False,
    verbose: bool = False,
) -> List[Dict[str, object]]:
    last_refresh = float(pairing_state.get("v2_inventory_refresh_ts", 0.0) or 0.0)
    cached = pairing_state.get("v2_cached_openclaw_agents")
    if (
        not force
        and isinstance(cached, list)
        and cached
        and (time.time() - last_refresh) < float(LOCAL_V2_AGENT_CACHE_TTL_SEC)
    ):
        _sync_runtime_agent_inventory(
            pairing_state,
            cached,
            refreshed_at=time.time(),
            source="local_v2_cache",
        )
        if verbose:
            bridge.log(
                f"[bridge-v2][inventory] cache-hit age_sec={int(time.time() - last_refresh)} count={len(cached)}",
                True,
            )
        return list(cached)

    try:
        if verbose:
            bridge.log(
                f"[bridge-v2][inventory] refresh-start force={str(bool(force)).lower()}",
                True,
            )
        result = bridge.execute_openclaw_agents_list(
            bridge.BridgeCall(
                msg_id=f"bridge_v2_inventory_{int(time.time() * 1000)}",
                bridge_id="desktop_openclaw",
                tool="openclaw.agents.list",
                args={},
                raw={},
            ),
            pairing_state,
        )
        agents = result.get("agents") if isinstance(result, dict) else []
        agents = agents if isinstance(agents, list) else []
        refreshed_at = time.time()
        pairing_state["v2_cached_openclaw_agents"] = agents
        pairing_state["v2_inventory_refresh_ts"] = refreshed_at
        _sync_runtime_agent_inventory(
            pairing_state,
            agents,
            refreshed_at=refreshed_at,
            source="local_v2_refresh",
        )
        if verbose:
            sample = [
                {
                    "agent_id": str(item.get("agent_id") or "").strip(),
                    "name": str(item.get("name") or "").strip(),
                }
                for item in agents[:8]
                if isinstance(item, dict)
            ]
            bridge.log(
                f"[bridge-v2][inventory] refresh-done count={len(agents)} sample={json.dumps(sample, ensure_ascii=False)}",
                True,
            )
        return list(agents)
    except Exception as e:
        if verbose:
            bridge.log(f"[bridge-v2][inventory] refresh failed err={str(e)}", True)
        return list(cached or []) if isinstance(cached, list) else []


def main() -> int:
    args = bridge.parse_args()
    roots: List[Path] = []
    selected_services = bridge.resolve_selected_services(args.service, args.bridge_id)
    accepted_bridge_ids = bridge.resolve_accepted_bridge_ids(args.bridge_id, selected_services)
    if args.service:
        requested = []
        for s in args.service:
            ss = str(s or "").strip()
            if ss and ss not in requested:
                requested.append(ss)
        dropped = [s for s in requested if s not in selected_services]
        if dropped:
            print(
                f"[bridge-v2] requested services ignored by bridge_id policy ({args.bridge_id}): {', '.join(dropped)}",
                flush=True,
            )

    headers, resolved_mode = bridge.auth_headers(
        auth_mode=args.auth_mode,
        id_token=args.id_token,
        owner_key=args.owner_key,
        owner_key_header=args.owner_key_header,
    )
    bridge.print_auth_diagnostics(headers, resolved_mode)
    if not args.id_token:
        print("[bridge-v2][auth] waiting for idToken via /register-query from UI client", flush=True)
    if args.team_name and args.query_id and bridge.has_auth_credential(headers):
        bridge.run_auth_probe(
            retrieve_endpoint=args.retrieve_endpoint,
            headers=headers,
            team_name=args.team_name,
            query_id=args.query_id,
        )
    else:
        print("[bridge-v2][auth] probe skipped (missing seed query or credential)", flush=True)

    active_queries: Dict[Tuple[str, str], bridge.ActiveQuery] = {}
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
        "v2_cached_openclaw_agents": [],
        "v2_inventory_refresh_ts": 0.0,
        "v2_debug_version": LOCAL_V2_DEBUG_VERSION,
    }

    pairing_file = bridge.persist_pairing_code_file(bridge_id=args.bridge_id, pairing_state=pairing_state)
    if pairing_file is not None:
        print(f"[bridge-v2][pair] pairing code file: {pairing_file}", flush=True)
    if args.team_name and args.query_id:
        seed = bridge.ActiveQuery(
            team_name=args.team_name,
            query_id=args.query_id,
            user_id=str(args.user_id or "").strip(),
            dialog_index=-1,
            last_seen_ts=time.time(),
            auth_headers=headers if bridge.has_auth_credential(headers) else None,
        )
        active_queries[(seed.team_name, seed.query_id)] = seed

    server = bridge.start_registration_server(
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
    print(f"[bridge-v2] registration endpoint listening on http://127.0.0.1:{args.listen_port}/register-query", flush=True)

    seen_call_ids: set[tuple[str, str, str]] = set()

    bridge.log(f"[bridge-v2] started bridge_id={args.bridge_id}", verbose=True)
    bridge.log("[bridge-v2] accepted_bridge_ids=" + ", ".join(accepted_bridge_ids), verbose=True)
    bridge.log("[bridge-v2] selected_services=" + ", ".join(selected_services), verbose=True)
    bridge._print_pairing_banner(
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
                poll_headers = q.auth_headers if bridge.has_auth_credential(q.auth_headers) else headers
                if not bridge.has_auth_credential(poll_headers):
                    bridge.record_bridge_debug_event(
                        pairing_state,
                        "poll_skipped_no_auth",
                        team_name=q.team_name,
                        query_id=q.query_id,
                        user_id=q.user_id,
                    )
                    continue
                bridge_calls: List[bridge.BridgeCall] = []
                try:
                    outbox_messages: List[object] = []
                    for poll_bridge_id in accepted_bridge_ids:
                        debug_meta: Dict[str, object] = {}
                        backend_debug_version = ""
                        backend_cfg_version = ""
                        outbox_resp = bridge.poll_bridge_messages(
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
                            parsed = bridge.try_load_json(raw_body)
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
                        bridge.record_bridge_debug_event(
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
                                "localBridgeDebugVersion": LOCAL_V2_DEBUG_VERSION,
                            },
                        )
                        if isinstance(current_messages, list) and current_messages:
                            outbox_messages.extend(current_messages)
                    if args.verbose:
                        bridge.log(
                            f"[bridge-v2] fetch team={q.team_name} query={q.query_id} "
                            f"accepted_bridge_ids={sorted(list(accepted_bridge_ids))} "
                            f"fetched_count={len(outbox_messages)}",
                            verbose=True,
                        )
                    if args.verbose:
                        preview = str(outbox_messages)
                        if len(preview) > 320:
                            preview = preview[:317] + "..."
                        bridge.log(f"[bridge-v2] msgs team={q.team_name} query={q.query_id}: {preview}", verbose=True)
                    bridge_calls.extend(bridge.extract_bridge_calls_from_messages(outbox_messages))

                    resp = bridge.poll_dialog(
                        retrieve_endpoint=args.retrieve_endpoint,
                        headers=poll_headers,
                        team_name=q.team_name,
                        query_id=q.query_id,
                        dialog_index=q.dialog_index,
                    )
                    q.poll_errors = 0
                except bridge.BridgeError as e:
                    q.poll_errors += 1
                    bridge.record_bridge_debug_event(
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
                    bridge.log(f"[bridge-v2] poll error team={q.team_name} query={q.query_id}: {e}", verbose=True)
                    if q.poll_errors >= 5:
                        with active_lock:
                            active_queries.pop((q.team_name, q.query_id), None)
                        bridge.record_bridge_debug_event(
                            pairing_state,
                            "query_dropped_after_poll_errors",
                            team_name=q.team_name,
                            query_id=q.query_id,
                            user_id=q.user_id,
                            payload={"pollErrors": q.poll_errors},
                        )
                        bridge.log(f"[bridge-v2] dropped query after repeated poll errors: team={q.team_name} query={q.query_id}", verbose=True)
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

                status_only = body_norm.startswith("<status>") and body_norm.endswith("</status>")
                should_log_body = bool(args.verbose and body)
                if should_log_body and status_only and same_as_last:
                    should_log_body = (q.unchanged_polls % 20 == 0)
                if should_log_body:
                    preview = body.replace("\n", " ")
                    if len(preview) > 180:
                        preview = preview[:177] + "..."
                    suffix = f" (repeat x{q.unchanged_polls + 1})" if same_as_last and status_only else ""
                    bridge.log(f"[bridge-v2] body team={q.team_name} query={q.query_id}: {preview}{suffix}", verbose=True)

                if status_only and same_as_last and q.unchanged_polls >= 10:
                    loop_sleep = max(loop_sleep, 0.35)

                calls = list(bridge_calls)
                calls.extend(bridge.extract_bridge_calls(body))
                bridge_type_marker = '"type"' in body and ("bridge_call" in body or "&quot;bridge_call&quot;" in body)
                if args.verbose and bridge_type_marker and not calls:
                    preview = html.unescape(body).replace("\n", " ")
                    if len(preview) > 320:
                        preview = preview[:317] + "..."
                    bridge.log(f"[bridge-v2] found bridge_call marker but parsed 0 calls. body_preview={preview}", verbose=True)

                for call in calls:
                    if not bridge.is_actionable_bridge_call(call, accepted_bridge_ids):
                        if args.verbose:
                            ctype = str((call.raw or {}).get("type", "")) if isinstance(call.raw, dict) else ""
                            bridge.log(
                                f"[bridge-v2] ignoring non-actionable call msg_id={call.msg_id or '<none>'} type={ctype or '<none>'} bridge_id={call.bridge_id or '<none>'}",
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
                    bridge.record_bridge_debug_event(
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
                    bridge.log(
                        f"[bridge-v2] handling msg_id={call.msg_id} tool={call.tool} "
                        f"bridge_id={call.bridge_id} team={q.team_name} query={q.query_id} "
                        f"queue_wait_s={queue_wait_s} claim_delay_s={claim_delay_s} post_claim_delay_s={post_claim_delay_s}",
                        verbose=True,
                    )
                    response_bridge_id = str(call.bridge_id or "").strip() or str(args.bridge_id)

                    try:
                        if not str(pairing_state.get("bridge_secret", "") or "").strip():
                            raise bridge.BridgeError("Services are not currently enabled for this request. Please enable desktop automation.")
                        if call.tool == "openclaw.run_task" and isinstance(call.args, dict):
                            requested_agent_id = str(
                                call.args.get("agent_id")
                                or call.args.get("agent")
                                or ""
                            ).strip()
                            if requested_agent_id:
                                _ensure_cached_openclaw_agent(pairing_state, requested_agent_id)
                        result = bridge.execute_call(
                            call,
                            roots,
                            pairing_state,
                            selected_services,
                            args.max_chars_default,
                            args.max_chars_hard,
                            args.max_find_results_default,
                            args.max_find_results_hard,
                        )
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
                            result_status = str(result.get("status", "") or "") if isinstance(result, dict) else ""
                            bridge.log(
                                f"[bridge-v2] result msg_id={call.msg_id} ok=True status={result_status} "
                                f"result_keys={list(result.keys()) if isinstance(result, dict) else []}",
                                verbose=True,
                            )
                        result_obj = bridge.build_result(call, response_bridge_id, ok=True, result=result)
                        if call.tool == "openclaw.get_status" and isinstance(result, dict):
                            status_val = str(result.get("status", "") or "").strip().lower()
                            if status_val in {"completed", "failed", "canceled"}:
                                q.openclaw_terminal_ts = time.time()
                                q.openclaw_terminal_task_id = str(result.get("task_id", "") or "").strip()
                                if args.verbose:
                                    bridge.log(
                                        f"[bridge-v2][oc] terminal status captured team={q.team_name} query={q.query_id} "
                                        f"task_id={q.openclaw_terminal_task_id or '<unknown>'} status={status_val}",
                                        verbose=True,
                                    )
                        elif call.tool == "openclaw.run_task":
                            q.openclaw_terminal_ts = 0.0
                            q.openclaw_terminal_task_id = ""
                            q.current_openclaw_task_id = str(result.get("task_id", "") or "").strip()
                            runtime = bridge._ensure_openclaw_runtime(pairing_state)
                            tasks = runtime["tasks"]
                            task_ref = tasks.get(q.current_openclaw_task_id)
                            if isinstance(task_ref, dict):
                                task_ref["team_name"] = q.team_name
                                task_ref["query_id"] = q.query_id
                            result_agent_id = str(
                                result.get("agent_id")
                                or result.get("agent")
                                or (call.args.get("agent_id") if isinstance(call.args, dict) else "")
                                or (call.args.get("agent") if isinstance(call.args, dict) else "")
                                or ""
                            ).strip()
                            _ensure_cached_openclaw_agent(pairing_state, result_agent_id)
                        result_status = str(result.get("status", "") or "").strip() if isinstance(result, dict) else ""
                        result_launch_timing = (
                            dict(result.get("launch_timing") or {})
                            if isinstance(result, dict) and isinstance(result.get("launch_timing"), dict)
                            else {}
                        )
                        bridge.record_bridge_debug_event(
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
                    except bridge.BridgeError as e:
                        bridge.record_bridge_debug_event(
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
                            bridge.log(
                                f"[bridge-v2] result msg_id={call.msg_id} ok=False error={str(e)}",
                                verbose=True,
                            )
                        result_obj = bridge.build_result(call, response_bridge_id, ok=False, error=str(e))

                    try:
                        result_source = bridge._bridge_call_source_name(call.raw)
                        post_resp = bridge.post_bridge_result(
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
                            bridge.log(
                                f"[bridge-v2] post msg_id={call.msg_id} queued={post_resp.get('queued')} "
                                f"total={post_resp.get('total')} errors={post_resp.get('errors')}",
                                verbose=True,
                            )
                        bridge.record_bridge_debug_event(
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
                    except bridge.BridgeError as e:
                        bridge.record_bridge_debug_event(
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
                        bridge.log(f"[bridge-v2] failed posting bridge_result for {call.msg_id}: {e}", verbose=True)

                if "EOD" in body:
                    with active_lock:
                        active_queries.pop((q.team_name, q.query_id), None)
                    bridge.log(f"[bridge-v2] EOD reached, retired query team={q.team_name} query={q.query_id}", verbose=True)
                    if args.exit_on_eod:
                        with active_lock:
                            if not active_queries:
                                bridge.log("[bridge-v2] no active queries remain, exiting.", verbose=True)
                                return 0

            time.sleep(loop_sleep)
    finally:
        server.shutdown()
        server.server_close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
