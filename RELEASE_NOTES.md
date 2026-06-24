# Bridge, Workrooms, and Enterprise Services Release Notes

This note tracks the release shape for Desktop Bridge v2, Enterprise Bridge, Workrooms, and brokered services.


## June 24, 2026 Reliability Update

- Enterprise Bridge now runs claimed jobs on background worker threads so heartbeat and polling continue while long OpenClaw tasks are running.
- OpenClaw agent provisioning now adopts an already-existing matching OpenClaw agent instead of treating that condition as a fatal create failure.
- This update fixes cases where a bridge appeared offline during a long task or where an existing OpenClaw agent prevented a workroom agent from running.


## June 24, 2026 GitHub Read Tools Update

- GitHub MCP now includes read-only commit listing for release verification and recent-change checks.
- GitHub MCP now includes read-only repository file access for files such as `README.md` and `RELEASE_NOTES.md`.

## Public Bridge Package

Publish these files to the public `sentienta-bridge` GitHub repository:

- `sentienta_bridge.py`: shared desktop bridge core
- `sentienta_bridge_v2.py`: recommended personal desktop bridge entrypoint
- `sentienta_bridge_enterprise.py`: headless enterprise bridge entrypoint
- `start_sentienta_bridge.ps1`: desktop bridge v2 launcher
- `start_sentienta_bridge.cmd`: command prompt launcher
- `requirements-enterprise.txt`: optional enterprise dependency list
- `README.md`: bridge mode, setup, security, troubleshooting docs

Do not publish `sentienta_bridge_dev.py` or any launcher that enables `local_fs`.

## User-Facing Docs

Public docs should expose:

- Desktop Automation for all users
- Workrooms
- Enterprise Bridge
- MCP Services

If a staged release needs to hide a document before launch, client-side visibility gating can keep it out of the Docs UI, but it is not a security boundary for static files.

## Workrooms

Workrooms are shared collaboration spaces where multiple Sentienta users can participate in one transcript and bring selected agents into that shared context.

Expected behavior:

- Workrooms navigation and docs are available to eligible users.
- Workrooms are designed for desktop layouts.
- Home can render a selected Workroom transcript as a compact presentation layer.
- Command Center remains the management surface for participants, agents, tasks, artifacts, and activity.

## Enterprise Bridge

The enterprise bridge is a headless bridge for organization-controlled Workroom agents. It is started by a Sentienta
enterprise owner or admin using normal Sentienta login credentials. Enterprise owner/admin authority comes from Sentienta
enterprise organization membership, not from general platform administration access.

Current startup model:

- `--admin-username`
- `SENTIENTA_ADMIN_PASSWORD`
- optional stable `--bridge-id`
- `--service openclaw_exec`
- optional `--service mcp`
- optional `SENTIENTA_SLACK_TOKEN` / `--slack-token` for Slack MCP tools
- recommended idle polling: `--poll-interval-secs 5`
- `boto3`
- local state under `~/.sentienta-bridge`

Legacy `--worker-*` flags and `SENTIENTA_WORKER_PASSWORD` remain compatibility aliases. `--owner-user-id` is a legacy
override and should normally be omitted. Runtime state files and secrets must not be committed.

Enterprise service access is governed by the Enterprise Admin Services policy. The agent editor uses that policy for
selection visibility, and the Workroom runtime enforces it again before queuing bridge work.

## June 2026 MCP / Broker Status

The bridge treats automation as a brokered service model rather than an OpenClaw-only desktop path.

Current service model:

- `Cloud`: normal server-side agent execution.
- Desktop Broker Service: user-paired local bridge services.
- Enterprise Broker Service: organization-controlled bridge services.
- Services are selected separately from runtime, currently including OpenClaw, Slack, GitHub, and Zapier (beta).

Current MCP/Slack status:

- MCP registry/status/tool listing works.
- Slack channel history read works when the Slack bot token has the required scopes and the bot is in the channel.
- Slack channel post works through an approval-required flow.
- Slack approval execution posts to the channel and records governance activity.
- Slack thread replies remain disabled.

Current MCP/GitHub status:

- GitHub repository status/details are supported.
- GitHub issue listing is supported.
- GitHub issue creation is approval-gated.
- GitHub PR creation, merges, repository content writes, and destructive repo actions remain disabled.

Current MCP/Zapier (beta) status:

- Zapier (beta) MCP server connection is configured through the bridge, using a sensitive Zapier (beta) MCP server URL and optional bearer token.
- Zapier (beta) tool/action listing is supported through the remote MCP server.
- Zapier (beta) action execution is approval-gated through `mcp.zapier.actions.run` and `mcp.zapier.actions.execute_write`.
- Zapier (beta) action execution depends on the user/admin configuring tools in the Zapier (beta) server first; Sentienta should not invent Zapier (beta) tool names.

Governance status:

- Enterprise Admin includes service policy controls.
- Enterprise Admin includes an Activity tab for governed action records.
- Activity export is intentionally generic so it can later include MCP, OpenClaw, server-side tools, and other brokered actions.
- Runtime policy is enforced before bridge queueing, not only in the agent editor UI.

OpenClaw compatibility status:

- OpenClaw remains a separate service path.
- Native OpenClaw status, agent listing, task listing, and result retrieval have been regression-tested after MCP changes.
- OpenClaw task completion and halt/cancel states should clear visible Workroom status chips.
- OpenClaw terminal formatting noise is stripped before Workroom display.

Multi-service routing:

- Agents with more than one enabled service use a fast service-intent router to decide whether a request is a normal answer, OpenClaw action, MCP service action, clarification, or unsupported request.
- The router receives compact recent Workroom transcript context so requests like "post that picture" or "compare those two answers and send the conclusion" can map to the right service call.
- The router proposes intent and arguments only; the bridge remains responsible for service policy, approval, credentials, execution, and activity records.
