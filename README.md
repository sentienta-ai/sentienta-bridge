# Sentienta Bridge

Sentienta Bridge connects Sentienta to external automation services without
exposing those services directly to the internet. The bridge can run local
OpenClaw tasks and, when configured, broker approved MCP services such as Slack,
GitHub, and Zapier (beta).

There are two bridge modes in this package:

- **Desktop Bridge v2**: the normal local bridge for an individual Sentienta
  user. This is the recommended path for Teams and participant-owned Workroom
  agents.
- **Enterprise Bridge**: a headless bridge process for organization-run
  Workroom agents and enterprise services. It is started by a Sentienta
  enterprise owner or admin.

## Requirements

Desktop Bridge v2:

- Windows
- Python 3.10+
- OpenClaw installed and available as `openclaw` or `openclaw.cmd`

Enterprise Bridge:

- The Desktop Bridge requirements
- `boto3`
- A Sentienta account that is an active enterprise owner or admin

Install the enterprise dependency with:

```powershell
python -m pip install -r requirements-enterprise.txt
```

## Files

- `sentienta_bridge.py`: shared bridge core and OpenClaw execution logic
- `sentienta_bridge_v2.py`: recommended desktop bridge entrypoint
- `sentienta_bridge_enterprise.py`: headless enterprise bridge entrypoint
- `start_sentienta_bridge.ps1`: PowerShell launcher for Desktop Bridge v2
- `start_sentienta_bridge.cmd`: Command Prompt launcher for Desktop Bridge v2
- `requirements-enterprise.txt`: optional enterprise dependency list
- `RELEASE_NOTES.md`: current bridge, Workrooms, Enterprise Bridge, and brokered service release status

## Desktop Bridge v2 Quick Start

PowerShell:

```powershell
python .\sentienta_bridge_v2.py --service openclaw_exec
```

Or use the launcher:

```powershell
.\start_sentienta_bridge.ps1
```

When the bridge starts, it prints a pairing passcode and writes the same details
to:

```text
C:\Users\<username>\.sentienta-bridge\pairing-code.json
```

In Sentienta, open **Desktop Automation**, enter the passcode, and connect.
Keep the terminal window open while you want Desktop Automation available.

## What Desktop Bridge v2 Does

The desktop bridge:

- starts a local registration endpoint on `http://127.0.0.1:8765/register-query`
- pairs the browser with the local bridge using a short passcode flow
- exposes OpenClaw agent listing, health checks, task execution, status, and
  cancellation
- can expose optional MCP services for the paired user when started with
  `--service mcp` and the required service credentials
- maintains a local OpenClaw task cache for result/status continuity
- stages local media results through short-lived localhost URLs
- keeps OpenClaw execution local to the owner running the bridge

## Workrooms

For Workrooms, ownership still matters:

- participant-owned OpenClaw agents run through that participant's own Desktop
  Bridge v2
- if the participant's bridge is offline, their desktop-backed agents are
  unavailable
- enterprise agents run through a registered Enterprise Bridge instead
  of a participant desktop bridge

For Workrooms that include enterprise agents, access is stricter than ordinary
Workroom participation:

- every visible Workroom user must be an active enterprise member of the
  organization that owns the enterprise agent
- being in the same email domain is not enough by itself
- contractors or outside users can participate if an enterprise owner/admin
  explicitly adds them as active enterprise members
- an active enterprise member can use enterprise resources in allowed Workrooms,
  but cannot administer the organization or run the Enterprise Bridge unless
  promoted to admin

Invites use two visible roles:

- `participant`: a direct invite from the Workroom host
- `guest`: an invite from a non-host participant; the host must admit the guest
  after the guest accepts

## Enterprise Bridge

The Enterprise Bridge is a long-running headless process. It registers itself
with Sentienta, advertises available OpenClaw agents and MCP services, polls for
queued bridge jobs, executes them through the selected service, and posts results
back to Sentienta. The bridge uses normal Sentienta login credentials for an
account that is an active owner or admin in the enterprise organization. General
Sentienta platform administration access is separate from customer enterprise
administration.

Enterprise roles:

- **Owner**: created during organization setup by Sentienta platform
  administration. The owner can open Enterprise Admin, create admins, manage
  members/admins, and run the Enterprise Bridge. The owner should not be edited
  or removed from Enterprise Admin.
- **Admin**: created by an enterprise owner/admin. Admins can manage enterprise
  members, trusted domains, service policy, and run the Enterprise Bridge.
- **Member**: allowed to use enterprise resources, including enterprise agents
  in Workrooms where membership is required. Members cannot administer the
  organization and cannot run the Enterprise Bridge.

Trusted domains help identify expected organization users, but trusted domain
membership alone does not grant bridge/admin authority. Explicit owner/admin
membership is required to run the Enterprise Bridge.

Example:

```powershell
$env:SENTIENTA_ADMIN_PASSWORD = "<admin-password>"

python .\sentienta_bridge_enterprise.py `
  --admin-username admin@example.com `
  --bridge-id bridge_acme_01 `
  --service openclaw_exec `
  --service mcp
```

Add `--verbose` while testing. For normal operation, omit it so the console does
not fill with detailed polling messages.

MCP services are enabled by environment variables or matching command-line
options:

```powershell
$env:SENTIENTA_SLACK_TOKEN = "<slack-bot-token>"
$env:SENTIENTA_GITHUB_TOKEN = "<github-token>"
$env:SENTIENTA_GITHUB_DEFAULT_REPO = "organization/repository"
$env:SENTIENTA_ZAPIER_MCP_SERVER_URL = "<zapier-mcp-server-url>"
```

Zapier is currently marked as beta in Sentienta because available actions and
behavior depend heavily on the connected Zapier MCP server and app configuration.

For the current service status, supported MCP tools, governance scope, and
release notes, see [`RELEASE_NOTES.md`](./RELEASE_NOTES.md).

The bridge stores local configuration and session state under:

```text
C:\Users\<username>\.sentienta-bridge\
```

Important files:

- `enterprise-worker.json`: bridge id and local bridge configuration
- `enterprise-session.json`: local sign-in session state

Do not commit these files. Do not place admin passwords in source code. Prefer
environment variables or the `--admin-password-env` option.

At startup, the Enterprise Bridge validates the provided username and password
and checks that the account is currently an active enterprise owner/admin. A
cached local session is not enough by itself. If credentials are missing or
invalid, the bridge prompts for valid enterprise admin credentials instead of
starting with stale or unauthorized authority.

Current enterprise flags:

| Flag | Environment variable | Default | Notes |
| --- | --- | --- | --- |
| `--admin-username` | `SENTIENTA_ADMIN_USERNAME` | empty | Enterprise owner/admin login email. Legacy alias: `--worker-username`; legacy env: `SENTIENTA_WORKER_USERNAME`. |
| `--admin-password` | `SENTIENTA_ADMIN_PASSWORD` | empty | Enterprise owner/admin login password. Legacy alias: `--worker-password`; legacy env: `SENTIENTA_WORKER_PASSWORD`. |
| `--admin-password-env` | n/a | `SENTIENTA_ADMIN_PASSWORD` | Name of the env var to read when `--admin-password` is omitted. Legacy alias: `--worker-password-env`. |
| `--bridge-id` | n/a | generated and persisted | Optional explicit enterprise bridge identifier, for example `bridge_acme_01`. |
| `--service` | n/a | bridge policy / supported services | Repeatable. Use `openclaw_exec` for OpenClaw and `mcp` for MCP services. |
| `--poll-interval-secs` | `SENTIENTA_WORKER_POLL_INTERVAL_SECS` | `1` | Idle delay between job polls. |
| `--heartbeat-interval-secs` | `SENTIENTA_WORKER_HEARTBEAT_INTERVAL_SECS` | `15` | Enterprise bridge heartbeat interval. |
| `--limit` | n/a | `10` | Maximum queued jobs fetched per poll. |
| `--once` | n/a | false | Run one poll cycle and exit. Useful for smoke tests. |
| `--verbose` | n/a | false | Enable detailed bridge logging. |
| `--query-endpoint` | n/a | Sentienta production query API | Override only for testing or non-production environments. |
| `--worker-config-file` | n/a | `%USERPROFILE%\.sentienta-bridge\enterprise-worker.json` | Persisted bridge config path. |
| `--worker-session-file` | n/a | `%USERPROFILE%\.sentienta-bridge\enterprise-session.json` | Local sign-in session path. |
| `--owner-user-id` | `SENTIENTA_WORKER_OWNER_USER_ID` | empty | Legacy override. Usually omit. |
| `--openclaw-cli` | `SENTIENTA_OPENCLAW_CLI` | `openclaw` | OpenClaw executable path or command name. |
| `--openclaw-default-agent` | `SENTIENTA_OPENCLAW_DEFAULT_AGENT` | `main` | Default OpenClaw agent id. |
| `--openclaw-default-timeout-ms` | `SENTIENTA_OPENCLAW_DEFAULT_TIMEOUT_MS` | `210000` | Default OpenClaw task timeout in milliseconds. |
| `--slack-token` | `SENTIENTA_SLACK_TOKEN` | empty | Slack bot token for approved Slack MCP actions. |
| `--github-token` | `SENTIENTA_GITHUB_TOKEN` | empty | GitHub token for approved GitHub MCP actions. |
| `--github-default-repo` | `SENTIENTA_GITHUB_DEFAULT_REPO` | empty | Default GitHub repository for repository and issue actions. |
| `--zapier-mcp-server-url` | `SENTIENTA_ZAPIER_MCP_SERVER_URL` | empty | Zapier (beta) MCP server URL. Treat as a secret. |
| `--zapier-mcp-token` | `SENTIENTA_ZAPIER_MCP_TOKEN` | empty | Optional Zapier (beta) bearer token. |
| `--max-chars-default` | n/a | `40000` | Default response character limit for tool results. |
| `--max-chars-hard` | n/a | `500000` | Hard response character limit for tool results. |
| `--max-find-results-default` | n/a | `20` | Default result limit for find/search-style operations. |
| `--max-find-results-hard` | n/a | `200` | Hard result limit for find/search-style operations. |

Useful testing command:

```powershell
python .\sentienta_bridge_enterprise.py `
  --admin-username admin@example.com `
  --bridge-id bridge_acme_01 `
  --service openclaw_exec `
  --service mcp `
  --poll-interval-secs 5 `
  --heartbeat-interval-secs 15 `
  --verbose
```

Legacy `--worker-username`, `--worker-password`,
`--worker-password-env`, `SENTIENTA_WORKER_USERNAME`, and
`SENTIENTA_WORKER_PASSWORD` values are still accepted as compatibility aliases.
New installs should use the `admin` flag and environment variable names.

## Troubleshooting

If the bridge cannot find OpenClaw:

```powershell
python .\sentienta_bridge_v2.py --service openclaw_exec --openclaw-cli "C:\Path\To\openclaw.cmd"
```

If you want verbose desktop bridge logging:

```powershell
python .\sentienta_bridge_v2.py --service openclaw_exec --verbose
```

Before creating managed OpenClaw agents from Sentienta, confirm that OpenClaw is
running and that the OpenClaw `main` agent can answer a simple prompt. If `main`
has stale auth state, managed agent creation will be blocked until `main` is
healthy.

## Security Notes

- The bridge exposes only the services selected at startup and allowed by
  Sentienta policy.
- The bridge listens on localhost.
- Pairing secrets and enterprise session tokens are local runtime state and
  must not be committed.
- Enterprise Bridge API calls are authorized by the signed-in Sentienta account
  and checked against the organization's active owner/admin membership records.
- Enterprise agent use in Workrooms is checked against active enterprise member
  records for the organization that owns the enterprise agent.
- MCP write actions can require explicit approval before execution. Brokered
  activity is recorded for enterprise review.
- The root/developer `sentienta_bridge_dev.py` and any `local_fs` service
  launcher are intentionally not part of this public package.
