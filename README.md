# Sentienta Bridge for OpenClaw

Sentienta Bridge connects Sentienta to an OpenClaw runtime without exposing the
OpenClaw runtime directly to the internet.

There are two bridge modes in this package:

- **Desktop Bridge v2**: the normal local bridge for an individual Sentienta
  user. This is the recommended path for Teams and participant-owned Workroom
  agents.
- **Enterprise Bridge**: a headless bridge process for organization-run
  Workroom agents. It is started by a Sentienta enterprise owner or admin.

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
- `requirements-enterprise.txt`: optional enterprise worker dependency list

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

## Enterprise Bridge

The Enterprise Bridge is a long-running headless process. It registers itself
with Sentienta, advertises available OpenClaw agents, polls for queued bridge
jobs, executes them locally, and posts results back to Sentienta. The bridge
uses normal Sentienta login credentials for an account that is an active owner
or admin in the enterprise organization. The Cognito `sentienta_admin` group is
for Sentienta platform administration and is not required to operate a customer
enterprise bridge.

Example:

```powershell
$env:SENTIENTA_ADMIN_PASSWORD = "<admin-password>"

python .\sentienta_bridge_enterprise.py `
  --admin-username admin@example.com `
  --bridge-id bridge_acme_01 `
  --service openclaw_exec
```

Add `--verbose` while testing. For normal operation, omit it so the console does
not fill with poll-debug messages.

The bridge stores local configuration and session state under:

```text
C:\Users\<username>\.sentienta-bridge\
```

Important files:

- `enterprise-worker.json`: bridge id and local bridge configuration
- `enterprise-session.json`: cached Cognito session tokens

Do not commit these files. Do not place admin passwords in source code. Prefer
environment variables or the `--admin-password-env` option.

Current enterprise flags:

| Flag | Environment variable | Default | Notes |
| --- | --- | --- | --- |
| `--admin-username` | `SENTIENTA_ADMIN_USERNAME` | empty | Enterprise owner/admin login email. Legacy alias: `--worker-username`; legacy env: `SENTIENTA_WORKER_USERNAME`. |
| `--admin-password` | `SENTIENTA_ADMIN_PASSWORD` | empty | Enterprise owner/admin login password. Legacy alias: `--worker-password`; legacy env: `SENTIENTA_WORKER_PASSWORD`. |
| `--admin-password-env` | n/a | `SENTIENTA_ADMIN_PASSWORD` | Name of the env var to read when `--admin-password` is omitted. Legacy alias: `--worker-password-env`. |
| `--bridge-id` | n/a | generated and persisted | Optional explicit enterprise bridge identifier, for example `bridge_acme_01`. |
| `--service` | n/a | bridge policy / supported services | Repeatable. Current public service is `openclaw_exec`. |
| `--poll-interval-secs` | `SENTIENTA_WORKER_POLL_INTERVAL_SECS` | `1` | Idle delay between job polls. |
| `--heartbeat-interval-secs` | `SENTIENTA_WORKER_HEARTBEAT_INTERVAL_SECS` | `15` | Enterprise bridge heartbeat interval. |
| `--limit` | n/a | `10` | Maximum queued jobs fetched per poll. |
| `--once` | n/a | false | Run one poll cycle and exit. Useful for smoke tests. |
| `--verbose` | n/a | false | Enable detailed bridge logging. |
| `--query-endpoint` | n/a | Sentienta production query API | Override only for testing or non-production environments. |
| `--worker-config-file` | n/a | `%USERPROFILE%\.sentienta-bridge\enterprise-worker.json` | Persisted bridge config path. |
| `--worker-session-file` | n/a | `%USERPROFILE%\.sentienta-bridge\enterprise-session.json` | Cached Cognito session path. |
| `--cognito-region` | `SENTIENTA_COGNITO_REGION` | `us-west-2` | Cognito region for Sentienta login. |
| `--cognito-client-id` | `SENTIENTA_COGNITO_CLIENT_ID` | Sentienta app client id | Cognito app client id for Sentienta login. |
| `--owner-user-id` | `SENTIENTA_WORKER_OWNER_USER_ID` | empty | Legacy override. Usually omit. |
| `--openclaw-cli` | `SENTIENTA_OPENCLAW_CLI` | `openclaw` | OpenClaw executable path or command name. |
| `--openclaw-default-agent` | `SENTIENTA_OPENCLAW_DEFAULT_AGENT` | `main` | Default OpenClaw agent id. |
| `--openclaw-default-timeout-ms` | `SENTIENTA_OPENCLAW_DEFAULT_TIMEOUT_MS` | `210000` | Default OpenClaw task timeout in milliseconds. |
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

- The public bridge exposes only the `openclaw_exec` service.
- The bridge listens on localhost.
- Pairing secrets and enterprise session tokens are local runtime state and
  must not be committed.
- Enterprise Bridge API calls are authorized by the signed-in Sentienta account
  and checked against the organization's owner/admin membership records.
- The root/developer `sentienta_bridge_dev.py` and any `local_fs` service
  launcher are intentionally not part of this public package.
