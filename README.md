# Sentienta Bridge for OpenClaw

Sentienta Bridge connects Sentienta to an OpenClaw runtime without exposing the
OpenClaw runtime directly to the internet.

There are two bridge modes in this package:

- **Desktop Bridge v2**: the normal local bridge for an individual Sentienta
  user. This is the recommended path for Teams and participant-owned Workroom
  agents.
- **Enterprise Bridge Worker**: a headless bridge process for organization-run
  Workroom agents. This is intended for operators and beta/enterprise users.

## Requirements

Desktop Bridge v2:

- Windows
- Python 3.10+
- OpenClaw installed and available as `openclaw` or `openclaw.cmd`

Enterprise Bridge Worker:

- The Desktop Bridge requirements
- `boto3`
- A Sentienta worker account provisioned for enterprise bridge use

Install the enterprise dependency with:

```powershell
python -m pip install -r requirements-enterprise.txt
```

## Files

- `sentienta_bridge.py`: shared bridge core and OpenClaw execution logic
- `sentienta_bridge_v2.py`: recommended desktop bridge entrypoint
- `sentienta_bridge_enterprise.py`: headless enterprise worker entrypoint
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
- enterprise agents run through a registered Enterprise Bridge Worker instead
  of a participant desktop bridge

## Enterprise Bridge Worker

The enterprise worker is a long-running headless process. It registers itself
with Sentienta, advertises available OpenClaw agents, polls for queued bridge
jobs, executes them locally, and posts results back to Sentienta.

Example:

```powershell
$env:SENTIENTA_WORKER_USERNAME = "worker@example.com"
$env:SENTIENTA_WORKER_PASSWORD = "<worker-password>"
$env:SENTIENTA_WORKER_OWNER_USER_ID = "owner-user-id"

python .\sentienta_bridge_enterprise.py --service openclaw_exec --verbose
```

The worker stores local configuration and session state under:

```text
C:\Users\<username>\.sentienta-bridge\
```

Important files:

- `enterprise-worker.json`: bridge id, worker username, owner user id
- `enterprise-session.json`: cached Cognito session tokens

Do not commit these files. Do not place worker passwords in source code. Prefer
environment variables or the `--worker-password-env` option.

Useful enterprise options:

```powershell
python .\sentienta_bridge_enterprise.py `
  --worker-username worker@example.com `
  --owner-user-id owner-user-id `
  --service openclaw_exec `
  --poll-interval-secs 1 `
  --heartbeat-interval-secs 15 `
  --verbose
```

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
- The root/developer `sentienta_bridge_dev.py` and any `local_fs` service
  launcher are intentionally not part of this public package.
