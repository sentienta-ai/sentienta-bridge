# Sentienta Bridge for OpenClaw

A minimal local bridge that lets Sentienta connect to a local OpenClaw installation.

The bridge runs on your own machine and acts as a local connector between:
- the Sentienta browser UI
- your local OpenClaw installation

Sentienta does not talk directly to your local OpenClaw runtime over the internet. Instead, the browser pairs with the local bridge, and the bridge executes OpenClaw operations on your machine.

This public build supports:
- OpenClaw agent listing
- OpenClaw health checks
- OpenClaw task execution through the OpenClaw CLI

## Requirements

- Windows
- Python 3.10+
- OpenClaw installed and available as `openclaw` or `openclaw.cmd`

## Files

- `sentienta_bridge.py`: public bridge implementation
- `start_sentienta_bridge.ps1`: PowerShell launcher
- `start_sentienta_bridge.cmd`: Command Prompt launcher

## Quick start

PowerShell:

```powershell
python .\sentienta_bridge.py --service openclaw_exec
```

Or use the launcher:

```powershell
.\start_sentienta_bridge.ps1
```

When the bridge starts, it prints startup lines in the terminal, including a pairing passcode.

Typical startup output includes lines like:

```text
[bridge][pair] passcode=123456 expires_at=...
[bridge] registration endpoint listening on http://127.0.0.1:8765/register-query
```

You will need that passcode in Sentienta when connecting Desktop Automation to the bridge.

## Pairing with Sentienta

1. Start the bridge locally.
2. Keep the terminal window open.
3. Copy or note the pairing passcode shown in the bridge startup output.
4. In Sentienta, open `Desktop Automation`.
5. Enter the passcode and connect.

After successful pairing:
- Sentienta stores a bridge session secret in the browser
- you do not need to keep re-entering the passcode during normal use
- if the bridge is restarted or the pairing expires, pair again with the new passcode

## What the bridge does

The bridge:
- starts a local registration endpoint on `http://127.0.0.1:8765/register-query`
- pairs with Sentienta
- exposes OpenClaw tools to Sentienta
- polls Sentienta for bridge calls and posts back results

## Recommended OpenClaw setup

Before creating managed OpenClaw agents from Sentienta:
- confirm OpenClaw is running
- confirm the OpenClaw `main` agent can answer a simple prompt

If `main` has stale auth state, managed creation will be blocked until `main` is healthy.

## Troubleshooting

If the bridge cannot find OpenClaw:

```powershell
python .\sentienta_bridge.py --service openclaw_exec --openclaw-cli "C:\Path\To\openclaw.cmd"
```

If you want verbose bridge logging:

```powershell
python .\sentienta_bridge.py --service openclaw_exec --verbose
```

## Repository contents

This repository is intentionally limited to the public OpenClaw bridge.
