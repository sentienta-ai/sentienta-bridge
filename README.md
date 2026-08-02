# Sentienta Bridge

The Sentienta Bridge connects Sentienta agents to OpenClaw and approved local resources.

This public release includes two supported modes:

- **Personal Bridge** for resources controlled by one Sentienta user.
- **Enterprise Bridge** for organization-controlled OpenClaw agents and local resources.

Slack and GitHub are native Sentienta Connected Services and do not require a bridge. General MCP and Zapier are not included in this release.

## Files

- `sentienta_bridge.py` - shared bridge core and execution logic
- `sentienta_bridge_v2.py` - recommended personal bridge entry point
- `sentienta_bridge_enterprise.py` - headless enterprise bridge entry point
- `requirements-enterprise.txt` - Enterprise Bridge dependencies
- `start_sentienta_bridge.ps1` and `start_sentienta_bridge.cmd` - Windows launch helpers
- `RELEASE_NOTES.md` - current release changes

## Requirements

- Python 3.10 or later
- A Sentienta account
- OpenClaw installed and available on the bridge host when using OpenClaw
- Network access to Sentienta

Install enterprise dependencies with:

```powershell
python -m pip install -r requirements-enterprise.txt
```

## Personal Bridge

Start the personal bridge on the user's computer:

```powershell
python .\sentienta_bridge_v2.py --service openclaw_exec
```

In Sentienta, open **Account Settings**, create a bridge pairing passcode, and pair the running bridge. Keep the passcode private.

Use the personal bridge for participant-owned OpenClaw agents and approved local resources. When one of these agents participates in a Workroom, execution still runs through that participant's paired bridge.

## Enterprise Bridge

Start the Enterprise Bridge on an organization-controlled host. Use the registration information it displays to register it in Sentienta Enterprise Admin.

```powershell
python .\sentienta_bridge_enterprise.py --service openclaw_exec
```

Enterprise Owners and Admins control registration and resource availability. Users must be active members of the organization to use organization-controlled agents. Workroom membership alone does not grant access.

## Validation

Before relying on a bridge:

1. Confirm the bridge is paired or registered to the intended account or organization.
2. Confirm OpenClaw is reachable.
3. Confirm the expected OpenClaw agents are advertised.
4. Run a read-only request.
5. Verify the result returns to the intended Sentienta conversation.

## Security

- Run bridges only on trusted hosts.
- Protect pairing and registration credentials.
- Give local tools the minimum access needed.
- Keep bridge and OpenClaw software current.
- Re-pair or re-register after host replacement or suspected compromise.
- Review consequential actions before approval.

## Troubleshooting

Use `--verbose` for diagnostic output. If an agent is unavailable, check the Sentienta connection, bridge identity, OpenClaw status, and advertised inventory. Re-pair after account or host changes.
