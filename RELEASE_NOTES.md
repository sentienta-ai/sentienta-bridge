# Sentienta Bridge Release Notes

## August 3, 2026 - Documentation Correction

- Added complete clone/download, installation, pairing, registration, validation, security, and troubleshooting instructions.
- Corrected the Enterprise Bridge example to require an active Sentienta Owner/Admin identity and a password supplied through an environment variable.
- Clarified that the personal bridge generates the six-digit pairing passcode.
- Clarified that Slack and GitHub are native Connected Services and that general MCP services and Zapier are not part of the public release.
- Improved the local approval page with user-facing service names and clearer Bridge wording.

## August 2, 2026 - Public Workrooms Release

This release prepares the personal and Enterprise Bridges for the Sentienta Workrooms public release.

### Included

- Current shared bridge core in `sentienta_bridge.py`
- Personal bridge entry point in `sentienta_bridge_v2.py`
- Enterprise bridge entry point in `sentienta_bridge_enterprise.py`
- Personal execution for participant-owned OpenClaw agents and approved local resources
- Enterprise execution for organization-controlled OpenClaw agents and approved local resources
- Workroom routing for personal and enterprise agents
- Enterprise polling backoff using the server-provided retry interval
- A 60-second default Enterprise Bridge heartbeat interval

### Connected Services

Slack and GitHub are supported by native Sentienta Connected Services. They do not require bridge credentials or bridge execution.

### Not Included

General MCP and Zapier are not part of this public release. Older experimental bridge options for those services are not a supported public interface.

### Upgrade and Validation

Replace all three Python files together, restart the bridge, then verify:

1. The bridge pairs or registers to the intended identity.
2. OpenClaw and the expected agents are online.
3. A read-only request completes.
4. The result returns to the correct personal conversation or Workroom.
5. Enterprise membership rules prevent unauthorized use of organization-controlled agents.
