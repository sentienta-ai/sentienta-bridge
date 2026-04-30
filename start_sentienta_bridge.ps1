$ErrorActionPreference = "Stop"

$ProjectRoot = $PSScriptRoot
$BridgeScript = Join-Path $ProjectRoot "sentienta_bridge_v2.py"
$ListenPort = 8765

$OpenClawCmdPath = Join-Path $env:USERPROFILE "AppData\\Roaming\\npm\\openclaw.cmd"
$OpenClawCli = if (Test-Path $OpenClawCmdPath) { $OpenClawCmdPath } else { "openclaw" }

if (-not (Test-Path $BridgeScript)) {
    throw "Bridge script not found: $BridgeScript"
}

Set-Location $ProjectRoot

python $BridgeScript `
  --service openclaw_exec `
  --openclaw-cli $OpenClawCli `
  --listen-port $ListenPort
