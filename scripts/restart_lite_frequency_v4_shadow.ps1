[CmdletBinding()]
param()
$ErrorActionPreference="Stop"
& (Join-Path $PSScriptRoot "stop_lite_frequency_v4_shadow.ps1")
if($LASTEXITCODE -ne 0){exit $LASTEXITCODE}
& (Join-Path $PSScriptRoot "start_lite_frequency_v4_shadow.ps1")
exit $LASTEXITCODE
