# Removes the LAN inbound firewall rule for Dashboard V3 (TCP 8503).
# Requires an elevated (admin) shell. Touches only this one named rule.
$ErrorActionPreference = "Stop"
$ruleName = "Poly Alpha Sniper Dashboard V3 LAN"

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
    ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Output "This script must be run as Administrator to remove a firewall rule."
    Write-Output "Right-click PowerShell -> 'Run as administrator', then run:"
    Write-Output "  powershell -ExecutionPolicy Bypass -File scripts\disable_dashboard_v3_lan_firewall.ps1"
    exit 1
}

$existing = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
if (-not $existing) {
    Write-Output "No firewall rule named '$ruleName' found -- nothing to remove."
    exit 0
}
Remove-NetFirewallRule -DisplayName $ruleName
Write-Output "Removed firewall rule: $ruleName"
