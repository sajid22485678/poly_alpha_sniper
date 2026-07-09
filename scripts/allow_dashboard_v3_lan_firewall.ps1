# Adds an inbound Windows Firewall rule allowing LAN devices to reach ONLY the
# read-only Dashboard V3 port (TCP 8503) on the PRIVATE profile (home/work
# networks), scoped to the local subnet. Requires an elevated (admin) shell.
#
# It opens ONLY the dashboard port -- never any trading/broker/API port -- and
# never the Public profile. Remove it any time with
# disable_dashboard_v3_lan_firewall.ps1.
$ErrorActionPreference = "Stop"
$port = 8503
$ruleName = "Poly Alpha Sniper Dashboard V3 LAN"

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
    ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Output "This script must be run as Administrator to add a firewall rule."
    Write-Output "Right-click PowerShell -> 'Run as administrator', then run:"
    Write-Output "  powershell -ExecutionPolicy Bypass -File scripts\allow_dashboard_v3_lan_firewall.ps1"
    exit 1
}

$existing = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Output "Firewall rule already present: $ruleName"
    exit 0
}

New-NetFirewallRule -DisplayName $ruleName `
    -Direction Inbound -Action Allow -Protocol TCP -LocalPort $port `
    -Profile Private -RemoteAddress LocalSubnet -Enabled True | Out-Null

Write-Output "Added inbound firewall rule '$ruleName':"
Write-Output "  TCP $port, Private profile only, RemoteAddress = LocalSubnet."
Write-Output "  (No Public-profile exposure; no trading/broker ports opened.)"
