# Read-only status for the LAN Dashboard V3. Reports whether the port is
# listening, on which address, the owning process, whether the firewall rule
# exists, and the phone URL. Starts/stops nothing; reads no secrets.
$ErrorActionPreference = "SilentlyContinue"
$port = 8503

$listeners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if (-not $listeners) {
    Write-Output ("Dashboard V3 (port {0}): NOT LISTENING" -f $port)
} else {
    Write-Output ("Dashboard V3 (port {0}): LISTENING" -f $port)
    foreach ($l in $listeners) {
        $proc = Get-Process -Id $l.OwningProcess -ErrorAction SilentlyContinue
        $bind = if ($l.LocalAddress -in @('0.0.0.0', '::')) { "$($l.LocalAddress) (LAN-reachable)" } else { "$($l.LocalAddress) (loopback only)" }
        Write-Output ("  bind {0}  pid {1}  proc {2}" -f $bind, $l.OwningProcess, $proc.ProcessName)
    }
}

$rule = Get-NetFirewallRule -DisplayName "Poly Alpha Sniper Dashboard V3 LAN" -ErrorAction SilentlyContinue
if ($rule) {
    $pf = ($rule.Profile -join ',')
    Write-Output ("Firewall rule : PRESENT (enabled={0}, profile={1})" -f $rule.Enabled, $pf)
} else {
    Write-Output "Firewall rule : ABSENT (LAN devices may be blocked until you run allow_dashboard_v3_lan_firewall.ps1 as admin)"
}

Write-Output ""
& "$PSScriptRoot\print_mobile_dashboard_url.ps1"
