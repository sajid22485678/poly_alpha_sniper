# Stops ONLY the Dashboard V3 Next.js server (the process owning the port-8503
# listener), plus any node child processes whose command line references this
# repo's dashboard_v3. Never touches the trading bot, unrelated node processes,
# Python, or secrets.
$ErrorActionPreference = "SilentlyContinue"
$port = 8503

$stopped = @()

# 1) the process owning the port-8503 listener, if it is node/next.
$listeners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
foreach ($l in $listeners) {
    $p = Get-CimInstance Win32_Process -Filter "ProcessId=$($l.OwningProcess)" -ErrorAction SilentlyContinue
    if ($p -and $p.Name -in @('node.exe', 'node')) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        $stopped += $p.ProcessId
    } elseif ($p) {
        Write-Output ("Refusing to stop pid {0} ({1}) on port {2} -- not a node process." -f $p.ProcessId, $p.Name, $port)
    }
}

# 2) any node processes for THIS repo's dashboard_v3 (next dev/start), by cmdline.
Get-CimInstance Win32_Process -Filter "Name='node.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like '*poly_alpha_sniper*dashboard_v3*' -or ($_.CommandLine -like '*dashboard_v3*' -and $_.CommandLine -like '*next*') } |
    ForEach-Object {
        if ($_.ProcessId -notin $stopped) {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
            $stopped += $_.ProcessId
        }
    }

if ($stopped.Count -gt 0) {
    Write-Output ("Stopped Dashboard V3 LAN process ids: {0}" -f ($stopped -join ', '))
} else {
    Write-Output "No Dashboard V3 process found to stop."
}
Write-Output "Note: this did not touch the trading bot or the auto-export loop."
