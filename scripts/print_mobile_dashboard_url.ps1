# Prints the PC + phone URLs for the read-only Dashboard V3 over LAN.
# Read-only: discovers the local IPv4 and prints URLs. Touches nothing,
# starts/stops nothing, reads no secrets.
$ErrorActionPreference = "SilentlyContinue"
$port = 8503
$hostname = [System.Net.Dns]::GetHostName()

# Freshest active, non-loopback, non-virtual IPv4 (Dhcp/Manual origin) on an Up adapter.
$ip = Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { $_.IPAddress -ne '127.0.0.1' -and $_.PrefixOrigin -in @('Dhcp', 'Manual') } |
    ForEach-Object {
        $a = Get-NetAdapter -InterfaceIndex $_.InterfaceIndex -ErrorAction SilentlyContinue
        [PSCustomObject]@{ IP = $_.IPAddress; Status = $a.Status; Virtual = $a.Virtual }
    } |
    Where-Object { $_.Status -eq 'Up' -and $_.Virtual -ne $true } |
    Select-Object -First 1 -ExpandProperty IP
if (-not $ip) { $ip = "<no-LAN-IPv4-found>" }

Write-Output "============================================================"
Write-Output " Poly Alpha Sniper - Dashboard V3 mobile access (LAN only)"
Write-Output "============================================================"
Write-Output ("PC hostname : {0}" -f $hostname)
Write-Output ("LAN IPv4    : {0}" -f $ip)
Write-Output ("Port        : {0} (read-only dashboard)" -f $port)
Write-Output ("PC URL      : http://localhost:{0}" -f $port)
Write-Output ("Phone URL   : http://{0}:{1}" -f $ip, $port)
Write-Output ""
Write-Output "Connect the phone to the SAME Wi-Fi / router as this PC."
Write-Output "Sign in with any username and the password you set at launch."
Write-Output "Do NOT expose this URL outside your LAN. Do NOT port-forward or tunnel."
