# Mobile / LAN access to the read-only Dashboard V3

This lets you open the **read-only** Poly Alpha Sniper dashboard from your
phone, on your **home network only**. It never exposes the dashboard to the
public internet, never disables authentication, and never touches the trading
bot, orders, or secrets.

## What this does (and does not) do

- **Does**: bind the read-only dashboard to your LAN (`0.0.0.0:8503`) so a
  phone on the same Wi-Fi/router can view it, behind a viewing password.
- **Does not**: start/stop the trading bot, place/cancel orders, add any
  write/POST endpoint, disable auth, open the Public firewall profile,
  port-forward your router, or use any public tunnel (ngrok/cloudflared).

The dashboard stays exactly as read-only as it is on `localhost`.

## Requirements

- The phone must be on the **same Wi-Fi / router** as this PC.
  (This PC may be wired Ethernet and the phone on Wi-Fi — that's fine as long
  as they share the same router / subnet, e.g. `192.168.x.x`.)
- The trading bot and the auto-export loop can be running or not; the
  dashboard only reads the exported JSON in
  `D:\claude\agent_readonly\poly_alpha_sniper\`.

## One-time firewall rule (run once, as Administrator)

Windows blocks inbound LAN connections by default, so add a narrow rule that
opens **only** the dashboard port on your **private** network profile:

```
Right-click PowerShell -> "Run as administrator", then:
  cd D:\claude\poly_alpha_sniper
  powershell -ExecutionPolicy Bypass -File scripts\allow_dashboard_v3_lan_firewall.ps1
```

This adds an inbound TCP rule for port **8503**, **Private profile only**,
scoped to the **local subnet**. It never opens any trading/broker/API port and
never the Public profile. Remove it any time (also as admin):

```
  powershell -ExecutionPolicy Bypass -File scripts\disable_dashboard_v3_lan_firewall.ps1
```

## Start the LAN dashboard

```
  cd D:\claude\poly_alpha_sniper
  scripts\start_dashboard_v3_lan.bat
```

It will:
1. Ask you to **set a viewing password** (required — it refuses to start LAN
   mode without one, so the dashboard is never exposed unauthenticated).
2. Print the **PC URL** and **Phone URL**.
3. Start the read-only dashboard bound to `0.0.0.0:8503`.

> Note: the password you type is visible on screen (it's a LAN viewing
> password you choose, not a stored secret). Pick something simple you can
> type on your phone.

## Get / re-print the phone URL

```
  powershell -ExecutionPolicy Bypass -File scripts\print_mobile_dashboard_url.ps1
```

Example output:

```
PC hostname : sajid
LAN IPv4    : 192.168.100.8
Port        : 8503 (read-only dashboard)
PC URL      : http://localhost:8503
Phone URL   : http://192.168.100.8:8503
```

## Open it on the phone

1. Connect the phone to the same Wi-Fi as this PC.
2. Open the **Phone URL** (e.g. `http://192.168.100.8:8503`) in the phone
   browser.
3. When prompted, sign in with **any username** and the **password** you set
   at launch.

## Check status

```
  powershell -ExecutionPolicy Bypass -File scripts\status_dashboard_v3_lan.ps1
```

Shows whether port 8503 is listening (and on which address), whether the
firewall rule is present, and the phone URL.

## Stop the LAN dashboard

```
  powershell -ExecutionPolicy Bypass -File scripts\stop_dashboard_v3_lan.ps1
```

Stops only the Dashboard V3 Next.js server. It does **not** touch the trading
bot or the auto-export loop. (You can also just close the window that
`start_dashboard_v3_lan.bat` opened.)

## Safety notes

- **LAN only.** Do not port-forward your router and do not use a public tunnel
  (ngrok, cloudflared, etc.). If you ever need remote access, use a VPN into
  your home network instead — never a public URL.
- **Keep the password on.** LAN mode refuses to start without one.
- The dashboard remains **read-only**: no order buttons, no POST endpoints, no
  ability to change bot/live state. It only displays the exported shadow
  diagnostics JSON.
- Trading stays in shadow mode: `mode=shadow_live`, `dry_run=True`,
  `live_enabled=False`. Nothing here changes that.
