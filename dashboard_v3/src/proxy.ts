import { NextRequest, NextResponse } from "next/server";

/**
 * Optional LAN Basic-Auth gate for the read-only dashboard.
 *
 * ACTIVE ONLY when DASHBOARD_LAN_PASSWORD is present in the process
 * environment. The LAN launcher (scripts/start_dashboard_v3_lan.bat) sets it
 * from a value the user types at launch -- it is NEVER read from .env, never
 * a pre-existing secret/API key, and never logged. When the variable is unset
 * (normal loopback `npm run dev`), this proxy is a pass-through and the
 * localhost experience is unchanged.
 *
 * SAFETY: this gate only RESTRICTS viewing. It adds no write path, no POST
 * handler, and no order/mutation capability -- the dashboard stays read-only.
 *
 * (Next.js 16 "proxy" file convention -- the successor to "middleware".)
 */
export function proxy(req: NextRequest): NextResponse {
  const password = process.env.DASHBOARD_LAN_PASSWORD;
  if (!password) return NextResponse.next(); // no LAN password -> loopback dev, no auth

  const header = req.headers.get("authorization") || "";
  if (header.startsWith("Basic ")) {
    try {
      const decoded = atob(header.slice(6));
      const sep = decoded.indexOf(":");
      const supplied = sep >= 0 ? decoded.slice(sep + 1) : "";
      if (constantTimeEquals(supplied, password)) return NextResponse.next();
    } catch {
      /* malformed header -> fall through to 401 */
    }
  }
  return new NextResponse("Authentication required.", {
    status: 401,
    headers: { "WWW-Authenticate": 'Basic realm="Poly Alpha Sniper Dashboard (LAN)"' },
  });
}

/** Length-checked, constant-time-ish comparison to avoid a trivial timing oracle. */
function constantTimeEquals(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

export const config = {
  // Gate the page and the /api/snapshot data route. Next.js internal static
  // assets (JS/CSS chunks, images, favicon) are excluded so the browser can
  // load the shell after the single Basic-Auth prompt -- the sensitive data
  // (the snapshot API and rendered page) is what's protected.
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
