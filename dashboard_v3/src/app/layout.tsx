import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Poly Alpha Sniper — Dashboard v3",
  description: "Read-only shadow-mode control room (SHADOW ONLY — NO LIVE TRADING)",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full flex flex-col bg-v3-bg">{children}</body>
    </html>
  );
}
