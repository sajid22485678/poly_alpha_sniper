import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Poly Alpha Frequency V4 Shadow",
  description: "Read-only telemetry for the isolated Frequency V4 shadow engine",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
