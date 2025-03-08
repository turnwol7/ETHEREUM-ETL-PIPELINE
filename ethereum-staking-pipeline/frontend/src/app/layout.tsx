import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: 'Ethereum Staking Dashboard',
  description: 'Real-time Ethereum staking analytics',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
