import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Stock Trend Analysis Risk Engine (S.T.A.R.E)",
  description: "Personalized market trend and risk analysis from S.T.A.R.E."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>{children}</body>
    </html>
  );
}
