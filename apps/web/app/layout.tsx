import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "S.T.A.R.E Portal",
  description: "Personalized market trend analysis for S.T.A.R.E."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
