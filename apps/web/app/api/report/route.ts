import { NextResponse } from "next/server";
import { getLatestReport } from "../../../lib/portal-api";

export const dynamic = "force-dynamic";

export async function GET() {
  const report = await getLatestReport();

  if (!report?.update) {
    return NextResponse.json(
      { error: "Market data is temporarily unavailable." },
      {
        status: 503,
        headers: { "cache-control": "no-store" }
      }
    );
  }

  return NextResponse.json(report, {
    headers: { "cache-control": "no-store" }
  });
}
