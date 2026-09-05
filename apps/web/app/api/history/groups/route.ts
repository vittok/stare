import { NextRequest, NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const apiUrl = process.env.FASTAPI_URL?.replace(/\/$/, "");
  if (!apiUrl) return NextResponse.json({ error: "History service is not configured." }, { status: 503 });
  try {
    const response = await fetch(`${apiUrl}/api/history/groups?${request.nextUrl.searchParams}`, {
      cache: "no-store",
      signal: AbortSignal.timeout(15_000)
    });
    const payload = await response.json();
    return NextResponse.json(payload, { status: response.status, headers: { "cache-control": "no-store" } });
  } catch {
    return NextResponse.json({ error: "Historical group data is temporarily unavailable." }, { status: 503 });
  }
}
