const API_WAKE_TIMEOUT_MS = 90_000;

async function wakeApi(): Promise<void> {
  const apiUrl = process.env.FASTAPI_URL?.replace(/\/$/, "");
  if (!apiUrl) {
    console.warn("FASTAPI_URL is not configured; skipping API wake-up.");
    return;
  }

  try {
    const response = await fetch(`${apiUrl}/health`, {
      cache: "no-store",
      signal: AbortSignal.timeout(API_WAKE_TIMEOUT_MS)
    });

    if (!response.ok) {
      console.warn(`API wake-up returned ${response.status}.`);
    }
  } catch (error) {
    console.warn("API wake-up request failed.", error);
  }
}

export function register(): void {
  // Do not hold up the portal cold start while Render wakes the API service.
  void wakeApi();
}
