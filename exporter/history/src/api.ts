import type {
  CandlesResponse,
  EventsResponse,
  IntegrityResponse,
  LoadCandlesResponse,
  PositionsResponse
} from "./types";

declare global {
  interface Window {
    __HISTORY_RUNTIME_CONFIG__?: {
      apiBaseUrl?: string;
    };
  }
}

const runtimeBase = window.__HISTORY_RUNTIME_CONFIG__?.apiBaseUrl?.trim() || "";
const apiBase = runtimeBase ? runtimeBase.replace(/\/+$/, "") : "/visual-history/api/v1";

async function requestJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers || {});
  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  headers.set("Cache-Control", "no-cache");
  headers.set("Pragma", "no-cache");
  const response = await fetch(`${apiBase}${path}`, {
    ...init,
    cache: init?.cache ?? "no-store",
    headers
  });

  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    payload = undefined;
  }

  if (!response.ok) {
    const message =
      typeof payload === "object" && payload && "error" in payload
        ? String((payload as { error: string }).error)
        : `request failed: ${response.status}`;
    throw new Error(message);
  }

  return payload as T;
}

export async function fetchPositions(params: {
  date: string;
  exchange?: string;
  symbol?: string;
  run_id?: string;
  strategy?: string;
  version?: string;
  before?: number;
  limit?: number;
  tz_offset_min?: number;
}): Promise<PositionsResponse> {
  const search = new URLSearchParams();
  if (params.date) search.set("date", params.date);
  if (params.exchange) search.set("exchange", params.exchange);
  if (params.symbol) search.set("symbol", params.symbol);
  if (params.run_id) search.set("run_id", params.run_id);
  if (params.strategy) search.set("strategy", params.strategy);
  if (params.version) search.set("version", params.version);
  if (params.before) search.set("before", String(params.before));
  if (params.limit) search.set("limit", String(params.limit));
  if (typeof params.tz_offset_min === "number" && Number.isFinite(params.tz_offset_min)) {
    search.set("tz_offset_min", String(Math.trunc(params.tz_offset_min)));
  }
  return requestJSON<PositionsResponse>(`/positions?${search.toString()}`);
}

export async function fetchEvents(positionID: number): Promise<EventsResponse> {
  return requestJSON<EventsResponse>(`/positions/${positionID}/events`);
}

export async function loadCandles(positionID: number, payload: {
  timeframes: string[];
  max_per_request?: number;
  force?: boolean;
}): Promise<LoadCandlesResponse> {
  return requestJSON<LoadCandlesResponse>(`/positions/${positionID}/candles/load`, {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export async function fetchCandles(positionID: number): Promise<CandlesResponse> {
  return requestJSON<CandlesResponse>(`/positions/${positionID}/candles`);
}

export async function fetchIntegrity(positionID: number): Promise<IntegrityResponse> {
  return requestJSON<IntegrityResponse>(`/positions/${positionID}/integrity`);
}
