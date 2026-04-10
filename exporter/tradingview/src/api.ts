import type {
  TradingViewBacktestOverlayResponse,
  TradingViewBacktestTaskResponse,
  TradingViewBacktestTasksResponse,
  TradingViewCandlesResponse,
  TradingViewExporterStatusResponse,
  TradingViewPositionEventsResponse,
  TradingViewPositionDelegateRequest,
  TradingViewPositionDelegateResponse,
  TradingViewRuntimeResponse,
  TradingViewTradeRequest,
  TradingViewTradeResponse
} from "./types";

declare global {
  interface Window {
    __TRADINGVIEW_RUNTIME_CONFIG__?: {
      apiBaseUrl?: string;
    };
  }
}

const runtimeBase = window.__TRADINGVIEW_RUNTIME_CONFIG__?.apiBaseUrl?.trim() || "";
const apiBase = runtimeBase ? runtimeBase.replace(/\/+$/, "") : "/tradingview/api/v1";
const tradingViewAPIPrefix = "/tradingview/api/v1";
const publicBase = normalizePublicBase(import.meta.env.BASE_URL || "/");

function normalizePublicBase(rawBase: string): string {
  const trimmed = rawBase.trim();
  if (trimmed.length === 0 || trimmed === "/") {
    return "";
  }
  let base = trimmed;
  if (!base.startsWith("/")) {
    base = `/${base}`;
  }
  return base.replace(/\/+$/, "");
}

function withPublicBase(url: string, base: string): string {
  if (!url.startsWith("/") || url.startsWith("//") || base === "") {
    return url;
  }
  if (url === base || url.startsWith(`${base}/`)) {
    return url;
  }
  return `${base}${url}`;
}

function buildServicePath(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (!runtimeBase) {
    return withPublicBase(normalizedPath, publicBase);
  }
  const apiURL = new URL(apiBase, window.location.href);
  let prefix = apiURL.pathname.replace(/\/+$/, "");
  if (prefix.endsWith(tradingViewAPIPrefix)) {
    prefix = prefix.slice(0, -tradingViewAPIPrefix.length);
  }
  const basePath = prefix && prefix !== "/" ? prefix : "";
  return withPublicBase(`${basePath}${normalizedPath}`, publicBase);
}

async function requestJSON<T>(path: string): Promise<T> {
  const response = await fetch(`${apiBase}${path}`, {
    cache: "no-store",
    headers: {
      "Cache-Control": "no-cache",
      Pragma: "no-cache"
    }
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

async function requestJSONWithInit<T>(path: string, init: RequestInit): Promise<T> {
  const response = await fetch(`${apiBase}${path}`, {
    cache: "no-store",
    headers: {
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      "Content-Type": "application/json"
    },
    ...init
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

async function requestAbsoluteJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const requestHeaders = {
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    ...(init?.headers || {})
  };
  const response = await fetch(path, {
    cache: "no-store",
    ...init,
    headers: requestHeaders
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

export async function fetchRuntime(
  exchange?: string,
  options?: { lite?: boolean }
): Promise<TradingViewRuntimeResponse> {
  const search = new URLSearchParams();
  if (exchange) {
    search.set("exchange", exchange);
  }
  if (options?.lite) {
    search.set("lite", "1");
  }
  const suffix = search.size > 0 ? `?${search.toString()}` : "";
  return requestJSON<TradingViewRuntimeResponse>(`/runtime${suffix}`);
}

export async function fetchExporterStatus(): Promise<TradingViewExporterStatusResponse> {
  return requestAbsoluteJSON<TradingViewExporterStatusResponse>(buildServicePath("/status"), {
    method: "GET",
  });
}

export async function fetchCandles(params: {
  exchange: string;
  symbol: string;
  timeframe: string;
}): Promise<TradingViewCandlesResponse> {
  const search = new URLSearchParams();
  search.set("exchange", params.exchange);
  search.set("symbol", params.symbol);
  search.set("timeframe", params.timeframe);
  const payload = await requestJSON<TradingViewCandlesResponse>(`/candles?${search.toString()}`);
  return normalizeCandlesResponse(payload);
}

export async function createBacktestTask(params: {
  exchange: string;
  symbol: string;
  display_symbol: string;
  chart_timeframe: string;
  range_start_ms: number;
  range_end_ms: number;
  price_low: number;
  price_high: number;
  selection_direction: string;
}): Promise<TradingViewBacktestTaskResponse> {
  return requestJSONWithInit<TradingViewBacktestTaskResponse>("/backtests", {
    method: "POST",
    body: JSON.stringify(params)
  });
}

export async function submitTrade(params: TradingViewTradeRequest): Promise<TradingViewTradeResponse> {
  return requestJSONWithInit<TradingViewTradeResponse>("/trade", {
    method: "POST",
    body: JSON.stringify(params)
  });
}

export async function delegatePositionStrategy(
  params: TradingViewPositionDelegateRequest
): Promise<TradingViewPositionDelegateResponse> {
  return requestJSONWithInit<TradingViewPositionDelegateResponse>("/position-delegate", {
    method: "POST",
    body: JSON.stringify(params)
  });
}

export async function fetchBacktestTasks(params?: {
  date?: string;
  tz_offset_min?: number;
}): Promise<TradingViewBacktestTasksResponse> {
  const search = new URLSearchParams();
  if (params?.date) {
    search.set("date", params.date);
  }
  if (typeof params?.tz_offset_min === "number" && Number.isFinite(params.tz_offset_min)) {
    search.set("tz_offset_min", String(params.tz_offset_min));
  }
  const suffix = search.size > 0 ? `?${search.toString()}` : "";
  return requestJSON<TradingViewBacktestTasksResponse>(`/backtests${suffix}`);
}

export async function fetchBacktestTask(taskID: number): Promise<TradingViewBacktestTaskResponse> {
  return requestJSON<TradingViewBacktestTaskResponse>(`/backtests/${taskID}`);
}

export async function retryBacktestTask(taskID: number): Promise<TradingViewBacktestTaskResponse> {
  return requestJSONWithInit<TradingViewBacktestTaskResponse>(`/backtests/${taskID}/retry`, {
    method: "POST"
  });
}

export async function fetchBacktestOverlay(taskID: number): Promise<TradingViewBacktestOverlayResponse> {
  return requestJSON<TradingViewBacktestOverlayResponse>(`/backtests/${taskID}/overlay`);
}

export async function fetchPositionEvents(params: {
  position_id?: number;
  exchange?: string;
  symbol?: string;
  position_side?: string;
  margin_mode?: string;
  entry_time?: string;
  strategy?: string;
  version?: string;
  event_limit?: number;
}): Promise<TradingViewPositionEventsResponse> {
  const search = new URLSearchParams();
  if (typeof params.position_id === "number" && Number.isFinite(params.position_id) && params.position_id > 0) {
    search.set("position_id", String(Math.trunc(params.position_id)));
  }
  if (params.exchange) {
    search.set("exchange", params.exchange);
  }
  if (params.symbol) {
    search.set("symbol", params.symbol);
  }
  if (params.position_side) {
    search.set("position_side", params.position_side);
  }
  if (params.margin_mode) {
    search.set("margin_mode", params.margin_mode);
  }
  if (params.entry_time) {
    search.set("entry_time", params.entry_time);
  }
  if (params.strategy) {
    search.set("strategy", params.strategy);
  }
  if (params.version) {
    search.set("version", params.version);
  }
  if (typeof params.event_limit === "number" && Number.isFinite(params.event_limit) && params.event_limit > 0) {
    search.set("event_limit", String(Math.trunc(params.event_limit)));
  }
  const suffix = search.size > 0 ? `?${search.toString()}` : "";
  return requestAbsoluteJSON<TradingViewPositionEventsResponse>(buildServicePath(`/positions/events${suffix}`), {
    method: "GET"
  });
}

export function normalizeCandlesResponse(payload: TradingViewCandlesResponse): TradingViewCandlesResponse {
  const candles = Array.isArray(payload?.candles) ? payload.candles : [];
  const indicators = Array.isArray(payload?.indicators)
    ? payload.indicators.map((item) => ({
        ...item,
        legend_color: typeof item?.legend_color === "string" ? item.legend_color : undefined,
        points: Array.isArray(item?.points) ? item.points : []
      }))
    : [];
  return {
    ...payload,
    candles,
    indicators
  };
}

export function openRealtimeSocket(): WebSocket {
  const wsURL = new URL(buildServicePath("/ws/stream"), window.location.href);
  wsURL.protocol = wsURL.protocol === "https:" ? "wss:" : "ws:";
  wsURL.search = "";
  wsURL.hash = "";
  return new WebSocket(wsURL.toString());
}
