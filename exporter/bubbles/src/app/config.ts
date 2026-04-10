import type { DataSourceMode, SizeMetric, ColorMetric } from "./types";

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

function withPublicBase(url: string, publicBase: string): string {
  if (!url.startsWith("/") || url.startsWith("//") || publicBase === "") {
    return url;
  }
  if (url === publicBase || url.startsWith(`${publicBase}/`)) {
    return url;
  }
  return `${publicBase}${url}`;
}

function toWsUrl(url: string): string {
  if (url.startsWith("wss://") || url.startsWith("ws://")) {
    return url;
  }
  if (url.startsWith("https://")) {
    return `wss://${url.slice("https://".length)}`;
  }
  if (url.startsWith("http://")) {
    return `ws://${url.slice("http://".length)}`;
  }
  if (url.startsWith("/")) {
    if (typeof window !== "undefined") {
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      return `${protocol}//${window.location.host}${url}`;
    }
    return `ws://127.0.0.1:3100${url}`;
  }
  return url;
}

type RuntimeConfig = {
  gobotBaseUrl?: string;
  restUrl?: string;
  statusUrl?: string;
  accountUrl?: string;
  positionUrl?: string;
  historyUrl?: string;
  groupsUrl?: string;
  wsUrl?: string;
};

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function trimTrailingSlash(url: string): string {
  return url.replace(/\/+$/, "");
}

function readRuntimeConfig(): RuntimeConfig {
  if (typeof window === "undefined") {
    return {};
  }

  const raw = (window as Window & { __BUBBLES_RUNTIME_CONFIG__?: unknown })
    .__BUBBLES_RUNTIME_CONFIG__;
  if (!raw || typeof raw !== "object") {
    return {};
  }

  const config = raw as Record<string, unknown>;
  return {
    gobotBaseUrl: normalizeOptionalString(config.gobotBaseUrl),
    restUrl: normalizeOptionalString(config.restUrl),
    statusUrl: normalizeOptionalString(config.statusUrl),
    accountUrl: normalizeOptionalString(config.accountUrl),
    positionUrl: normalizeOptionalString(config.positionUrl),
    historyUrl: normalizeOptionalString(config.historyUrl),
    groupsUrl: normalizeOptionalString(config.groupsUrl),
    wsUrl: normalizeOptionalString(config.wsUrl)
  };
}

function toDefaultWsEndpoint(baseUrl: string): string {
  const normalized = trimTrailingSlash(baseUrl);
  if (normalized.startsWith("https://")) {
    return `wss://${normalized.slice("https://".length)}/ws/stream`;
  }
  if (normalized.startsWith("http://")) {
    return `ws://${normalized.slice("http://".length)}/ws/stream`;
  }
  return `${normalized}/ws/stream`;
}

const defaultSignalsUrl = "/signals";
const defaultStatusUrl = "/status";
const defaultAccountUrl = "/account";
const defaultPositionUrl = "/positions";
const defaultHistoryUrl = "/history";
const defaultGroupsUrl = "/groups";
const defaultWsUrl = "/ws/stream";
const runtimeConfig = readRuntimeConfig();
const publicBase = normalizePublicBase(import.meta.env.BASE_URL || "/");
const runtimeBaseUrl = runtimeConfig.gobotBaseUrl
  ? trimTrailingSlash(runtimeConfig.gobotBaseUrl)
  : undefined;

// 环境变量以 VITE_ 前缀读取，详见 README 的阶段切换说明。
// 集成模式下前端由 gobot exporter 托管，所有 API 通过 publicBase 前缀的相对路径访问。
export const AppConfig = {
  dataSource: (import.meta.env.VITE_DATA_SOURCE || "mock") as DataSourceMode,
  restUrl: withPublicBase(
    runtimeConfig.restUrl ||
      import.meta.env.VITE_REST_URL ||
      (runtimeBaseUrl ? `${runtimeBaseUrl}/signals` : defaultSignalsUrl),
    publicBase
  ),
  statusUrl: withPublicBase(
    runtimeConfig.statusUrl ||
      import.meta.env.VITE_STATUS_URL ||
      (runtimeBaseUrl ? `${runtimeBaseUrl}/status` : defaultStatusUrl),
    publicBase
  ),
  accountUrl: withPublicBase(
    runtimeConfig.accountUrl ||
      import.meta.env.VITE_ACCOUNT_URL ||
      (runtimeBaseUrl ? `${runtimeBaseUrl}/account` : defaultAccountUrl),
    publicBase
  ),
  positionUrl: withPublicBase(
    runtimeConfig.positionUrl ||
      import.meta.env.VITE_POSITION_URL ||
      (runtimeBaseUrl ? `${runtimeBaseUrl}/positions` : defaultPositionUrl),
    publicBase
  ),
  historyUrl: withPublicBase(
    runtimeConfig.historyUrl ||
      import.meta.env.VITE_HISTORY_URL ||
      (runtimeBaseUrl ? `${runtimeBaseUrl}/history` : defaultHistoryUrl),
    publicBase
  ),
  groupsUrl: withPublicBase(
    runtimeConfig.groupsUrl ||
      import.meta.env.VITE_GROUPS_URL ||
      (runtimeBaseUrl ? `${runtimeBaseUrl}/groups` : defaultGroupsUrl),
    publicBase
  ),
  wsUrl: toWsUrl(
    withPublicBase(
      runtimeConfig.wsUrl ||
        import.meta.env.VITE_WS_URL ||
        (runtimeBaseUrl ? toDefaultWsEndpoint(runtimeBaseUrl) : defaultWsUrl),
      publicBase
    )
  ),
  gobotBaseUrl: runtimeBaseUrl || "",
  dataStaleCheckIntervalMs: Number(
    import.meta.env.VITE_WS_DATA_CHECK_INTERVAL_MS ||
      import.meta.env.VITE_REST_POLL_INTERVAL_MS ||
      15000
  ),
  mockUpdateIntervalMs: Number(import.meta.env.VITE_MOCK_UPDATE_INTERVAL_MS || 1000),
  maxRenderDpr: Number(import.meta.env.VITE_MAX_RENDER_DPR || 1.5),
  initialBubbleCount: Number(import.meta.env.VITE_BUBBLE_COUNT || 200),
  minBubbleCount: 50,
  maxBubbleCount: 1000,
  initialSizeMetric: (import.meta.env.VITE_SIZE_METRIC || "marketCap") as SizeMetric,
  initialColorMetric: (import.meta.env.VITE_COLOR_METRIC || "change24h") as ColorMetric
};

export const EngineDefaults = {
  noiseStrength: 1.2,
  noiseSpeed: 0.45,
  damping: 0.28,
  restitution: 0.78,
  densityGridCols: 14,
  densityGridRows: 8,
  densityStrength: 0.82,
  softWallStrength: 0.9,
  softWallBandRatio: 0.07,
  maxSpeed: 180,
  driftCancelStrength: 0.92,
  centroidReturnStrength: 7.5,
  centroidDeadZoneRatio: 0.06,
  imbalanceCheckInterval: 3.0,
  imbalanceThresholdRatio: 0.7,
  imbalanceBoostMultiplier: 1.6,
  imbalanceBoostDuration: 2.0,
  throwStrength: 2.6,
  explodeStrength: 900,
  explodeRadius: 220,
  radiusSmoothing: 4.2,
  dataSmoothing: 3.0
};
