export type SizeMetric = "marketCap" | "volume24h" | "price" | "rank";
export type ColorMetric = "change24h" | "change7d";
export type TrendType =
  | "bullish"
  | "bullishPullback"
  | "bearish"
  | "bearishPullback"
  | "range"
  | "none";

export type TrendFilter = Record<TrendType, boolean>;

export type SignalOHLCVBar = {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type BubbleCandleBar = SignalOHLCVBar;

export type BubbleCandleEvent = {
  id: string;
  source: string;
  type: string;
  level: "info" | "warning" | "success" | "error";
  eventAtMs: number;
  title: string;
  summary: string;
  detail?: Record<string, unknown>;
};

export type BubbleCandlePosition = {
  positionKey?: string;
  isOpen?: boolean;
  positionSide?: string;
  entryPrice?: number;
  exitPrice?: number;
};

export type BubbleCandleSeries = {
  timeframe: string;
  requested: number;
  returned: number;
  bars: BubbleCandleBar[];
};

export type BubbleCandleItem = {
  exchange: string;
  symbol: string;
  series: Record<string, BubbleCandleSeries>;
  events?: BubbleCandleEvent[];
  eventsTotal?: number;
  eventsTruncated?: boolean;
  position?: BubbleCandlePosition;
};

export type BubbleCandlesSnapshot = {
  fetchedAt: number;
  items: BubbleCandleItem[];
  warnings: string[];
};

export type BubbleCandlesFetchRequestItem = {
  exchange: string;
  symbol: string;
  timeframes: string[];
  limit?: number;
  position?: {
    positionId?: number;
    positionKey?: string;
    positionSide?: string;
    marginMode?: string;
    entryTime?: string;
    strategyName?: string;
    strategyVersion?: string;
  };
};

export type BubbleCandlesFetchRequest = {
  requests: BubbleCandlesFetchRequestItem[];
  closedOnly?: boolean;
  includeEvents?: boolean;
  eventLimit?: number;
};

export type BubbleDatum = {
  id: string;
  symbol: string;
  name: string;
  marketCap: number;
  volume24h: number;
  price: number;
  change24h: number;
  change7d: number;
  rank: number;
  logoUrl?: string;
  exchange?: string;
  timeframe?: string;
  comboKey?: string;
  groupId?: string;
  strategyTimeframes?: string[];
  strategy?: string;
  strategyVersion?: string;
  entry?: number;
  exit?: number;
  sl?: number;
  tp?: number;
  action?: number;
  highSide?: number;
  midSide?: number;
  side?: number;
  trendType?: TrendType;
  trendingTimestamp?: number;
  triggerTimestamp?: number;
  ohlcv?: SignalOHLCVBar[];
  sourceType?: "signal" | "positionFallback";
};

export type BubbleUpdate = Partial<Omit<BubbleDatum, "id">> & { id: string };

export type DataSourceMode = "mock" | "restws";

export type PollStatus = "idle" | "loading" | "ok" | "error";

export type AccountSnapshot = {
  exchange: string;
  currency: string;
  fundingUsdt: number;
  tradingUsdt: number;
  totalUsdt: number;
  perTradeUsdt?: number;
  dailyProfitUsdt?: number;
  updatedAtMs?: number;
  fetchedAt: number;
};

export type PositionItem = {
  positionId?: number;
  exchange: string;
  symbol: string;
  timeframe: string;
  groupId?: string;
  positionSide?: string;
  marginMode?: string;
  leverageMultiplier?: number;
  marginAmount?: number;
  entryPrice?: number;
  exitPrice?: number;
  entryQuantity?: number;
  entryTime?: string;
  exitTime?: string;
  takeProfitPrice?: number;
  stopLossPrice?: number;
  currentPrice?: number;
  unrealizedProfitAmount?: number;
  unrealizedProfitRate?: number;
  profitAmount?: number;
  profitRate?: number;
  maxFloatingProfitAmount?: number;
  maxFloatingProfitRate?: number;
  maxFloatingLossAmount?: number;
  maxFloatingLossRate?: number;
  holdingTime?: string;
  status?: string;
  strategyName?: string;
  strategyVersion?: string;
  updatedTime?: string;
};

export type PositionSnapshot = {
  count: number;
  positions: PositionItem[];
  fetchedAt: number;
};

export type TrendGroupCandidate = {
  candidateKey?: string;
  candidateState?: string;
  isSelected?: boolean;
  priorityScore?: number;
  hasOpenPosition?: boolean;
  [key: string]: unknown;
};

export type TrendGroupItem = {
  groupId: string;
  strategy?: string;
  primaryTimeframe?: string;
  side?: string;
  anchorTrendingTimestampMs?: number;
  state?: string;
  lockStage?: string;
  selectedCandidateKey?: string;
  entryCount?: number;
  candidates: TrendGroupCandidate[];
};

export type TrendGroupsSnapshot = {
  enabled?: boolean;
  mode?: string;
  groupsTotal: number;
  groupsActive: number;
  groups: TrendGroupItem[];
  fetchedAt: number;
};

export type WsPongStatus = {
  requestId?: string;
  serverTs?: number;
  receivedAt: number;
  rttMs?: number;
  runtimeSeconds?: number;
  runtimeHuman?: string;
  singletonUuid?: string;
  modules: BackendModuleStatus[];
};

export type DataSourceStatus = {
  mode: DataSourceMode;
  restStatus: PollStatus;
  accountStatus: PollStatus;
  positionStatus: PollStatus;
  wsStatus: "idle" | "connecting" | "open" | "closed" | "error";
  wsConnectedAt?: number;
  lastSnapshotAt?: number;
  lastUpdateAt?: number;
  lastAccountAt?: number;
  lastPositionAt?: number;
  lastPingAt?: number;
  lastPongAt?: number;
  heartbeatMissedCycles?: number;
  heartbeatStale?: boolean;
  lastPong?: WsPongStatus;
  errorMessage?: string;
};

export type BackendModuleStatus = {
  name: string;
  state: string;
  updatedAt?: string;
};

export type BackendCacheSummary = {
  exchangeCount: number;
  symbolCount: number;
  timeframeCount: number;
  signalCount: number;
};

export type BackendStatus = {
  fetchedAt: number;
  runtimeSeconds?: number;
  runtimeHuman?: string;
  singletonUuid?: string;
  versionTag?: string;
  versionCommit?: string;
  buildTime?: string;
  cache: BackendCacheSummary;
  modules: BackendModuleStatus[];
};
