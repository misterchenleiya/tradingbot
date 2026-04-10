import { create } from "zustand";
import type {
  AccountSnapshot,
  BackendStatus,
  BubbleCandlesFetchRequest,
  BubbleCandlesSnapshot,
  BubbleDatum,
  BubbleUpdate,
  ColorMetric,
  DataSourceStatus,
  PositionItem,
  PositionSnapshot,
  SignalOHLCVBar,
  SizeMetric,
  TrendGroupsSnapshot
} from "./types";
import { AppConfig } from "./config";

const PREFERRED_SIGNAL_EXCHANGES = ["binance", "okx", "bitget"] as const;

export type Stats = {
  fps: number;
  dtMs: number;
  bubbleCount: number;
};

export type TimeframeTab = {
  value: string;
  active: boolean;
};

export type ExchangeTab = {
  value: string;
  active: boolean;
};

function sortData(a: BubbleDatum, b: BubbleDatum): number {
  const rankA = a.rank || 0;
  const rankB = b.rank || 0;
  if (rankA && rankB && rankA !== rankB) return rankA - rankB;
  const capDiff = b.marketCap - a.marketCap;
  if (capDiff !== 0) return capDiff;
  const symbolDiff = a.symbol.localeCompare(b.symbol);
  if (symbolDiff !== 0) return symbolDiff;
  return a.id.localeCompare(b.id);
}

function timeframeSortValue(timeframe: string): number {
  const match = /^(\d+)([mhdw])$/i.exec(timeframe.trim());
  if (!match) return Number.MAX_SAFE_INTEGER;
  const num = Number(match[1]);
  const unit = match[2].toLowerCase();
  switch (unit) {
    case "m":
      return num;
    case "h":
      return num * 60;
    case "d":
      return num * 60 * 24;
    case "w":
      return num * 60 * 24 * 7;
    default:
      return Number.MAX_SAFE_INTEGER;
  }
}

function exchangeSortValue(exchange: string): number {
  const value = exchange.trim().toLowerCase();
  switch (value) {
    case "binance":
      return 0;
    case "okx":
      return 1;
    case "bitget":
      return 2;
    default:
      return 1000;
  }
}

function normalizeExchangeToken(exchange?: string): string {
  return (exchange || "").trim().toLowerCase();
}

function splitExchangeTokens(exchange?: string): string[] {
  const normalized = normalizeExchangeToken(exchange);
  if (!normalized) return [];
  return normalized
    .split("/")
    .map((item) => item.trim().toLowerCase())
    .filter((item) => item.length > 0);
}

function sortSignalExchanges(exchanges: string[]): string[] {
  const unique = Array.from(
    new Set(
      exchanges
        .map((item) => normalizeExchangeToken(item))
        .filter((item) => item.length > 0)
    )
  );
  const preferred = PREFERRED_SIGNAL_EXCHANGES.filter((item) => unique.includes(item));
  const rest = unique
    .filter((item) => !PREFERRED_SIGNAL_EXCHANGES.includes(item as (typeof PREFERRED_SIGNAL_EXCHANGES)[number]))
    .sort((a, b) => a.localeCompare(b));
  return [...preferred, ...rest];
}

function mergeExchangeDisplayValue(left?: string, right?: string): string | undefined {
  const merged = sortSignalExchanges([
    ...splitExchangeTokens(left),
    ...splitExchangeTokens(right)
  ]);
  return merged.length > 0 ? merged.join("/") : undefined;
}

function isPositionFallbackSignal(item: BubbleDatum): boolean {
  return item.sourceType === "positionFallback";
}

function normalizeSignalSymbol(value?: string): string {
  return (value || "").trim().toUpperCase();
}

function normalizeSignalTimeframe(value?: string): string {
  return (value || "").trim().toLowerCase();
}

function normalizeGroupId(value?: string): string {
  return (value || "").trim().toLowerCase();
}

function parsePositionTimeMs(value?: string): number | undefined {
  if (!value) return undefined;
  const raw = value.trim();
  if (!raw) return undefined;

  if (/^\d{10,13}$/.test(raw)) {
    const ts = Number(raw);
    if (Number.isFinite(ts)) {
      return raw.length <= 10 ? ts * 1000 : ts;
    }
  }

  const utcMatch = raw.match(
    /^(\d{4})[/-](\d{2})[/-](\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (utcMatch) {
    const [, yyyy, mm, dd, hh = "0", mi = "0", ss = "0"] = utcMatch;
    const ts = Date.UTC(
      Number(yyyy),
      Number(mm) - 1,
      Number(dd),
      Number(hh),
      Number(mi),
      Number(ss)
    );
    if (Number.isFinite(ts)) return ts;
  }

  const direct = Date.parse(raw);
  if (Number.isFinite(direct)) return direct;

  const normalized = raw.replace(" ", "T");
  const parsed = Date.parse(normalized);
  if (Number.isFinite(parsed)) return parsed;

  return undefined;
}

function resolvePositionHighSide(item: PositionItem): number | undefined {
  const side = (item.positionSide || "").trim().toLowerCase();
  if (side === "long") return 1;
  if (side === "short") return -1;
  return undefined;
}

function isClosedPosition(item: PositionItem): boolean {
  const status = (item.status || "").trim().toLowerCase();
  if (!status) return false;
  if (status.includes("closed") || status.includes("close")) return true;
  if (status.includes("平仓") || status.includes("已平")) return true;
  return false;
}

function buildPositionFallbackSignal(item: PositionItem, index: number): BubbleDatum | null {
  if (isClosedPosition(item)) return null;
  const highSide = resolvePositionHighSide(item);
  if (highSide !== 1 && highSide !== -1) return null;

  const symbol = normalizeSignalSymbol(item.symbol);
  const timeframe = normalizeSignalTimeframe(item.timeframe);
  if (!symbol || !timeframe) return null;

  const exchange = normalizeExchangeToken(item.exchange);
  const strategy = normalizeSignalKeyPart(item.strategyName, "default");
  const strategyVersion = normalizeSignalKeyPart(item.strategyVersion, "default");
  const groupId = normalizeGroupId(item.groupId);
  const eventTs =
    parsePositionTimeMs(item.entryTime) ??
    parsePositionTimeMs(item.updatedTime) ??
    Date.now();

  const exchangePart = exchange || "unknown";
  const id = `position-fallback|${exchangePart}|${symbol}|${timeframe}|${strategy}|${strategyVersion}|hs:${highSide}`;

  return {
    id,
    symbol,
    name: exchange ? `${exchange.toUpperCase()} · ${strategy}` : strategy,
    marketCap: 160,
    volume24h: 1,
    price: eventTs,
    change24h: highSide > 0 ? 18 : -18,
    change7d: 0,
    rank: 1_000_000 + index,
    exchange: exchange || undefined,
    timeframe,
    groupId: groupId || undefined,
    strategy,
    strategyVersion,
    highSide,
    side: highSide,
    trendType: highSide > 0 ? "bullish" : "bearish",
    trendingTimestamp: eventTs,
    triggerTimestamp: eventTs,
    sourceType: "positionFallback"
  };
}

function resolveEffectiveHighSide(item: BubbleDatum): number | undefined {
  if (typeof item.highSide === "number" && Number.isFinite(item.highSide)) {
    const normalized = Math.trunc(item.highSide);
    if (normalized === 1 || normalized === -1) {
      return normalized;
    }
  }
  return undefined;
}

function normalizeSignalKeyPart(value: string | undefined, fallback: string): string {
  const trimmed = (value || "").trim();
  return trimmed || fallback;
}

function buildMergedSignalId(item: BubbleDatum): string {
  const symbolPart = normalizeSignalKeyPart(item.symbol, "UNKNOWN").toUpperCase();
  const timeframePart = normalizeSignalKeyPart(item.timeframe, "unknown").toLowerCase();
  const strategyPart = normalizeSignalKeyPart(item.strategy, "default").toLowerCase();
  const highSidePart = resolveEffectiveHighSide(item) ?? 0;
  const groupPart = normalizeGroupId(item.groupId) || "none";
  return `merged|${symbolPart}|${timeframePart}|${strategyPart}|hs:${highSidePart}|gid:${groupPart}`;
}

function signalEventTs(item: BubbleDatum): number {
  const triggerTs = typeof item.triggerTimestamp === "number" ? item.triggerTimestamp : 0;
  const trendingTs = typeof item.trendingTimestamp === "number" ? item.trendingTimestamp : 0;
  return Math.max(0, triggerTs, trendingTs);
}

function composeDisplaySignals(
  signalDataList: BubbleDatum[],
  positionSnapshot?: PositionSnapshot
): BubbleDatum[] {
  if (!positionSnapshot || !Array.isArray(positionSnapshot.positions) || positionSnapshot.positions.length === 0) {
    return [...signalDataList].sort(sortData);
  }

  const signalKeySet = new Set(signalDataList.map((item) => buildMergedSignalId(item)));
  const fallbackList: BubbleDatum[] = [];
  for (let i = 0; i < positionSnapshot.positions.length; i += 1) {
    const fallback = buildPositionFallbackSignal(positionSnapshot.positions[i], i);
    if (!fallback) continue;
    const fallbackKey = buildMergedSignalId(fallback);
    if (signalKeySet.has(fallbackKey)) continue;
    fallbackList.push(fallback);
  }

  if (fallbackList.length === 0) {
    return [...signalDataList].sort(sortData);
  }
  return [...signalDataList, ...fallbackList].sort(sortData);
}

function buildPositionFallbackSignature(snapshot?: PositionSnapshot): string {
  if (!snapshot || !Array.isArray(snapshot.positions) || snapshot.positions.length === 0) {
    return "";
  }
  const keys = new Set<string>();
  for (let i = 0; i < snapshot.positions.length; i += 1) {
    const fallback = buildPositionFallbackSignal(snapshot.positions[i], i);
    if (!fallback) continue;
    keys.add(buildMergedSignalId(fallback));
  }
  if (keys.size === 0) return "";
  return Array.from(keys).sort((a, b) => a.localeCompare(b)).join("|");
}

function shouldReplaceMergedSignal(existing: BubbleDatum, candidate: BubbleDatum): boolean {
  const existingPriority = isPositionFallbackSignal(existing) ? 0 : 1;
  const candidatePriority = isPositionFallbackSignal(candidate) ? 0 : 1;
  if (candidatePriority !== existingPriority) {
    return candidatePriority > existingPriority;
  }
  return signalEventTs(candidate) >= signalEventTs(existing);
}

function mergeSignalsByHighSide(items: BubbleDatum[]): BubbleDatum[] {
  const buckets = new Map<string, BubbleDatum>();
  for (const item of items) {
    const mergedId = buildMergedSignalId(item);
    const existing = buckets.get(mergedId);
    if (!existing) {
      buckets.set(mergedId, {
        ...item,
        id: mergedId,
        exchange: mergeExchangeDisplayValue(undefined, item.exchange)
      });
      continue;
    }
    const mergedExchange = mergeExchangeDisplayValue(existing.exchange, item.exchange);
    const latest = shouldReplaceMergedSignal(existing, item)
      ? { ...existing, ...item, id: mergedId }
      : { ...existing, id: mergedId };
    latest.exchange = mergedExchange;
    buckets.set(mergedId, latest);
  }
  return Array.from(buckets.values());
}

function mergeTimeframeTabs(previous: TimeframeTab[], activeValues: string[]): TimeframeTab[] {
  const activeSet = new Set(activeValues);
  return Array.from(activeSet)
    .sort((a, b) => timeframeSortValue(a) - timeframeSortValue(b))
    .map((value) => ({ value, active: true }));
}

function mergeCheckedTimeframes(
  previousChecked: string[],
  activeValues: string[]
): string[] {
  const activeSet = new Set(activeValues);
  const selected = previousChecked.find((value) => activeSet.has(value));
  if (selected) return [selected];
  if (activeValues.length === 0) return [];
  const next = [...activeValues].sort((a, b) => timeframeSortValue(a) - timeframeSortValue(b));
  return next.length > 0 ? [next[0]] : [];
}

function mergeExchangeTabs(previous: ExchangeTab[], activeValues: string[]): ExchangeTab[] {
  const activeSet = new Set(activeValues);
  return Array.from(activeSet)
    .sort((a, b) => {
      const av = exchangeSortValue(a);
      const bv = exchangeSortValue(b);
      if (av !== bv) return av - bv;
      return a.localeCompare(b);
    })
    .map((value) => ({ value, active: true }));
}

function mergeCheckedExchanges(
  previousTabs: ExchangeTab[],
  previousChecked: string[],
  activeValues: string[]
): string[] {
  const known = new Set(previousTabs.map((tab) => tab.value));
  const activeSet = new Set(activeValues);
  const checked = new Set(previousChecked.filter((value) => activeSet.has(value)));
  for (const value of activeValues) {
    if (!known.has(value)) {
      checked.add(value);
    }
  }
  return Array.from(checked).sort((a, b) => exchangeSortValue(a) - exchangeSortValue(b));
}

function deriveFilteredData(
  allDataList: BubbleDatum[],
  checkedTimeframes: string[],
  checkedExchanges: string[],
  searchQuery: string
): {
  dataList: BubbleDatum[];
  dataMap: Map<string, BubbleDatum>;
} {
  const keyword = searchQuery.trim().toLowerCase();
  const checkedSet = new Set(checkedTimeframes);
  const checkedExchangeSet = new Set(checkedExchanges.map((exchange) => exchange.toLowerCase()));
  const filtered = allDataList.filter((item) => {
    if (!resolveEffectiveHighSide(item)) return false;
    const timeframe = (item.timeframe || "").trim();
    if (timeframe && !checkedSet.has(timeframe)) return false;
    const exchange = (item.exchange || "").trim().toLowerCase();
    if (exchange && !checkedExchangeSet.has(exchange)) return false;
    if (!keyword) return true;
    return (
      item.symbol.toLowerCase().includes(keyword) ||
      item.name.toLowerCase().includes(keyword)
    );
  });

  const merged = mergeSignalsByHighSide(filtered);

  const dataMap = new Map<string, BubbleDatum>();
  for (const item of merged) {
    dataMap.set(item.id, item);
  }

  return {
    dataList: merged,
    dataMap
  };
}

function sameOhlcvBar(left: SignalOHLCVBar, right: SignalOHLCVBar): boolean {
  return (
    left.ts === right.ts &&
    left.open === right.open &&
    left.high === right.high &&
    left.low === right.low &&
    left.close === right.close &&
    left.volume === right.volume
  );
}

function sameOhlcvSeries(left?: SignalOHLCVBar[], right?: SignalOHLCVBar[]): boolean {
  if (left === right) return true;
  if (!left || !right) return !left && !right;
  if (left.length !== right.length) return false;
  for (let i = 0; i < left.length; i += 1) {
    if (!sameOhlcvBar(left[i], right[i])) return false;
  }
  return true;
}

function sameBubbleDatumForRender(left: BubbleDatum, right: BubbleDatum): boolean {
  return (
    left.id === right.id &&
    left.symbol === right.symbol &&
    left.name === right.name &&
    left.marketCap === right.marketCap &&
    left.volume24h === right.volume24h &&
    left.price === right.price &&
    left.change24h === right.change24h &&
    left.change7d === right.change7d &&
    left.rank === right.rank &&
    left.logoUrl === right.logoUrl &&
    left.exchange === right.exchange &&
    left.timeframe === right.timeframe &&
    left.groupId === right.groupId &&
    left.strategy === right.strategy &&
    left.strategyVersion === right.strategyVersion &&
    left.entry === right.entry &&
    left.exit === right.exit &&
    left.sl === right.sl &&
    left.tp === right.tp &&
    left.action === right.action &&
    left.highSide === right.highSide &&
    left.midSide === right.midSide &&
    left.side === right.side &&
    left.trendType === right.trendType &&
    left.trendingTimestamp === right.trendingTimestamp &&
    left.triggerTimestamp === right.triggerTimestamp &&
    left.sourceType === right.sourceType &&
    sameOhlcvSeries(left.ohlcv, right.ohlcv)
  );
}

function diffRenderState(
  previousDataMap: Map<string, BubbleDatum>,
  nextDataMap: Map<string, BubbleDatum>
): { structural: boolean; patches: BubbleUpdate[] } {
  if (previousDataMap.size !== nextDataMap.size) {
    return { structural: true, patches: [] };
  }

  for (const key of previousDataMap.keys()) {
    if (!nextDataMap.has(key)) {
      return { structural: true, patches: [] };
    }
  }

  const patches: BubbleUpdate[] = [];
  for (const [key, nextItem] of nextDataMap.entries()) {
    const previousItem = previousDataMap.get(key);
    if (!previousItem) {
      return { structural: true, patches: [] };
    }
    if (sameBubbleDatumForRender(previousItem, nextItem)) continue;
    patches.push({ ...nextItem });
  }

  return {
    structural: false,
    patches
  };
}

function canKeepSelectedGroupId(
  selectedGroupId: string | undefined,
  dataList: BubbleDatum[]
): string | undefined {
  const normalized = normalizeGroupId(selectedGroupId);
  if (!normalized) return undefined;
  const visibleCount = dataList.reduce((count, item) => {
    return count + (normalizeGroupId(item.groupId) === normalized ? 1 : 0);
  }, 0);
  return visibleCount >= 2 ? normalized : undefined;
}

type AppState = {
  hasLiveSnapshot: boolean;
  bubbleCount: number;
  sizeMetric: SizeMetric;
  colorMetric: ColorMetric;
  allDataMap: Map<string, BubbleDatum>;
  allDataList: BubbleDatum[];
  dataMap: Map<string, BubbleDatum>;
  dataList: BubbleDatum[];
  renderSnapshotVersion: number;
  renderPatchVersion: number;
  renderPatches: BubbleUpdate[];
  selectedBubbleId?: string;
  selectedGroupId?: string;
  timeframeTabs: TimeframeTab[];
  checkedTimeframes: string[];
  exchangeTabs: ExchangeTab[];
  checkedExchanges: string[];
  searchQuery: string;
  stats: Stats;
  dataSourceStatus: DataSourceStatus;
  backendStatus?: BackendStatus;
  accountSnapshot?: AccountSnapshot;
  positionSnapshot?: PositionSnapshot;
  historySnapshot?: PositionSnapshot;
  groupsSnapshot?: TrendGroupsSnapshot;
  historyLoadMoreHandler?: () => void;
  candleFetchHandler?: (request: BubbleCandlesFetchRequest) => Promise<BubbleCandlesSnapshot | undefined>;
  setSnapshot: (data: BubbleDatum[]) => void;
  applyUpdates: (updates: BubbleUpdate[]) => void;
  setSelectedBubbleId: (id?: string) => void;
  setSelectedGroupId: (groupId?: string) => void;
  clearSelectedBubbleId: () => void;
  clearSelectedGroupId: () => void;
  toggleTimeframeChecked: (timeframe: string) => void;
  toggleExchangeChecked: (exchange: string) => void;
  setSearchQuery: (query: string) => void;
  clearSearchQuery: () => void;
  setStats: (stats: Partial<Stats>) => void;
  setDataSourceStatus: (status: Partial<DataSourceStatus>) => void;
  setBackendStatus: (status?: BackendStatus) => void;
  setAccountSnapshot: (snapshot?: AccountSnapshot) => void;
  setPositionSnapshot: (snapshot?: PositionSnapshot) => void;
  setHistorySnapshot: (snapshot?: PositionSnapshot) => void;
  setGroupsSnapshot: (snapshot?: TrendGroupsSnapshot) => void;
  setHistoryLoadMoreHandler: (handler?: () => void) => void;
  setCandleFetchHandler: (
    handler?: (request: BubbleCandlesFetchRequest) => Promise<BubbleCandlesSnapshot | undefined>
  ) => void;
  setHasLiveSnapshot: (hasLiveSnapshot: boolean) => void;
  requestMoreHistory: () => void;
  requestBubbleCandles: (request: BubbleCandlesFetchRequest) => Promise<BubbleCandlesSnapshot | undefined>;
};

const initialStatus: DataSourceStatus = {
  mode: AppConfig.dataSource,
  restStatus: "idle",
  accountStatus: "idle",
  positionStatus: "idle",
  wsStatus: "idle"
};

function createViewState(
  allDataList: BubbleDatum[],
  allDataMap: Map<string, BubbleDatum>,
  previousTabs: TimeframeTab[],
  previousCheckedTimeframes: string[],
  previousExchangeTabs: ExchangeTab[],
  previousCheckedExchanges: string[],
  searchQuery: string
): Pick<AppState, "allDataList" | "allDataMap" | "dataList" | "dataMap" | "timeframeTabs" | "checkedTimeframes" | "exchangeTabs" | "checkedExchanges"> {
  const activeTimeframes = Array.from(
    new Set(
      allDataList
        .map((item) => item.timeframe || "")
        .filter((timeframe): timeframe is string => timeframe.length > 0)
    )
  ).sort((a, b) => timeframeSortValue(a) - timeframeSortValue(b));

  const timeframeTabs = mergeTimeframeTabs(previousTabs, activeTimeframes);
  const checkedTimeframes = mergeCheckedTimeframes(previousCheckedTimeframes, activeTimeframes);
  const activeExchanges = Array.from(
    new Set(
      allDataList
        .map((item) => (item.exchange || "").trim().toLowerCase())
        .filter((exchange): exchange is string => exchange.length > 0)
    )
  ).sort((a, b) => exchangeSortValue(a) - exchangeSortValue(b));
  const exchangeTabs = mergeExchangeTabs(previousExchangeTabs, activeExchanges);
  const checkedExchanges = mergeCheckedExchanges(previousExchangeTabs, previousCheckedExchanges, activeExchanges);
  const filtered = deriveFilteredData(allDataList, checkedTimeframes, checkedExchanges, searchQuery);

  return {
    allDataList,
    allDataMap,
    dataList: filtered.dataList,
    dataMap: filtered.dataMap,
    timeframeTabs,
    checkedTimeframes,
    exchangeTabs,
    checkedExchanges
  };
}

export const useAppStore = create<AppState>((set, get) => ({
  hasLiveSnapshot: false,
  bubbleCount: AppConfig.initialBubbleCount,
  sizeMetric: AppConfig.initialSizeMetric,
  colorMetric: AppConfig.initialColorMetric,
  allDataMap: new Map(),
  allDataList: [],
  dataMap: new Map(),
  dataList: [],
  renderSnapshotVersion: 0,
  renderPatchVersion: 0,
  renderPatches: [],
  selectedBubbleId: undefined,
  selectedGroupId: undefined,
  timeframeTabs: [],
  checkedTimeframes: [],
  exchangeTabs: [],
  checkedExchanges: [],
  searchQuery: "",
  stats: { fps: 0, dtMs: 0, bubbleCount: 0 },
  dataSourceStatus: initialStatus,
  backendStatus: undefined,
  accountSnapshot: undefined,
  positionSnapshot: undefined,
  historySnapshot: undefined,
  groupsSnapshot: undefined,
  historyLoadMoreHandler: undefined,
  candleFetchHandler: undefined,
  setSnapshot: (data) => {
    const previousSnapshotVersion = get().renderSnapshotVersion;
    const signalDataList = [...data].sort(sortData);
    const signalDataMap = new Map<string, BubbleDatum>();
    for (const item of signalDataList) {
      signalDataMap.set(item.id, item);
    }
    const displayDataList = composeDisplaySignals(signalDataList, get().positionSnapshot);
    const next = createViewState(
      displayDataList,
      signalDataMap,
      get().timeframeTabs,
      get().checkedTimeframes,
      get().exchangeTabs,
      get().checkedExchanges,
      get().searchQuery
    );
    const selectedBubbleId = get().selectedBubbleId;
    const selectedGroupId = canKeepSelectedGroupId(
      get().selectedGroupId,
      next.dataList
    );
    set({
      ...next,
      renderSnapshotVersion: previousSnapshotVersion + 1,
      renderPatches: [],
      selectedBubbleId:
        selectedBubbleId && next.dataMap.has(selectedBubbleId) ? selectedBubbleId : undefined,
      selectedGroupId
    });
  },
  applyUpdates: (updates) => {
    const previousVisibleDataMap = get().dataMap;
    const previousSnapshotVersion = get().renderSnapshotVersion;
    const previousPatchVersion = get().renderPatchVersion;
    const map = new Map(get().allDataMap);
    for (const update of updates) {
      const prev = map.get(update.id);
      if (!prev) continue;
      map.set(update.id, { ...prev, ...update, id: prev.id });
    }
    const signalDataList = Array.from(map.values()).sort(sortData);
    const displayDataList = composeDisplaySignals(signalDataList, get().positionSnapshot);
    const next = createViewState(
      displayDataList,
      map,
      get().timeframeTabs,
      get().checkedTimeframes,
      get().exchangeTabs,
      get().checkedExchanges,
      get().searchQuery
    );
    const renderDelta = diffRenderState(previousVisibleDataMap, next.dataMap);
    const selectedBubbleId = get().selectedBubbleId;
    const selectedGroupId = canKeepSelectedGroupId(
      get().selectedGroupId,
      next.dataList
    );
    set({
      ...next,
      renderSnapshotVersion: renderDelta.structural
        ? previousSnapshotVersion + 1
        : previousSnapshotVersion,
      renderPatchVersion:
        !renderDelta.structural && renderDelta.patches.length > 0
          ? previousPatchVersion + 1
          : previousPatchVersion,
      renderPatches: renderDelta.structural ? [] : renderDelta.patches,
      selectedBubbleId:
        selectedBubbleId && next.dataMap.has(selectedBubbleId) ? selectedBubbleId : undefined,
      selectedGroupId
    });
  },
  setSelectedBubbleId: (id) => {
    if (!id) {
      set({ selectedBubbleId: undefined });
      return;
    }
    if (!get().dataMap.has(id)) return;
    set({ selectedBubbleId: id, selectedGroupId: undefined });
  },
  setSelectedGroupId: (groupId) => {
    const normalized = normalizeGroupId(groupId);
    if (!normalized) {
      set({ selectedGroupId: undefined });
      return;
    }
    const nextSelected = canKeepSelectedGroupId(normalized, get().dataList);
    if (!nextSelected) return;
    set({ selectedGroupId: nextSelected, selectedBubbleId: undefined });
  },
  clearSelectedBubbleId: () => set({ selectedBubbleId: undefined }),
  clearSelectedGroupId: () => set({ selectedGroupId: undefined }),
  toggleTimeframeChecked: (timeframe) => {
    const target = get().timeframeTabs.find((tab) => tab.value === timeframe);
    if (!target || !target.active) return;
    const currentSelected = get().checkedTimeframes[0];
    if (currentSelected === timeframe) return;
    const checkedTimeframes = [timeframe];
    const filtered = deriveFilteredData(
      get().allDataList,
      checkedTimeframes,
      get().checkedExchanges,
      get().searchQuery
    );
    set({
      checkedTimeframes,
      renderSnapshotVersion: get().renderSnapshotVersion + 1,
      renderPatches: [],
      dataList: filtered.dataList,
      dataMap: filtered.dataMap,
      selectedGroupId: canKeepSelectedGroupId(get().selectedGroupId, filtered.dataList)
    });
  },
  toggleExchangeChecked: (exchange) => {
    const normalized = exchange.trim().toLowerCase();
    if (!normalized) return;
    const target = get().exchangeTabs.find((tab) => tab.value === normalized);
    if (!target || !target.active) return;
    const checkedSet = new Set(get().checkedExchanges);
    if (checkedSet.has(normalized)) {
      checkedSet.delete(normalized);
    } else {
      checkedSet.add(normalized);
    }
    const checkedExchanges = Array.from(checkedSet).sort((a, b) => exchangeSortValue(a) - exchangeSortValue(b));
    const filtered = deriveFilteredData(
      get().allDataList,
      get().checkedTimeframes,
      checkedExchanges,
      get().searchQuery
    );
    set({
      checkedExchanges,
      renderSnapshotVersion: get().renderSnapshotVersion + 1,
      renderPatches: [],
      dataList: filtered.dataList,
      dataMap: filtered.dataMap,
      selectedGroupId: canKeepSelectedGroupId(get().selectedGroupId, filtered.dataList)
    });
  },
  setSearchQuery: (searchQuery) => {
    const filtered = deriveFilteredData(
      get().allDataList,
      get().checkedTimeframes,
      get().checkedExchanges,
      searchQuery
    );
    set({
      searchQuery,
      renderSnapshotVersion: get().renderSnapshotVersion + 1,
      renderPatches: [],
      dataList: filtered.dataList,
      dataMap: filtered.dataMap,
      selectedGroupId: canKeepSelectedGroupId(get().selectedGroupId, filtered.dataList)
    });
  },
  clearSearchQuery: () => {
    const filtered = deriveFilteredData(
      get().allDataList,
      get().checkedTimeframes,
      get().checkedExchanges,
      ""
    );
    set({
      searchQuery: "",
      renderSnapshotVersion: get().renderSnapshotVersion + 1,
      renderPatches: [],
      dataList: filtered.dataList,
      dataMap: filtered.dataMap,
      selectedGroupId: canKeepSelectedGroupId(get().selectedGroupId, filtered.dataList)
    });
  },
  setStats: (stats) => set({ stats: { ...get().stats, ...stats } }),
  setDataSourceStatus: (status) =>
    set({ dataSourceStatus: { ...get().dataSourceStatus, ...status } }),
  setBackendStatus: (status) => set({ backendStatus: status }),
  setAccountSnapshot: (snapshot) => set({ accountSnapshot: snapshot }),
  setPositionSnapshot: (snapshot) => {
    const previousPositionSnapshot = get().positionSnapshot;
    const prevFallbackSignature = buildPositionFallbackSignature(previousPositionSnapshot);
    const nextFallbackSignature = buildPositionFallbackSignature(snapshot);
    if (prevFallbackSignature === nextFallbackSignature) {
      set({ positionSnapshot: snapshot });
      return;
    }
    const signalDataMap = get().allDataMap;
    const signalDataList = Array.from(signalDataMap.values()).sort(sortData);
    const displayDataList = composeDisplaySignals(signalDataList, snapshot);
    const next = createViewState(
      displayDataList,
      signalDataMap,
      get().timeframeTabs,
      get().checkedTimeframes,
      get().exchangeTabs,
      get().checkedExchanges,
      get().searchQuery
    );
    const selectedBubbleId = get().selectedBubbleId;
    const selectedGroupId = canKeepSelectedGroupId(
      get().selectedGroupId,
      next.dataList
    );
    set({
      ...next,
      positionSnapshot: snapshot,
      renderSnapshotVersion: get().renderSnapshotVersion + 1,
      renderPatches: [],
      selectedBubbleId:
        selectedBubbleId && next.dataMap.has(selectedBubbleId) ? selectedBubbleId : undefined,
      selectedGroupId
    });
  },
  setHistorySnapshot: (snapshot) => set({ historySnapshot: snapshot }),
  setGroupsSnapshot: (snapshot) => {
    const selectedGroupId = canKeepSelectedGroupId(get().selectedGroupId, get().dataList);
    set({ groupsSnapshot: snapshot, selectedGroupId });
  },
  setHistoryLoadMoreHandler: (handler) => set({ historyLoadMoreHandler: handler }),
  setCandleFetchHandler: (handler) => set({ candleFetchHandler: handler }),
  setHasLiveSnapshot: (hasLiveSnapshot) => set({ hasLiveSnapshot }),
  requestMoreHistory: () => {
    get().historyLoadMoreHandler?.();
  },
  requestBubbleCandles: async (request) => {
    return get().candleFetchHandler?.(request);
  }
}));
