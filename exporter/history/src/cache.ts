import type { HistoryPosition, PositionFilterOptions, PositionRunOption } from "./types";

const POSITION_CACHE_KEY = "visual-history:position-list-cache:v5";
const CACHE_VERSION = 5;
const POSITION_SESSION_CACHE_KEY = "visual-history:position-session-cache:v1";
const POSITION_SESSION_CACHE_VERSION = 1;
const MAX_CACHE_ENTRIES = 12;
const UI_STATE_CACHE_KEY = "visual-history:ui-state-cache:v1";
const UI_STATE_CACHE_VERSION = 2;
const MOBILE_LAYOUT_QUERY = "(max-width: 980px)";

interface PositionCacheEntry {
  key: string;
  loaded_date: string;
  loaded_run_id: string;
  loaded_strategy: string;
  loaded_version: string;
  loaded_exchange: string;
  loaded_symbol: string;
  tz_offset_min: number;
  positions: HistoryPosition[];
  has_more: boolean;
  next_before_ms: number | null;
  filter_options: PositionFilterOptions;
  updated_at_ms: number;
}

interface PositionCacheEnvelope {
  version: number;
  selected_date: string;
  selected_run_id: string;
  selected_strategy: string;
  selected_version: string;
  selected_exchange: string;
  selected_symbol: string;
  entries: PositionCacheEntry[];
}

interface PositionSessionEnvelope {
  version: number;
  selected_date: string;
  selected_run_id: string;
  selected_strategy: string;
  selected_version: string;
  selected_exchange: string;
  selected_symbol: string;
  snapshot: PositionCacheEntry | null;
}

interface UIStateCacheEnvelope {
  version: number;
  selected_position_uid: string;
  loaded_position_uid: string;
  active_timeframe: string;
  chart_maximized: boolean;
  bottom_expanded: boolean;
  // legacy fields for backward compatibility with old cached payload.
  selected_position_key?: string;
  loaded_position_key?: string;
}

export interface CachedPositionSnapshot {
  loadedDate: string;
  loadedRunID: string;
  loadedStrategy: string;
  loadedVersion: string;
  loadedExchange: string;
  loadedSymbol: string;
  positions: HistoryPosition[];
  hasMore: boolean;
  nextBeforeMS: number | null;
  filterOptions: PositionFilterOptions;
  updatedAtMS: number;
}

interface InitialPositionCacheState {
  selectedDate: string;
  selectedRunID: string;
  selectedStrategy: string;
  selectedVersion: string;
  selectedExchange: string;
  selectedSymbol: string;
  snapshot: CachedPositionSnapshot | null;
}

interface SavePositionSelectionParams {
  selectedDate: string;
  selectedRunID: string;
  selectedStrategy: string;
  selectedVersion: string;
  selectedExchange: string;
  selectedSymbol: string;
}

interface SavePositionSnapshotParams extends SavePositionSelectionParams {
  loadedDate: string;
  loadedRunID: string;
  loadedStrategy: string;
  loadedVersion: string;
  loadedExchange: string;
  loadedSymbol: string;
  tzOffsetMin: number;
  positions: HistoryPosition[];
  hasMore: boolean;
  nextBeforeMS: number | null;
  filterOptions: PositionFilterOptions;
}

export interface UIStateCacheState {
  selectedPositionUID: string;
  loadedPositionUID: string;
  activeTimeframe: string;
  chartMaximized: boolean;
  bottomExpanded: boolean;
}

export interface SaveUIStateCacheParams {
  selectedPositionUID: string;
  loadedPositionUID: string;
  activeTimeframe: string;
  chartMaximized: boolean;
  bottomExpanded: boolean;
}

export function loadInitialPositionCacheState(
	tzOffsetMin: number,
	fallbackDate: string
): InitialPositionCacheState {
	const session = readSessionCache();
	if (!session) {
		return {
			selectedDate: fallbackDate,
			selectedRunID: "",
			selectedStrategy: "",
			selectedVersion: "",
			selectedExchange: "",
			selectedSymbol: "",
			snapshot: null
		};
	}

	const selectedDate = isValidDateInput(session.selected_date) ? session.selected_date : fallbackDate;
	const selectedRunID = normalizeFilterValue(session.selected_run_id);
	const selectedStrategy = normalizeFilterValue(session.selected_strategy);
	const selectedVersion = normalizeFilterValue(session.selected_version);
	const selectedExchange = normalizeFilterValue(session.selected_exchange);
	const selectedSymbol = normalizeFilterValue(session.selected_symbol);

  if (!session?.snapshot) {
    return {
      selectedDate,
      selectedRunID,
      selectedStrategy,
      selectedVersion,
      selectedExchange,
      selectedSymbol,
      snapshot: null
    };
  }

  return {
    selectedDate,
    selectedRunID,
    selectedStrategy,
    selectedVersion,
    selectedExchange,
    selectedSymbol,
    snapshot: entryToCachedSnapshot(session.snapshot)
  };
}

export function loadCachedPositionSnapshot(params: {
  date: string;
  tzOffsetMin: number;
  runID?: string;
  strategy?: string;
  version?: string;
  exchange?: string;
  symbol?: string;
}): CachedPositionSnapshot | null {
  const cache = readCache();
  if (!cache || !isValidDateInput(params.date)) {
    return null;
  }
  const key = buildEntryKey(
    params.date,
    params.tzOffsetMin,
    normalizeFilterValue(params.runID),
    normalizeFilterValue(params.strategy),
    normalizeFilterValue(params.version),
    normalizeFilterValue(params.exchange),
    normalizeFilterValue(params.symbol)
  );
  const entry = cache.entries.find((item) => item.key === key);
  return entry ? entryToCachedSnapshot(entry) : null;
}

export function savePositionSelectionCache(params: SavePositionSelectionParams): void {
  if (!isValidDateInput(params.selectedDate)) {
    return;
  }
  const cache = readCache() || createEmptyCache(params.selectedDate);
  cache.selected_date = params.selectedDate;
  cache.selected_run_id = normalizeFilterValue(params.selectedRunID);
  cache.selected_strategy = normalizeFilterValue(params.selectedStrategy);
  cache.selected_version = normalizeFilterValue(params.selectedVersion);
  cache.selected_exchange = normalizeFilterValue(params.selectedExchange);
  cache.selected_symbol = normalizeFilterValue(params.selectedSymbol);
  writeCache(cache);

  const session = readSessionCache() || createEmptySessionCache(params.selectedDate);
  session.selected_date = params.selectedDate;
  session.selected_run_id = normalizeFilterValue(params.selectedRunID);
  session.selected_strategy = normalizeFilterValue(params.selectedStrategy);
  session.selected_version = normalizeFilterValue(params.selectedVersion);
  session.selected_exchange = normalizeFilterValue(params.selectedExchange);
  session.selected_symbol = normalizeFilterValue(params.selectedSymbol);
  writeSessionCache(session);
}

export function savePositionSnapshotCache(params: SavePositionSnapshotParams): void {
  if (!isValidDateInput(params.selectedDate) || !isValidDateInput(params.loadedDate)) {
    return;
  }
  const loadedRunID = normalizeFilterValue(params.loadedRunID);
  const loadedStrategy = normalizeFilterValue(params.loadedStrategy);
  const loadedVersion = normalizeFilterValue(params.loadedVersion);
  const loadedExchange = normalizeFilterValue(params.loadedExchange);
  const loadedSymbol = normalizeFilterValue(params.loadedSymbol);
  const cache = readCache() || createEmptyCache(params.selectedDate);
  const localEntry: PositionCacheEntry = {
    key: buildEntryKey(params.loadedDate, params.tzOffsetMin, loadedRunID, loadedStrategy, loadedVersion, loadedExchange, loadedSymbol),
    loaded_date: params.loadedDate,
    loaded_run_id: loadedRunID,
    loaded_strategy: loadedStrategy,
    loaded_version: loadedVersion,
    loaded_exchange: loadedExchange,
    loaded_symbol: loadedSymbol,
    tz_offset_min: Math.trunc(params.tzOffsetMin),
    positions: Array.isArray(params.positions) ? params.positions : [],
    has_more: Boolean(params.hasMore),
    next_before_ms: toNullableInt(params.nextBeforeMS),
    filter_options: normalizeFilterOptions(params.filterOptions),
    updated_at_ms: Date.now()
  };
  const sessionEntry: PositionCacheEntry = {
    ...localEntry,
    positions: Array.isArray(params.positions) ? params.positions : []
  };

  cache.selected_date = params.selectedDate;
  cache.selected_run_id = normalizeFilterValue(params.selectedRunID);
  cache.selected_strategy = normalizeFilterValue(params.selectedStrategy);
  cache.selected_version = normalizeFilterValue(params.selectedVersion);
  cache.selected_exchange = normalizeFilterValue(params.selectedExchange);
  cache.selected_symbol = normalizeFilterValue(params.selectedSymbol);

  const rest = cache.entries.filter((item) => item.key !== localEntry.key);
  cache.entries = [localEntry, ...rest].slice(0, MAX_CACHE_ENTRIES);
  writeCache(cache);

  const session = readSessionCache() || createEmptySessionCache(params.selectedDate);
  session.selected_date = params.selectedDate;
  session.selected_run_id = normalizeFilterValue(params.selectedRunID);
  session.selected_strategy = normalizeFilterValue(params.selectedStrategy);
  session.selected_version = normalizeFilterValue(params.selectedVersion);
  session.selected_exchange = normalizeFilterValue(params.selectedExchange);
  session.selected_symbol = normalizeFilterValue(params.selectedSymbol);
  session.snapshot = sessionEntry;
  writeSessionCache(session);
}

export function loadUIStateCache(): UIStateCacheState {
  const cache = readUIStateCache();
  const mobileViewport = isMobileViewport();
  if (!cache) {
    return {
      selectedPositionUID: "",
      loadedPositionUID: "",
      activeTimeframe: "",
      chartMaximized: false,
      bottomExpanded: !mobileViewport
    };
  }
  return {
    selectedPositionUID: normalizeFilterValue(cache.selected_position_uid || cache.selected_position_key),
    loadedPositionUID: normalizeFilterValue(cache.loaded_position_uid || cache.loaded_position_key),
    activeTimeframe: normalizeFilterValue(cache.active_timeframe).toLowerCase(),
    chartMaximized: Boolean(cache.chart_maximized),
    bottomExpanded: mobileViewport ? false : Boolean(cache.bottom_expanded)
  };
}

export function saveUIStateCache(params: SaveUIStateCacheParams): void {
  if (typeof window === "undefined" || !window.sessionStorage) {
    return;
  }
  const payload: UIStateCacheEnvelope = {
    version: UI_STATE_CACHE_VERSION,
    selected_position_uid: normalizeFilterValue(params.selectedPositionUID),
    loaded_position_uid: normalizeFilterValue(params.loadedPositionUID),
    active_timeframe: normalizeFilterValue(params.activeTimeframe).toLowerCase(),
    chart_maximized: Boolean(params.chartMaximized),
    bottom_expanded: Boolean(params.bottomExpanded)
  };
  try {
    window.sessionStorage.setItem(UI_STATE_CACHE_KEY, JSON.stringify(payload));
  } catch {
    // Ignore cache write failure (quota/private mode) to keep UI usable.
  }
}

function createEmptyCache(selectedDate: string): PositionCacheEnvelope {
  return {
    version: CACHE_VERSION,
    selected_date: selectedDate,
    selected_run_id: "",
    selected_strategy: "",
    selected_version: "",
    selected_exchange: "",
    selected_symbol: "",
    entries: []
  };
}

function createEmptySessionCache(selectedDate: string): PositionSessionEnvelope {
  return {
    version: POSITION_SESSION_CACHE_VERSION,
    selected_date: selectedDate,
    selected_run_id: "",
    selected_strategy: "",
    selected_version: "",
    selected_exchange: "",
    selected_symbol: "",
    snapshot: null
  };
}

function buildEntryKey(date: string, tzOffsetMin: number, runID: string, strategy: string, version: string, exchange: string, symbol: string): string {
  return [
    date,
    Math.trunc(tzOffsetMin),
    normalizeFilterValue(runID),
    normalizeFilterValue(strategy),
    normalizeFilterValue(version),
    normalizeFilterValue(exchange),
    normalizeFilterValue(symbol)
  ].join("|");
}

function isValidDateInput(date: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(date.trim());
}

function normalizeFilterValue(value: string | null | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeFilterOptions(input: Partial<PositionFilterOptions> | null | undefined): PositionFilterOptions {
  return {
    run_ids: normalizeStringArray(input?.run_ids),
    run_options: normalizeRunOptions(input?.run_options),
    strategies: normalizeStringArray(input?.strategies),
    versions: normalizeStringArray(input?.versions),
    exchanges: normalizeStringArray(input?.exchanges),
    symbols: normalizeStringArray(input?.symbols)
  };
}

function normalizeRunOptions(value: unknown): PositionRunOption[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const seen = new Set<string>();
  const out: PositionRunOption[] = [];
  for (const item of value) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const valueText = typeof (item as { value?: unknown }).value === "string"
      ? (item as { value: string }).value.trim()
      : "";
    if (!valueText || seen.has(valueText)) {
      continue;
    }
    const labelText = typeof (item as { label?: unknown }).label === "string"
      ? (item as { label: string }).label.trim()
      : valueText;
    const singletonIDRaw = (item as { singleton_id?: unknown }).singleton_id;
    const singletonID =
      typeof singletonIDRaw === "number" && Number.isFinite(singletonIDRaw) && singletonIDRaw > 0
        ? Math.trunc(singletonIDRaw)
        : undefined;
    seen.add(valueText);
    out.push({
      value: valueText,
      label: labelText || valueText,
      singleton_id: singletonID
    });
  }
  return out;
}

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of value) {
    const normalized = typeof item === "string" ? item.trim() : "";
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

function toNullableInt(value: number | null | undefined): number | null {
  if (value == null) {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return Math.trunc(parsed);
}

function entryToCachedSnapshot(entry: PositionCacheEntry): CachedPositionSnapshot {
  return {
    loadedDate: entry.loaded_date,
    loadedRunID: entry.loaded_run_id,
    loadedStrategy: entry.loaded_strategy,
    loadedVersion: entry.loaded_version,
    loadedExchange: entry.loaded_exchange,
    loadedSymbol: entry.loaded_symbol,
    positions: Array.isArray(entry.positions) ? entry.positions : [],
    hasMore: Boolean(entry.has_more),
    nextBeforeMS: toNullableInt(entry.next_before_ms),
    filterOptions: normalizeFilterOptions(entry.filter_options),
    updatedAtMS: Number.isFinite(entry.updated_at_ms) ? entry.updated_at_ms : 0
  };
}

function isMobileViewport(): boolean {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
    return false;
  }
  try {
    return window.matchMedia(MOBILE_LAYOUT_QUERY).matches;
  } catch {
    return false;
  }
}

function readCache(): PositionCacheEnvelope | null {
  if (typeof window === "undefined" || !window.localStorage) {
    return null;
  }
  const raw = window.localStorage.getItem(POSITION_CACHE_KEY);
  if (!raw) {
    return null;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") {
    return null;
  }

  const obj = parsed as Partial<PositionCacheEnvelope>;
  if (Number(obj.version) !== CACHE_VERSION) {
    return null;
  }

  const selectedDate = typeof obj.selected_date === "string" ? obj.selected_date.trim() : "";
  const entries = Array.isArray(obj.entries) ? obj.entries : [];
  const normalizedEntries: PositionCacheEntry[] = [];

  for (const item of entries) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const row = item as Partial<PositionCacheEntry>;
    const loadedDate = typeof row.loaded_date === "string" ? row.loaded_date.trim() : "";
    if (!isValidDateInput(loadedDate)) {
      continue;
    }
    const tzOffsetMin = Number(row.tz_offset_min);
    if (!Number.isFinite(tzOffsetMin)) {
      continue;
    }
    const loadedRunID = normalizeFilterValue(row.loaded_run_id);
    const loadedStrategy = normalizeFilterValue(row.loaded_strategy);
    const loadedVersion = normalizeFilterValue(row.loaded_version);
    const loadedExchange = normalizeFilterValue(row.loaded_exchange);
    const loadedSymbol = normalizeFilterValue(row.loaded_symbol);
    normalizedEntries.push({
      key: buildEntryKey(loadedDate, tzOffsetMin, loadedRunID, loadedStrategy, loadedVersion, loadedExchange, loadedSymbol),
      loaded_date: loadedDate,
      loaded_run_id: loadedRunID,
      loaded_strategy: loadedStrategy,
      loaded_version: loadedVersion,
      loaded_exchange: loadedExchange,
      loaded_symbol: loadedSymbol,
      tz_offset_min: Math.trunc(tzOffsetMin),
      positions: Array.isArray(row.positions) ? (row.positions as HistoryPosition[]) : [],
      has_more: Boolean(row.has_more),
      next_before_ms: toNullableInt(row.next_before_ms),
      filter_options: normalizeFilterOptions(row.filter_options),
      updated_at_ms: Number.isFinite(row.updated_at_ms) ? Number(row.updated_at_ms) : 0
    });
  }

  return {
    version: CACHE_VERSION,
    selected_date: isValidDateInput(selectedDate) ? selectedDate : "",
    selected_run_id: normalizeFilterValue(obj.selected_run_id),
    selected_strategy: normalizeFilterValue(obj.selected_strategy),
    selected_version: normalizeFilterValue(obj.selected_version),
    selected_exchange: normalizeFilterValue(obj.selected_exchange),
    selected_symbol: normalizeFilterValue(obj.selected_symbol),
    entries: normalizedEntries.slice(0, MAX_CACHE_ENTRIES)
  };
}

function writeCache(cache: PositionCacheEnvelope): void {
  if (typeof window === "undefined" || !window.localStorage) {
    return;
  }
  try {
    window.localStorage.setItem(POSITION_CACHE_KEY, JSON.stringify(cache));
  } catch {
    // Ignore cache write failure (quota/private mode) to keep UI usable.
  }
}

function readSessionCache(): PositionSessionEnvelope | null {
  if (typeof window === "undefined" || !window.sessionStorage) {
    return null;
  }
  const raw = window.sessionStorage.getItem(POSITION_SESSION_CACHE_KEY);
  if (!raw) {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") {
    return null;
  }
  const obj = parsed as Partial<PositionSessionEnvelope>;
  if (Number(obj.version) !== POSITION_SESSION_CACHE_VERSION) {
    return null;
  }
  const snapshotRaw = obj.snapshot;
  let snapshot: PositionCacheEntry | null = null;
  if (snapshotRaw && typeof snapshotRaw === "object") {
    const row = snapshotRaw as Partial<PositionCacheEntry>;
    const loadedDate = typeof row.loaded_date === "string" ? row.loaded_date.trim() : "";
    const tzOffsetMin = Number(row.tz_offset_min);
    if (isValidDateInput(loadedDate) && Number.isFinite(tzOffsetMin)) {
      const loadedRunID = normalizeFilterValue(row.loaded_run_id);
      const loadedStrategy = normalizeFilterValue(row.loaded_strategy);
      const loadedVersion = normalizeFilterValue(row.loaded_version);
      const loadedExchange = normalizeFilterValue(row.loaded_exchange);
      const loadedSymbol = normalizeFilterValue(row.loaded_symbol);
      snapshot = {
        key: buildEntryKey(loadedDate, tzOffsetMin, loadedRunID, loadedStrategy, loadedVersion, loadedExchange, loadedSymbol),
        loaded_date: loadedDate,
        loaded_run_id: loadedRunID,
        loaded_strategy: loadedStrategy,
        loaded_version: loadedVersion,
        loaded_exchange: loadedExchange,
        loaded_symbol: loadedSymbol,
        tz_offset_min: Math.trunc(tzOffsetMin),
        positions: Array.isArray(row.positions) ? (row.positions as HistoryPosition[]) : [],
        has_more: Boolean(row.has_more),
        next_before_ms: toNullableInt(row.next_before_ms),
        filter_options: normalizeFilterOptions(row.filter_options),
        updated_at_ms: Number.isFinite(row.updated_at_ms) ? Number(row.updated_at_ms) : 0
      };
    }
  }
  return {
    version: POSITION_SESSION_CACHE_VERSION,
    selected_date: isValidDateInput(typeof obj.selected_date === "string" ? obj.selected_date.trim() : "") ? String(obj.selected_date).trim() : "",
    selected_run_id: normalizeFilterValue(obj.selected_run_id),
    selected_strategy: normalizeFilterValue(obj.selected_strategy),
    selected_version: normalizeFilterValue(obj.selected_version),
    selected_exchange: normalizeFilterValue(obj.selected_exchange),
    selected_symbol: normalizeFilterValue(obj.selected_symbol),
    snapshot
  };
}

function writeSessionCache(cache: PositionSessionEnvelope): void {
  if (typeof window === "undefined" || !window.sessionStorage) {
    return;
  }
  try {
    window.sessionStorage.setItem(POSITION_SESSION_CACHE_KEY, JSON.stringify(cache));
  } catch {
    // Ignore cache write failure (quota/private mode) to keep UI usable.
  }
}

function readUIStateCache(): UIStateCacheEnvelope | null {
  if (typeof window === "undefined" || !window.sessionStorage) {
    return null;
  }
  const raw = window.sessionStorage.getItem(UI_STATE_CACHE_KEY);
  if (!raw) {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") {
    return null;
  }
  const obj = parsed as Partial<UIStateCacheEnvelope>;
  const version = Number(obj.version);
  if (version !== 1 && version !== UI_STATE_CACHE_VERSION) {
    return null;
  }
  const selectedUID = normalizeFilterValue(
    obj.selected_position_uid || obj.selected_position_key
  );
  const loadedUID = normalizeFilterValue(
    obj.loaded_position_uid || obj.loaded_position_key
  );
  return {
    version: UI_STATE_CACHE_VERSION,
    selected_position_uid: selectedUID,
    loaded_position_uid: loadedUID,
    active_timeframe: normalizeFilterValue(obj.active_timeframe).toLowerCase(),
    chart_maximized: Boolean(obj.chart_maximized),
    bottom_expanded: Boolean(obj.bottom_expanded)
  };
}
