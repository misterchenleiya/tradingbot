import { useEffect, useMemo, useRef, useState } from "react";
import { fetchCandles, fetchEvents, fetchPositions, loadCandles } from "./api";
import {
  loadCachedPositionSnapshot,
  loadUIStateCache,
  loadInitialPositionCacheState,
  saveUIStateCache,
  savePositionSnapshotCache,
  savePositionSelectionCache
} from "./cache";
import { ChartPanel, type ChartPanelHandle } from "./components/ChartPanel";
import { EventPanel } from "./components/EventPanel";
import { PositionList } from "./components/PositionList";
import { getDigitIndex, shouldIgnoreShortcut } from "./shortcuts";
import type {
  HistoryEvent,
  HistoryPosition,
  IntegrityResponse,
  PositionFilterOptions,
  PositionRunOption,
  TimeframeCandles
} from "./types";

export function App(): JSX.Element {
  const initRef = useRef<{
    tzOffsetMin: number;
    selectedDate: string;
    selectedRunID: string;
    selectedStrategy: string;
    selectedVersion: string;
    selectedExchange: string;
    selectedSymbol: string;
    loadedDate: string;
    loadedRunID: string;
    loadedStrategy: string;
    loadedVersion: string;
    loadedExchange: string;
    loadedSymbol: string;
    positions: HistoryPosition[];
    filterOptions: PositionFilterOptions;
    hasMore: boolean;
    nextBefore: number | null;
    hasSnapshot: boolean;
    shouldAutoLoadInitial: boolean;
    urlPreset: URLInitialFilters | null;
    uiState: ReturnType<typeof loadUIStateCache>;
  } | null>(null);
  if (initRef.current == null) {
    const tzOffsetMin = new Date().getTimezoneOffset();
    const today = formatDateInput(new Date());
    const initial = loadInitialPositionCacheState(tzOffsetMin, today);
    const uiState = loadUIStateCache();
    const urlPreset = readInitialFiltersFromURL(today);
    const useURLPreset = urlPreset.hasPreset;
    const initialSnapshot =
      useURLPreset && initial.snapshot && !matchesSnapshotFilters(initial.snapshot, urlPreset)
        ? null
        : initial.snapshot;
    initRef.current = {
      tzOffsetMin,
      selectedDate: useURLPreset ? urlPreset.selectedDate : initial.selectedDate,
      selectedRunID: useURLPreset ? urlPreset.selectedRunID : initial.selectedRunID,
      selectedStrategy: useURLPreset ? urlPreset.selectedStrategy : initial.selectedStrategy,
      selectedVersion: useURLPreset ? urlPreset.selectedVersion : initial.selectedVersion,
      selectedExchange: useURLPreset ? urlPreset.selectedExchange : initial.selectedExchange,
      selectedSymbol: useURLPreset ? urlPreset.selectedSymbol : initial.selectedSymbol,
      loadedDate: initialSnapshot?.loadedDate || (useURLPreset ? urlPreset.selectedDate : initial.selectedDate),
      loadedRunID: initialSnapshot?.loadedRunID || (useURLPreset ? urlPreset.selectedRunID : initial.selectedRunID),
      loadedStrategy: initialSnapshot?.loadedStrategy || (useURLPreset ? urlPreset.selectedStrategy : initial.selectedStrategy),
      loadedVersion: initialSnapshot?.loadedVersion || (useURLPreset ? urlPreset.selectedVersion : initial.selectedVersion),
      loadedExchange: initialSnapshot?.loadedExchange || (useURLPreset ? urlPreset.selectedExchange : initial.selectedExchange),
      loadedSymbol: initialSnapshot?.loadedSymbol || (useURLPreset ? urlPreset.selectedSymbol : initial.selectedSymbol),
      positions: initialSnapshot?.positions || [],
      filterOptions: useURLPreset
        ? normalizePositionFilterOptions(undefined)
        : normalizePositionFilterOptions(initialSnapshot?.filterOptions),
      hasMore: initialSnapshot?.hasMore || false,
      nextBefore: initialSnapshot?.nextBeforeMS ?? null,
      hasSnapshot: Boolean(initialSnapshot),
      shouldAutoLoadInitial: useURLPreset && !Boolean(initialSnapshot),
      urlPreset: useURLPreset ? urlPreset : null,
      uiState
    };
  }

  const [selectedDate, setSelectedDate] = useState<string>(initRef.current.selectedDate);
  const [selectedRunID, setSelectedRunID] = useState<string>(initRef.current.selectedRunID);
  const [selectedStrategy, setSelectedStrategy] = useState<string>(initRef.current.selectedStrategy);
  const [selectedVersion, setSelectedVersion] = useState<string>(initRef.current.selectedVersion);
  const [selectedExchange, setSelectedExchange] = useState<string>(initRef.current.selectedExchange);
  const [selectedSymbol, setSelectedSymbol] = useState<string>(initRef.current.selectedSymbol);
  const [loadedDate, setLoadedDate] = useState<string>(initRef.current.loadedDate);
  const [loadedRunID, setLoadedRunID] = useState<string>(initRef.current.loadedRunID);
  const [loadedStrategy, setLoadedStrategy] = useState<string>(initRef.current.loadedStrategy);
  const [loadedVersion, setLoadedVersion] = useState<string>(initRef.current.loadedVersion);
  const [loadedExchange, setLoadedExchange] = useState<string>(initRef.current.loadedExchange);
  const [loadedSymbol, setLoadedSymbol] = useState<string>(initRef.current.loadedSymbol);
  const [positions, setPositions] = useState<HistoryPosition[]>(initRef.current.positions);
  const [filterOptions, setFilterOptions] = useState<PositionFilterOptions>(initRef.current.filterOptions);
  const [hasMore, setHasMore] = useState(initRef.current.hasMore);
  const [nextBefore, setNextBefore] = useState<number | null>(initRef.current.nextBefore);
  const [loadingPositions, setLoadingPositions] = useState(false);
  const loadingPositionsRef = useRef(false);
  const autoLoadHandledRef = useRef<boolean>(!initRef.current.shouldAutoLoadInitial);
  const selectedPositionUIDRef = useRef<string>("");
  const selectRequestRef = useRef(0);
  const [selectedPosition, setSelectedPosition] = useState<HistoryPosition | null>(null);
  const [events, setEvents] = useState<HistoryEvent[]>([]);
  const [candlesByTF, setCandlesByTF] = useState<Record<string, TimeframeCandles>>({});
  const [activeTimeframe, setActiveTimeframe] = useState<string>(initRef.current.uiState.activeTimeframe);
  const [integrity, setIntegrity] = useState<IntegrityResponse | null>(null);

  const [chartLoading, setChartLoading] = useState(false);
  const [loadedPositionUID, setLoadedPositionUID] = useState<string>("");
  const [loadProgress, setLoadProgress] = useState("");

  const [bottomExpanded, setBottomExpanded] = useState(initRef.current.uiState.bottomExpanded);
  const [chartMaximized, setChartMaximized] = useState(initRef.current.uiState.chartMaximized);
  const [integrityText, setIntegrityText] = useState("-");
  const [autoLoadNotice, setAutoLoadNotice] = useState<AutoLoadNotice | null>(null);
  const chartPanelRef = useRef<ChartPanelHandle | null>(null);
  const restoreHandledRef = useRef<boolean>(initRef.current.shouldAutoLoadInitial || !initRef.current.hasSnapshot);
  const restoreRunningRef = useRef(false);

  const chartPosition = useMemo(() => {
    if (!loadedPositionUID) {
      return selectedPosition;
    }
    if (positionUIDOf(selectedPosition) === loadedPositionUID) {
      return selectedPosition;
    }
    const matched = positions.find((item) => positionUIDOf(item) === loadedPositionUID);
    return matched || selectedPosition;
  }, [loadedPositionUID, positions, selectedPosition]);

  useEffect(() => {
    savePositionSelectionCache({
      selectedDate,
      selectedRunID,
      selectedStrategy,
      selectedVersion,
      selectedExchange,
      selectedSymbol
    });
  }, [selectedDate, selectedRunID, selectedStrategy, selectedVersion, selectedExchange, selectedSymbol]);

  useEffect(() => {
    saveUIStateCache({
      selectedPositionUID: positionUIDOf(selectedPosition),
      loadedPositionUID,
      activeTimeframe,
      chartMaximized,
      bottomExpanded
    });
  }, [selectedPosition, loadedPositionUID, activeTimeframe, chartMaximized, bottomExpanded]);

  useEffect(() => {
    if (!initRef.current?.shouldAutoLoadInitial) {
      return;
    }
    void reloadPositions();
  }, []);

  const statusText = useMemo(() => {
    if (!chartPosition) {
      return "未选择仓位";
    }
    return `${chartPosition.exchange} / ${chartPosition.symbol} / ${activeTimeframe || "-"}`;
  }, [activeTimeframe, chartPosition]);

  const loadedTimeframes = useMemo(() => {
    const keys = Object.keys(candlesByTF).map((item) => item.trim()).filter((item) => item.length > 0);
    return sortTimeframes(keys);
  }, [candlesByTF]);

  async function maybeAutoLoadFromURL(loadedPositions: HistoryPosition[]): Promise<void> {
    if (autoLoadHandledRef.current) {
      return;
    }
    autoLoadHandledRef.current = true;
    const preset = initRef.current?.urlPreset;
    if (!preset) {
      return;
    }
    const matched = loadedPositions.filter((item) => matchesURLPreset(item, preset));
    if (matched.length !== 1) {
      setAutoLoadNotice(buildAutoLoadNotice(preset, matched.length));
      return;
    }
    setAutoLoadNotice(null);
    const target = matched[0];
    await handleSelectPosition(target);
    await handleLoadChart(target);
  }

  function applyPositionSnapshot(
    snapshot: {
      loadedDate: string;
      loadedRunID: string;
      loadedStrategy: string;
      loadedVersion: string;
      positions: HistoryPosition[];
      hasMore: boolean;
      nextBeforeMS: number | null;
      filterOptions: PositionFilterOptions;
    },
    loadedContext: {
      loadedDate: string;
      loadedRunID: string;
      loadedStrategy: string;
      loadedVersion: string;
      loadedExchange: string;
      loadedSymbol: string;
    }
  ): void {
    setPositions(snapshot.positions);
    setFilterOptions(snapshot.filterOptions);
    setHasMore(snapshot.hasMore);
    setNextBefore(snapshot.nextBeforeMS);
    setSelectedPosition(null);
    selectedPositionUIDRef.current = "";
    setEvents([]);
    setCandlesByTF({});
    setActiveTimeframe("");
    setIntegrity(null);
    setLoadedPositionUID("");
    setIntegrityText("-");
    setLoadedDate(loadedContext.loadedDate);
    setLoadedRunID(loadedContext.loadedRunID);
    setLoadedStrategy(loadedContext.loadedStrategy);
    setLoadedVersion(loadedContext.loadedVersion);
    setLoadedExchange(loadedContext.loadedExchange);
    setLoadedSymbol(loadedContext.loadedSymbol);
  }

  async function reloadPositions(): Promise<void> {
    if (loadingPositionsRef.current) {
      return;
    }
    loadingPositionsRef.current = true;
    setLoadingPositions(true);
    selectRequestRef.current += 1;
    const requestID = selectRequestRef.current;
    const cachedSnapshot = loadCachedPositionSnapshot({
      date: selectedDate,
      tzOffsetMin: initRef.current?.tzOffsetMin || 0,
      runID: selectedRunID,
      strategy: selectedStrategy,
      version: selectedVersion,
      exchange: selectedExchange,
      symbol: selectedSymbol
    });
    if (cachedSnapshot) {
      applyPositionSnapshot(cachedSnapshot, {
        loadedDate: selectedDate,
        loadedRunID: selectedRunID,
        loadedStrategy: selectedStrategy,
        loadedVersion: selectedVersion,
        loadedExchange: selectedExchange,
        loadedSymbol: selectedSymbol
      });
    }
    try {
      const response = await fetchPositions({
        date: selectedDate,
        exchange: selectedExchange,
        symbol: selectedSymbol,
        run_id: selectedRunID,
        strategy: selectedStrategy,
        version: selectedVersion,
        limit: 50,
        tz_offset_min: initRef.current?.tzOffsetMin
      });
      if (requestID !== selectRequestRef.current) {
        return;
      }
      const normalizedFilterOptions = normalizePositionFilterOptions(response.filter_options);
      applyPositionSnapshot(
        {
          loadedDate: selectedDate,
          loadedRunID: selectedRunID,
          loadedStrategy: selectedStrategy,
          loadedVersion: selectedVersion,
          positions: response.positions,
          hasMore: response.has_more,
          nextBeforeMS: response.next_before_ms || null,
          filterOptions: normalizedFilterOptions
        },
        {
          loadedDate: selectedDate,
          loadedRunID: selectedRunID,
          loadedStrategy: selectedStrategy,
          loadedVersion: selectedVersion,
          loadedExchange: selectedExchange,
          loadedSymbol: selectedSymbol
        }
      );
      savePositionSnapshotCache({
        selectedDate,
        selectedRunID,
        selectedStrategy,
        selectedVersion,
        selectedExchange,
        selectedSymbol,
        loadedDate: selectedDate,
        loadedRunID: selectedRunID,
        loadedStrategy: selectedStrategy,
        loadedVersion: selectedVersion,
        loadedExchange: selectedExchange,
        loadedSymbol: selectedSymbol,
        tzOffsetMin: initRef.current?.tzOffsetMin || 0,
        positions: response.positions,
        filterOptions: normalizedFilterOptions,
        hasMore: response.has_more,
        nextBeforeMS: response.next_before_ms || null
      });
      await maybeAutoLoadFromURL(response.positions);
    } catch (error) {
      window.alert(`加载历史仓位失败: ${toErrorMessage(error)}`);
    } finally {
      loadingPositionsRef.current = false;
      setLoadingPositions(false);
    }
  }

  async function loadMorePositions(): Promise<void> {
    if (loadingPositionsRef.current || !hasMore || !nextBefore) {
      return;
    }
    loadingPositionsRef.current = true;
    setLoadingPositions(true);
    try {
      const response = await fetchPositions({
        date: loadedDate,
        exchange: loadedExchange,
        symbol: loadedSymbol,
        run_id: loadedRunID,
        strategy: loadedStrategy,
        version: loadedVersion,
        before: nextBefore,
        limit: 50,
        tz_offset_min: initRef.current?.tzOffsetMin
      });
      const normalizedFilterOptions = normalizePositionFilterOptions(response.filter_options);
      setFilterOptions(normalizedFilterOptions);
      setPositions((prev) => {
        const merged = mergePositions(prev, response.positions);
        savePositionSnapshotCache({
          selectedDate,
          selectedRunID,
          selectedStrategy,
          selectedVersion,
          selectedExchange,
          selectedSymbol,
          loadedDate,
          loadedRunID,
          loadedStrategy,
          loadedVersion,
          loadedExchange,
          loadedSymbol,
          tzOffsetMin: initRef.current?.tzOffsetMin || 0,
          positions: merged,
          filterOptions: normalizedFilterOptions,
          hasMore: response.has_more,
          nextBeforeMS: response.next_before_ms || null
        });
        return merged;
      });
      setHasMore(response.has_more);
      setNextBefore(response.next_before_ms || null);
    } catch (error) {
      window.alert(`加载更早仓位失败: ${toErrorMessage(error)}`);
    } finally {
      loadingPositionsRef.current = false;
      setLoadingPositions(false);
    }
  }

  async function handleSelectPosition(position: HistoryPosition): Promise<void> {
    setAutoLoadNotice(null);
    const requestID = ++selectRequestRef.current;
    setSelectedPosition(position);
    const targetUID = positionUIDOf(position);
    selectedPositionUIDRef.current = targetUID;
    if (!loadedPositionUID || loadedPositionUID === targetUID) {
      setIntegrity(null);
      setIntegrityText("-");
    } else {
      return;
    }
    try {
      const response = await fetchEvents(position.id);
      if (selectRequestRef.current !== requestID) {
        return;
      }
      setEvents(response.events);
    } catch (error) {
      if (selectRequestRef.current !== requestID) {
        return;
      }
      setEvents([]);
      setIntegrityText(`事件加载失败: ${toErrorMessage(error)}`);
    }
  }

  async function handleLoadChart(position: HistoryPosition, preferredTimeframe?: string): Promise<void> {
    if (chartLoading) {
      return;
    }
    setChartLoading(true);
    setLoadProgress("检查主库K线完整性...");

    try {
      await loadCandles(position.id, {
        timeframes: position.timeframes,
        force: true
      });
      setLoadProgress("读取主库K线与事件...");
      const [candlesResponse, eventsResponse] = await Promise.all([
        fetchCandles(position.id),
        fetchEvents(position.id)
      ]);
      const normalizedTimeframes = normalizeTimeframeBlocks(candlesResponse.timeframes);
      const keys = Object.keys(normalizedTimeframes);
      if (keys.length === 0) {
        throw new Error("K线数据为空");
      }
      const sortedKeys = sortTimeframes(keys);
      if (selectedPositionUIDRef.current !== positionUIDOf(position)) {
        return;
      }
      const normalizedPreferred = resolveNormalizedTimeframeKey(normalizedTimeframes, preferredTimeframe || "");
      setSelectedPosition(position);
      setPositions((prev) => upsertPosition(prev, position));
      setCandlesByTF(normalizedTimeframes);
      setIntegrity(candlesResponse.integrity);
      setIntegrityText(formatIntegritySummary(candlesResponse.integrity));
      setActiveTimeframe((prev) => {
        if (normalizedPreferred && normalizedTimeframes[normalizedPreferred]) {
          return normalizedPreferred;
        }
        return prev && normalizedTimeframes[prev] ? prev : pickPreferredTimeframe(sortedKeys, position.timeframes);
      });
      setLoadedPositionUID(positionUIDOf(position));
      setEvents(eventsResponse.events);
      setLoadProgress("K线加载完成");
    } catch (error) {
      const detail = toErrorMessage(error);
      if (detail.includes("history_candles_incomplete")) {
        const missingTimeframe = detail.split(":").pop()?.trim() || "";
        const suffix = missingTimeframe ? `（周期：${missingTimeframe}）` : "";
        setIntegrityText(`历史K线不完整，拒绝查看${suffix}`);
        window.alert(`该历史仓位所需K线不完整，无法查看${suffix}`);
        return;
      }
      const message = `加载K线失败: ${detail}`;
      const retry = window.confirm(`${message}\n是否重试？`);
      if (retry) {
        await handleLoadChart(position, preferredTimeframe);
        return;
      }
    } finally {
      setChartLoading(false);
      setTimeout(() => setLoadProgress(""), 1200);
    }
  }

  useEffect(() => {
    if (restoreHandledRef.current) {
      return;
    }
    if (restoreRunningRef.current) {
      return;
    }
    if (loadingPositions) {
      return;
    }
    const cached = initRef.current?.uiState;
    if (!cached) {
      restoreHandledRef.current = true;
      return;
    }
    const targetUID = (cached.loadedPositionUID || cached.selectedPositionUID || "").trim();
    if (!targetUID) {
      restoreHandledRef.current = true;
      return;
    }
    void (async () => {
      restoreRunningRef.current = true;
      const matched = positions.find((item) => {
        const uid = positionUIDOf(item);
        if (uid === targetUID) {
          return true;
        }
        return Number.isFinite(item.id) && item.id > 0 && targetUID === `h:${Math.trunc(item.id)}`;
      });
      if (!matched) {
        if (positions.length > 0) {
          restoreHandledRef.current = true;
        }
        restoreRunningRef.current = false;
        return;
      }
      await handleSelectPosition(matched);
      await handleLoadChart(matched, cached.activeTimeframe);
      restoreHandledRef.current = true;
      restoreRunningRef.current = false;
    })();
  }, [loadingPositions, positions]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (shouldIgnoreShortcut(event)) {
        return;
      }
      const key = event.key;
      const lower = key.toLowerCase();
      const hasCtrlOrMeta = event.ctrlKey || event.metaKey;

      if (key === "?" && !hasCtrlOrMeta && !event.altKey) {
        event.preventDefault();
        chartPanelRef.current?.toggleShortcutModal();
        return;
      }

      if (!hasCtrlOrMeta && !event.altKey && event.shiftKey && lower === "f") {
        event.preventDefault();
        setChartMaximized((prev) => !prev);
        return;
      }

      if (!selectedPosition) {
        return;
      }

      if (event.altKey && !hasCtrlOrMeta && !event.shiftKey && (event.code === "KeyR" || lower === "r")) {
        event.preventDefault();
        chartPanelRef.current?.resetView();
        return;
      }
      if (hasCtrlOrMeta && !event.altKey && !event.shiftKey && key === "ArrowUp") {
        event.preventDefault();
        chartPanelRef.current?.zoomIn();
        return;
      }
      if (hasCtrlOrMeta && !event.altKey && !event.shiftKey && key === "ArrowDown") {
        event.preventDefault();
        chartPanelRef.current?.zoomOut();
        return;
      }
      if (!hasCtrlOrMeta && !event.altKey && !event.shiftKey && key === "ArrowLeft") {
        event.preventDefault();
        chartPanelRef.current?.panLeft();
        return;
      }
      if (!hasCtrlOrMeta && !event.altKey && !event.shiftKey && key === "ArrowRight") {
        event.preventDefault();
        chartPanelRef.current?.panRight();
        return;
      }
      if (!event.altKey && !hasCtrlOrMeta && !event.shiftKey) {
        const index = getDigitIndex(key);
        if (index == null) {
          return;
        }
        const next = loadedTimeframes[index];
        if (!next) {
          return;
        }
        event.preventDefault();
        setActiveTimeframe(next);
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [loadedTimeframes, selectedPosition]);

  return (
    <div className="vh-root">
      {chartLoading ? (
        <div className="vh-progress">
          <div className="vh-progress-bar" />
          <span>{loadProgress}</span>
        </div>
      ) : null}
      {autoLoadNotice ? (
        <section className="vh-autoload-notice" role="status">
          <div className="vh-autoload-notice-title">{autoLoadNotice.summary}</div>
          <div className="vh-autoload-notice-detail">{autoLoadNotice.detail}</div>
        </section>
      ) : null}

      <main className={chartMaximized ? "vh-main is-chart-maximized" : "vh-main"}>
        <section
          className={`vh-left-column ${bottomExpanded ? "is-events-expanded" : "is-events-collapsed"} ${
            chartMaximized ? "is-chart-maximized" : ""
          }`}
        >
          <ChartPanel
            ref={chartPanelRef}
            position={chartPosition}
            candlesByTF={candlesByTF}
            activeTimeframe={activeTimeframe}
            onTimeframeChange={setActiveTimeframe}
            chartMaximized={chartMaximized}
            onToggleMaximized={() => setChartMaximized((prev) => !prev)}
            loading={chartLoading}
            loadProgress={loadProgress}
            events={events}
            integrity={integrity}
          />
          {!chartMaximized ? (
            <EventPanel events={events} expanded={bottomExpanded} onToggle={() => setBottomExpanded((prev) => !prev)} />
          ) : null}
        </section>

        <PositionList
          positions={positions}
          selectedPosition={selectedPosition}
          selectedDate={selectedDate}
          selectedRunID={selectedRunID}
          selectedStrategy={selectedStrategy}
          selectedVersion={selectedVersion}
          selectedExchange={selectedExchange}
          selectedSymbol={selectedSymbol}
          filterOptions={filterOptions}
          loading={loadingPositions}
          chartLoading={chartLoading}
          loadedPositionUID={loadedPositionUID}
          hasMore={hasMore}
          onDateChange={setSelectedDate}
          onRunIDChange={setSelectedRunID}
          onStrategyChange={setSelectedStrategy}
          onVersionChange={setSelectedVersion}
          onExchangeChange={setSelectedExchange}
          onSymbolChange={setSelectedSymbol}
          onSelectPosition={(position) => {
            void handleSelectPosition(position);
          }}
          onLoadChart={(position) => {
            void handleLoadChart(position);
          }}
          onLoadMore={() => {
            void loadMorePositions();
          }}
          onRefresh={() => {
            void reloadPositions();
          }}
        />
      </main>

      <footer className="vh-statusbar">
        <span>API: /visual-history/api/v1</span>
        <span>{statusText}</span>
        <span>{integrityText}</span>
      </footer>
    </div>
  );
}

type URLInitialFilters = {
  hasPreset: boolean;
  selectedDate: string;
  selectedRunID: string;
  selectedStrategy: string;
  selectedVersion: string;
  selectedExchange: string;
  selectedSymbol: string;
};

type AutoLoadNotice = {
  summary: string;
  detail: string;
};

function readInitialFiltersFromURL(fallbackDate: string): URLInitialFilters {
  if (typeof window === "undefined") {
    return {
      hasPreset: false,
      selectedDate: fallbackDate,
      selectedRunID: "",
      selectedStrategy: "",
      selectedVersion: "",
      selectedExchange: "",
      selectedSymbol: ""
    };
  }

  const search = new URLSearchParams(window.location.search);
  const selectedRunID = normalizeFilterValue(search.get("run_id"));
  const selectedStrategy = normalizeFilterValue(search.get("strategy"));
  const selectedVersion = normalizeFilterValue(search.get("version"));
  const selectedExchange = normalizeFilterValue(search.get("exchange"));
  const selectedSymbol = normalizeFilterValue(search.get("symbol"));

  const dateRaw = normalizeFilterValue(search.get("date") || search.get("open_date"));
  const selectedDate = isValidDateInput(dateRaw) ? dateRaw : fallbackDate;
  const hasPreset = Boolean(
    dateRaw ||
      selectedRunID ||
      selectedStrategy ||
      selectedVersion ||
      selectedExchange ||
      selectedSymbol
  );

  return {
    hasPreset,
    selectedDate,
    selectedRunID,
    selectedStrategy,
    selectedVersion,
    selectedExchange,
    selectedSymbol
  };
}

function normalizeFilterValue(value: string | null | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeMatchToken(value: string | null | undefined): string {
  return normalizeFilterValue(value).toLowerCase();
}

function matchesURLPreset(position: HistoryPosition, preset: URLInitialFilters): boolean {
  if (preset.selectedRunID && normalizeMatchToken(position.run_id) !== normalizeMatchToken(preset.selectedRunID)) {
    return false;
  }
  if (preset.selectedStrategy && normalizeMatchToken(position.strategy_name) !== normalizeMatchToken(preset.selectedStrategy)) {
    return false;
  }
  if (preset.selectedVersion && normalizeMatchToken(position.strategy_version) !== normalizeMatchToken(preset.selectedVersion)) {
    return false;
  }
  if (preset.selectedExchange && normalizeMatchToken(position.exchange) !== normalizeMatchToken(preset.selectedExchange)) {
    return false;
  }
  if (preset.selectedSymbol && normalizeMatchToken(position.symbol) !== normalizeMatchToken(preset.selectedSymbol)) {
    return false;
  }
  return true;
}

function matchesSnapshotFilters(
  snapshot: {
    loadedDate: string;
    loadedRunID: string;
    loadedStrategy: string;
    loadedVersion: string;
    loadedExchange: string;
    loadedSymbol: string;
  },
  preset: URLInitialFilters
): boolean {
  return (
    normalizeFilterValue(snapshot.loadedDate) === normalizeFilterValue(preset.selectedDate) &&
    normalizeMatchToken(snapshot.loadedRunID) === normalizeMatchToken(preset.selectedRunID) &&
    normalizeMatchToken(snapshot.loadedStrategy) === normalizeMatchToken(preset.selectedStrategy) &&
    normalizeMatchToken(snapshot.loadedVersion) === normalizeMatchToken(preset.selectedVersion) &&
    normalizeMatchToken(snapshot.loadedExchange) === normalizeMatchToken(preset.selectedExchange) &&
    normalizeMatchToken(snapshot.loadedSymbol) === normalizeMatchToken(preset.selectedSymbol)
  );
}

function buildAutoLoadNotice(preset: URLInitialFilters, matchedCount: number): AutoLoadNotice {
  const summary =
    matchedCount <= 0
      ? "未找到匹配仓位，已取消自动加载 K 线"
      : `匹配到 ${matchedCount} 条仓位，已取消自动加载 K 线`;
  const filters = buildURLPresetFiltersLabel(preset);
  return {
    summary,
    detail: `条件：${filters}。请在右侧仓位列表中手动选择目标仓位后点击“加载 K 线”。`
  };
}

function buildURLPresetFiltersLabel(preset: URLInitialFilters): string {
  const parts: string[] = [];
  if (preset.selectedDate) {
    parts.push(`date=${preset.selectedDate}`);
  }
  if (preset.selectedExchange) {
    parts.push(`exchange=${preset.selectedExchange}`);
  }
  if (preset.selectedSymbol) {
    parts.push(`symbol=${preset.selectedSymbol}`);
  }
  if (preset.selectedStrategy) {
    parts.push(`strategy=${preset.selectedStrategy}`);
  }
  if (preset.selectedVersion) {
    parts.push(`version=${preset.selectedVersion}`);
  }
  if (preset.selectedRunID) {
    parts.push(`run_id=${preset.selectedRunID}`);
  }
  if (parts.length === 0) {
    return "(空)";
  }
  return parts.join(", ");
}

function isValidDateInput(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value.trim());
}

function formatDateInput(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function normalizeTimeframeBlocks(source: Record<string, TimeframeCandles> | null | undefined): Record<string, TimeframeCandles> {
  const out: Record<string, TimeframeCandles> = {};
  if (!source || typeof source !== "object") {
    return out;
  }
  for (const [rawKey, frame] of Object.entries(source)) {
    const key = rawKey.trim().toLowerCase();
    if (!key || out[key]) {
      continue;
    }
    out[key] = frame;
  }
  return out;
}

function resolveNormalizedTimeframeKey(
  timeframes: Record<string, TimeframeCandles>,
  preferred: string
): string {
  const target = preferred.trim().toLowerCase();
  if (!target) {
    return "";
  }
  return Object.prototype.hasOwnProperty.call(timeframes, target) ? target : "";
}

function pickPreferredTimeframe(candles: string[], fallback: string[]): string {
  const normalized = candles
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  const sorted = sortTimeframes(normalized);
  if (sorted.length > 0) {
    return sorted[sorted.length - 1];
  }
  const fallbackSorted = sortTimeframes(fallback);
  return fallbackSorted[fallbackSorted.length - 1] || "";
}

function sortTimeframes(values: string[]): string[] {
  const normalized = values
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  return [...normalized].sort((a, b) => {
    const left = timeframeOrderWeight(a);
    const right = timeframeOrderWeight(b);
    if (left === right) {
      return a.localeCompare(b);
    }
    return left - right;
  });
}

function timeframeOrderWeight(value: string): number {
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return Number.MAX_SAFE_INTEGER;
  }
  const unit = normalized.slice(-1);
  const step = Number(normalized.slice(0, -1));
  if (!Number.isFinite(step) || step <= 0) {
    return Number.MAX_SAFE_INTEGER;
  }
  switch (unit) {
    case "m":
      return step;
    case "h":
      return step * 60;
    case "d":
      return step * 60 * 24;
    case "w":
      return step * 60 * 24 * 7;
    default:
      return Number.MAX_SAFE_INTEGER;
  }
}

function formatIntegritySummary(integrity: IntegrityResponse): string {
  const events = integrity.events || { signals: 0, orders: 0 };
  const check = integrity.check || { ok: false };
  const summary = integrity.summary || { missing_bars: 0, discontinuities: 0 };
  const candles = integrity.candles || { total_rows: 0 };
  const eventCount = Number(events.signals || 0) + Number(events.orders || 0);
  if (Boolean(check.ok)) {
    return `完整性通过 | events=${eventCount} | bars=${Number(candles.total_rows || 0)}`;
  }
  const missingBars = Number(summary.missing_bars || 0);
  const discontinuities = Number(summary.discontinuities || 0);
  const details: string[] = [];
  if (missingBars > 0) {
    details.push(`missing=${missingBars}`);
  }
  if (discontinuities > 0) {
    details.push(`gaps=${discontinuities}`);
  }
  if (details.length === 0) {
    details.push("数据待补齐");
  }
  return `K线不完整 | events=${eventCount} | bars=${Number(candles.total_rows || 0)} | ${details.join(" / ")}`;
}

function normalizePositionFilterOptions(input: Partial<PositionFilterOptions> | null | undefined): PositionFilterOptions {
  const normalizeArray = (value: unknown): string[] => {
    if (!Array.isArray(value)) {
      return [];
    }
    return value.filter((item): item is string => typeof item === "string").map((item) => item.trim()).filter((item) => item.length > 0);
  };
  const normalizeRunOptions = (value: unknown): PositionRunOption[] => {
    if (!Array.isArray(value)) {
      return [];
    }
    const seen = new Set<string>();
    const out: PositionRunOption[] = [];
    for (const item of value) {
      if (!item || typeof item !== "object") {
        continue;
      }
      const optionValue = typeof (item as { value?: unknown }).value === "string"
        ? (item as { value: string }).value.trim()
        : "";
      if (!optionValue || seen.has(optionValue)) {
        continue;
      }
      const optionLabel = typeof (item as { label?: unknown }).label === "string"
        ? (item as { label: string }).label.trim()
        : optionValue;
      const singletonIDRaw = (item as { singleton_id?: unknown }).singleton_id;
      const singletonID =
        typeof singletonIDRaw === "number" && Number.isFinite(singletonIDRaw) && singletonIDRaw > 0
          ? Math.trunc(singletonIDRaw)
          : undefined;
      seen.add(optionValue);
      out.push({
        value: optionValue,
        label: optionLabel || optionValue,
        singleton_id: singletonID
      });
    }
    return out;
  };
  return {
    run_ids: normalizeArray(input?.run_ids),
    run_options: normalizeRunOptions(input?.run_options),
    strategies: normalizeArray(input?.strategies),
    versions: normalizeArray(input?.versions),
    exchanges: normalizeArray(input?.exchanges),
    symbols: normalizeArray(input?.symbols)
  };
}

function mergePositions(prev: HistoryPosition[], next: HistoryPosition[]): HistoryPosition[] {
  if (next.length === 0) {
    return prev;
  }
  const exists = new Set<string>();
  const out: HistoryPosition[] = [];
  for (const item of prev) {
    exists.add(positionUIDOf(item));
    out.push(item);
  }
  for (const item of next) {
    const uid = positionUIDOf(item);
    if (exists.has(uid)) {
      continue;
    }
    exists.add(uid);
    out.push(item);
  }
  return out;
}

function upsertPosition(prev: HistoryPosition[], next: HistoryPosition): HistoryPosition[] {
  const nextUID = positionUIDOf(next);
  if (!nextUID) {
    return prev;
  }
  let replaced = false;
  const out = prev.map((item) => {
    if (positionUIDOf(item) !== nextUID) {
      return item;
    }
    replaced = true;
    return next;
  });
  if (replaced) {
    return out;
  }
  return [next, ...out];
}

function positionUIDOf(position: HistoryPosition | null | undefined): string {
  if (!position) {
    return "";
  }
  if (typeof position.position_uid === "string" && position.position_uid.trim().length > 0) {
    return position.position_uid.trim();
  }
  if (Number.isFinite(position.id) && position.id > 0) {
    return `h:${Math.trunc(position.id)}`;
  }
  return "";
}
