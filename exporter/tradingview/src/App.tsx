import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import {
  createBacktestTask,
  delegatePositionStrategy,
  fetchBacktestOverlay,
  fetchBacktestTask,
  fetchBacktestTasks,
  fetchCandles,
  fetchExporterStatus,
  fetchPositionEvents,
  fetchRuntime,
  normalizeCandlesResponse,
  openRealtimeSocket,
  retryBacktestTask,
  submitTrade
} from "./api";
import {
  CandlesChart,
  type ChartViewportSnapshot,
  type CandlesChartHandle,
  type CandlesChartIndicatorLegendItem,
  type CandlesChartPositionLevel,
  type CandlesChartPositionLevelMove
} from "./CandlesChart";
import type { ChartRangeSelection } from "./chartOverlays";
import {
  CONNECTION_STATUS_HELP_ROWS,
  EVENT_LEGEND_ROWS,
  SHORTCUT_HELP_ROWS,
  type EventLegendPreviewKind,
  getDigitIndex,
  shouldIgnoreShortcut
} from "./shortcuts";
import { buildSignalLinkItems, type TradingViewSignalLinkItem } from "./signalLinks";
import tradingViewBrandIcon from "./assets/icon-tradingview.svg";
import okxBrandIcon from "./assets/icon-okx.png";
import binanceBrandIcon from "./assets/icon-binance.png";
import bitgetBrandIcon from "./assets/icon-bitget.png";
import type {
  TradingViewCandlesResponse,
  TradingViewBacktestOverlayResponse,
  TradingViewBacktestTask,
  TradingViewBacktestTasksResponse,
  TradingViewEventEntry,
  TradingViewExporterStatusResponse,
  TradingViewFunds,
  TradingViewHistoryPosition,
  TradingViewPosition,
  TradingViewPositionEventsResponse,
  TradingViewRealtimeAccount,
  TradingViewRealtimeMessage,
  TradingViewRuntimeResponse,
  TradingViewStrategyOption,
  TradingViewSymbol
} from "./types";

type BottomTab = "orders" | "positions" | "history" | "backtests";
type ConnectionStatus = "connected" | "slow" | "disconnected" | "warmup";
type TradeOrderSettingMode = "quantity" | "cost";
type TradeOrderQuantityUnit = "BTC" | "USDT" | "张";
type TradeOrderTab = "limit" | "market";
interface ViewportTransfer {
  sourceKey: string;
  targetKey: string;
  token: string;
}

interface PendingBacktestDisplay {
  taskID: number;
  exchange: string;
  symbol: string;
  rangeStartMS: number;
  rangeEndMS: number;
}

interface PendingBacktestEventJump {
  taskID: number;
  eventID: string;
  eventAtMS: number;
  eventPrice?: number;
  exchange: string;
  symbol: string;
}

interface CurrentPositionRow {
  rowKey: string;
  item: TradingViewPosition;
}

interface TradingViewPositionEventsLoadState {
  loading: boolean;
  error: string;
  snapshot?: TradingViewPositionEventsResponse;
}

interface TradingViewTradeNotice {
  message: string;
  success: boolean;
}

interface TradingViewPersistedState {
  version: 3;
  selectedExchange: string;
  selectedSymbol: string;
  selectedTimeframe: string;
  bottomTab: BottomTab;
  visibleIndicators: Record<string, boolean>;
  sidebarScrollTop: number;
  viewportSnapshots: Record<string, ChartViewportSnapshot>;
  tradeLeverageValue: number;
  tradeOrderTab: TradeOrderTab;
  takeProfitPct: string;
  stopLossPct: string;
}

const STATUS_POLL_MS = 5000;
const BACKTEST_STATUS_POLL_MS = 1500;
const POSITION_EVENTS_POLL_MS = 1500;
const BRAND_ICON_SRC_BY_TONE: Record<string, string> = {
  tradingview: tradingViewBrandIcon,
  binance: binanceBrandIcon,
  okx: okxBrandIcon,
  bitget: bitgetBrandIcon
};
const POSITION_EVENTS_LIMIT = 200;
const REALTIME_RETRY_MS = 2000;
const REALTIME_CANDLES_LIMIT = 320;
const STATUS_TOOLTIP_CACHE_MS = 5000;
const WS_PING_MS = 5000;
const WS_SLOW_THRESHOLD_MS = 100;
const TRADINGVIEW_UI_STATE_KEY = "tradingview.ui-state.v3";
const DEFAULT_VISIBLE_INDICATORS = {
  "ema-5": true,
  "ema-20": true,
  "ema-60": false,
  "ema-120": false
} satisfies Record<string, boolean>;

export function App(): JSX.Element {
  const tradePanelDisabled = true;
  const initialPersistedStateRef = useRef<TradingViewPersistedState>(loadTradingViewPersistedState());
  const chartRef = useRef<CandlesChartHandle | null>(null);
  const tradeModeMenuRef = useRef<HTMLDivElement | null>(null);
  const sidebarListRef = useRef<HTMLDivElement | null>(null);
  const symbolRowRefs = useRef<Map<string, HTMLButtonElement | null>>(new Map());
  const timeframeBySymbolRef = useRef<Map<string, string>>(new Map());
  const isMountedRef = useRef(true);
  const runtimeRef = useRef<TradingViewRuntimeResponse | null>(null);
  const exporterStatusRef = useRef<TradingViewExporterStatusResponse | null>(null);
  const exporterStatusLoadingRef = useRef(false);
  const candlesRef = useRef<TradingViewCandlesResponse | null>(null);
  const viewportSnapshotsRef = useRef<Record<string, ChartViewportSnapshot>>(normalizeViewportSnapshots(initialPersistedStateRef.current.viewportSnapshots));
  const persistTimerRef = useRef<number>(0);
  const tradeNoticeTimerRef = useRef<number>(0);
  const persistUIStateNowRef = useRef<() => void>(() => {});
  const sidebarScrollTopRef = useRef<number>(normalizeScrollTop(initialPersistedStateRef.current.sidebarScrollTop));
  const restoreSidebarScrollPendingRef = useRef(sidebarScrollTopRef.current > 0);
  const exporterStatusFetchedAtRef = useRef(0);
  const hasPersistedTradeLeverageRef = useRef(initialPersistedStateRef.current.tradeLeverageValue > 0);
  const tradePriceSeedKeyRef = useRef("");
  const tzOffsetMinRef = useRef(typeof Date === "function" ? new Date().getTimezoneOffset() : 0);
  const runtimeLiteRequestSeqRef = useRef(0);
  const runtimeFullRequestSeqRef = useRef(0);
  const runtimeBootstrapSeqRef = useRef(0);
  const candlesRequestSeqRef = useRef(0);
  const selectedExchangeRef = useRef("");
  const selectedSymbolRef = useRef("");
  const selectedTimeframeRef = useRef("");
  const realtimeSocketRef = useRef<WebSocket | null>(null);
  const realtimeRetryTimerRef = useRef<number>(0);
  const realtimePingTimerRef = useRef<number>(0);
  const realtimePingRequestRef = useRef<{ requestID: string; sentAtMS: number } | null>(null);
  const [runtime, setRuntime] = useState<TradingViewRuntimeResponse | null>(null);
  const [exporterStatus, setExporterStatus] = useState<TradingViewExporterStatusResponse | null>(null);
  const [exporterStatusLoading, setExporterStatusLoading] = useState(false);
  const [runtimeLoading, setRuntimeLoading] = useState(true);
  const [, setRuntimeError] = useState("");
  const [selectedExchange, setSelectedExchange] = useState(initialPersistedStateRef.current.selectedExchange || "");
  const [selectedSymbol, setSelectedSymbol] = useState(initialPersistedStateRef.current.selectedSymbol || "");
  const [selectedTimeframe, setSelectedTimeframe] = useState(initialPersistedStateRef.current.selectedTimeframe || "");
  const [symbolSearch, setSymbolSearch] = useState("");
  const [bottomTab, setBottomTab] = useState<BottomTab>(normalizeBottomTab(initialPersistedStateRef.current.bottomTab));
  const [candles, setCandles] = useState<TradingViewCandlesResponse | null>(null);
  const [candlesLoading, setCandlesLoading] = useState(false);
  const [, setCandlesError] = useState("");
  const [visibleIndicators, setVisibleIndicators] = useState<Record<string, boolean>>(() => ({
    ...DEFAULT_VISIBLE_INDICATORS,
    ...normalizeVisibleIndicators(initialPersistedStateRef.current.visibleIndicators)
  }));
  const [showLeverageModal, setShowLeverageModal] = useState(false);
  const [showOrderSettingsModal, setShowOrderSettingsModal] = useState(false);
  const [showTradeModeMenu, setShowTradeModeMenu] = useState(false);
  const [closeModalPosition, setCloseModalPosition] = useState<TradingViewPosition | null>(null);
  const [closePercent, setClosePercent] = useState(80);
  const [closeSubmitting, setCloseSubmitting] = useState(false);
  const [delegateModalPosition, setDelegateModalPosition] = useState<TradingViewPosition | null>(null);
  const [delegateSubmitting, setDelegateSubmitting] = useState(false);
  const [delegateIdentity, setDelegateIdentity] = useState("");
  const [showShortcutModal, setShowShortcutModal] = useState(false);
  const [tradeOrderSettingMode, setTradeOrderSettingMode] = useState<TradeOrderSettingMode>("cost");
  const [tradeOrderQuantityUnit, setTradeOrderQuantityUnit] = useState<TradeOrderQuantityUnit>("USDT");
  const [tradeLeverageValue, setTradeLeverageValue] = useState(() => normalizeTradeLeverage(initialPersistedStateRef.current.tradeLeverageValue));
  const [tradeOrderTab, setTradeOrderTab] = useState<TradeOrderTab>(normalizeTradeOrderTab(initialPersistedStateRef.current.tradeOrderTab));
  const [tradePriceInput, setTradePriceInput] = useState("");
  const [takeProfitPct, setTakeProfitPct] = useState(initialPersistedStateRef.current.takeProfitPct || "30");
  const [stopLossPct, setStopLossPct] = useState(initialPersistedStateRef.current.stopLossPct || "-5");
  const [tradeSubmitting, setTradeSubmitting] = useState(false);
  const [tradeNotice, setTradeNotice] = useState<TradingViewTradeNotice | null>(null);
  const [showStatusPopover, setShowStatusPopover] = useState(false);
  const [viewportTransfer, setViewportTransfer] = useState<ViewportTransfer | null>(null);
  const [chartMaximized, setChartMaximized] = useState(false);
  const [realtimeConnected, setRealtimeConnected] = useState(false);
  const [realtimeLatencyMS, setRealtimeLatencyMS] = useState<number | null>(null);
  const [backtestDays, setBacktestDays] = useState<TradingViewBacktestTasksResponse[]>([]);
  const [backtestsLoading, setBacktestsLoading] = useState(false);
  const [backtestsLoadingMore, setBacktestsLoadingMore] = useState(false);
  const [, setBacktestError] = useState("");
  const [visibleBacktestTaskID, setVisibleBacktestTaskID] = useState(0);
  const [backtestOverlayByTaskID, setBacktestOverlayByTaskID] = useState<Record<number, TradingViewBacktestOverlayResponse>>({});
  const [expandedBacktestTaskIDs, setExpandedBacktestTaskIDs] = useState<Record<number, boolean>>({});
  const [selectedBacktestEventIDs, setSelectedBacktestEventIDs] = useState<Record<number, string>>({});
  const [expiredBacktestTaskIDs, setExpiredBacktestTaskIDs] = useState<Record<number, boolean>>({});
  const [pendingBacktestDisplay, setPendingBacktestDisplay] = useState<PendingBacktestDisplay | null>(null);
  const [pendingBacktestEventJump, setPendingBacktestEventJump] = useState<PendingBacktestEventJump | null>(null);
  const [activePositionEventRowKey, setActivePositionEventRowKey] = useState("");
  const [positionEventsStateByRowKey, setPositionEventsStateByRowKey] = useState<Record<string, TradingViewPositionEventsLoadState>>({});

  runtimeRef.current = runtime;
  exporterStatusRef.current = exporterStatus;
  exporterStatusLoadingRef.current = exporterStatusLoading;
  candlesRef.current = candles;
  selectedExchangeRef.current = selectedExchange;
  selectedSymbolRef.current = selectedSymbol;
  selectedTimeframeRef.current = selectedTimeframe;

  const persistUIStateNow = useCallback(() => {
    writeTradingViewPersistedState({
      version: 3,
      selectedExchange: selectedExchangeRef.current.trim(),
      selectedSymbol: selectedSymbolRef.current.trim(),
      selectedTimeframe: selectedTimeframeRef.current.trim(),
      bottomTab,
      visibleIndicators,
      sidebarScrollTop: sidebarListRef.current?.scrollTop ?? sidebarScrollTopRef.current,
      viewportSnapshots: viewportSnapshotsRef.current,
      tradeLeverageValue,
      tradeOrderTab,
      takeProfitPct,
      stopLossPct
    });
  }, [bottomTab, stopLossPct, takeProfitPct, tradeLeverageValue, tradeOrderTab, visibleIndicators]);

  const schedulePersistUIState = useCallback(() => {
    if (persistTimerRef.current) {
      window.clearTimeout(persistTimerRef.current);
    }
    persistTimerRef.current = window.setTimeout(() => {
      persistTimerRef.current = 0;
      persistUIStateNow();
    }, 120);
  }, [persistUIStateNow]);

  const showTradeNotice = useCallback((success: boolean) => {
    if (tradeNoticeTimerRef.current) {
      window.clearTimeout(tradeNoticeTimerRef.current);
    }
    setTradeNotice({
      message: success ? "下单成功" : "下单失败",
      success
    });
    tradeNoticeTimerRef.current = window.setTimeout(() => {
      tradeNoticeTimerRef.current = 0;
      setTradeNotice(null);
    }, 2000);
  }, []);

  const handleViewportSnapshotChange = useCallback(
    (key: string, snapshot: ChartViewportSnapshot) => {
      viewportSnapshotsRef.current = {
        ...viewportSnapshotsRef.current,
        [key]: snapshot
      };
      schedulePersistUIState();
    },
    [schedulePersistUIState]
  );

  const handleSidebarListScroll = useCallback(() => {
    const nextScrollTop = sidebarListRef.current?.scrollTop ?? 0;
    sidebarScrollTopRef.current = normalizeScrollTop(nextScrollTop);
    schedulePersistUIState();
  }, [schedulePersistUIState]);

  persistUIStateNowRef.current = persistUIStateNow;

  useEffect(() => {
    return () => {
      isMountedRef.current = false;
      if (persistTimerRef.current) {
        window.clearTimeout(persistTimerRef.current);
        persistTimerRef.current = 0;
      }
      if (tradeNoticeTimerRef.current) {
        window.clearTimeout(tradeNoticeTimerRef.current);
        tradeNoticeTimerRef.current = 0;
      }
      persistUIStateNowRef.current();
      if (realtimeRetryTimerRef.current) {
        window.clearTimeout(realtimeRetryTimerRef.current);
      }
      if (realtimePingTimerRef.current) {
        window.clearTimeout(realtimePingTimerRef.current);
      }
      if (realtimeSocketRef.current) {
        realtimeSocketRef.current.close();
      }
    };
  }, []);

  useEffect(() => {
    schedulePersistUIState();
  }, [
    schedulePersistUIState,
    selectedExchange,
    selectedSymbol,
    selectedTimeframe,
    visibleIndicators,
    tradeLeverageValue,
    tradeOrderTab,
    takeProfitPct,
    stopLossPct
  ]);

  useEffect(() => {
    if (!showTradeModeMenu) {
      return;
    }
    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (!tradeModeMenuRef.current || (target && tradeModeMenuRef.current.contains(target))) {
        return;
      }
      setShowTradeModeMenu(false);
    };
    document.addEventListener("pointerdown", handlePointerDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
    };
  }, [showTradeModeMenu]);

  useEffect(() => {
    if (!selectedSymbol || !selectedTimeframe) {
      return;
    }
    timeframeBySymbolRef.current.set(selectedSymbol, selectedTimeframe);
  }, [selectedSymbol, selectedTimeframe]);

  function applyRuntimeResponse(response: TradingViewRuntimeResponse, options?: { preserveHeavy?: boolean }): void {
    const preserveHeavy = options?.preserveHeavy === true;
    const currentExchange = selectedExchangeRef.current;
    const currentSymbol = selectedSymbolRef.current;
    const currentTimeframe = selectedTimeframeRef.current;
    const merged = preserveHeavy ? mergeRuntimePayload(runtimeRef.current, response) : response;
    setRuntime(merged);
    setRuntimeError("");

    const exchangeValid = merged.exchanges.some((item) => item.name === currentExchange);
    const symbolValid = merged.symbols.some((item) => item.symbol === currentSymbol);
    const nextExchange = exchangeValid ? currentExchange : merged.selected_exchange;
    const nextSymbol = symbolValid ? currentSymbol : merged.default_symbol;
    const nextTimeframe = resolvePreferredTimeframe(
      nextSymbol,
      merged,
      symbolValid ? currentTimeframe : merged.default_timeframe,
      timeframeBySymbolRef.current
    );

    if (nextExchange !== currentExchange) {
      setSelectedExchange(nextExchange);
    }
    if (nextSymbol !== currentSymbol) {
      setSelectedSymbol(nextSymbol);
    }
    if (nextTimeframe && nextTimeframe !== currentTimeframe) {
      timeframeBySymbolRef.current.set(nextSymbol, nextTimeframe);
      setSelectedTimeframe(nextTimeframe);
    }
  }

  async function refreshRuntime(
    options?: { showLoading?: boolean; lite?: boolean }
  ): Promise<TradingViewRuntimeResponse | null> {
    const showLoading = options?.showLoading !== false;
    const lite = options?.lite === true;
    const requestSeqRef = lite ? runtimeLiteRequestSeqRef : runtimeFullRequestSeqRef;
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    const requestedExchange = selectedExchangeRef.current.trim();
    if (showLoading) {
      setRuntimeLoading(true);
      setRuntimeError("");
    }
    try {
      const response = await fetchRuntime(requestedExchange || undefined, { lite });
      if (!isMountedRef.current || requestSeq !== requestSeqRef.current) {
        return null;
      }
      if (requestedExchange !== "" && !stringsEqualIgnoreCase(selectedExchangeRef.current, requestedExchange)) {
        return null;
      }
      applyRuntimeResponse(response, { preserveHeavy: lite });
      return response;
    } catch (error) {
      if (!isMountedRef.current || requestSeq !== requestSeqRef.current) {
        return null;
      }
      setRuntimeError(toErrorMessage(error));
      return null;
    } finally {
      if (isMountedRef.current && requestSeq === requestSeqRef.current) {
        setRuntimeLoading(false);
      }
    }
  }

  async function bootstrapRuntime(options?: { showLoading?: boolean }): Promise<void> {
    const bootstrapSeq = runtimeBootstrapSeqRef.current + 1;
    runtimeBootstrapSeqRef.current = bootstrapSeq;
    const bootstrap = await refreshRuntime({ showLoading: options?.showLoading, lite: true });
    if (!isMountedRef.current || bootstrapSeq !== runtimeBootstrapSeqRef.current) {
      return;
    }
    const currentExchange = bootstrap?.selected_exchange?.trim() || selectedExchangeRef.current.trim();
    if (!currentExchange) {
      return;
    }
    void refreshRuntime({ showLoading: false, lite: false });
  }

  async function refreshCandles(options?: {
    showLoading?: boolean;
    resetData?: boolean;
    preserveOnError?: boolean;
  }): Promise<void> {
    if (!hasActiveChartSelection(runtimeRef.current, selectedExchangeRef.current, selectedSymbolRef.current, selectedTimeframeRef.current)) {
      return;
    }

    const exchange = selectedExchangeRef.current;
    const symbol = selectedSymbolRef.current;
    const timeframe = selectedTimeframeRef.current;
    const showLoading = options?.showLoading !== false;
    const resetData = options?.resetData === true;
    const preserveOnError = options?.preserveOnError === true;
    const requestSeq = candlesRequestSeqRef.current + 1;
    candlesRequestSeqRef.current = requestSeq;

    if (showLoading) {
      setCandlesLoading(true);
      setCandlesError("");
    }
    if (resetData) {
      setCandles(null);
    }

    try {
      const response = await fetchCandles({ exchange, symbol, timeframe });
      if (!isMountedRef.current || requestSeq !== candlesRequestSeqRef.current) {
        return;
      }
      setCandlesError("");
      if (shouldUpdateCandles(candlesRef.current, response)) {
        setCandles(response);
      }
    } catch (error) {
      if (!isMountedRef.current || requestSeq !== candlesRequestSeqRef.current) {
        return;
      }
      if (!preserveOnError) {
        setCandles(null);
      }
      setCandlesError(toErrorMessage(error));
    } finally {
      if (isMountedRef.current && requestSeq === candlesRequestSeqRef.current) {
        setCandlesLoading(false);
      }
    }
  }

  const refreshCurrentBacktestDay = useCallback(
    async (options?: { showLoading?: boolean }): Promise<void> => {
      const showLoading = options?.showLoading !== false;
      if (showLoading) {
        setBacktestsLoading(true);
      }
      try {
        const response = await fetchBacktestTasks({
          tz_offset_min: tzOffsetMinRef.current
        });
        if (!isMountedRef.current) {
          return;
        }
        setBacktestDays((previous) => replaceBacktestDay(previous, response));
        setBacktestError("");
      } catch (error) {
        if (!isMountedRef.current) {
          return;
        }
        setBacktestError(toErrorMessage(error));
      } finally {
        if (isMountedRef.current && showLoading) {
          setBacktestsLoading(false);
        }
      }
    },
    []
  );

  const loadOlderBacktestDay = useCallback(async (): Promise<void> => {
    if (backtestsLoadingMore) {
      return;
    }
    const targetDate = resolvePreviousBacktestDate(backtestDays);
    if (!targetDate) {
      return;
    }
    setBacktestsLoadingMore(true);
    try {
      const response = await fetchBacktestTasks({
        date: targetDate,
        tz_offset_min: tzOffsetMinRef.current
      });
      if (!isMountedRef.current) {
        return;
      }
      setBacktestDays((previous) => replaceBacktestDay(previous, response));
      setBacktestError("");
    } catch (error) {
      if (!isMountedRef.current) {
        return;
      }
      setBacktestError(toErrorMessage(error));
    } finally {
      if (isMountedRef.current) {
        setBacktestsLoadingMore(false);
      }
    }
  }, [backtestDays, backtestsLoadingMore]);

  const loadBacktestOverlayForTask = useCallback(async (taskID: number): Promise<TradingViewBacktestOverlayResponse> => {
    const response = await fetchBacktestOverlay(taskID);
    if (!isMountedRef.current) {
      return response;
    }
    setBacktestOverlayByTaskID((previous) => ({
      ...previous,
      [taskID]: response
    }));
    setBacktestError("");
    return response;
  }, []);

  useEffect(() => {
    if (
      selectedExchange &&
      runtimeRef.current &&
      runtimeRef.current.selected_exchange === selectedExchange &&
      runtimeRef.current.bootstrap_complete
    ) {
      return;
    }
    void bootstrapRuntime({ showLoading: runtimeRef.current === null });
  }, [selectedExchange]);

  useEffect(() => {
    void refreshCandles({
      showLoading: true,
      resetData: true,
      preserveOnError: false
    });
  }, [selectedExchange, selectedSymbol, selectedTimeframe]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      if (!isDocumentVisible()) {
        return;
      }
      void refreshRuntime({ showLoading: false, lite: true });
      if (!runtimeRef.current?.bootstrap_complete) {
        void refreshRuntime({ showLoading: false, lite: false });
      }
      void refreshCandles({
        showLoading: false,
        resetData: false,
        preserveOnError: true
      });
      void refreshCurrentBacktestDay({ showLoading: false });
    }, STATUS_POLL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [refreshCurrentBacktestDay]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      if (!isDocumentVisible()) {
        return;
      }
      void refreshRuntime({ showLoading: false, lite: true });
      if (!runtimeRef.current?.bootstrap_complete) {
        void refreshRuntime({ showLoading: false, lite: false });
      }
      void refreshCandles({
        showLoading: false,
        resetData: false,
        preserveOnError: true
      });
      void refreshCurrentBacktestDay({ showLoading: false });
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [refreshCurrentBacktestDay]);

  useEffect(() => {
    void refreshCurrentBacktestDay({ showLoading: true });
  }, [refreshCurrentBacktestDay]);

  const pushRealtimeSubscription = useCallback((socket?: WebSocket | null) => {
    const currentSocket = socket || realtimeSocketRef.current;
    if (!currentSocket || currentSocket.readyState !== WebSocket.OPEN) {
      return;
    }
    const exchange = selectedExchangeRef.current.trim();
    if (!exchange) {
      return;
    }
    const symbol = selectedSymbolRef.current.trim();
    const timeframe = selectedTimeframeRef.current.trim();
    const streams = ["account", "position", "symbols"];
    const subscription: Record<string, unknown> = {
      streams,
      account_filter: { exchange },
      position_filter: { exchange },
      symbols_filter: { exchange }
    };
    if (symbol && timeframe) {
      streams.push("candles");
      subscription.candles_filter = {
        exchange,
        symbol,
        timeframe,
        limit: REALTIME_CANDLES_LIMIT
      };
    }
    currentSocket.send(
      JSON.stringify({
        type: "subscribe",
        subscription
      })
    );
  }, []);

  useEffect(() => {
    let cancelled = false;

    const clearRetryTimer = () => {
      if (realtimeRetryTimerRef.current) {
        window.clearTimeout(realtimeRetryTimerRef.current);
        realtimeRetryTimerRef.current = 0;
      }
    };

    const clearPingTimer = () => {
      if (realtimePingTimerRef.current) {
        window.clearTimeout(realtimePingTimerRef.current);
        realtimePingTimerRef.current = 0;
      }
      realtimePingRequestRef.current = null;
    };

    const schedulePing = (socket: WebSocket) => {
      clearPingTimer();
      realtimePingTimerRef.current = window.setTimeout(() => {
        realtimePingTimerRef.current = 0;
        if (cancelled || socket.readyState !== WebSocket.OPEN) {
          return;
        }
        const requestID = `tv-ping-${Date.now()}`;
        realtimePingRequestRef.current = {
          requestID,
          sentAtMS: Date.now()
        };
        socket.send(
          JSON.stringify({
            type: "ping",
            request_id: requestID
          })
        );
        schedulePing(socket);
      }, WS_PING_MS);
    };

    const closeSocket = () => {
      const socket = realtimeSocketRef.current;
      clearPingTimer();
      if (isMountedRef.current) {
        setRealtimeConnected(false);
        setRealtimeLatencyMS(null);
      }
      if (!socket) {
        return;
      }
      socket.onopen = null;
      socket.onclose = null;
      socket.onerror = null;
      socket.onmessage = null;
      socket.close();
      realtimeSocketRef.current = null;
    };

    const scheduleReconnect = () => {
      if (cancelled || realtimeRetryTimerRef.current) {
        return;
      }
      realtimeRetryTimerRef.current = window.setTimeout(() => {
        realtimeRetryTimerRef.current = 0;
        connect();
      }, REALTIME_RETRY_MS);
    };

    const connect = () => {
      if (cancelled) {
        return;
      }
      clearRetryTimer();
      closeSocket();

      const socket = openRealtimeSocket();
      realtimeSocketRef.current = socket;

      socket.onopen = () => {
        if (cancelled) {
          socket.close();
          return;
        }
        setRealtimeConnected(true);
        setRealtimeLatencyMS(null);
        pushRealtimeSubscription(socket);
        realtimePingRequestRef.current = {
          requestID: `tv-ping-${Date.now()}`,
          sentAtMS: Date.now()
        };
        socket.send(
          JSON.stringify({
            type: "ping",
            request_id: realtimePingRequestRef.current.requestID
          })
        );
        schedulePing(socket);
      };

      socket.onmessage = (event) => {
        let payload: TradingViewRealtimeMessage;
        try {
          payload = JSON.parse(String(event.data)) as TradingViewRealtimeMessage;
        } catch (error) {
          console.warn("tradingview realtime parse failed", error);
          return;
        }
        if (payload.type === "pong") {
          const pendingPing = realtimePingRequestRef.current;
          if (!pendingPing) {
            return;
          }
          if (payload.request_id && pendingPing.requestID !== payload.request_id) {
            return;
          }
          setRealtimeLatencyMS(Math.max(0, Date.now() - pendingPing.sentAtMS));
          realtimePingRequestRef.current = null;
          return;
        }
        if (payload.type !== "snapshot" && payload.type !== "diff") {
          return;
        }
        setRuntime((previous) => {
          let next = previous;
          if (payload.symbols) {
            next = mergeRuntimeSymbolsSnapshot(next, payload.symbols, selectedExchangeRef.current);
          }
          if (payload.account || payload.position) {
            next = mergeRuntimeRealtime(next, payload.account, payload.position?.positions);
          }
          return next;
        });
        if (payload.candles) {
          const nextData = normalizeCandlesResponse(payload.candles);
          if (
            nextData.exchange === selectedExchangeRef.current &&
            nextData.symbol === selectedSymbolRef.current &&
            nextData.timeframe === selectedTimeframeRef.current
          ) {
            setCandlesError("");
            setCandles((previous) => mergeRealtimeCandles(previous, nextData));
          }
        }
        if (payload.position) {
          void refreshRuntime({ showLoading: false });
        }
      };

      socket.onerror = (event) => {
        console.warn("tradingview realtime socket error", event);
      };

      socket.onclose = () => {
        clearPingTimer();
        setRealtimeConnected(false);
        setRealtimeLatencyMS(null);
        if (realtimeSocketRef.current === socket) {
          realtimeSocketRef.current = null;
        }
        scheduleReconnect();
      };
    };

    connect();

    return () => {
      cancelled = true;
      clearRetryTimer();
      closeSocket();
    };
  }, [pushRealtimeSubscription]);

  useEffect(() => {
    if (!selectedExchange) {
      return;
    }
    pushRealtimeSubscription();
  }, [pushRealtimeSubscription, selectedExchange, selectedSymbol, selectedTimeframe]);

  const selectedSymbolMeta = useMemo<TradingViewSymbol | null>(() => {
    return runtime?.symbols.find((item) => item.symbol === selectedSymbol) || null;
  }, [runtime, selectedSymbol]);

  const selectedDisplaySymbol = selectedSymbolMeta?.display_symbol || formatDisplaySymbol(selectedSymbol);
  const displayedIndicators = useMemo(() => candles?.indicators || [], [candles]);
  const indicatorLegend = useMemo<CandlesChartIndicatorLegendItem[]>(() => {
    return displayedIndicators.map((item) => {
      const lastPoint = item.points[item.points.length - 1];
      return {
        id: item.id,
        label: item.label,
        color: item.legend_color || item.color,
        value: formatIndicatorValue(lastPoint?.value),
        visible: visibleIndicators[item.id] !== false
      };
    });
  }, [displayedIndicators, visibleIndicators]);

  const currentPositions = useMemo(() => runtime?.positions || [], [runtime]);
  const historyPositions = useMemo(() => runtime?.history_positions || [], [runtime]);
  const currentOrders = useMemo(() => runtime?.orders || [], [runtime]);
  const currentPositionRows = useMemo<CurrentPositionRow[]>(
    () => {
      const selectedExchangeKey = normalizeExchangeName(selectedExchange);
      const selectedSymbolKey = normalizeSymbolKey(selectedSymbol);
      return currentPositions
        .map((item) => ({
          rowKey: buildCurrentPositionRowKey(item),
          item
        }))
        .sort((left, right) => {
          const leftActive = activePositionEventRowKey !== "" && left.rowKey === activePositionEventRowKey;
          const rightActive = activePositionEventRowKey !== "" && right.rowKey === activePositionEventRowKey;
          if (leftActive !== rightActive) {
            return leftActive ? -1 : 1;
          }
          const leftSelected =
            normalizeExchangeName(left.item.exchange) === selectedExchangeKey &&
            normalizeSymbolKey(left.item.symbol) === selectedSymbolKey;
          const rightSelected =
            normalizeExchangeName(right.item.exchange) === selectedExchangeKey &&
            normalizeSymbolKey(right.item.symbol) === selectedSymbolKey;
          if (leftSelected !== rightSelected) {
            return leftSelected ? -1 : 1;
          }
          const symbolDiff = formatDisplaySymbol(left.item.symbol).localeCompare(formatDisplaySymbol(right.item.symbol), "zh-Hans-CN");
          if (symbolDiff !== 0) {
            return symbolDiff;
          }
          const exchangeDiff = normalizeExchangeName(left.item.exchange).localeCompare(normalizeExchangeName(right.item.exchange), "zh-Hans-CN");
          if (exchangeDiff !== 0) {
            return exchangeDiff;
          }
          const sideDiff = (left.item.position_side || "").localeCompare(right.item.position_side || "", "zh-Hans-CN");
          if (sideDiff !== 0) {
            return sideDiff;
          }
          const entryDiff = parseTradingViewDateTimeMS(left.item.entry_time) - parseTradingViewDateTimeMS(right.item.entry_time);
          if (entryDiff !== 0) {
            return entryDiff;
          }
          return left.rowKey.localeCompare(right.rowKey, "zh-Hans-CN");
        });
    },
    [activePositionEventRowKey, currentPositions, selectedExchange, selectedSymbol]
  );
  const positionLinkItemsByRowKey = useMemo(() => {
    const map = new Map<string, TradingViewSignalLinkItem[]>();
    const symbols = runtime?.symbols || [];
    for (const row of currentPositionRows) {
      map.set(
        row.rowKey,
        buildSignalLinkItems(
          {
            exchange: row.item.exchange,
            symbol: row.item.symbol,
            timeframe: selectedTimeframe
          },
          symbols
        )
      );
    }
    return map;
  }, [currentPositionRows, runtime, selectedTimeframe]);
  const funds: TradingViewFunds | null = runtime?.funds || null;
  const strategyOptions = useMemo<TradingViewStrategyOption[]>(() => runtime?.strategy_options || [], [runtime]);
  const tradeReadOnly = runtime?.read_only !== false;
  const hasOpenModal =
    showShortcutModal ||
    showLeverageModal ||
    showOrderSettingsModal ||
    closeModalPosition !== null ||
    delegateModalPosition !== null ||
    showTradeModeMenu;
  const chartViewportKey = useMemo(() => buildViewportKey(selectedSymbol, selectedTimeframe), [selectedSymbol, selectedTimeframe]);
  const activeBacktestTask = useMemo(
    () => findActiveBacktestTask(backtestDays),
    [backtestDays]
  );
  const displayedBacktestEvents = useMemo<TradingViewEventEntry[]>(() => {
    const overlay = visibleBacktestTaskID > 0 ? backtestOverlayByTaskID[visibleBacktestTaskID] : undefined;
    if (!overlay) {
      return [];
    }
    if (overlay.task.exchange !== selectedExchange || overlay.task.symbol !== selectedSymbol) {
      return [];
    }
    return Array.isArray(overlay.events) ? overlay.events : [];
  }, [backtestOverlayByTaskID, selectedExchange, selectedSymbol, visibleBacktestTaskID]);
  const matchedCurrentPositionRows = useMemo<CurrentPositionRow[]>(
    () =>
      currentPositionRows.filter(
        (item) =>
          normalizeExchangeName(item.item.exchange) === normalizeExchangeName(selectedExchange) &&
          normalizeSymbolKey(item.item.symbol) === normalizeSymbolKey(selectedSymbol)
      ),
    [currentPositionRows, selectedExchange, selectedSymbol]
  );
  const activePositionEventRow = useMemo<CurrentPositionRow | null>(() => {
    if (activePositionEventRowKey) {
      const matched = matchedCurrentPositionRows.find((item) => item.rowKey === activePositionEventRowKey);
      if (matched) {
        return matched;
      }
    }
    return matchedCurrentPositionRows[0] || null;
  }, [activePositionEventRowKey, matchedCurrentPositionRows]);
  const displayedPositionEvents = useMemo<TradingViewEventEntry[]>(() => {
    if (!activePositionEventRow) {
      return [];
    }
    return positionEventsStateByRowKey[activePositionEventRow.rowKey]?.snapshot?.events || [];
  }, [activePositionEventRow, positionEventsStateByRowKey]);
  const displayedChartEvents = useMemo<TradingViewEventEntry[]>(() => {
    if (activePositionEventRow) {
      return displayedPositionEvents;
    }
    return displayedBacktestEvents;
  }, [activePositionEventRow, displayedBacktestEvents, displayedPositionEvents]);
  const hasOlderBacktestDays = useMemo(() => {
    if (backtestDays.length === 0) {
      return false;
    }
    return backtestDays[backtestDays.length - 1].has_more_days;
  }, [backtestDays]);
  const connectionStatus = useMemo<ConnectionStatus>(() => {
    if (!realtimeConnected) {
      return "disconnected";
    }
    const hasWarmupModule = Object.values(exporterStatus?.modules || {}).some((item) => {
      const normalizedState = item?.state?.trim().toLowerCase();
      return normalizedState === "warmup" || normalizedState === "warming";
    });
    if (hasWarmupModule) {
      return "warmup";
    }
    if (realtimeLatencyMS != null && realtimeLatencyMS > WS_SLOW_THRESHOLD_MS) {
      return "slow";
    }
    return "connected";
  }, [exporterStatus, realtimeConnected, realtimeLatencyMS]);
  const exporterStatusModules = useMemo(
    () =>
      Object.entries(exporterStatus?.modules || {})
        .map(([key, item]) => ({
          name: item?.name?.trim() || key,
          state: item?.state?.trim() || "--"
        }))
        .sort((left, right) => left.name.localeCompare(right.name, "zh-Hans-CN")),
    [exporterStatus]
  );

  const selectedPositionForTrade = useMemo(() => {
    return (
      currentPositions.find(
        (item) =>
          normalizeExchangeName(item.exchange) === normalizeExchangeName(selectedExchange) &&
          normalizeSymbolKey(item.symbol) === normalizeSymbolKey(selectedSymbol)
      ) || null
    );
  }, [currentPositions, selectedExchange, selectedSymbol]);
  const selectedPositionLevels = useMemo<CandlesChartPositionLevel[]>(() => {
    return buildChartPositionLevels(currentPositions, selectedExchange, selectedSymbol);
  }, [currentPositions, selectedExchange, selectedSymbol]);
  const latestCandleClose = useMemo(() => {
    const lastCandle = candles?.candles?.[candles.candles.length - 1];
    if (!lastCandle || !Number.isFinite(lastCandle.close) || lastCandle.close <= 0) {
      return 0;
    }
    return lastCandle.close;
  }, [candles]);
  const tradeAnchorPrice = useMemo(() => {
    if (selectedPositionForTrade?.current_price && selectedPositionForTrade.current_price > 0) {
      return selectedPositionForTrade.current_price;
    }
    if (selectedSymbolMeta?.last_price && selectedSymbolMeta.last_price > 0) {
      return selectedSymbolMeta.last_price;
    }
    if (latestCandleClose > 0) {
      return latestCandleClose;
    }
    return 0;
  }, [latestCandleClose, selectedPositionForTrade, selectedSymbolMeta]);
  const tradeEntryPrice = useMemo(() => {
    if (selectedPositionForTrade?.entry_price && selectedPositionForTrade.entry_price > 0) {
      return selectedPositionForTrade.entry_price;
    }
    return tradeAnchorPrice;
  }, [selectedPositionForTrade, tradeAnchorPrice]);
  const tradeAvailableUSDT = useMemo(() => {
    return funds?.trading_usdt && funds.trading_usdt > 0 ? funds.trading_usdt : 0;
  }, [funds]);
  const tradeCanOpenLongUSDT = useMemo(() => tradeAvailableUSDT * tradeLeverageValue, [tradeAvailableUSDT, tradeLeverageValue]);
  const tradeCanOpenShortUSDT = useMemo(() => tradeAvailableUSDT * tradeLeverageValue, [tradeAvailableUSDT, tradeLeverageValue]);
  const tradeBestBidPrice = useMemo(() => {
    if (!(tradeAnchorPrice > 0)) {
      return 0;
    }
    return tradeAnchorPrice * 0.9992;
  }, [tradeAnchorPrice]);
  const tradeBestAskPrice = useMemo(() => {
    if (!(tradeAnchorPrice > 0)) {
      return 0;
    }
    return tradeAnchorPrice * 1.0008;
  }, [tradeAnchorPrice]);
  const tradeEstimatedLongLiqPrice = useMemo(
    () => estimateTradeLiquidationPrice(tradeEntryPrice, tradeLeverageValue, "long"),
    [tradeEntryPrice, tradeLeverageValue]
  );
  const tradeEstimatedShortLiqPrice = useMemo(
    () => estimateTradeLiquidationPrice(tradeEntryPrice, tradeLeverageValue, "short"),
    [tradeEntryPrice, tradeLeverageValue]
  );

  const handleRunBacktestArea = useCallback(
    async (selection: ChartRangeSelection): Promise<void> => {
      const exchange = selectedExchangeRef.current.trim();
      const symbol = selectedSymbolRef.current.trim();
      const chartTimeframe = selectedTimeframeRef.current.trim();
      if (!exchange || !symbol || !chartTimeframe) {
        return;
      }
      setBottomTab("backtests");
      setBacktestError("");
      try {
        const response = await createBacktestTask({
          exchange,
          symbol,
          display_symbol: selectedDisplaySymbol,
          chart_timeframe: chartTimeframe,
          range_start_ms: selection.rangeStartMS,
          range_end_ms: selection.rangeEndMS,
          price_low: selection.priceLow,
          price_high: selection.priceHigh,
          selection_direction: selection.selectionDirection
        });
        if (!isMountedRef.current) {
          return;
        }
        setBacktestDays((previous) =>
          upsertBacktestTaskDay(previous, response.task, tzOffsetMinRef.current)
        );
      } catch (error) {
        if (!isMountedRef.current) {
          return;
        }
        setBacktestError(toErrorMessage(error));
      }
    },
    [selectedDisplaySymbol]
  );

  const loadExporterStatus = useCallback(
    async (options?: { force?: boolean }) => {
      const force = options?.force === true;
      const now = Date.now();
      if (!force && exporterStatusRef.current && now - exporterStatusFetchedAtRef.current < STATUS_TOOLTIP_CACHE_MS) {
        return exporterStatusRef.current;
      }
      if (exporterStatusLoadingRef.current && !force) {
        return exporterStatusRef.current;
      }
      exporterStatusLoadingRef.current = true;
      setExporterStatusLoading(true);
      try {
        const response = await fetchExporterStatus();
        if (!isMountedRef.current) {
          return exporterStatusRef.current;
        }
        exporterStatusFetchedAtRef.current = Date.now();
        exporterStatusRef.current = response;
        setExporterStatus(response);
        return response;
      } catch (error) {
        console.warn("load tradingview exporter status failed", error);
        return exporterStatusRef.current;
      } finally {
        exporterStatusLoadingRef.current = false;
        if (isMountedRef.current) {
          setExporterStatusLoading(false);
        }
      }
    },
    []
  );

  const handleStatusPopoverOpen = useCallback(() => {
    setShowStatusPopover(true);
    void loadExporterStatus();
  }, [loadExporterStatus]);

  const handleStatusPopoverClose = useCallback(() => {
    setShowStatusPopover(false);
  }, []);

  useEffect(() => {
    void loadExporterStatus({ force: true });
    const timer = window.setInterval(() => {
      if (!isDocumentVisible()) {
        return;
      }
      void loadExporterStatus({ force: true });
    }, STATUS_POLL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [loadExporterStatus]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      if (!isDocumentVisible()) {
        return;
      }
      void loadExporterStatus({ force: true });
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [loadExporterStatus]);

  const handleRetryBacktestTask = useCallback(async (task: TradingViewBacktestTask): Promise<void> => {
    setBottomTab("backtests");
    setBacktestError("");
    setPendingBacktestDisplay((current) => (current?.taskID === task.id ? null : current));
    setVisibleBacktestTaskID((current) => (current === task.id ? 0 : current));
    setExpiredBacktestTaskIDs((previous) => {
      if (!previous[task.id]) {
        return previous;
      }
      const next = { ...previous };
      delete next[task.id];
      return next;
    });
    setBacktestOverlayByTaskID((previous) => {
      const next = { ...previous };
      delete next[task.id];
      return next;
    });
    try {
      const response = await retryBacktestTask(task.id);
      if (!isMountedRef.current) {
        return;
      }
      setBacktestDays((previous) =>
        upsertBacktestTaskDay(previous, response.task, tzOffsetMinRef.current)
      );
    } catch (error) {
      if (!isMountedRef.current) {
        return;
      }
      setBacktestError(toErrorMessage(error));
    }
  }, []);

  const showBacktestTask = useCallback(
    (task: TradingViewBacktestTask): void => {
      setBottomTab("backtests");
      setBacktestError("");
      setPendingBacktestDisplay({
        taskID: task.id,
        exchange: task.exchange,
        symbol: task.symbol,
        rangeStartMS: task.range_start_ms,
        rangeEndMS: task.range_end_ms
      });
      setExpiredBacktestTaskIDs((previous) => {
        if (!previous[task.id]) {
          return previous;
        }
        const next = { ...previous };
        delete next[task.id];
        return next;
      });
      handleSelectMarketSymbol(task.exchange, task.symbol);
    },
    [handleSelectMarketSymbol]
  );

  const handleToggleBacktestVisibility = useCallback(
    async (task: TradingViewBacktestTask): Promise<void> => {
      if (task.status === "failed") {
        await handleRetryBacktestTask(task);
        return;
      }
      if (task.status !== "succeeded") {
        return;
      }
      if (visibleBacktestTaskID === task.id && task.exchange === selectedExchangeRef.current && task.symbol === selectedSymbolRef.current) {
        setVisibleBacktestTaskID(0);
        setPendingBacktestDisplay((current) => (current?.taskID === task.id ? null : current));
        setPendingBacktestEventJump((current) => (current?.taskID === task.id ? null : current));
        return;
      }
      showBacktestTask(task);
    },
    [handleRetryBacktestTask, showBacktestTask, visibleBacktestTaskID]
  );

  const handleToggleBacktestEvents = useCallback(
    async (task: TradingViewBacktestTask): Promise<void> => {
      if (expandedBacktestTaskIDs[task.id]) {
        setExpandedBacktestTaskIDs((previous) => ({
          ...previous,
          [task.id]: false
        }));
        setSelectedBacktestEventIDs((previous) => {
          if (!previous[task.id]) {
            return previous;
          }
          const next = { ...previous };
          delete next[task.id];
          return next;
        });
        return;
      }
      if (task.status !== "succeeded") {
        return;
      }
      if (!backtestOverlayByTaskID[task.id]) {
        try {
          await loadBacktestOverlayForTask(task.id);
        } catch (error) {
          if (isMountedRef.current) {
            setBacktestError(toErrorMessage(error));
          }
          return;
        }
      }
      setExpandedBacktestTaskIDs((previous) => ({
        ...previous,
        [task.id]: true
      }));
    },
    [backtestOverlayByTaskID, expandedBacktestTaskIDs, loadBacktestOverlayForTask]
  );

  const handleSelectBacktestEventRow = useCallback((taskID: number, eventID: string): void => {
    setSelectedBacktestEventIDs((previous) => {
      if (previous[taskID] === eventID) {
        return previous;
      }
      return {
        ...previous,
        [taskID]: eventID
      };
    });
  }, []);

  const handleActivateBacktestEvent = useCallback(
    async (task: TradingViewBacktestTask, event: TradingViewEventEntry): Promise<void> => {
      handleSelectBacktestEventRow(task.id, event.id);
      setPendingBacktestEventJump({
        taskID: task.id,
        eventID: event.id,
        eventAtMS: event.event_at_ms,
        eventPrice: resolveEventCrosshairPrice(event),
        exchange: task.exchange,
        symbol: task.symbol
      });
      if (!isBacktestOverlayDisplayed(task, visibleBacktestTaskID, selectedExchangeRef.current, selectedSymbolRef.current)) {
        showBacktestTask(task);
      }
    },
    [handleSelectBacktestEventRow, showBacktestTask, visibleBacktestTaskID]
  );

  function handleSelectMarketSymbol(exchange: string, symbol: string): void {
    const nextExchange = normalizeExchangeName(exchange);
    const nextSymbol = (symbol || "").trim();
    if (!nextExchange || !nextSymbol) {
      return;
    }
    const timeframes = runtimeRef.current?.timeframes || [];
    const currentSymbol = selectedSymbolRef.current;
    const currentTimeframe = selectedTimeframeRef.current;
    const nextTimeframe = timeframes.includes(currentTimeframe)
      ? currentTimeframe
      : resolvePreferredTimeframe(nextSymbol, runtimeRef.current, currentTimeframe, timeframeBySymbolRef.current);
    if (currentSymbol && currentTimeframe && nextTimeframe) {
      setViewportTransfer({
        sourceKey: buildViewportKey(currentSymbol, currentTimeframe),
        targetKey: buildViewportKey(nextSymbol, nextTimeframe),
        token: buildViewportTransferToken(currentSymbol, currentTimeframe, nextSymbol, nextTimeframe)
      });
    }
    if (nextExchange !== selectedExchangeRef.current) {
      setSelectedExchange(nextExchange);
    }
    if (nextSymbol !== currentSymbol) {
      setSelectedSymbol(nextSymbol);
    }
    if (nextTimeframe) {
      timeframeBySymbolRef.current.set(nextSymbol, nextTimeframe);
      if (nextTimeframe !== currentTimeframe) {
        setSelectedTimeframe(nextTimeframe);
      }
    }
  }

  const loadPositionEventsForRow = useCallback(async (row: CurrentPositionRow): Promise<TradingViewPositionEventsResponse> => {
    setPositionEventsStateByRowKey((previous) => ({
      ...previous,
      [row.rowKey]: {
        ...previous[row.rowKey],
        loading: true,
        error: ""
      }
    }));
    try {
      const response = await fetchPositionEvents({
        position_id: row.item.position_id,
        exchange: row.item.exchange,
        symbol: row.item.symbol,
        position_side: row.item.position_side,
        margin_mode: row.item.margin_mode,
        entry_time: row.item.entry_time,
        strategy: row.item.strategy_name,
        version: row.item.strategy_version,
        event_limit: POSITION_EVENTS_LIMIT
      });
      if (!isMountedRef.current) {
        return response;
      }
      setPositionEventsStateByRowKey((previous) => ({
        ...previous,
        [row.rowKey]: {
          loading: false,
          error: "",
          snapshot: response
        }
      }));
      return response;
    } catch (error) {
      if (isMountedRef.current) {
        setPositionEventsStateByRowKey((previous) => ({
          ...previous,
          [row.rowKey]: {
            ...previous[row.rowKey],
            loading: false,
            error: toErrorMessage(error)
          }
        }));
      }
      throw error;
    }
  }, []);

  const handleChartPricePick = useCallback(
    (price: number) => {
      if (tradeOrderTab !== "limit" || !(price > 0)) {
        return;
      }
      setTradePriceInput(formatInputValue(price));
    },
    [tradeOrderTab]
  );

  const handleSubmitTrade = useCallback(
    async (side: "long" | "short") => {
      if (tradeSubmitting || tradeReadOnly) {
        return;
      }
      const exchange = selectedExchangeRef.current.trim();
      const symbol = selectedSymbolRef.current.trim();
      const timeframe = selectedTimeframeRef.current.trim();
      if (!exchange || !symbol) {
        showTradeNotice(false);
        return;
      }
      const orderType = tradeOrderTab;
      const entry =
        orderType === "limit" ? parseTradeInputNumber(tradePriceInput) : firstPositiveNumber(tradeAnchorPrice, selectedSymbolMeta?.last_price);
      if (!(entry > 0)) {
        showTradeNotice(false);
        return;
      }
      const takeProfitPrice = resolveTradeTargetPrice(entry, side, takeProfitPct, "tp");
      const stopLossPrice = resolveTradeTargetPrice(entry, side, stopLossPct, "sl");
      setTradeSubmitting(true);
      try {
        const response = await submitTrade({
          action: "open",
          exchange,
          symbol,
          side,
          timeframe,
          order_type: orderType,
          amount: 0,
          entry,
          tp: takeProfitPrice,
          sl: stopLossPrice
        });
        if (!isMountedRef.current) {
          return;
        }
        if (response.risk_error || response.execution_error) {
          showTradeNotice(false);
          return;
        }
        if (response.manual_order) {
          setRuntime((previous) =>
            previous
              ? {
                  ...previous,
                  orders: upsertRuntimeOrders(previous.orders, response.manual_order)
                }
              : previous
          );
          setBottomTab("orders");
        } else if (response.executed || response.position_found) {
          setBottomTab("positions");
        } else {
          showTradeNotice(false);
          return;
        }
        showTradeNotice(true);
        void refreshRuntime({ showLoading: false });
      } catch (error) {
        console.warn("submit tradingview trade failed", error);
        if (isMountedRef.current) {
          showTradeNotice(false);
        }
      } finally {
        if (isMountedRef.current) {
          setTradeSubmitting(false);
        }
      }
    },
    [
      selectedSymbolMeta?.last_price,
      stopLossPct,
      takeProfitPct,
      tradeAnchorPrice,
      tradeOrderTab,
      tradePriceInput,
      tradeReadOnly,
      tradeSubmitting,
      showTradeNotice
    ]
  );

  const handleMovePositionLevel = useCallback(
    async (move: CandlesChartPositionLevelMove) => {
      if (tradeReadOnly || tradeSubmitting) {
        return;
      }
      const position =
        currentPositions.find((item) => item.position_id === move.positionID) ||
        currentPositions.find(
          (item) =>
            move.positionID === 0 &&
            item.position_id === 0 &&
            normalizeExchangeName(item.exchange) === normalizeExchangeName(selectedExchangeRef.current) &&
            normalizeSymbolKey(item.symbol) === normalizeSymbolKey(selectedSymbolRef.current) &&
            (item.position_side || "").trim().toLowerCase() === move.side
        );
      if (!position) {
        return;
      }
      const tp = move.kind === "tp" ? move.price : firstPositiveNumber(position.take_profit_price);
      const sl = move.kind === "sl" ? move.price : firstPositiveNumber(position.stop_loss_price);
      setTradeSubmitting(true);
      try {
        const response = await submitTrade({
          action: "move_tpsl",
          exchange: position.exchange,
          symbol: position.symbol,
          side: position.position_side,
          timeframe: selectedTimeframeRef.current,
          entry: firstPositiveNumber(position.current_price, position.entry_price),
          tp,
          sl
        });
        if (response.risk_error || response.execution_error || (!response.executed && !response.position_found)) {
          if (isMountedRef.current) {
            showTradeNotice(false);
          }
          return;
        }
        if (isMountedRef.current) {
          showTradeNotice(true);
          void refreshRuntime({ showLoading: false });
        }
      } catch (error) {
        console.warn("move tradingview tp/sl failed", error);
        if (isMountedRef.current) {
          showTradeNotice(false);
        }
      } finally {
        if (isMountedRef.current) {
          setTradeSubmitting(false);
        }
      }
    },
    [currentPositions, refreshRuntime, showTradeNotice, tradeReadOnly, tradeSubmitting]
  );

  const handleSubmitClose = useCallback(async () => {
    const position = closeModalPosition;
    if (!position || closeSubmitting || tradeReadOnly) {
      return;
    }
    setCloseSubmitting(true);
    try {
      const response = await submitTrade({
        action: "partial_close",
        exchange: position.exchange,
        symbol: position.symbol,
        side: position.position_side,
        timeframe: selectedTimeframeRef.current,
        entry: firstPositiveNumber(position.current_price, position.entry_price),
        tp: firstPositiveNumber(position.take_profit_price),
        sl: firstPositiveNumber(position.stop_loss_price)
      });
      if (response.risk_error || response.execution_error || (!response.executed && !response.position_found)) {
        if (isMountedRef.current) {
          showTradeNotice(false);
        }
        return;
      }
      if (!isMountedRef.current) {
        return;
      }
      setCloseModalPosition(null);
      showTradeNotice(true);
      void refreshRuntime({ showLoading: false });
    } catch (error) {
      console.warn("submit tradingview partial close failed", error);
      if (isMountedRef.current) {
        showTradeNotice(false);
      }
    } finally {
      if (isMountedRef.current) {
        setCloseSubmitting(false);
      }
    }
  }, [closeModalPosition, closeSubmitting, refreshRuntime, showTradeNotice, tradeReadOnly]);

  const handleSubmitFullClose = useCallback(
    async (position: TradingViewPosition) => {
      if (tradeReadOnly || tradeSubmitting) {
        return;
      }
      setTradeSubmitting(true);
      try {
        const response = await submitTrade({
          action: "full_close",
          exchange: position.exchange,
          symbol: position.symbol,
          side: position.position_side,
          timeframe: selectedTimeframeRef.current,
          entry: firstPositiveNumber(position.current_price, position.entry_price),
          tp: firstPositiveNumber(position.take_profit_price),
          sl: firstPositiveNumber(position.stop_loss_price)
        });
        if (response.risk_error || response.execution_error || (!response.executed && !response.position_found)) {
          if (isMountedRef.current) {
            showTradeNotice(false);
          }
          return;
        }
        if (!isMountedRef.current) {
          return;
        }
        setRuntime((previous) =>
          previous
            ? {
                ...previous,
                positions: previous.positions.filter((item) => !isSameTradingViewPosition(item, position))
              }
            : previous
        );
        setCloseModalPosition((current) => (current && isSameTradingViewPosition(current, position) ? null : current));
        showTradeNotice(true);
        void refreshRuntime({ showLoading: false });
      } catch (error) {
        console.warn("submit tradingview full close failed", error);
        if (isMountedRef.current) {
          showTradeNotice(false);
        }
      } finally {
        if (isMountedRef.current) {
          setTradeSubmitting(false);
        }
      }
    },
    [refreshRuntime, showTradeNotice, tradeReadOnly, tradeSubmitting]
  );

  const handleOpenDelegateModal = useCallback((position: TradingViewPosition) => {
    setDelegateModalPosition(position);
  }, []);

  const handleSubmitPositionDelegate = useCallback(async () => {
    const position = delegateModalPosition;
    if (!position || !delegateIdentity || delegateSubmitting) {
      return;
    }
    const option = strategyOptions.find((item) => buildStrategyOptionIdentity(item) === delegateIdentity);
    if (!option) {
      return;
    }
    setDelegateSubmitting(true);
    try {
      const response = await delegatePositionStrategy({
        exchange: position.exchange,
        symbol: position.symbol,
        side: position.position_side,
        strategy_name: option.strategy_name,
        trade_timeframes: option.trade_timeframes
      });
      if (!isMountedRef.current) {
        return;
      }
      if (!response.delegated) {
        showTradeNotice(false);
        return;
      }
      setDelegateModalPosition(null);
      showTradeNotice(true);
      void refreshRuntime({ showLoading: false });
    } catch (error) {
      console.warn("delegate tradingview position failed", error);
      if (isMountedRef.current) {
        showTradeNotice(false);
      }
    } finally {
      if (isMountedRef.current) {
        setDelegateSubmitting(false);
      }
    }
  }, [delegateIdentity, delegateModalPosition, delegateSubmitting, refreshRuntime, showTradeNotice, strategyOptions]);

  const filteredSymbols = useMemo(() => {
    const source = runtime?.symbols || [];
    const keyword = symbolSearch.trim().toLowerCase();
    const matched = keyword
      ? source.filter((item) => {
          const haystack = `${item.display_symbol} ${item.symbol} ${item.base || ""} ${item.quote || ""}`.toLowerCase();
          return haystack.includes(keyword);
        })
      : [...source];
    matched.sort((a, b) => b.turnover_24h - a.turnover_24h || b.change_24h_pct - a.change_24h_pct);
    return matched;
  }, [runtime, symbolSearch]);

  useEffect(() => {
    if (!restoreSidebarScrollPendingRef.current || runtimeLoading) {
      return;
    }
    const list = sidebarListRef.current;
    if (!list) {
      return;
    }
    window.requestAnimationFrame(() => {
      list.scrollTop = sidebarScrollTopRef.current;
      restoreSidebarScrollPendingRef.current = false;
    });
  }, [filteredSymbols.length, runtimeLoading]);

  useEffect(() => {
    if (runtimeLoading || restoreSidebarScrollPendingRef.current || !selectedSymbol) {
      return;
    }
    const row = symbolRowRefs.current.get(selectedSymbol);
    if (!row) {
      return;
    }
    window.requestAnimationFrame(() => {
      row.scrollIntoView({ block: "nearest" });
    });
  }, [runtimeLoading, selectedExchange, selectedSymbol, filteredSymbols.length]);

  useEffect(() => {
    const seedKey = `${selectedExchange}|${selectedSymbol}|${tradeOrderTab}`;
    if (tradePriceSeedKeyRef.current === seedKey) {
      return;
    }
    tradePriceSeedKeyRef.current = seedKey;
    if (tradeOrderTab !== "limit") {
      return;
    }
    if (!(tradeAnchorPrice > 0)) {
      setTradePriceInput("");
      return;
    }
    setTradePriceInput(formatInputValue(tradeAnchorPrice));
  }, [selectedExchange, selectedSymbol, tradeAnchorPrice, tradeOrderTab]);

  useEffect(() => {
    if (!closeModalPosition) {
      return;
    }
    setClosePercent(80);
  }, [closeModalPosition]);

  useEffect(() => {
    if (hasPersistedTradeLeverageRef.current) {
      return;
    }
    const fallback = selectedPositionForTrade?.leverage_multiplier;
    if (!Number.isFinite(fallback) || !fallback || fallback <= 0) {
      return;
    }
    setTradeLeverageValue(fallback);
  }, [selectedPositionForTrade]);

  useEffect(() => {
    if (!delegateModalPosition) {
      setDelegateIdentity("");
      return;
    }
    const currentIdentity = buildPositionStrategyIdentity(delegateModalPosition);
    const matchedCurrent = strategyOptions.find((item) => buildStrategyOptionIdentity(item) === currentIdentity);
    if (matchedCurrent) {
      setDelegateIdentity(buildStrategyOptionIdentity(matchedCurrent));
      return;
    }
    setDelegateIdentity(strategyOptions.length > 0 ? buildStrategyOptionIdentity(strategyOptions[0]) : "");
  }, [delegateModalPosition, strategyOptions]);

  useEffect(() => {
    if (!activeBacktestTask || !isBacktestTaskActive(activeBacktestTask.status)) {
      return;
    }
    let cancelled = false;
    const tick = async () => {
      try {
        const response = await fetchBacktestTask(activeBacktestTask.id);
        if (cancelled || !isMountedRef.current) {
          return;
        }
        setBacktestDays((previous) =>
          upsertBacktestTaskDay(previous, response.task, tzOffsetMinRef.current)
        );
        if (response.task.status === "succeeded") {
          if (visibleBacktestTaskID === response.task.id) {
            await loadBacktestOverlayForTask(response.task.id);
          }
          void refreshCurrentBacktestDay({ showLoading: false });
        } else if (response.task.status === "failed") {
          setBacktestError(response.task.error_message || "");
          if (visibleBacktestTaskID === response.task.id) {
            setVisibleBacktestTaskID(0);
          }
        }
      } catch (error) {
        if (cancelled || !isMountedRef.current) {
          return;
        }
        setBacktestError(toErrorMessage(error));
      }
    };
    void tick();
    const timer = window.setInterval(() => {
      void tick();
    }, BACKTEST_STATUS_POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [activeBacktestTask, loadBacktestOverlayForTask, refreshCurrentBacktestDay, visibleBacktestTaskID]);

  useEffect(() => {
    if (!pendingBacktestDisplay) {
      return;
    }
    if (selectedExchange !== pendingBacktestDisplay.exchange || selectedSymbol !== pendingBacktestDisplay.symbol) {
      return;
    }
    if (!candles || candles.exchange !== selectedExchange || candles.symbol !== selectedSymbol || candles.timeframe !== selectedTimeframe) {
      return;
    }
    const focusRange = resolveBacktestVisibleRange(candles.candles, selectedTimeframe, pendingBacktestDisplay.rangeStartMS, pendingBacktestDisplay.rangeEndMS);
    if (!focusRange) {
      setExpiredBacktestTaskIDs((previous) => ({
        ...previous,
        [pendingBacktestDisplay.taskID]: true
      }));
      setVisibleBacktestTaskID((current) => (current === pendingBacktestDisplay.taskID ? 0 : current));
      setPendingBacktestDisplay((current) => (current?.taskID === pendingBacktestDisplay.taskID ? null : current));
      setPendingBacktestEventJump((current) => (current?.taskID === pendingBacktestDisplay.taskID ? null : current));
      return;
    }
    let cancelled = false;
    const apply = async () => {
      try {
        if (!backtestOverlayByTaskID[pendingBacktestDisplay.taskID]) {
          await loadBacktestOverlayForTask(pendingBacktestDisplay.taskID);
        }
        if (cancelled || !isMountedRef.current) {
          return;
        }
        setExpiredBacktestTaskIDs((previous) => {
          if (!previous[pendingBacktestDisplay.taskID]) {
            return previous;
          }
          const next = { ...previous };
          delete next[pendingBacktestDisplay.taskID];
          return next;
        });
        setVisibleBacktestTaskID(pendingBacktestDisplay.taskID);
        chartRef.current?.focusLogicalRange(focusRange.from, focusRange.to);
      } catch (error) {
        if (!cancelled && isMountedRef.current) {
          setBacktestError(toErrorMessage(error));
        }
      } finally {
        if (!cancelled && isMountedRef.current) {
          setPendingBacktestDisplay((current) => (current?.taskID === pendingBacktestDisplay.taskID ? null : current));
        }
      }
    };
    void apply();
    return () => {
      cancelled = true;
    };
  }, [
    backtestOverlayByTaskID,
    candles,
    loadBacktestOverlayForTask,
    pendingBacktestDisplay,
    selectedExchange,
    selectedSymbol,
    selectedTimeframe
  ]);

  useEffect(() => {
    if (!pendingBacktestEventJump) {
      return;
    }
    if (selectedExchange !== pendingBacktestEventJump.exchange || selectedSymbol !== pendingBacktestEventJump.symbol) {
      return;
    }
    if (visibleBacktestTaskID !== pendingBacktestEventJump.taskID) {
      return;
    }
    if (!candles || candles.exchange !== selectedExchange || candles.symbol !== selectedSymbol || candles.timeframe !== selectedTimeframe) {
      return;
    }
    const currentRange = chartRef.current?.getVisibleLogicalRange() ?? null;
    const focusRange = resolveCenteredBacktestEventVisibleRange(
      candles.candles,
      selectedTimeframe,
      pendingBacktestEventJump.eventAtMS,
      currentRange
    );
    if (!focusRange) {
      setPendingBacktestEventJump((current) => (current?.eventID === pendingBacktestEventJump.eventID ? null : current));
      return;
    }
    chartRef.current?.focusLogicalRange(focusRange.from, focusRange.to);
    chartRef.current?.focusCrosshairAtTime(pendingBacktestEventJump.eventAtMS, pendingBacktestEventJump.eventPrice);
    setPendingBacktestEventJump((current) => (current?.eventID === pendingBacktestEventJump.eventID ? null : current));
  }, [candles, pendingBacktestEventJump, selectedExchange, selectedSymbol, selectedTimeframe, visibleBacktestTaskID]);

  useEffect(() => {
    if (!activePositionEventRowKey) {
      return;
    }
    if (currentPositionRows.some((item) => item.rowKey === activePositionEventRowKey)) {
      return;
    }
    setActivePositionEventRowKey("");
  }, [activePositionEventRowKey, currentPositionRows]);

  useEffect(() => {
    if (!activePositionEventRow) {
      return;
    }
    let stopped = false;
    const sync = async (): Promise<void> => {
      try {
        await loadPositionEventsForRow(activePositionEventRow);
      } catch (error) {
        if (!stopped) {
          console.warn("poll tradingview position events failed", error);
        }
      }
    };
    void sync();
    const timer = window.setInterval(() => {
      if (!isDocumentVisible()) {
        return;
      }
      void sync();
    }, POSITION_EVENTS_POLL_MS);
    return () => {
      stopped = true;
      window.clearInterval(timer);
    };
  }, [activePositionEventRow, loadPositionEventsForRow]);

  function handleSelectSymbol(nextSymbol: string): void {
    if (!nextSymbol || nextSymbol === selectedSymbol) {
      return;
    }
    const timeframes = runtime?.timeframes || [];
    const nextTimeframe = timeframes.includes(selectedTimeframe)
      ? selectedTimeframe
      : resolvePreferredTimeframe(nextSymbol, runtime, selectedTimeframe, timeframeBySymbolRef.current);
    if (selectedSymbol && selectedTimeframe && nextTimeframe) {
      setViewportTransfer({
        sourceKey: buildViewportKey(selectedSymbol, selectedTimeframe),
        targetKey: buildViewportKey(nextSymbol, nextTimeframe),
        token: buildViewportTransferToken(selectedSymbol, selectedTimeframe, nextSymbol, nextTimeframe)
      });
    }
    setSelectedSymbol(nextSymbol);
    if (nextTimeframe) {
      timeframeBySymbolRef.current.set(nextSymbol, nextTimeframe);
      setSelectedTimeframe(nextTimeframe);
    }
  }

  function handleSelectTimeframe(nextTimeframe: string): void {
    if (!nextTimeframe || nextTimeframe === selectedTimeframe) {
      return;
    }
    if (selectedSymbol && selectedTimeframe) {
      setViewportTransfer({
        sourceKey: buildViewportKey(selectedSymbol, selectedTimeframe),
        targetKey: buildViewportKey(selectedSymbol, nextTimeframe),
        token: buildViewportTransferToken(selectedSymbol, selectedTimeframe, selectedSymbol, nextTimeframe)
      });
      timeframeBySymbolRef.current.set(selectedSymbol, nextTimeframe);
    }
    setSelectedTimeframe(nextTimeframe);
  }

  function toggleChartMaximized(): void {
    setChartMaximized((previous) => !previous);
  }

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const key = event.key;
      const lower = key.toLowerCase();
      const hasCtrlOrMeta = event.ctrlKey || event.metaKey;
      if (event.key === "Escape" && hasOpenModal) {
        event.preventDefault();
        if (showShortcutModal) {
          setShowShortcutModal(false);
          return;
        }
        if (showTradeModeMenu) {
          setShowTradeModeMenu(false);
          return;
        }
        if (showOrderSettingsModal) {
          setShowOrderSettingsModal(false);
          return;
        }
        if (closeModalPosition) {
          setCloseModalPosition(null);
          return;
        }
        if (delegateModalPosition) {
          setDelegateModalPosition(null);
          return;
        }
        if (showLeverageModal) {
          setShowLeverageModal(false);
        }
        return;
      }
      if (hasOpenModal) {
        return;
      }
      if (shouldIgnoreShortcut(event)) {
        return;
      }
      const chart = chartRef.current;
      if (key === "?") {
        event.preventDefault();
        setShowShortcutModal((prev) => !prev);
        return;
      }
      if (!hasCtrlOrMeta && !event.altKey && event.shiftKey && lower === "f") {
        event.preventDefault();
        toggleChartMaximized();
        return;
      }
      if (event.altKey && !hasCtrlOrMeta && !event.shiftKey && (event.code === "KeyR" || lower === "r")) {
        event.preventDefault();
        chart?.resetView();
        return;
      }
      if (hasCtrlOrMeta && !event.altKey && !event.shiftKey && key === "ArrowUp") {
        event.preventDefault();
        chart?.zoomIn();
        return;
      }
      if (hasCtrlOrMeta && !event.altKey && !event.shiftKey && key === "ArrowDown") {
        event.preventDefault();
        chart?.zoomOut();
        return;
      }
      if (!event.altKey && !hasCtrlOrMeta && !event.shiftKey && key === "ArrowLeft") {
        event.preventDefault();
        chart?.panLeft();
        return;
      }
      if (!event.altKey && !hasCtrlOrMeta && !event.shiftKey && key === "ArrowRight") {
        event.preventDefault();
        chart?.panRight();
        return;
      }
      if (event.altKey || hasCtrlOrMeta || event.shiftKey) {
        return;
      }
      const index = getDigitIndex(key);
      if (index === null) {
        return;
      }
      const nextTimeframe = runtime?.timeframes[index];
      if (!nextTimeframe || nextTimeframe === selectedTimeframe) {
        return;
      }
      event.preventDefault();
      handleSelectTimeframe(nextTimeframe);
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [
    closeModalPosition,
    delegateModalPosition,
    hasOpenModal,
    runtime,
    selectedSymbol,
    selectedTimeframe,
    showLeverageModal,
    showOrderSettingsModal,
    showShortcutModal,
    showTradeModeMenu
  ]);

  return (
    <div className={`tv-app ${chartMaximized ? "is-chart-maximized" : ""}`}>
      {tradeNotice ? (
        <div className={`tv-trade-notice ${tradeNotice.success ? "is-success" : "is-error"}`} role="status" aria-live="polite">
          {tradeNotice.message}
        </div>
      ) : null}
      <header className="tv-topbar">
        <div className="tv-topbar-grid">
          <div className="tv-topbar-exchanges">
            <div className="tv-exchange-tabs" role="tablist" aria-label="交易所切换">
              {(runtime?.exchanges || []).map((item) => (
                <button
                  key={item.name}
                  type="button"
                  role="tab"
                  aria-selected={selectedExchange === item.name}
                  className={`tv-exchange-tab ${selectedExchange === item.name ? "is-active" : ""}`}
                  onClick={() => setSelectedExchange(item.name)}
                  disabled={runtimeLoading || !runtime}
                >
                  {item.display_name}
                </button>
              ))}
            </div>
          </div>
          <div className="tv-topbar-market">
            <div className="tv-symbol-summary">
              <div className="tv-symbol-title-row">
                <div className="tv-symbol-name">{selectedDisplaySymbol || "--"}</div>
                {selectedSymbolMeta?.market_type ? (
                  <span className="tv-badge">{marketTypeLabel(selectedSymbolMeta.market_type)}</span>
                ) : null}
                {selectedSymbolMeta?.is_held ? <span className="tv-badge is-held">持仓中</span> : null}
              </div>
            </div>
            <div className="tv-topbar-metrics">
              <Metric label="最新价格" value={formatPrice(selectedSymbolMeta?.last_price)} tone={priceTone(selectedSymbolMeta?.change_24h_pct || 0)} />
              <Metric label="24 小时涨跌幅" value={formatSignedPercent(selectedSymbolMeta?.change_24h_pct)} tone={priceTone(selectedSymbolMeta?.change_24h_pct || 0)} />
              <Metric label="24 小时最高" value={formatPrice(selectedSymbolMeta?.high_24h)} />
              <Metric label="24 小时最低" value={formatPrice(selectedSymbolMeta?.low_24h)} />
              <Metric label="24 小时成交额" value={formatTurnover(selectedSymbolMeta?.turnover_24h)} />
            </div>
          </div>
          <div className="tv-topbar-mode" aria-label="当前运行模式">
            <div className="tv-mode-status">
              <div className="tv-symbol-name">{formatRuntimeMode(runtime?.mode)}</div>
              <div
                className="tv-status-anchor"
                onMouseEnter={handleStatusPopoverOpen}
                onMouseLeave={handleStatusPopoverClose}
              >
                <button
                  type="button"
                  className="tv-status-trigger"
                  aria-label={connectionStatusAriaLabel(connectionStatus)}
                  onFocus={handleStatusPopoverOpen}
                  onBlur={handleStatusPopoverClose}
                >
                  <span className={`tv-connection-dot tv-connection-dot-large is-${connectionStatus}`} />
                </button>
                {showStatusPopover ? (
                  <div className="tv-status-popover" role="dialog" aria-label="运行状态详情">
                    <div className="tv-status-popover-section">
                      <div className="tv-status-popover-title">版本</div>
                      <div className="tv-status-popover-grid">
                        <div className="tv-status-popover-key">tag</div>
                        <div className="tv-status-popover-value">{formatStatusPopoverValue(exporterStatus?.version?.tag)}</div>
                        <div className="tv-status-popover-key">commit</div>
                        <div className="tv-status-popover-value">{formatStatusPopoverValue(exporterStatus?.version?.commit)}</div>
                        <div className="tv-status-popover-key">build_time</div>
                        <div className="tv-status-popover-value">{formatStatusPopoverValue(exporterStatus?.version?.build_time)}</div>
                        <div className="tv-status-popover-key">runtime</div>
                        <div className="tv-status-popover-value">
                          {exporterStatusLoading && !exporterStatus ? "加载中..." : formatStatusPopoverValue(exporterStatus?.runtime?.human)}
                        </div>
                        <div className="tv-status-popover-key">mode</div>
                        <div className="tv-status-popover-value">{formatStatusPopoverValue(runtime?.mode)}</div>
                      </div>
                    </div>
                    <div className="tv-status-popover-section">
                      <div className="tv-status-popover-title">模块状态</div>
                      {exporterStatusModules.length > 0 ? (
                        <div className="tv-status-module-list">
                          {exporterStatusModules.map((item) => (
                            <div key={item.name} className="tv-status-module-row">
                              <span className="tv-status-module-name">{item.name}</span>
                              <span className="tv-status-module-state">{item.state}</span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="tv-status-popover-empty">
                          {exporterStatusLoading ? "加载中..." : "当前没有模块状态"}
                        </div>
                      )}
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        </div>
      </header>

      <div className="tv-main-grid">
        <aside className="tv-sidebar">
          <div className="tv-sidebar-controls">
            <input
              className="tv-search-input"
              value={symbolSearch}
              onChange={(event) => setSymbolSearch(event.target.value)}
              placeholder="搜索交易对"
            />
          </div>
          <div ref={sidebarListRef} className="tv-sidebar-list" onScroll={handleSidebarListScroll}>
            {runtimeLoading ? <div className="tv-empty-block">加载交易对中...</div> : null}
            {!runtimeLoading && filteredSymbols.length === 0 ? (
              <div className="tv-empty-block">当前没有可展示的活跃交易对</div>
            ) : null}
            {filteredSymbols.map((item) => (
              <button
                key={item.symbol}
                ref={(node) => {
                  symbolRowRefs.current.set(item.symbol, node);
                }}
                className={`tv-market-row ${selectedSymbol === item.symbol ? "is-selected" : ""}`}
                onClick={() => handleSelectSymbol(item.symbol)}
              >
                <div className="tv-market-col tv-market-col-symbol">
                  <div className="tv-market-symbol-line">
                    <span>{item.display_symbol}</span>
                  </div>
                  <div className="tv-market-meta">{formatTurnover(item.turnover_24h)}</div>
                </div>
                <div className="tv-market-col tv-market-col-price">{formatPrice(item.last_price)}</div>
                <div className={`tv-market-col tv-market-col-change ${priceTone(item.change_24h_pct)}`}>
                  {formatSignedPercent(item.change_24h_pct)}
                </div>
              </button>
            ))}
          </div>
        </aside>

        <section className="tv-center">
          <div className="tv-chart-toolbar">
            <div className="tv-toolbar-group">
              {(runtime?.timeframes || []).map((item) => (
                <button
                  key={item}
                  className={`tv-toolbar-button ${selectedTimeframe === item ? "is-active" : ""}`}
                  onClick={() => handleSelectTimeframe(item)}
                >
                  {item}
                </button>
              ))}
            </div>
            <div className="tv-toolbar-group">
              <button className="tv-toolbar-button tv-toolbar-icon-button" onClick={() => setShowShortcutModal(true)} aria-label="快捷键" title="快捷键">
                ?
              </button>
            </div>
          </div>
          <div className="tv-chart-panel">
            <CandlesChart
              ref={chartRef}
              candles={candles?.candles || []}
              indicators={displayedIndicators.filter((item) => visibleIndicators[item.id] !== false)}
              events={displayedChartEvents}
              visibleIndicators={visibleIndicators}
              indicatorLegend={indicatorLegend}
              viewportKey={chartViewportKey}
              initialViewportSnapshots={viewportSnapshotsRef.current}
              transferViewportFromKey={viewportTransfer?.targetKey === chartViewportKey ? viewportTransfer.sourceKey : ""}
              transferViewportToken={viewportTransfer?.targetKey === chartViewportKey ? viewportTransfer.token : ""}
              onViewportSnapshotChange={handleViewportSnapshotChange}
              onToggleIndicator={(id) =>
                setVisibleIndicators((prev) => ({
                  ...prev,
                  [id]: prev[id] === false
                }))
              }
              onRunBacktestArea={(selection) => {
                void handleRunBacktestArea(selection);
              }}
              onPricePick={tradePanelDisabled ? undefined : handleChartPricePick}
              positionLevels={selectedPositionLevels}
              onMovePositionLevel={
                tradePanelDisabled
                  ? undefined
                  : (move) => {
                      void handleMovePositionLevel(move);
                    }
              }
              loading={candlesLoading}
              emptyText="先从左侧选择一个活跃交易对"
            />
          </div>
        </section>

        <aside className="tv-trade-panel">
          <div className={`tv-trade-shell ${tradePanelDisabled ? "is-disabled" : ""}`}>
            <div className="tv-trade-mode-bar">
              <div ref={tradeModeMenuRef} className="tv-trade-mode-menu-anchor">
                <button
                  className="tv-trade-mode tv-trade-mode-trigger"
                  type="button"
                  disabled={tradePanelDisabled}
                  onClick={() => setShowTradeModeMenu((previous) => !previous)}
                  aria-haspopup="menu"
                  aria-expanded={showTradeModeMenu}
                >
                  <span>逐仓</span>
                  <ChevronDownIcon />
                </button>
                {showTradeModeMenu ? (
                  <div className="tv-trade-mode-menu" role="menu" aria-label="保证金模式">
                    <button type="button" className="tv-trade-mode-menu-item is-disabled" role="menuitem" onClick={() => setShowTradeModeMenu(false)}>
                      <span>全仓</span>
                    </button>
                    <button type="button" className="tv-trade-mode-menu-item is-active" role="menuitem" onClick={() => setShowTradeModeMenu(false)}>
                      <span>逐仓</span>
                    </button>
                    <button type="button" className="tv-trade-mode-menu-item is-disabled" role="menuitem" onClick={() => setShowTradeModeMenu(false)}>
                      <span>批量修改</span>
                      <ChevronRightIcon />
                    </button>
                  </div>
                ) : null}
              </div>
              <button
                className="tv-trade-mode tv-trade-leverage-trigger"
                type="button"
                disabled={tradePanelDisabled}
                onClick={() => setShowLeverageModal(true)}
              >
                <span className="tv-trade-leverage-pair">
                  <span className="is-positive">{formatLeverage(tradeLeverageValue)}</span>
                  <span className="is-negative">{formatLeverage(tradeLeverageValue)}</span>
                </span>
                <ChevronDownIcon />
              </button>
            </div>
            <div className="tv-order-strip">
              <button
                className={`tv-order-tab ${tradeOrderTab === "limit" ? "is-active" : ""}`}
                type="button"
                disabled={tradePanelDisabled}
                onClick={() => setTradeOrderTab("limit")}
              >
                限价委托
              </button>
              <button
                className={`tv-order-tab ${tradeOrderTab === "market" ? "is-active" : ""}`}
                type="button"
                disabled={tradePanelDisabled}
                onClick={() => setTradeOrderTab("market")}
              >
                市价委托
              </button>
              <button className="tv-icon-button" type="button" aria-label="委托说明" disabled={tradePanelDisabled}>
                <InfoIcon />
              </button>
            </div>
            {tradeOrderTab === "limit" ? (
              <div className="tv-form-block">
                <label>价格 (USDT)</label>
                <input value={tradePriceInput} disabled={tradePanelDisabled} onChange={(event) => setTradePriceInput(event.target.value)} />
              </div>
            ) : null}
            <div className="tv-form-block">
              <button className="tv-form-select-head" type="button" disabled>
                <span>成本 (USDT)</span>
                <ChevronDownIcon />
              </button>
              <input value="" readOnly disabled />
            </div>
            <div className="tv-slider-row">
              <div className="tv-slider-track">
                <span />
                <span />
                <span />
                <span />
                <span />
              </div>
              <div className="tv-slider-labels">
                <span>0%</span>
                <span>25%</span>
                <span>50%</span>
                <span>75%</span>
                <span>100%</span>
              </div>
            </div>
            <div className="tv-trade-summary">
              <div className="tv-trade-summary-row is-top">
                <div className="tv-trade-summary-item">
                  <span>可用</span>
                  <strong>{formatUSDTValue(tradeAvailableUSDT)}</strong>
                </div>
                <button className="tv-icon-button tv-trade-summary-plus" type="button" aria-label="更多可用资金信息" disabled={tradePanelDisabled}>
                  <PlusIcon />
                </button>
              </div>
              <div className="tv-trade-summary-row is-bottom">
                <div className="tv-trade-summary-item">
                  <span>可开多</span>
                  <strong>{formatUSDTValue(tradeCanOpenLongUSDT)}</strong>
                </div>
                <div className="tv-trade-summary-item is-right">
                  <span>可开空</span>
                  <strong>{formatUSDTValue(tradeCanOpenShortUSDT)}</strong>
                </div>
              </div>
            </div>
            <div className="tv-trade-divider" />
            <div className="tv-form-block">
              <label>止盈收益率 (%)</label>
              <input value={takeProfitPct} disabled={tradePanelDisabled} onChange={(event) => setTakeProfitPct(event.target.value)} />
            </div>
            <div className="tv-form-block">
              <label>止损收益率 (%)</label>
              <input value={stopLossPct} disabled={tradePanelDisabled} onChange={(event) => setStopLossPct(event.target.value)} />
            </div>
            <div className="tv-trade-actions">
              <button
                className="tv-long-button"
                disabled={tradePanelDisabled || tradeReadOnly || tradeSubmitting}
                onClick={() => {
                  void handleSubmitTrade("long");
                }}
              >
                开多
              </button>
              <button
                className="tv-short-button"
                disabled={tradePanelDisabled || tradeReadOnly || tradeSubmitting}
                onClick={() => {
                  void handleSubmitTrade("short");
                }}
              >
                开空
              </button>
            </div>
            <div className="tv-trade-side-grid">
              <div className="tv-trade-side-meta">
                <div className="tv-trade-side-meta-row">
                  <span>数量</span>
                  <strong>--</strong>
                </div>
                <div className="tv-trade-side-meta-row">
                  <span>最高买价</span>
                  <strong>{formatPrice(tradeBestBidPrice)}</strong>
                </div>
                <div className="tv-trade-side-meta-row">
                  <span>预估强平价</span>
                  <strong>{formatPrice(tradeEstimatedLongLiqPrice)}</strong>
                </div>
              </div>
              <div className="tv-trade-side-meta">
                <div className="tv-trade-side-meta-row is-right">
                  <span>数量</span>
                  <strong>--</strong>
                </div>
                <div className="tv-trade-side-meta-row is-right">
                  <span>最低卖价</span>
                  <strong>{formatPrice(tradeBestAskPrice)}</strong>
                </div>
                <div className="tv-trade-side-meta-row is-right">
                  <span>预估强平价</span>
                  <strong>{formatPrice(tradeEstimatedShortLiqPrice)}</strong>
                </div>
              </div>
            </div>
          </div>
        </aside>
      </div>

      <section className="tv-bottom-dock">
        <div className="tv-bottom-left">
          <div className="tv-bottom-tabs">
            <button className={`tv-bottom-tab ${bottomTab === "orders" ? "is-active" : ""}`} onClick={() => setBottomTab("orders")}>
              当前委托
              {currentOrders.length > 0 ? <span className="tv-bottom-tab-badge">{currentOrders.length}</span> : null}
            </button>
            <button className={`tv-bottom-tab ${bottomTab === "positions" ? "is-active" : ""}`} onClick={() => setBottomTab("positions")}>
              当前仓位
              {currentPositions.length > 0 ? <span className="tv-bottom-tab-badge">{currentPositions.length}</span> : null}
            </button>
            <button className={`tv-bottom-tab ${bottomTab === "history" ? "is-active" : ""}`} onClick={() => setBottomTab("history")}>
              历史仓位
            </button>
            <button className={`tv-bottom-tab ${bottomTab === "backtests" ? "is-active" : ""}`} onClick={() => setBottomTab("backtests")}>
              回测数据
            </button>
          </div>
          <div className="tv-bottom-content">
            {bottomTab === "orders" ? (
              currentOrders.length > 0 ? (
                <div className="tv-orders-table">
                  <div className="tv-orders-header">
                    <span />
                    <span>交易所 / 交易对</span>
                    <span>方向</span>
                    <span>委托类型</span>
                    <span>委托价格</span>
                    <span>止盈 / 止损</span>
                    <span>创建时间</span>
                    <span>状态</span>
                  </div>
                  {currentOrders.map((item) => (
                    <div key={`order-${item.id}`} className="tv-orders-row">
                      <div className={`tv-direction-bar ${directionBarClass(item.position_side)}`} />
                      <MarketIdentityCell
                        exchange={item.exchange}
                        symbol={item.display_symbol || formatDisplaySymbol(item.symbol)}
                        leverage={item.leverage_multiplier}
                        onClick={() => handleSelectMarketSymbol(item.exchange, item.symbol)}
                      />
                      <span className={priceTone(resolveOrderDirectionTone(item.position_side))}>{directionLabel(item.position_side)}</span>
                      <span>{formatOrderTypeLabel(item.order_type)}</span>
                      <span>{formatPrice(item.price)}</span>
                      <span>
                        TP {formatPrice(item.take_profit_price)} / SL {formatPrice(item.stop_loss_price)}
                      </span>
                      <span>{formatBacktestDateTime(item.started_at_ms || item.updated_at_ms || 0)}</span>
                      <span>{formatOrderStatus(item.result_status)}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="tv-empty-block">当前没有人工干预委托</div>
              )
            ) : null}

            {bottomTab === "positions" ? (
              currentPositionRows.length > 0 ? (
                <div className="tv-position-table">
                  <div className="tv-position-header">
                    <span />
                    <span>交易所 / 交易对</span>
                    <span>保证金</span>
                    <span>持仓数量</span>
                    <span>最新价格</span>
                    <span>开仓均价</span>
                    <span>浮动收益</span>
                    <span>最大浮盈</span>
                    <span>最大浮亏</span>
                    <span>操作</span>
                    <span>止盈 / 止损</span>
                    <span>开仓时间</span>
                    <span>持仓时长</span>
                    <span>策略</span>
                    <span>版本</span>
                    <span>链接</span>
                  </div>
                  {currentPositionRows.map((row) => {
                    const item = row.item;
                    return (
                      <div key={`position-${row.rowKey}`} className="tv-position-item">
                        <div className="tv-position-row">
                          <div className={`tv-direction-bar ${directionBarClass(item.position_side)}`} />
                          <MarketIdentityCell
                            className="tv-position-symbol"
                            exchange={item.exchange}
                            symbol={lookupDisplaySymbol(item.symbol, runtime?.symbols || [])}
                            leverage={item.leverage_multiplier}
                            onClick={() => {
                              setActivePositionEventRowKey(row.rowKey);
                              handleSelectMarketSymbol(item.exchange, item.symbol);
                            }}
                          />
                          <div className="tv-position-cell">
                            <strong>{formatTurnover(item.margin_amount)}</strong>
                          </div>
                          <div className="tv-position-cell">
                            <strong>{formatAmount(item.entry_quantity || item.entry_value)}</strong>
                          </div>
                          <div className="tv-position-cell">
                            <strong>{formatPrice(item.current_price)}</strong>
                          </div>
                          <div className="tv-position-cell">
                            <strong>{formatPrice(item.entry_price)}</strong>
                          </div>
                          <div className="tv-position-cell">
                            {renderCurrentPositionProfit(item, runtime?.mode)}
                          </div>
                          <div className="tv-position-cell tv-position-cell--extreme">
                            {renderPositionExtreme(item.max_floating_profit_amount, item.max_floating_profit_rate, runtime?.mode)}
                          </div>
                          <div className="tv-position-cell tv-position-cell--extreme">
                            {renderPositionExtreme(item.max_floating_loss_amount, item.max_floating_loss_rate, runtime?.mode, true)}
                          </div>
                          <div className="tv-position-actions">
                            <button
                              type="button"
                              className="tv-dock-action-button"
                              onClick={() => setCloseModalPosition(item)}
                              disabled={tradeReadOnly || closeSubmitting || tradeSubmitting}
                            >
                              平仓
                            </button>
                            <button
                              type="button"
                              className="tv-dock-action-button"
                              disabled={tradeReadOnly || tradeSubmitting}
                              onClick={() => {
                                void handleSubmitFullClose(item);
                              }}
                            >
                              市价全平
                            </button>
                          </div>
                          <div className="tv-position-tpsl">
                            <div>止盈 {formatPositionTargetWithRate(item, item.take_profit_price)}</div>
                            <div>止损 {formatPositionTargetWithRate(item, item.stop_loss_price)}</div>
                          </div>
                          <div className="tv-position-cell">
                            <strong>{item.entry_time || "--"}</strong>
                          </div>
                          <div className="tv-position-cell">
                            <strong>{formatOpenPositionHoldingDuration(item)}</strong>
                          </div>
                          <div className="tv-position-cell">
                            <strong>{(item.strategy_name || "").trim() || "--"}</strong>
                          </div>
                          <div className="tv-position-cell">
                            <strong>{(item.strategy_version || "").trim() || "--"}</strong>
                          </div>
                          <div className="tv-position-links-cell">
                            {renderPositionLinks(
                              row.rowKey,
                              lookupDisplaySymbol(item.symbol, runtime?.symbols || []),
                              positionLinkItemsByRowKey.get(row.rowKey) || []
                            )}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <div className="tv-empty-block">当前没有持仓</div>
              )
            ) : null}

            {bottomTab === "history" ? (
              historyPositions.length > 0 ? (
                <div className="tv-history-table">
                  <div className="tv-history-header">
                    <span />
                    <span>交易所 / 交易对</span>
                    <span>方向</span>
                    <span>仓位状态</span>
                    <span>开仓时间</span>
                    <span>平仓时间</span>
                    <span>实现收益</span>
                  </div>
                  {historyPositions.map((item) => (
                    <div key={buildHistoryPositionRowKey(item)} className="tv-history-row">
                      <div className={`tv-direction-bar ${directionBarClass(item.position_side)}`} />
                      <MarketIdentityCell
                        exchange={item.exchange}
                        symbol={lookupDisplaySymbol(item.symbol, runtime?.symbols || [])}
                        leverage={item.leverage_multiplier}
                        onClick={() => handleSelectMarketSymbol(item.exchange, item.symbol)}
                      />
                      <span className={priceTone(item.profit_amount)}>{directionLabel(item.position_side)}</span>
                      <span>{formatHistoryCloseStatus(item.close_status)}</span>
                      <span>{item.entry_time || "--"}</span>
                      <span>{item.exit_time || "--"}</span>
                      <span>{renderHistoryPositionProfit(item, runtime?.mode)}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="tv-empty-block">当前时间范围内没有历史平仓记录</div>
              )
            ) : null}

            {bottomTab === "backtests" ? (
              backtestDays.length > 0 ? (
                <div className="tv-backtest-table">
                  <div className="tv-backtest-header">
                    <span />
                    <span>交易所 / 交易对</span>
                    <span>开仓价格</span>
                    <span>平仓价格</span>
                    <span>已实现收益率</span>
                    <span>开仓时间</span>
                    <span>平仓时间</span>
                    <span>持仓时间</span>
                    <span>回测周期</span>
                    <span>回测范围</span>
                    <span>状态</span>
                    <span>创建时间</span>
                    <span>回测耗时</span>
                    <span>控制</span>
                    <span>展开</span>
                  </div>
                  {backtestDays.map((day) => (
                    <div key={day.date} className="tv-backtest-day-group">
                      <div className="tv-backtest-day-label">{day.date}</div>
                              {day.tasks.map((task) => {
                        const expanded = expandedBacktestTaskIDs[task.id] === true;
                        const taskOverlay = backtestOverlayByTaskID[task.id];
                        const taskEvents = (taskOverlay?.events || []).filter(
                          (item) => item.source === "signal" || item.source === "execution"
                        );
                        const isDisplayed = isBacktestOverlayDisplayed(task, visibleBacktestTaskID, selectedExchange, selectedSymbol);
                        return (
                          <div key={`backtest-${task.id}`} className="tv-backtest-item">
                            <div className="tv-backtest-row">
                              <div className={`tv-direction-bar ${directionBarClass(task.position_side)}`} />
                              <MarketIdentityCell
                                exchange={task.exchange}
                                symbol={task.display_symbol || task.symbol}
                                leverage={task.leverage_multiplier}
                              />
                              <span>{formatBacktestPrice(task.open_price)}</span>
                              <span>{formatBacktestPrice(task.close_price)}</span>
                              <span>{formatBacktestProfitRate(task.realized_profit_rate)}</span>
                              <span>{formatBacktestDateTime(task.open_time_ms)}</span>
                              <span>{formatBacktestDateTime(task.close_time_ms)}</span>
                              <span>{formatBacktestHoldingDuration(task)}</span>
                              <span>{formatBacktestTradeTimeframes(task.trade_timeframes)}</span>
                              <span>{formatBacktestRange(task.range_start_ms, task.range_end_ms)}</span>
                              <span className={`tv-backtest-status is-${normalizeBacktestStatusClass(task, expiredBacktestTaskIDs)}`}>
                                {formatBacktestStatus(task, expiredBacktestTaskIDs)}
                              </span>
                              <span>{formatBacktestDateTime(task.created_at_ms)}</span>
                              <span>{formatBacktestElapsed(task)}</span>
                              <span className="tv-backtest-control">
                                <button
                                  type="button"
                                  className={`tv-dock-action-button tv-backtest-control-button is-${normalizeBacktestControlClass(task, isDisplayed)}`}
                                  onClick={() => {
                                    void handleToggleBacktestVisibility(task);
                                  }}
                                  disabled={task.status === "pending" || task.status === "running"}
                                >
                                  {formatBacktestControlLabel(task, isDisplayed)}
                                </button>
                              </span>
                              <span className="tv-backtest-expand">
                                <button
                                  type="button"
                                  className="tv-dock-icon-button tv-backtest-expand-button"
                                  onClick={() => {
                                    void handleToggleBacktestEvents(task);
                                  }}
                                  disabled={task.status !== "succeeded"}
                                  aria-label={expanded ? "收起事件" : "展开事件"}
                                >
                                  <span className="tv-backtest-expand-icon">{expanded ? "▾" : "▸"}</span>
                                </button>
                              </span>
                            </div>
                            {expanded ? (
                              <TradingViewEventTable
                                className="tv-backtest-events"
                                events={taskEvents}
                                selectedEventID={selectedBacktestEventIDs[task.id] || ""}
                                emptyText="当前回测没有可展示的策略/风控事件"
                                onSelectEvent={(eventID) => handleSelectBacktestEventRow(task.id, eventID)}
                                onActivateEvent={(event) => {
                                  void handleActivateBacktestEvent(task, event);
                                }}
                              />
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  ))}
                  {hasOlderBacktestDays ? (
                    <button
                      type="button"
                      className="tv-backtest-load-more"
                      onClick={() => {
                        void loadOlderBacktestDay();
                      }}
                      disabled={backtestsLoadingMore}
                    >
                      {backtestsLoadingMore ? "加载中..." : "↓ 加载更早一天"}
                    </button>
                  ) : null}
                </div>
              ) : backtestsLoading ? (
                <div className="tv-empty-block">加载回测记录中...</div>
              ) : (
                <div className="tv-empty-block">当前没有回测记录</div>
              )
            ) : null}
          </div>
        </div>

        <aside className="tv-funds-panel">
          <div className="tv-panel-header">
            <div>
              <div className="tv-panel-title">{(funds?.exchange || selectedExchange || "--").toUpperCase()}</div>
              <div className="tv-panel-subtitle">{`${funds?.currency || "USDT"} 资产`}</div>
            </div>
          </div>
          <div className="tv-funds-list">
            <FundsRow label="币种权益" value={formatTurnover(funds?.total_equity_usdt)} />
            <FundsRow label="可用" value={formatTurnover(funds?.trading_usdt)} />
            <FundsRow label="浮动收益" value={formatSignedNumber(funds?.floating_profit_usdt)} tone={priceTone(funds?.floating_profit_usdt || 0)} />
            <FundsRow label="占用" value={formatTurnover(funds?.margin_in_use_usdt)} />
          </div>
        </aside>
      </section>

      {showLeverageModal ? (
        <Modal title="调整杠杆" onClose={() => setShowLeverageModal(false)} className="tv-leverage-modal">
          <div className="tv-modal-section">
            <div className="tv-modal-symbol-line tv-modal-symbol-line-trade">
              <span className="tv-modal-symbol-avatar">{(selectedSymbolMeta?.base || selectedDisplaySymbol || "C").charAt(0)}</span>
              <strong>{selectedDisplaySymbol}</strong>
              {selectedSymbolMeta?.market_type ? <span className="tv-mini-chip">{marketTypeLabel(selectedSymbolMeta.market_type)}</span> : null}
              <span className="tv-mini-chip">逐仓</span>
            </div>
            <div className="tv-modal-leverage-block">
              <div className="tv-modal-side-title">做多</div>
              <div className="tv-modal-input-shell is-long">
                <input value={formatLeverageDecimal(tradeLeverageValue)} readOnly />
                <button type="button" className="tv-modal-stepper" tabIndex={-1} aria-hidden="true">
                  <span />
                  <span />
                </button>
              </div>
              <div className="tv-modal-pills">
                {[1, 2, 3, 5, 10, 20, 30].map((item) => (
                  <button
                    key={`long-${item}`}
                    type="button"
                    className={`tv-modal-pill ${item === tradeLeverageValue ? "is-active" : ""}`}
                    onClick={() => setTradeLeverageValue(item)}
                  >
                    {item}x
                  </button>
                ))}
                <button type="button" className="tv-modal-pill tv-modal-pill-more" tabIndex={-1} aria-hidden="true">
                  <ChevronRightIcon />
                </button>
              </div>
              <div className="tv-modal-footnote">
                <div>
                  杠杆倍数调整后最大可开 <strong>{formatUSDTValue(tradeCanOpenLongUSDT)}</strong>
                </div>
                <div>
                  所需保证金 <strong>{formatUSDTValue(0)}</strong>
                </div>
                <div>
                  预估强平价 <strong>{formatPrice(tradeEstimatedLongLiqPrice)}</strong>
                </div>
              </div>
            </div>
            <div className="tv-modal-divider" />
            <div className="tv-modal-leverage-block">
              <div className="tv-modal-side-title">做空</div>
              <div className="tv-modal-input-shell is-short">
                <input value={formatLeverageDecimal(tradeLeverageValue)} readOnly />
                <button type="button" className="tv-modal-stepper" tabIndex={-1} aria-hidden="true">
                  <span />
                  <span />
                </button>
              </div>
              <div className="tv-modal-pills">
                {[1, 2, 3, 5, 10, 20, 30].map((item) => (
                  <button
                    key={`short-${item}`}
                    type="button"
                    className={`tv-modal-pill ${item === tradeLeverageValue ? "is-active" : ""}`}
                    onClick={() => setTradeLeverageValue(item)}
                  >
                    {item}x
                  </button>
                ))}
                <button type="button" className="tv-modal-pill tv-modal-pill-more" tabIndex={-1} aria-hidden="true">
                  <ChevronRightIcon />
                </button>
              </div>
              <div className="tv-modal-footnote">
                <div>
                  杠杆倍数调整后最大可开 <strong>{formatUSDTValue(tradeCanOpenShortUSDT)}</strong>
                </div>
                <div>
                  所需保证金 <strong>{formatUSDTValue(0)}</strong>
                </div>
                <div>
                  预估强平价 <strong>{formatPrice(tradeEstimatedShortLiqPrice)}</strong>
                </div>
              </div>
            </div>
          </div>
          <div className="tv-modal-actions">
            <button onClick={() => setShowLeverageModal(false)}>取消</button>
            <button disabled>确认</button>
          </div>
        </Modal>
      ) : null}

      {showOrderSettingsModal ? (
        <Modal title="下单设置" onClose={() => setShowOrderSettingsModal(false)} className="tv-order-setting-modal">
          <div className="tv-modal-section">
            <div
              className={`tv-order-setting-option ${tradeOrderSettingMode === "quantity" ? "is-active" : ""}`}
              role="button"
              tabIndex={0}
              onClick={() => setTradeOrderSettingMode("quantity")}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  setTradeOrderSettingMode("quantity");
                }
              }}
            >
              <span className={`tv-order-setting-radio ${tradeOrderSettingMode === "quantity" ? "is-active" : ""}`} aria-hidden="true" />
              <div className="tv-order-setting-copy">
                <div className="tv-order-setting-title">按数量下单</div>
                <div className="tv-order-setting-description">
                  您输入的信息为开仓数量，修改杠杆倍数会改变成本
                </div>
                <div className="tv-order-setting-units">
                  {(["BTC", "USDT", "张"] as TradeOrderQuantityUnit[]).map((item) => (
                    <button
                      key={item}
                      type="button"
                      className={`tv-order-setting-unit ${tradeOrderQuantityUnit === item ? "is-active" : ""}`}
                      onClick={(event) => {
                        event.stopPropagation();
                        setTradeOrderQuantityUnit(item);
                      }}
                    >
                      {item}
                    </button>
                  ))}
                </div>
              </div>
            </div>
            <div
              className={`tv-order-setting-option ${tradeOrderSettingMode === "cost" ? "is-active" : ""}`}
              role="button"
              tabIndex={0}
              onClick={() => setTradeOrderSettingMode("cost")}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  setTradeOrderSettingMode("cost");
                }
              }}
            >
              <span className={`tv-order-setting-radio ${tradeOrderSettingMode === "cost" ? "is-active" : ""}`} aria-hidden="true" />
              <div className="tv-order-setting-copy">
                <div className="tv-order-setting-title">按成本下单 (USDT)</div>
                <div className="tv-order-setting-description">
                  您输入的信息为开仓成本（含合手续费），修改杠杆倍数不会改变成本
                </div>
              </div>
            </div>
          </div>
          <div className="tv-modal-actions">
            <button onClick={() => setShowOrderSettingsModal(false)}>取消</button>
            <button onClick={() => setShowOrderSettingsModal(false)}>确认</button>
          </div>
        </Modal>
      ) : null}

      {closeModalPosition ? (
        <Modal title="平仓" onClose={() => setCloseModalPosition(null)}>
          <div className="tv-modal-section">
            <div className="tv-modal-symbol-line">
              <strong>{lookupDisplaySymbol(closeModalPosition.symbol, runtime?.symbols || [])}</strong>
              <span className="tv-mini-chip">{directionLabel(closeModalPosition.position_side)}</span>
              <span className="tv-mini-chip">逐仓</span>
              <span className="tv-mini-chip">{formatLeverage(closeModalPosition.leverage_multiplier)}</span>
            </div>
            <div className="tv-modal-grid">
              <div>
                <div className="tv-modal-label">标记价格</div>
                <strong>{formatPrice(closeModalPosition.current_price)}</strong>
              </div>
              <div>
                <div className="tv-modal-label">开仓均价</div>
                <strong>{formatPrice(closeModalPosition.entry_price)}</strong>
              </div>
            </div>
            <div className="tv-form-block">
              <label>平仓价格 (USDT)</label>
              <input value={formatInputValue(closeModalPosition.current_price)} readOnly />
            </div>
            <div className="tv-form-block">
              <label>平仓数量 (USDT)</label>
              <input value={formatInputValue(resolvePositionClosePreviewAmount(closeModalPosition, closePercent))} readOnly />
            </div>
            <div className="tv-slider-row">
              <div className="tv-slider-current">
                <span>平仓比例</span>
                <strong>{closePercent}%</strong>
              </div>
              <div className="tv-slider-shell" style={{ "--tv-slider-fill": `${closePercent}%` } as CSSProperties}>
                <input
                  className="tv-slider-range"
                  type="range"
                  min={0}
                  max={100}
                  step={1}
                  value={closePercent}
                  onChange={(event) => setClosePercent(normalizeClosePercent(Number(event.target.value)))}
                  disabled={closeSubmitting || tradeReadOnly}
                />
              </div>
              <div className="tv-slider-track">
                <span />
                <span />
                <span />
                <span />
                <span />
              </div>
              <div className="tv-slider-labels">
                <span>0%</span>
                <span>25%</span>
                <span>50%</span>
                <span>75%</span>
                <span>100%</span>
              </div>
            </div>
            <div className="tv-modal-footnote">
              <div>持仓量 {formatAmount(closeModalPosition.entry_quantity || closeModalPosition.entry_value)}</div>
              <div>可平量 {formatAmount(closeModalPosition.entry_quantity || closeModalPosition.entry_value)}</div>
              <div className={priceTone(closeModalPosition.unrealized_profit_amount)}>
                预计收益 {formatSignedNumber(closeModalPosition.unrealized_profit_amount)}
              </div>
            </div>
          </div>
          <div className="tv-modal-actions">
            <button onClick={() => setCloseModalPosition(null)}>取消</button>
            <button className="tv-dock-action-button" onClick={() => void handleSubmitClose()} disabled={closeSubmitting || tradeReadOnly}>
              {closeSubmitting ? "提交中..." : "平仓"}
            </button>
          </div>
        </Modal>
      ) : null}

      {delegateModalPosition ? (
        <Modal title="委托给策略管理" onClose={() => setDelegateModalPosition(null)}>
          <div className="tv-modal-section">
            <div className="tv-modal-symbol-line">
              <strong>{lookupDisplaySymbol(delegateModalPosition.symbol, runtime?.symbols || [])}</strong>
              <span className="tv-mini-chip">{directionLabel(delegateModalPosition.position_side)}</span>
              <span className="tv-mini-chip">{formatLeverage(delegateModalPosition.leverage_multiplier)}</span>
            </div>
            <div className="tv-form-block">
              <label>当前管理方式</label>
              <div className="tv-modal-static-value">{formatPositionManager(delegateModalPosition)}</div>
            </div>
            <div className="tv-form-block">
              <label>策略时间周期组合</label>
              <div className="tv-delegate-options">
                {strategyOptions.length > 0 ? (
                  strategyOptions.map((item) => {
                    const identity = buildStrategyOptionIdentity(item);
                    return (
                      <button
                        key={identity}
                        type="button"
                        className={`tv-delegate-option ${delegateIdentity === identity ? "is-active" : ""}`}
                        onClick={() => setDelegateIdentity(identity)}
                      >
                        <span className="tv-delegate-option-title">{item.strategy_name}</span>
                        <span className="tv-delegate-option-meta">{item.trade_timeframes.join("/")}</span>
                      </button>
                    );
                  })
                ) : (
                  <div className="tv-empty-block">当前没有可委托的策略时间周期组合</div>
                )}
              </div>
            </div>
          </div>
          <div className="tv-modal-actions">
            <button onClick={() => setDelegateModalPosition(null)}>取消</button>
            <button onClick={() => void handleSubmitPositionDelegate()} disabled={delegateSubmitting || !delegateIdentity || strategyOptions.length === 0}>
              {delegateSubmitting ? "提交中..." : "确认委托"}
            </button>
          </div>
        </Modal>
      ) : null}

      {showShortcutModal ? (
        <div className="tv-shortcut-modal-backdrop" onClick={() => setShowShortcutModal(false)}>
          <div className="tv-shortcut-modal" onClick={(event) => event.stopPropagation()}>
            <header className="tv-shortcut-modal-header">
              <span>Keyboard Shortcuts &amp; Event Legend</span>
              <button type="button" className="tv-shortcut-close-btn" onClick={() => setShowShortcutModal(false)}>
                ×
              </button>
            </header>
            <div className="tv-shortcut-modal-body">
              <section className="tv-shortcut-section tv-shortcut-section-shortcuts">
                <div className="tv-shortcut-section-title">Keyboard Shortcuts</div>
                <table className="tv-shortcut-table">
                  <thead>
                    <tr>
                      <th>Action</th>
                      <th>Windows/Linux</th>
                      <th>macOS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {SHORTCUT_HELP_ROWS.map((item) => (
                      <tr key={`${item.action}-${item.winLinux}`}>
                        <td>{item.action}</td>
                        <td>{item.winLinux}</td>
                        <td>{item.mac}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>
              <section className="tv-shortcut-section tv-shortcut-section-legend">
                <div className="tv-shortcut-section-title">Event Legend</div>
                <div className="tv-event-legend-grid">
                  {EVENT_LEGEND_ROWS.map((item) => (
                    <div key={item.kind} className="tv-event-legend-row">
                      <div className="tv-event-legend-preview">{renderTradingViewEventLegendPreview(item.kind)}</div>
                      <div className="tv-event-legend-label">{item.label}</div>
                      <div className="tv-event-legend-desc">{item.description}</div>
                    </div>
                  ))}
                </div>
              </section>
              <section className="tv-shortcut-section tv-shortcut-section-status">
                <div className="tv-shortcut-section-title">Connection Status</div>
                <div className="tv-status-help-list">
                  {CONNECTION_STATUS_HELP_ROWS.map((item) => (
                    <div key={item.color} className="tv-status-help-row">
                      <span className={`tv-connection-dot is-${item.color}`} aria-hidden="true" />
                      <div className="tv-status-help-label">{item.label}</div>
                      <div className="tv-status-help-desc">{item.description}</div>
                    </div>
                  ))}
                </div>
              </section>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function renderTradingViewEventLegendPreview(kind: EventLegendPreviewKind): JSX.Element {
  switch (kind) {
    case "ENTRY":
      return (
        <div className="tv-event-legend-arrow is-entry">
          <span className="tv-event-legend-arrow-line" />
          <span className="tv-event-legend-arrow-head" />
        </div>
      );
    case "EXIT":
      return (
        <div className="tv-event-legend-arrow is-exit">
          <span className="tv-event-legend-arrow-line" />
          <span className="tv-event-legend-arrow-head" />
        </div>
      );
    case "EXECUTION":
      return <span className="tv-event-legend-badge is-execution">E</span>;
    case "TP":
      return <span className="tv-event-legend-badge is-tp">TP</span>;
    case "SL":
      return <span className="tv-event-legend-badge is-sl">SL</span>;
    case "ARMED":
      return <span className="tv-event-legend-badge is-armed">A</span>;
    case "TREND_N":
      return <span className="tv-event-legend-badge is-trend-n">N</span>;
    case "HIGH_H":
      return (
        <div className="tv-event-legend-variants">
          <span className="tv-event-legend-badge is-high-h is-bull">H</span>
          <span className="tv-event-legend-badge is-high-h is-bear">H</span>
          <span className="tv-event-legend-badge is-high-h is-neutral">H</span>
        </div>
      );
    case "MID_M":
      return (
        <div className="tv-event-legend-variants">
          <span className="tv-event-legend-badge is-mid-m is-bull">M</span>
          <span className="tv-event-legend-badge is-mid-m is-bear">M</span>
          <span className="tv-event-legend-badge is-mid-m is-neutral">M</span>
        </div>
      );
    case "R_2R":
      return <span className="tv-event-legend-badge is-r-2r">2R</span>;
    case "R_4R":
      return <span className="tv-event-legend-badge is-r-4r">4R</span>;
    default:
      return <span className="tv-event-legend-badge">{kind}</span>;
  }
}

function Metric(props: { label: string; value: string; tone?: string }): JSX.Element {
  return (
    <div className="tv-metric">
      <span className="tv-metric-label">{props.label}</span>
      <strong className={props.tone || ""}>{props.value}</strong>
    </div>
  );
}

function formatRuntimeMode(mode?: string): string {
  const normalized = (mode || "").trim().toLowerCase();
  switch (normalized) {
    case "live":
      return "LIVE";
    case "paper":
      return "PAPER";
    case "back-test":
      return "BACK-TEST";
    case "backtest":
      return "BACKTEST";
    case "real-time":
      return "REAL-TIME";
    default:
      return normalized ? normalized.toUpperCase() : "--";
  }
}

function formatStatusPopoverValue(value?: string): string {
  const normalized = (value || "").trim();
  return normalized || "--";
}

function replaceBacktestDay(
  days: TradingViewBacktestTasksResponse[],
  nextDay: TradingViewBacktestTasksResponse
): TradingViewBacktestTasksResponse[] {
  const merged = days.filter((item) => item.date !== nextDay.date);
  merged.push({
    ...nextDay,
    tasks: sortBacktestTasks(nextDay.tasks)
  });
  merged.sort((left, right) => right.date.localeCompare(left.date));
  return merged;
}

function upsertBacktestTaskDay(
  days: TradingViewBacktestTasksResponse[],
  task: TradingViewBacktestTask,
  tzOffsetMin: number
): TradingViewBacktestTasksResponse[] {
  const dayKey = formatBacktestDateOnly(task.created_at_ms, tzOffsetMin);
  const clone = days.map((item) => ({
    ...item,
    tasks: item.tasks.map((entry) => ({ ...entry }))
  }));
  const index = clone.findIndex((item) => item.date === dayKey);
  if (index < 0) {
    clone.push({
      date: dayKey,
      count: 1,
      has_more_days: false,
      tasks: [task]
    });
  } else {
    const current = clone[index];
    const nextTasks = current.tasks.filter((item) => item.id !== task.id);
    nextTasks.push(task);
    clone[index] = {
      ...current,
      count: nextTasks.length,
      tasks: sortBacktestTasks(nextTasks)
    };
  }
  clone.sort((left, right) => right.date.localeCompare(left.date));
  return clone;
}

function sortBacktestTasks(tasks: TradingViewBacktestTask[]): TradingViewBacktestTask[] {
  return [...tasks].sort((left, right) => {
    if (left.created_at_ms !== right.created_at_ms) {
      return right.created_at_ms - left.created_at_ms;
    }
    return right.id - left.id;
  });
}

function findActiveBacktestTask(days: TradingViewBacktestTasksResponse[]): TradingViewBacktestTask | null {
  for (const day of days) {
    const task = day.tasks.find((item) => isBacktestTaskActive(item.status));
    if (task) {
      return task;
    }
  }
  return null;
}

function resolvePreviousBacktestDate(days: TradingViewBacktestTasksResponse[]): string {
  if (days.length === 0) {
    return "";
  }
  const oldest = days[days.length - 1];
  if (!oldest.has_more_days) {
    return "";
  }
  return shiftBacktestDate(oldest.date, -1);
}

function shiftBacktestDate(dateLabel: string, days: number): string {
  const base = parseBacktestDate(dateLabel);
  if (!base) {
    return "";
  }
  base.setDate(base.getDate() + days);
  return formatDateOnly(base);
}

function parseBacktestDate(dateLabel: string): Date | null {
  const parts = dateLabel.trim().split("-");
  if (parts.length !== 3) {
    return null;
  }
  const year = Number(parts[0]);
  const month = Number(parts[1]);
  const day = Number(parts[2]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }
  return new Date(year, month - 1, day, 0, 0, 0, 0);
}

function formatBacktestDateOnly(timestampMS: number, tzOffsetMin: number): string {
  const date = toLocalDate(timestampMS, tzOffsetMin);
  return formatDateOnly(date);
}

function formatDateOnly(date: Date): string {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function formatBacktestDateTime(timestampMS: number): string {
  if (!Number.isFinite(timestampMS) || timestampMS <= 0) {
    return "--";
  }
  const date = new Date(timestampMS);
  return formatBacktestDateTimeValue(date);
}

function formatBacktestRange(startMS: number, endMS: number): string {
  if (!Number.isFinite(startMS) || !Number.isFinite(endMS) || startMS <= 0 || endMS <= 0) {
    return "--";
  }
  const start = new Date(startMS);
  const end = new Date(endMS);
  return `${formatBacktestDateTimeValue(start)} - ${formatBacktestDateTimeValue(end)}`;
}

function formatBacktestDateTimeValue(date: Date): string {
  return `${date.getFullYear()}/${pad2(date.getMonth() + 1)}/${pad2(date.getDate())} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`;
}

function formatBacktestTradeTimeframes(timeframes: string[]): string {
  if (!Array.isArray(timeframes) || timeframes.length === 0) {
    return "--";
  }
  return timeframes.filter((item) => item.trim() !== "").join("/");
}

function formatBacktestPrice(value?: number): string {
  if (!Number.isFinite(value) || !value || value <= 0) {
    return "--";
  }
  return formatPrice(value);
}

function formatBacktestProfitRate(value?: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  return formatSignedPercent((value || 0) * 100);
}

function formatBacktestHoldingDuration(task: TradingViewBacktestTask): string {
  const direct = Number(task.holding_duration_ms || 0);
  if (Number.isFinite(direct) && direct > 0) {
    return formatCompactDuration(direct);
  }
  const startMS = Number(task.open_time_ms || 0);
  const endMS = Number(task.close_time_ms || 0);
  if (Number.isFinite(startMS) && Number.isFinite(endMS) && startMS > 0 && endMS > startMS) {
    return formatCompactDuration(endMS - startMS);
  }
  return "--";
}

function formatBacktestElapsed(task: TradingViewBacktestTask): string {
  const startedAtMS = Number(task.started_at_ms || 0);
  if (!Number.isFinite(startedAtMS) || startedAtMS <= 0) {
    return "--";
  }
  const finishedAtMS = Number(task.finished_at_ms || 0);
  const endMS = finishedAtMS > startedAtMS ? finishedAtMS : Date.now();
  return formatCompactDuration(endMS - startedAtMS);
}

function formatCompactDuration(durationMS: number): string {
  if (!Number.isFinite(durationMS) || durationMS <= 0) {
    return "0s";
  }
  let remaining = Math.floor(durationMS / 1000);
  const hours = Math.floor(remaining / 3600);
  remaining -= hours * 3600;
  const minutes = Math.floor(remaining / 60);
  const seconds = remaining - minutes * 60;
  const parts: string[] = [];
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0 || hours > 0) {
    parts.push(`${minutes}m`);
  }
  parts.push(`${seconds}s`);
  return parts.join("");
}

function formatBacktestControlLabel(task: TradingViewBacktestTask, isDisplayed: boolean): string {
  switch (task.status.trim().toLowerCase()) {
    case "failed":
      return "重试";
    case "pending":
      return "排队中";
    case "running":
      return "运行中";
    case "succeeded":
      return isDisplayed ? "隐藏" : "显示";
    default:
      return "显示";
  }
}

function normalizeBacktestControlClass(task: TradingViewBacktestTask, isDisplayed: boolean): string {
  const status = task.status.trim().toLowerCase();
  if (status === "failed") {
    return "retry";
  }
  if (status === "succeeded") {
    return isDisplayed ? "hide" : "show";
  }
  return "disabled";
}

function formatBacktestEventTime(timestampMS: number): string {
  if (!Number.isFinite(timestampMS) || timestampMS <= 0) {
    return "--";
  }
  return formatBacktestDateTime(timestampMS);
}

function formatBacktestEventSource(source: string): string {
  switch (source.trim().toLowerCase()) {
    case "signal":
      return "策略事件";
    case "execution":
      return "风控/执行";
    default:
      return source || "--";
  }
}

function formatBacktestStatus(task: TradingViewBacktestTask, expiredTaskIDs: Record<number, boolean>): string {
  if (expiredTaskIDs[task.id]) {
    return "数据已过期";
  }
  switch (task.status.trim().toLowerCase()) {
    case "pending":
      return "排队中";
    case "running":
      return "运行中";
    case "succeeded":
      return "已完成";
    case "failed":
      return "失败";
    default:
      return task.status || "--";
  }
}

function normalizeBacktestStatusClass(task: TradingViewBacktestTask, expiredTaskIDs: Record<number, boolean>): string {
  if (expiredTaskIDs[task.id]) {
    return "expired";
  }
  switch (task.status.trim().toLowerCase()) {
    case "pending":
    case "running":
    case "succeeded":
    case "failed":
      return task.status.trim().toLowerCase();
    default:
      return "unknown";
  }
}

function isBacktestTaskActive(status: string): boolean {
  const normalized = status.trim().toLowerCase();
  return normalized === "pending" || normalized === "running";
}

function isBacktestOverlayDisplayed(
  task: TradingViewBacktestTask,
  visibleTaskID: number,
  selectedExchange: string,
  selectedSymbol: string
): boolean {
  return (
    visibleTaskID === task.id &&
    task.exchange === selectedExchange &&
    task.symbol === selectedSymbol
  );
}

function resolveBacktestVisibleRange(
  candles: TradingViewCandlesResponse["candles"],
  timeframe: string,
  rangeStartMS: number,
  rangeEndMS: number
): { from: number; to: number } | null {
  const stepMS = timeframeToMilliseconds(timeframe);
  if (stepMS <= 0 || !Array.isArray(candles) || candles.length === 0) {
    return null;
  }
  const startIndex = findCandleRangeIndex(candles, rangeStartMS, stepMS);
  const endIndex = findCandleRangeIndex(candles, rangeEndMS, stepMS);
  if (startIndex < 0 || endIndex < 0 || endIndex < startIndex) {
    return null;
  }
  for (let index = startIndex; index < endIndex; index += 1) {
    if (candles[index + 1].ts-candles[index].ts !== stepMS) {
      return null;
    }
  }
  return {
    from: startIndex - 0.5,
    to: endIndex + 0.5
  };
}

function resolveCenteredBacktestEventVisibleRange(
  candles: TradingViewCandlesResponse["candles"],
  timeframe: string,
  eventAtMS: number,
  currentRange: { from: number; to: number } | null
): { from: number; to: number } | null {
  const stepMS = timeframeToMilliseconds(timeframe);
  if (
    stepMS <= 0 ||
    !Array.isArray(candles) ||
    candles.length === 0 ||
    !currentRange ||
    !Number.isFinite(currentRange.from) ||
    !Number.isFinite(currentRange.to)
  ) {
    return null;
  }
  const index = findCandleRangeIndex(candles, eventAtMS, stepMS);
  if (index < 0) {
    return null;
  }
  const span = Math.max(2, currentRange.to - currentRange.from);
  const minFrom = -0.5;
  const maxTo = candles.length - 0.5;
  let from = index - span / 2;
  let to = from + span;
  if (from < minFrom) {
    from = minFrom;
    to = from + span;
  }
  if (to > maxTo) {
    to = maxTo;
    from = to - span;
  }
  from = Math.max(minFrom, from);
  to = Math.min(maxTo, to);
  if (to <= from) {
    return null;
  }
  return { from, to };
}

function resolveEventCrosshairPrice(event: TradingViewEventEntry): number | undefined {
  const detail = event.detail;
  return (
    readFirstNumber(detail, ["entry_price", "exit_price", "fill_price", "close_avg_px"]) ??
    readFirstNumber(detail, ["price", "avg_px", "tp_price", "sl_price"]) ??
    undefined
  );
}

function readFirstNumber(detail: Record<string, unknown> | undefined, keys: string[]): number | null {
  if (!detail) {
    return null;
  }
  for (const key of keys) {
    const value = detail[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return null;
}

function findCandleRangeIndex(
  candles: TradingViewCandlesResponse["candles"],
  targetMS: number,
  stepMS: number
): number {
  for (let index = 0; index < candles.length; index += 1) {
    const startMS = candles[index].ts;
    const endMS = startMS + stepMS;
    if (targetMS >= startMS && targetMS < endMS) {
      return index;
    }
  }
  return -1;
}

function timeframeToMilliseconds(value: string): number {
  const normalized = value.trim().toLowerCase();
  if (!normalized || normalized.length < 2) {
    return 0;
  }
  const step = Number(normalized.slice(0, -1));
  if (!Number.isFinite(step) || step <= 0) {
    return 0;
  }
  switch (normalized.slice(-1)) {
    case "m":
      return step * 60_000;
    case "h":
      return step * 3_600_000;
    case "d":
      return step * 86_400_000;
    case "w":
      return step * 604_800_000;
    default:
      return 0;
  }
}

function toLocalDate(timestampMS: number, tzOffsetMin: number): Date {
  void tzOffsetMin;
  if (!Number.isFinite(timestampMS)) {
    return new Date(0);
  }
  return new Date(timestampMS);
}

function pad2(value: number): string {
  if (!Number.isFinite(value)) {
    return "00";
  }
  return value < 10 ? `0${value}` : String(value);
}

function connectionStatusAriaLabel(status: ConnectionStatus): string {
  switch (status) {
    case "warmup":
      return "与 gobot 后端 websocket 连接正常，但系统仍在预热中";
    case "connected":
      return "与 gobot 后端 websocket 连接正常";
    case "slow":
      return "与 gobot 后端 websocket 连接正常，但延迟较高";
    case "disconnected":
    default:
      return "与 gobot 后端 websocket 连接中断";
  }
}

function FundsRow(props: { label: string; value: string; tone?: string }): JSX.Element {
  return (
    <div className="tv-funds-row">
      <span>{props.label}</span>
      <strong className={props.tone || ""}>{props.value}</strong>
    </div>
  );
}

function Modal(props: { title: string; onClose: () => void; children: React.ReactNode; className?: string }): JSX.Element {
  return (
    <div className="tv-modal-backdrop" role="presentation" onClick={props.onClose}>
      <div className={`tv-modal ${props.className || ""}`.trim()} role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
        <div className="tv-modal-header">
          <strong>{props.title}</strong>
          <button onClick={props.onClose}>×</button>
        </div>
        {props.children}
      </div>
    </div>
  );
}

function ChevronDownIcon(): JSX.Element {
  return (
    <svg viewBox="0 0 12 12" fill="none" aria-hidden="true">
      <path d="M3 4.5L6 7.5L9 4.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function ChevronRightIcon(): JSX.Element {
  return (
    <svg viewBox="0 0 12 12" fill="none" aria-hidden="true">
      <path d="M4.5 3L7.5 6L4.5 9" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function InfoIcon(): JSX.Element {
  return (
    <svg viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <circle cx="8" cy="8" r="5.75" stroke="currentColor" strokeWidth="1.3" />
      <path d="M8 7.1V10.2" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
      <circle cx="8" cy="5.1" r="0.8" fill="currentColor" />
    </svg>
  );
}

function PlusIcon(): JSX.Element {
  return (
    <svg viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M8 3.25V12.75" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
      <path d="M3.25 8H12.75" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
    </svg>
  );
}

function formatPrice(value?: number): string {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return "--";
  }
  if (value >= 1000) {
    return value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  if (value >= 1) {
    return value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  }
  return value.toLocaleString("en-US", { minimumFractionDigits: 4, maximumFractionDigits: 8 });
}

function formatIndicatorValue(value?: number): string {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return "--";
  }
  if (value >= 1000) {
    return value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  if (value >= 1) {
    return value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  }
  return value.toLocaleString("en-US", { minimumFractionDigits: 4, maximumFractionDigits: 6 });
}

function buildViewportKey(symbol: string, timeframe: string): string {
  if (!symbol || !timeframe) {
    return "";
  }
  return `${symbol}|${timeframe}`;
}

function buildViewportTransferToken(
  sourceSymbol: string,
  sourceTimeframe: string,
  targetSymbol: string,
  targetTimeframe: string
): string {
  return `${sourceSymbol}|${sourceTimeframe}->${targetSymbol}|${targetTimeframe}|${Date.now()}`;
}

function resolvePreferredTimeframe(
  symbol: string,
  runtime: TradingViewRuntimeResponse | null,
  fallback: string,
  rememberedBySymbol: Map<string, string>
): string {
  const timeframes = runtime?.timeframes || [];
  if (symbol) {
    const remembered = rememberedBySymbol.get(symbol);
    if (remembered && timeframes.includes(remembered)) {
      return remembered;
    }
  }
  if (fallback && timeframes.includes(fallback)) {
    return fallback;
  }
  if (runtime?.default_timeframe && timeframes.includes(runtime.default_timeframe)) {
    return runtime.default_timeframe;
  }
  return timeframes[0] || "";
}

function formatSignedPercent(value?: number): string {
  if (value == null || !Number.isFinite(value)) {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatSignedNumber(value?: number): string {
  if (value == null || !Number.isFinite(value)) {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} USDT`;
}

function formatTurnover(value?: number): string {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return "--";
  }
  if (value >= 100000000) {
    return `${(value / 100000000).toFixed(2)} 亿`;
  }
  if (value >= 10000) {
    return `${(value / 10000).toFixed(2)} 万`;
  }
  return `${value.toLocaleString("en-US", { maximumFractionDigits: 2 })} USDT`;
}

function formatAmount(value?: number): string {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return "--";
  }
  return value.toLocaleString("en-US", { maximumFractionDigits: 4 });
}

function formatInputValue(value?: number): string {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return "";
  }
  return value.toLocaleString("en-US", { maximumFractionDigits: 4 });
}

function resolvePositionClosePreviewAmount(position: TradingViewPosition, closePercent: number): number {
  const baseAmount = firstPositiveNumber(position.entry_quantity, position.entry_value);
  if (!(baseAmount > 0)) {
    return 0;
  }
  return baseAmount * normalizeClosePercent(closePercent) / 100;
}

function normalizeTradeLeverage(value?: number): number {
  if (!Number.isFinite(value) || !value || value <= 0) {
    return 10;
  }
  return Math.max(1, Math.round(value));
}

function normalizeClosePercent(value?: number): number {
  if (!Number.isFinite(value)) {
    return 80;
  }
  return Math.min(100, Math.max(0, Math.round(value || 0)));
}

function TradingViewEventTable(props: {
  className: string;
  events: TradingViewEventEntry[];
  selectedEventID?: string;
  loading?: boolean;
  error?: string;
  emptyText: string;
  onSelectEvent: (eventID: string) => void;
  onActivateEvent: (event: TradingViewEventEntry) => void;
}): JSX.Element {
  const {
    className,
    events,
    selectedEventID = "",
    loading = false,
    error = "",
    emptyText,
    onSelectEvent,
    onActivateEvent
  } = props;
  let body: JSX.Element;
  if (events.length > 0) {
    body = (
      <div className="tv-events-table-wrap">
        <table className="tv-events-table">
          <thead>
            <tr>
              <th>时间</th>
              <th>来源</th>
              <th>标题</th>
              <th>摘要</th>
            </tr>
          </thead>
          <tbody>
            {events.map((item) => {
              const isSelected = selectedEventID === item.id;
              return (
                <tr
                  key={item.id}
                  className={isSelected ? "is-selected" : ""}
                  onClick={() => onSelectEvent(item.id)}
                  onDoubleClick={() => onActivateEvent(item)}
                >
                  <td>{formatBacktestEventTime(item.event_at_ms)}</td>
                  <td>{formatBacktestEventSource(item.source)}</td>
                  <td>{item.title || item.type || "--"}</td>
                  <td>{item.summary || "--"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  } else if (loading) {
    body = <div className="tv-event-empty">持仓事件加载中...</div>;
  } else if (error) {
    body = <div className="tv-event-empty">{error}</div>;
  } else {
    body = <div className="tv-event-empty">{emptyText}</div>;
  }
  return (
    <div className={className}>
      <div className="tv-events-hint">单击高亮 · 双击跳转对应 K 线</div>
      {body}
    </div>
  );
}

function renderPositionLinks(rowKey: string, symbol: string, linkItems: TradingViewSignalLinkItem[]): JSX.Element {
  if (linkItems.length === 0) {
    return <span className="tv-position-links-empty">暂无链接</span>;
  }
  return (
    <div className="tv-position-links">
      {linkItems.map((link) => (
        <a
          key={`${rowKey}-${link.key}`}
          className="tv-position-link"
          href={link.url}
          target="_blank"
          rel="noopener noreferrer"
          title={`${link.label} - 打开 ${symbol} K 线`}
          aria-label={`${link.label} - 打开 ${symbol} K 线`}
        >
          <span
            className={`tv-position-link__icon tv-position-link__icon--${link.iconTone}${BRAND_ICON_SRC_BY_TONE[link.iconTone] ? " tv-position-link__icon--image" : ""}`}
          >
            {BRAND_ICON_SRC_BY_TONE[link.iconTone] ? (
              <img
                className={`tv-position-link__icon-image tv-position-link__icon-image--${link.iconTone}`}
                src={BRAND_ICON_SRC_BY_TONE[link.iconTone]}
                alt=""
                aria-hidden="true"
              />
            ) : (
              link.iconText
            )}
          </span>
        </a>
      ))}
    </div>
  );
}

function buildCurrentPositionRowKey(item: TradingViewPosition): string {
  return [
    "position",
    item.position_id > 0 ? String(item.position_id) : "--",
    item.exchange || "--",
    item.symbol || "--",
    item.position_side || "--",
    item.margin_mode || "--",
    item.entry_time || "--"
  ].join("|");
}

function normalizeTradeOrderTab(value?: string): TradeOrderTab {
  switch (value) {
    case "limit":
    case "market":
      return value;
    default:
      return "market";
  }
}

function normalizeBottomTab(value?: string): BottomTab {
  switch (value) {
    case "orders":
    case "positions":
    case "history":
    case "backtests":
      return value;
    default:
      return "positions";
  }
}

function normalizeVisibleIndicators(value?: Record<string, boolean>): Record<string, boolean> {
  if (!value || typeof value !== "object") {
    return {};
  }
  const next: Record<string, boolean> = {};
  for (const [key, flag] of Object.entries(value)) {
    next[key] = flag !== false;
  }
  return next;
}

function normalizeViewportSnapshots(value?: Record<string, ChartViewportSnapshot>): Record<string, ChartViewportSnapshot> {
  if (!value || typeof value !== "object") {
    return {};
  }
  const next: Record<string, ChartViewportSnapshot> = {};
  for (const [key, snapshot] of Object.entries(value)) {
    if (!snapshot || typeof snapshot !== "object") {
      continue;
    }
    const bars = Number(snapshot.bars);
    const latestOffset = Number(snapshot.latestOffset);
    const updatedAtMS = Number(snapshot.updatedAtMS);
    if (!Number.isFinite(bars) || !Number.isFinite(latestOffset) || !Number.isFinite(updatedAtMS)) {
      continue;
    }
    next[key] = {
      bars,
      latestOffset,
      updatedAtMS
    };
  }
  return next;
}

function normalizeScrollTop(value?: number): number {
  if (!Number.isFinite(value) || !value || value < 0) {
    return 0;
  }
  return value;
}

function loadTradingViewPersistedState(): TradingViewPersistedState {
  if (typeof window === "undefined") {
    return buildDefaultPersistedState();
  }
  try {
    const raw = window.localStorage.getItem(TRADINGVIEW_UI_STATE_KEY);
    if (!raw) {
      return buildDefaultPersistedState();
    }
    const parsed = JSON.parse(raw) as Partial<TradingViewPersistedState> | null;
    if (!parsed || parsed.version !== 3) {
      return buildDefaultPersistedState();
    }
    return {
      version: 3,
      selectedExchange: typeof parsed.selectedExchange === "string" ? parsed.selectedExchange : "",
      selectedSymbol: typeof parsed.selectedSymbol === "string" ? parsed.selectedSymbol : "",
      selectedTimeframe: typeof parsed.selectedTimeframe === "string" ? parsed.selectedTimeframe : "",
      bottomTab: normalizeBottomTab(parsed.bottomTab),
      visibleIndicators: normalizeVisibleIndicators(parsed.visibleIndicators),
      sidebarScrollTop: normalizeScrollTop(parsed.sidebarScrollTop),
      viewportSnapshots: normalizeViewportSnapshots(parsed.viewportSnapshots),
      tradeLeverageValue: normalizeTradeLeverage(parsed.tradeLeverageValue),
      tradeOrderTab: normalizeTradeOrderTab(parsed.tradeOrderTab),
      takeProfitPct: typeof parsed.takeProfitPct === "string" ? parsed.takeProfitPct : "30",
      stopLossPct: typeof parsed.stopLossPct === "string" ? parsed.stopLossPct : "-5"
    };
  } catch (error) {
    console.warn("failed to load tradingview persisted state", error);
    return buildDefaultPersistedState();
  }
}

function writeTradingViewPersistedState(state: TradingViewPersistedState): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(TRADINGVIEW_UI_STATE_KEY, JSON.stringify(state));
  } catch (error) {
    console.warn("failed to persist tradingview ui state", error);
  }
}

function buildDefaultPersistedState(): TradingViewPersistedState {
  return {
    version: 3,
    selectedExchange: "",
    selectedSymbol: "",
    selectedTimeframe: "",
    bottomTab: "positions",
    visibleIndicators: { ...DEFAULT_VISIBLE_INDICATORS },
    sidebarScrollTop: 0,
    viewportSnapshots: {},
    tradeLeverageValue: 10,
    tradeOrderTab: "market",
    takeProfitPct: "30",
    stopLossPct: "-5"
  };
}

function MarketIdentityCell({
  exchange,
  symbol,
  leverage,
  className = "",
  onClick
}: {
  exchange?: string;
  symbol: string;
  leverage?: number;
  className?: string;
  onClick?: () => void;
}): JSX.Element {
  return (
    <div className={["tv-market-identity", className].filter(Boolean).join(" ")}>
      <div className="tv-market-identity-exchange">{(exchange || "--").toUpperCase()}</div>
      <div className="tv-market-identity-symbol-row">
        {onClick ? (
          <button type="button" className="tv-market-identity-symbol-button" onClick={onClick}>
            {symbol || "--"}
          </button>
        ) : (
          <div className="tv-market-identity-symbol">{symbol || "--"}</div>
        )}
      </div>
      <div className="tv-market-identity-meta">
        <span className="tv-mini-chip">{formatDisplayLeverage(leverage)}</span>
      </div>
    </div>
  );
}

function parseTradeInputNumber(value: string): number {
  const normalized = value.replace(/,/g, "").trim();
  if (!normalized) {
    return 0;
  }
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return parsed;
}

function parseSignedTradeInputNumber(value: string): number {
  const normalized = value.replace(/,/g, "").trim();
  if (!normalized) {
    return 0;
  }
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return parsed;
}

function firstPositiveNumber(...values: Array<number | undefined>): number {
  for (const value of values) {
    if (value != null && Number.isFinite(value) && value > 0) {
      return value;
    }
  }
  return 0;
}

function resolveTradeTargetPrice(entryPrice: number, side: "long" | "short", percentText: string, kind: "tp" | "sl"): number {
  const percent = parseSignedTradeInputNumber(percentText);
  if (!(entryPrice > 0) || !(percent > 0 || percent < 0)) {
    return 0;
  }
  const ratio = percent / 100;
  const factor = side === "long" ? 1 + ratio : 1 - ratio;
  if (!(factor > 0)) {
    return 0;
  }
  const price = entryPrice * factor;
  if (!(price > 0)) {
    return 0;
  }
  if (kind === "sl" && side === "long" && price >= entryPrice) {
    return 0;
  }
  if (kind === "sl" && side === "short" && price <= entryPrice) {
    return 0;
  }
  return price;
}

function formatOrderTypeLabel(value?: string): string {
  switch ((value || "").trim().toLowerCase()) {
    case "limit":
      return "限价委托";
    case "market":
      return "市价委托";
    default:
      return "--";
  }
}

function formatOrderStatus(value?: string): string {
  switch ((value || "").trim().toLowerCase()) {
    case "pending":
      return "待成交";
    case "filled":
      return "已成交";
    case "rejected":
      return "已拒绝";
    case "canceled":
      return "已取消";
    case "expired":
      return "已失效";
    default:
      return value || "--";
  }
}

function resolveOrderDirectionTone(side?: string): number {
  return (side || "").trim().toLowerCase() === "short" ? -1 : 1;
}

function formatDisplayLeverage(value?: number): string {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return "--";
  }
  return formatLeverage(value);
}

function formatPositionManager(position: TradingViewPosition): string {
  const strategy = (position.strategy_name || "").trim();
  const timeframes = Array.isArray(position.strategy_timeframes) ? position.strategy_timeframes.filter((item) => item.trim() !== "") : [];
  if (!strategy || timeframes.length === 0) {
    return "人工管理";
  }
  return `${strategy} ${timeframes.join("/")}`;
}

function renderCurrentPositionProfit(position: TradingViewPosition, mode?: string): JSX.Element {
  const isPaper = stringsEqualIgnoreCase(mode || "", "paper");
  const tone = priceTone(isPaper ? position.unrealized_profit_rate : position.unrealized_profit_amount);
  if (isPaper) {
    return <strong className={tone}>{formatSignedPercent(position.unrealized_profit_rate * 100)}</strong>;
  }
  return (
    <>
      <strong className={tone}>{formatSignedNumber(position.unrealized_profit_amount)}</strong>
      <span className={`tv-field-sub ${tone}`}>({formatSignedPercent(position.unrealized_profit_rate * 100)})</span>
    </>
  );
}

function renderPositionExtreme(amount: number, rate: number, mode?: string, forceNegativeRate = false): JSX.Element {
  const isPaper = stringsEqualIgnoreCase(mode || "", "paper");
  const displayRate = forceNegativeRate ? -Math.abs(rate) : rate;
  if (isPaper) {
    return <strong>{formatSignedPercent(displayRate * 100)}</strong>;
  }
  return (
    <>
      <strong>{formatSignedNumber(amount)}</strong>
      <span className="tv-field-sub">({formatSignedPercent(displayRate * 100)})</span>
    </>
  );
}

function renderHistoryPositionProfit(position: TradingViewHistoryPosition, mode?: string): JSX.Element {
  const isPaper = stringsEqualIgnoreCase(mode || "", "paper");
  const tone = priceTone(isPaper ? position.profit_rate : position.profit_amount);
  if (isPaper) {
    return <strong className={tone}>{formatSignedPercent(position.profit_rate * 100)}</strong>;
  }
  return (
    <>
      <strong className={tone}>{formatSignedNumber(position.profit_amount)}</strong>
      <span className={`tv-field-sub ${tone}`}>({formatSignedPercent(position.profit_rate * 100)})</span>
    </>
  );
}

function formatPositionTargetWithRate(position: TradingViewPosition, targetPrice?: number): string {
  const formattedPrice = formatPrice(targetPrice);
  const rate = calculatePositionTargetReturnRate(
    position.position_side,
    position.entry_price,
    targetPrice,
    position.leverage_multiplier
  );
  if (formattedPrice === "--" || rate == null) {
    return formattedPrice;
  }
  return `${formattedPrice} (${formatSignedPercent(rate * 100)})`;
}

function calculatePositionTargetReturnRate(
  positionSide?: string,
  entryPrice?: number,
  targetPrice?: number,
  leverageMultiplier?: number
): number | null {
  if (
    !Number.isFinite(entryPrice) ||
    !entryPrice ||
    entryPrice <= 0 ||
    !Number.isFinite(targetPrice) ||
    !targetPrice ||
    targetPrice <= 0
  ) {
    return null;
  }
  const leverage = Number.isFinite(leverageMultiplier) && leverageMultiplier && leverageMultiplier > 0 ? leverageMultiplier : 1;
  const baseRate = (() => {
    switch ((positionSide || "").trim().toLowerCase()) {
      case "short":
        return (entryPrice - targetPrice) / entryPrice;
      case "long":
      default:
        return (targetPrice - entryPrice) / entryPrice;
    }
  })();
  return baseRate * leverage;
}

function formatOpenPositionHoldingDuration(position: TradingViewPosition): string {
  const startMS = parseTradingViewDateTimeMS(position.entry_time);
  if (!(startMS > 0)) {
    const direct = Number(position.holding_duration_ms || 0);
    if (Number.isFinite(direct) && direct > 0) {
      return formatCompactDuration(direct);
    }
    return "--";
  }
  const endMS = Date.now();
  if (endMS <= startMS) {
    return "0s";
  }
  return formatCompactDuration(endMS - startMS);
}

function parseTradingViewDateTimeMS(value?: string): number {
  const trimmed = (value || "").trim();
  if (!trimmed) {
    return 0;
  }
  const normalized = trimmed.includes("T") ? trimmed : trimmed.replace(" ", "T");
  const isoLike = /(?:[zZ]|[+-]\d{2}:\d{2})$/.test(normalized) ? normalized : `${normalized}Z`;
  const parsedUTC = Date.parse(isoLike);
  if (Number.isFinite(parsedUTC)) {
    return parsedUTC;
  }
  const parsedLocal = Date.parse(normalized);
  if (Number.isFinite(parsedLocal)) {
    return parsedLocal;
  }
  return 0;
}

function buildStrategyOptionIdentity(option: TradingViewStrategyOption): string {
  return `${option.strategy_name}|${option.trade_timeframes.join("/")}`;
}

function buildPositionStrategyIdentity(position: TradingViewPosition): string {
  const strategy = (position.strategy_name || "").trim();
  const timeframes = Array.isArray(position.strategy_timeframes) ? position.strategy_timeframes.filter((item) => item.trim() !== "") : [];
  if (!strategy || timeframes.length === 0) {
    return "";
  }
  return `${strategy}|${timeframes.join("/")}`;
}

function normalizeExchangeName(value?: string): string {
  return (value || "").trim().toLowerCase();
}

function normalizeSymbolKey(value?: string): string {
  return (value || "").trim().toUpperCase();
}

function isSameTradingViewPosition(left: TradingViewPosition, right: TradingViewPosition): boolean {
  if (left.position_id > 0 && right.position_id > 0) {
    return left.position_id === right.position_id;
  }
  return (
    normalizeExchangeName(left.exchange) === normalizeExchangeName(right.exchange) &&
    normalizeSymbolKey(left.symbol) === normalizeSymbolKey(right.symbol) &&
    (left.position_side || "").trim().toLowerCase() === (right.position_side || "").trim().toLowerCase()
  );
}

function buildChartPositionLevels(
  positions: TradingViewPosition[],
  exchange: string,
  symbol: string
): CandlesChartPositionLevel[] {
  const targetExchange = normalizeExchangeName(exchange);
  const targetSymbol = normalizeSymbolKey(symbol);
  if (!targetExchange || !targetSymbol) {
    return [];
  }
  return positions
    .filter(
      (item) =>
        normalizeExchangeName(item.exchange) === targetExchange && normalizeSymbolKey(item.symbol) === targetSymbol && item.entry_price > 0
    )
    .map((item) => ({
      positionID: item.position_id,
      side: (item.position_side || "").trim().toLowerCase() === "short" ? "short" : "long",
      entryPrice: item.entry_price,
      takeProfitPrice: item.take_profit_price > 0 ? item.take_profit_price : undefined,
      stopLossPrice: item.stop_loss_price > 0 ? item.stop_loss_price : undefined
    }));
}

function formatLeverage(value?: number): string {
  const rounded = value && Number.isFinite(value) && value > 0 ? Math.round(value) : 3;
  return `${rounded}x`;
}

function formatLeverageDecimal(value?: number): string {
  const normalized = value && Number.isFinite(value) && value > 0 ? value : 10;
  return `${normalized.toFixed(2)} x`;
}

function formatUSDTValue(value?: number): string {
  if (value == null || !Number.isFinite(value) || value < 0) {
    return "--";
  }
  return `${value.toLocaleString("en-US", {
    minimumFractionDigits: value >= 1 ? 2 : 4,
    maximumFractionDigits: value >= 1 ? 2 : 4
  })} USDT`;
}

function estimateTradeLiquidationPrice(price: number, leverage: number, side: "long" | "short"): number {
  if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(leverage) || leverage <= 0) {
    return 0;
  }
  const move = price / leverage * 0.82;
  if (side === "long") {
    return Math.max(price - move, 0);
  }
  return price + move;
}

function marketTypeLabel(value: string): string {
  switch (value.toLowerCase()) {
    case "swap":
    case "perpetual":
      return "永续";
    case "spot":
      return "现货";
    default:
      return value || "--";
  }
}

function directionLabel(value?: string): string {
  switch ((value || "").toLowerCase()) {
    case "long":
      return "多";
    case "short":
      return "空";
    default:
      return "--";
  }
}

function directionBarClass(value?: string): string {
  switch ((value || "").toLowerCase()) {
    case "long":
      return "is-long";
    case "short":
      return "is-short";
    default:
      return "";
  }
}

function formatHistoryCloseStatus(value?: string): string {
  switch ((value || "").toLowerCase()) {
    case "partial_close":
      return "部分平仓";
    case "full_close":
      return "全部平仓";
    default:
      return "--";
  }
}

function priceTone(value: number): string {
  if (value > 0) {
    return "is-positive";
  }
  if (value < 0) {
    return "is-negative";
  }
  return "";
}

function formatDisplaySymbol(symbol: string): string {
  if (!symbol) {
    return "--";
  }
  return symbol;
}

function lookupDisplaySymbol(symbol: string, items: TradingViewSymbol[]): string {
  return items.find((item) => item.symbol === symbol)?.display_symbol || formatDisplaySymbol(symbol);
}

function buildHistoryPositionRowKey(item: TradingViewHistoryPosition): string {
  return [
    "history",
    item.exchange || "--",
    item.symbol || "--",
    item.position_side || "--",
    item.margin_mode || "--",
    item.entry_time || "--"
  ].join("|");
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error || "unknown error");
}

function isDocumentVisible(): boolean {
  return typeof document === "undefined" || document.visibilityState === "visible";
}

function hasActiveChartSelection(
  runtime: TradingViewRuntimeResponse | null,
  exchange: string,
  symbol: string,
  timeframe: string
): boolean {
  if (!runtime || !exchange || !symbol || !timeframe) {
    return false;
  }
  if (runtime.selected_exchange !== exchange) {
    return false;
  }
  if (!runtime.timeframes.includes(timeframe)) {
    return false;
  }
  return runtime.symbols.some((item) => item.symbol === symbol);
}

function shouldUpdateCandles(previous: TradingViewCandlesResponse | null, next: TradingViewCandlesResponse): boolean {
  if (!previous) {
    return true;
  }
  if (
    previous.exchange !== next.exchange ||
    previous.symbol !== next.symbol ||
    previous.timeframe !== next.timeframe ||
    previous.display_symbol !== next.display_symbol ||
    previous.market_type !== next.market_type
  ) {
    return true;
  }
  if (previous.candles.length !== next.candles.length) {
    return true;
  }
  if (!sameCandleSnapshot(previous.candles[0], next.candles[0])) {
    return true;
  }
  if (!sameCandleSnapshot(previous.candles[previous.candles.length - 1], next.candles[next.candles.length - 1])) {
    return true;
  }
  if (previous.indicators.length !== next.indicators.length) {
    return true;
  }
  for (let index = 0; index < previous.indicators.length; index += 1) {
    const prevLine = previous.indicators[index];
    const nextLine = next.indicators[index];
    if (!nextLine) {
      return true;
    }
    if (
      prevLine.id !== nextLine.id ||
      prevLine.label !== nextLine.label ||
      prevLine.color !== nextLine.color ||
      prevLine.legend_color !== nextLine.legend_color ||
      prevLine.points.length !== nextLine.points.length
    ) {
      return true;
    }
    const prevLastPoint = prevLine.points[prevLine.points.length - 1];
    const nextLastPoint = nextLine.points[nextLine.points.length - 1];
    if (!sameIndicatorPoint(prevLastPoint, nextLastPoint)) {
      return true;
    }
  }
  return false;
}

function sameCandleSnapshot(
  previous: TradingViewCandlesResponse["candles"][number] | undefined,
  next: TradingViewCandlesResponse["candles"][number] | undefined
): boolean {
  if (!previous && !next) {
    return true;
  }
  if (!previous || !next) {
    return false;
  }
  return (
    previous.ts === next.ts &&
    previous.open === next.open &&
    previous.high === next.high &&
    previous.low === next.low &&
    previous.close === next.close &&
    previous.volume === next.volume
  );
}

function sameIndicatorPoint(
  previous: TradingViewCandlesResponse["indicators"][number]["points"][number] | undefined,
  next: TradingViewCandlesResponse["indicators"][number]["points"][number] | undefined
): boolean {
  if (!previous && !next) {
    return true;
  }
  if (!previous || !next) {
    return false;
  }
  return previous.ts === next.ts && previous.value === next.value;
}

function mergeRuntimeRealtime(
  previous: TradingViewRuntimeResponse | null,
  account?: TradingViewRealtimeAccount,
  positions?: TradingViewPosition[]
): TradingViewRuntimeResponse | null {
  if (!previous) {
    return previous;
  }
  const nextPositions = positions ? positions.map((item) => ({ ...item })) : previous.positions;
  const nextSymbols = positions ? mergeRealtimeSymbols(previous.symbols, nextPositions) : previous.symbols;
  const nextFunds = mergeRealtimeFunds(previous.funds, account, nextPositions);
  return {
    ...previous,
    positions: nextPositions,
    symbols: nextSymbols,
    funds: nextFunds
  };
}

function mergeRuntimePayload(
  previous: TradingViewRuntimeResponse | null,
  incoming: TradingViewRuntimeResponse
): TradingViewRuntimeResponse {
  if (!previous || previous.selected_exchange !== incoming.selected_exchange) {
    return incoming;
  }
  if (incoming.bootstrap_complete) {
    return incoming;
  }
  return {
    ...incoming,
    bootstrap_complete: previous.bootstrap_complete || incoming.bootstrap_complete,
    symbols: mergeBootstrapSymbols(previous.symbols, incoming.symbols),
    positions: previous.positions,
    orders: previous.orders,
    history_positions: previous.history_positions,
    funds: previous.funds,
    strategy_options:
      Array.isArray(incoming.strategy_options) && incoming.strategy_options.length > 0
        ? incoming.strategy_options
        : previous.strategy_options
  };
}

function upsertRuntimeOrders(previous: TradingViewOrder[], next: TradingViewOrder): TradingViewOrder[] {
  if (!next || next.id <= 0) {
    return previous;
  }
  const existingIndex = previous.findIndex((item) => item.id === next.id);
  if (existingIndex < 0) {
    return [next, ...previous];
  }
  const merged = [...previous];
  merged[existingIndex] = {
    ...previous[existingIndex],
    ...next
  };
  return merged;
}

function mergeRuntimeSymbolsSnapshot(
  previous: TradingViewRuntimeResponse | null,
  symbolsPayload: { exchange: string; symbols: TradingViewSymbol[] },
  selectedExchange: string
): TradingViewRuntimeResponse | null {
  if (!previous) {
    return previous;
  }
  if (!symbolsPayload || symbolsPayload.exchange !== selectedExchange || previous.selected_exchange !== selectedExchange) {
    return previous;
  }
  return {
    ...previous,
    symbols: Array.isArray(symbolsPayload.symbols) ? symbolsPayload.symbols.map((item) => ({ ...item })) : previous.symbols
  };
}

function mergeBootstrapSymbols(previous: TradingViewSymbol[], incoming: TradingViewSymbol[]): TradingViewSymbol[] {
  if (!Array.isArray(incoming) || incoming.length === 0) {
    return previous;
  }
  const previousBySymbol = new Map(previous.map((item) => [item.symbol, item] as const));
  return incoming.map((item) => {
    const before = previousBySymbol.get(item.symbol);
    if (!before) {
      return item;
    }
    return {
      ...item,
      last_price: item.last_price > 0 ? item.last_price : before.last_price,
      change_24h_pct: item.change_24h_pct !== 0 ? item.change_24h_pct : before.change_24h_pct,
      high_24h: item.high_24h > 0 ? item.high_24h : before.high_24h,
      low_24h: item.low_24h > 0 ? item.low_24h : before.low_24h,
      turnover_24h: item.turnover_24h > 0 ? item.turnover_24h : before.turnover_24h
    };
  });
}

function stringsEqualIgnoreCase(left: string, right: string): boolean {
  return left.trim().toLowerCase() === right.trim().toLowerCase();
}

function mergeRealtimeSymbols(symbols: TradingViewSymbol[], positions: TradingViewPosition[]): TradingViewSymbol[] {
  const positionBySymbol = new Map<string, TradingViewPosition>();
  for (const item of positions) {
    if (!item.symbol || positionBySymbol.has(item.symbol)) {
      continue;
    }
    positionBySymbol.set(item.symbol, item);
  }
  return symbols.map((item) => {
    const position = positionBySymbol.get(item.symbol);
    if (!position) {
      return {
        ...item,
        is_held: false,
        position_side: undefined,
        leverage_multiplier: undefined,
        margin_amount: undefined,
        unrealized_profit_amount: undefined,
        unrealized_profit_rate: undefined
      };
    }
    return {
      ...item,
      is_held: true,
      position_side: position.position_side,
      leverage_multiplier: position.leverage_multiplier,
      margin_amount: position.margin_amount,
      unrealized_profit_amount: position.unrealized_profit_amount,
      unrealized_profit_rate: position.unrealized_profit_rate,
      last_price: position.current_price > 0 ? position.current_price : item.last_price
    };
  });
}

function mergeRealtimeFunds(
  previous: TradingViewFunds,
  account: TradingViewRealtimeAccount | undefined,
  positions: TradingViewPosition[]
): TradingViewFunds {
  const floatingProfitUSDT = positions.reduce((sum, item) => sum + (Number.isFinite(item.unrealized_profit_amount) ? item.unrealized_profit_amount : 0), 0);
  const marginInUseUSDT = positions.reduce((sum, item) => sum + (Number.isFinite(item.margin_amount) ? item.margin_amount : 0), 0);
  return {
    exchange: account?.exchange || previous.exchange,
    currency: account?.currency || previous.currency,
    total_equity_usdt: account?.total_usdt ?? previous.total_equity_usdt,
    floating_profit_usdt: floatingProfitUSDT,
    margin_in_use_usdt: marginInUseUSDT,
    funding_usdt: account?.funding_usdt ?? previous.funding_usdt,
    trading_usdt: account?.trading_usdt ?? previous.trading_usdt,
    per_trade_usdt: account?.per_trade_usdt ?? previous.per_trade_usdt,
    daily_profit_usdt: account?.daily_profit_usdt ?? previous.daily_profit_usdt,
    closed_profit_rate: account?.closed_profit_rate ?? previous.closed_profit_rate,
    floating_profit_rate: account?.floating_profit_rate ?? previous.floating_profit_rate,
    total_profit_rate: account?.total_profit_rate ?? previous.total_profit_rate,
    updated_at_ms: account?.updated_at_ms ?? previous.updated_at_ms
  };
}

function mergeRealtimeCandles(
  previous: TradingViewCandlesResponse | null,
  incoming: TradingViewCandlesResponse
): TradingViewCandlesResponse {
  if (!previous) {
    return incoming;
  }
  if (
    previous.exchange !== incoming.exchange ||
    previous.symbol !== incoming.symbol ||
    previous.timeframe !== incoming.timeframe
  ) {
    return previous;
  }

  const merged: TradingViewCandlesResponse = {
    ...previous,
    display_symbol: incoming.display_symbol,
    market_type: incoming.market_type,
    candles: mergeCandlesTail(previous.candles, incoming.candles),
    indicators: mergeIndicatorLines(previous.indicators, incoming.indicators)
  };
  if (shouldUpdateCandles(previous, merged)) {
    return merged;
  }
  return previous;
}

function mergeCandlesTail(
  previous: TradingViewCandlesResponse["candles"],
  incoming: TradingViewCandlesResponse["candles"]
): TradingViewCandlesResponse["candles"] {
  if (previous.length === 0) {
    return incoming.map((item) => ({ ...item }));
  }
  if (incoming.length === 0) {
    return previous;
  }
  const mergedByTS = new Map<number, TradingViewCandlesResponse["candles"][number]>();
  for (const item of previous) {
    mergedByTS.set(item.ts, item);
  }
  for (const item of incoming) {
    mergedByTS.set(item.ts, { ...item });
  }
  return Array.from(mergedByTS.values()).sort((left, right) => left.ts - right.ts);
}

function mergeIndicatorLines(
  previous: TradingViewCandlesResponse["indicators"],
  incoming: TradingViewCandlesResponse["indicators"]
): TradingViewCandlesResponse["indicators"] {
  if (previous.length === 0) {
    return incoming.map((line) => ({
      ...line,
      points: line.points.map((point) => ({ ...point }))
    }));
  }
  if (incoming.length === 0) {
    return previous;
  }

  const previousByID = new Map(previous.map((line) => [line.id, line]));
  return incoming.map((line) => {
    const existing = previousByID.get(line.id);
    if (!existing) {
      return {
        ...line,
        points: line.points.map((point) => ({ ...point }))
      };
    }
    const pointsByTS = new Map<number, typeof line.points[number]>();
    for (const point of existing.points) {
      pointsByTS.set(point.ts, point);
    }
    for (const point of line.points) {
      pointsByTS.set(point.ts, { ...point });
    }
    return {
      ...existing,
      label: line.label,
      color: line.color,
      legend_color: line.legend_color,
      points: Array.from(pointsByTS.values()).sort((left, right) => left.ts - right.ts)
    };
  });
}
