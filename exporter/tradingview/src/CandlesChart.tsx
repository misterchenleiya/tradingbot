import {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type MutableRefObject
} from "react";
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  LineStyle,
  createChart,
  HistogramSeries,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
  type Time
} from "lightweight-charts";
import {
  buildTradingViewEventMarkers,
  type ChartRangeSelection,
  type TradingViewEventMarker,
  type TradingViewEventMarkerTooltipRow
} from "./chartOverlays";
import type { TradingViewCandle, TradingViewEventEntry, TradingViewIndicatorLine } from "./types";

interface CandlesChartProps {
  candles: TradingViewCandle[];
  indicators: TradingViewIndicatorLine[];
  events?: TradingViewEventEntry[];
  positionLevels?: CandlesChartPositionLevel[];
  visibleIndicators: Record<string, boolean>;
  indicatorLegend: CandlesChartIndicatorLegendItem[];
  viewportKey: string;
  initialViewportSnapshots?: Record<string, ChartViewportSnapshot>;
  transferViewportFromKey?: string;
  transferViewportToken?: string;
  onViewportSnapshotChange?: (key: string, snapshot: ChartViewportSnapshot) => void;
  onToggleIndicator: (id: string) => void;
  onRunBacktestArea?: (selection: ChartRangeSelection) => void;
  onPricePick?: (price: number) => void;
  onMovePositionLevel?: (level: CandlesChartPositionLevelMove) => void;
  loading: boolean;
  emptyText: string;
}

export interface CandlesChartIndicatorLegendItem {
  id: string;
  label: string;
  color: string;
  value: string;
  visible: boolean;
}

export interface CandlesChartHandle {
  resetView: () => void;
  zoomIn: () => void;
  zoomOut: () => void;
  panLeft: () => void;
  panRight: () => void;
  getVisibleLogicalRange: () => { from: number; to: number } | null;
  focusLogicalRange: (from: number, to: number) => void;
  focusCrosshairAtTime: (timeMS: number, price?: number | null) => void;
}

export interface CandlesChartPositionLevel {
  positionID: number;
  side: "long" | "short";
  entryPrice: number;
  takeProfitPrice?: number;
  stopLossPrice?: number;
}

export interface CandlesChartPositionLevelMove {
  positionID: number;
  side: "long" | "short";
  kind: "tp" | "sl";
  price: number;
}

export interface ChartViewportSnapshot {
  bars: number;
  latestOffset: number;
  updatedAtMS: number;
}

interface ChartSelectionDraft {
  startX: number;
  startY: number;
  endX: number;
  endY: number;
}

interface ChartContextMenuState {
  left: number;
  top: number;
}

interface SelectionStatsCard {
  deltaText: string;
  barsText: string;
  volumeText: string;
  placeAbove: boolean;
}

interface PositionLevelOverlay {
  id: string;
  positionID: number;
  side: "long" | "short";
  kind: "entry" | "tp" | "sl";
  price: number;
  y: number;
  axisOffset: number;
  lineLength: number;
  label: string;
  draggable: boolean;
}

interface PositionLevelDragState {
  positionID: number;
  side: "long" | "short";
  kind: "tp" | "sl";
  price: number;
  originalPrice: number;
}

interface MarkerTooltipState {
  markerID: string;
  price: number;
  clampDirection: "top" | "bottom" | null;
  title: string;
  rows: TradingViewEventMarkerTooltipRow[];
  left: number;
  top: number;
}

interface CandlesChartBenchmarkSnapshot {
  ready: boolean;
  timestampMS: number;
  viewportKey: string;
  candleCount: number;
  chartWidth: number;
  chartHeight: number;
  plotWidth: number;
  axisWidth: number;
  logicalFrom: number | null;
  logicalTo: number | null;
  visibleSpan: number | null;
  latestIndex: number | null;
  latestOffsetBars: number | null;
  rightPaddingBars: number | null;
  crosshairX: number | null;
  crosshairY: number | null;
}

interface CandlesChartBenchmarkBridge {
  version: number;
  getSnapshot: () => CandlesChartBenchmarkSnapshot;
  resetView: () => void;
  zoomIn: () => void;
  zoomOut: () => void;
  panLeft: () => void;
  panRight: () => void;
}

interface PriceDisplay {
  key: string;
  axisWidth: number;
  formatter: (value: number) => string;
}

const PRICE_AXIS_FONT = "11px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
const PRICE_AXIS_MIN_WIDTH = 52;
const PRICE_AXIS_MAX_WIDTH = 96;
const SCIENTIFIC_LEADING_ZERO_THRESHOLD = 5;
const CROSSHAIR_AXIS_LABEL_HEIGHT = 30;
const MIN_VISIBLE_LOGICAL_SPAN = 2;
const MIN_IMPLIED_BAR_SPACING = 0.5;
const MAX_IMPLIED_BAR_SPACING = 28;
const KEYBOARD_ZOOM_TARGET_RATIO_MIN = 0.52;
const KEYBOARD_ZOOM_TARGET_RATIO_MAX = 0.67;
const KEYBOARD_ZOOM_TARGET_RATIO_SPACING_RANGE = 6;
const WHEEL_ZOOM_TARGET_RATIO_STRONG_MIN = 0.95;
const WHEEL_ZOOM_TARGET_RATIO_STRONG_MAX = 0.969;
const WHEEL_ZOOM_TARGET_RATIO_LIGHT_MIN = 0.972;
const WHEEL_ZOOM_TARGET_RATIO_LIGHT_MAX = 0.986;
const WHEEL_ZOOM_TARGET_RATIO_SPACING_RANGE = 6;
const MIN_COMPUTED_BAR_SPACING_DELTA = 0.02;
const MAX_KEYBOARD_BAR_SPACING_DELTA = 3.2;
const MAX_WHEEL_BAR_SPACING_DELTA = 0.4;

function toSeriesTime(ts: number): Time {
  return Math.floor(ts / 1000) as Time;
}

export const CandlesChart = forwardRef<CandlesChartHandle, CandlesChartProps>(function CandlesChart(
  props: CandlesChartProps,
  ref
): JSX.Element {
  const {
    candles,
    indicators,
    events = [],
    positionLevels = [],
    visibleIndicators,
    indicatorLegend,
    viewportKey,
    initialViewportSnapshots,
    transferViewportFromKey,
    transferViewportToken,
    onViewportSnapshotChange,
    onToggleIndicator,
    onRunBacktestArea,
    onPricePick,
    onMovePositionLevel,
    loading,
    emptyText
  } = props;
  const shellRef = useRef<HTMLDivElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const crosshairAxisLabelRef = useRef<HTMLDivElement | null>(null);
  const crosshairAxisPriceRef = useRef<HTMLSpanElement | null>(null);
  const crosshairAxisPercentRef = useRef<HTMLSpanElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const indicatorSeriesRef = useRef<Map<string, ISeriesApi<"Line">>>(new Map());
  const viewportByKeyRef = useRef<Map<string, ChartViewportSnapshot>>(new Map());
  const lastAppliedTransferTokenRef = useRef("");
  const viewportKeyRef = useRef(viewportKey);
  const candleCountRef = useRef(0);
  const priceDisplayRef = useRef<PriceDisplay>(buildDynamicPriceDisplay([]));
  const latestClosedCloseRef = useRef<number>(Number.NaN);
  const chartMetricsRef = useRef({ width: 0, height: 0, plotWidth: 0 });
  const crosshairFrameRef = useRef(0);
  const pendingCrosshairParamRef = useRef<unknown>(null);
  const crosshairVisibleRef = useRef(false);
  const crosshairPointXRef = useRef<number>(Number.NaN);
  const crosshairPointYRef = useRef<number>(Number.NaN);
  const crosshairTranslateYRef = useRef<number>(Number.NaN);
  const crosshairWidthRef = useRef(0);
  const crosshairPriceTextRef = useRef("");
  const crosshairPercentTextRef = useRef("");
  const suppressViewportCaptureRef = useRef(false);
  const viewportCaptureResumeFrameRef = useRef(0);
  const candlesRef = useRef<TradingViewCandle[]>(candles);
  const selectionDraftRef = useRef<ChartSelectionDraft | null>(null);
  const isSelectingRef = useRef(false);
  const selectionHasMovedRef = useRef(false);
  const ignoreSelectionClearUntilRef = useRef(0);
  const [overlayVersion, setOverlayVersion] = useState(0);
  const [selection, setSelection] = useState<ChartRangeSelection | null>(null);
  const [selectionDraft, setSelectionDraft] = useState<ChartSelectionDraft | null>(null);
  const [contextMenu, setContextMenu] = useState<ChartContextMenuState | null>(null);
  const [draggingLevel, setDraggingLevel] = useState<PositionLevelDragState | null>(null);
  const [hoveredMarker, setHoveredMarker] = useState<MarkerTooltipState | null>(null);
  const draggingLevelRef = useRef<PositionLevelDragState | null>(null);

  const candleData = useMemo(
    () =>
      candles.map((item) => ({
        time: toSeriesTime(item.ts),
        open: item.open,
        high: item.high,
        low: item.low,
        close: item.close
      })),
    [candles]
  );

  const volumeData = useMemo(
    () =>
      candles.map((item) => ({
        time: toSeriesTime(item.ts),
        value: item.volume,
        color: item.close >= item.open ? "rgba(48, 209, 88, 0.55)" : "rgba(255, 69, 58, 0.55)"
      })),
    [candles]
  );
  const latestClosedClose = useMemo(() => resolveLatestClosedClose(candles), [candles]);
  const priceDisplay = useMemo(() => buildDynamicPriceDisplay(candleData), [candleData]);

  useEffect(() => {
    candlesRef.current = candles;
  }, [candles]);

  useEffect(() => {
    viewportKeyRef.current = viewportKey;
  }, [viewportKey]);

  useEffect(() => {
    if (!initialViewportSnapshots) {
      viewportByKeyRef.current = new Map();
      return;
    }
    viewportByKeyRef.current = new Map(
      Object.entries(initialViewportSnapshots).map(([key, snapshot]) => [
        key,
        {
          bars: snapshot.bars,
          latestOffset: snapshot.latestOffset,
          updatedAtMS: snapshot.updatedAtMS
        }
      ])
    );
  }, [initialViewportSnapshots]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }
    const chart = createChart(container, {
      layout: {
        background: { type: ColorType.Solid, color: "#111214" },
        textColor: "#b7bcc6",
        fontSize: 11
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.04)" },
        horzLines: { color: "rgba(255,255,255,0.04)" }
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,0.08)",
        scaleMargins: { top: 0.08, bottom: 0.24 }
      },
      timeScale: {
        borderColor: "rgba(255,255,255,0.08)",
        rightOffset: 8,
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter: (time) => formatAxisTimeLabel(time)
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "rgba(255,255,255,0.64)", width: 1, style: LineStyle.LargeDashed },
        horzLine: { color: "rgba(255,255,255,0.64)", width: 1, style: LineStyle.LargeDashed }
      },
      localization: {
        locale: "zh-CN",
        timeFormatter: formatCrosshairTimeLabel
      },
      handleScale: {
        mouseWheel: false
      },
      width: container.clientWidth,
      height: container.clientHeight
    });
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#2fb45a",
      downColor: "#d94d72",
      wickUpColor: "#2fb45a",
      wickDownColor: "#d94d72",
      borderVisible: false
    });
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      lastValueVisible: false,
      priceLineVisible: false
    });
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.78, bottom: 0 }
    });
    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;
    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (suppressViewportCaptureRef.current) {
        setOverlayVersion((previous) => previous + 1);
        return;
      }
      const key = viewportKeyRef.current;
      const candleCount = candleCountRef.current;
      if (!key || !range || candleCount <= 0 || !Number.isFinite(range.from) || !Number.isFinite(range.to)) {
        return;
      }
      const bars = Math.max(20, range.to - range.from);
      const latestIndex = candleCount - 1;
      const latestOffset = latestIndex - range.to;
      const snapshot = {
        bars,
        latestOffset,
        updatedAtMS: Date.now()
      };
      viewportByKeyRef.current.set(key, snapshot);
      onViewportSnapshotChange?.(key, snapshot);
      setOverlayVersion((previous) => previous + 1);
    });
    chart.subscribeCrosshairMove((param) => {
      pendingCrosshairParamRef.current = param;
      if (crosshairFrameRef.current) {
        return;
      }
      crosshairFrameRef.current = window.requestAnimationFrame(() => {
        crosshairFrameRef.current = 0;
        handleCrosshairMove(pendingCrosshairParamRef.current);
      });
    });

    const handleWheel = (event: WheelEvent) => {
      if (Math.abs(event.deltaY) <= 0 || Math.abs(event.deltaY) < Math.abs(event.deltaX)) {
        return;
      }
      if (event.cancelable) {
        event.preventDefault();
      }
      zoomChartFromWheelDelta(
        chart,
        chartMetricsRef.current.plotWidth,
        normalizeWheelDelta(event)
      );
    };
    container.addEventListener("wheel", handleWheel, { passive: false });

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) {
        return;
      }
      chart.applyOptions({
        width: Math.max(320, Math.floor(entry.contentRect.width)),
        height: Math.max(260, Math.floor(entry.contentRect.height))
      });
      refreshCrosshairGeometry();
      setOverlayVersion((previous) => previous + 1);
    });
    observer.observe(container);
    return () => {
      if (viewportCaptureResumeFrameRef.current) {
        window.cancelAnimationFrame(viewportCaptureResumeFrameRef.current);
      }
      container.removeEventListener("wheel", handleWheel);
      observer.disconnect();
      if (crosshairFrameRef.current) {
        window.cancelAnimationFrame(crosshairFrameRef.current);
      }
      indicatorSeriesRef.current.clear();
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
      chartRef.current = null;
      chart.remove();
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const volumeSeries = volumeSeriesRef.current;
    if (!chart || !candleSeries || !volumeSeries) {
      return;
    }
    suppressViewportCaptureRef.current = true;
    if (viewportCaptureResumeFrameRef.current) {
      window.cancelAnimationFrame(viewportCaptureResumeFrameRef.current);
      viewportCaptureResumeFrameRef.current = 0;
    }
    candleCountRef.current = candleData.length;
    priceDisplayRef.current = priceDisplay;
    latestClosedCloseRef.current = latestClosedClose;
    candleSeries.setData(candleData);
    volumeSeries.setData(volumeData);
    applyViewport(
      chart,
      candleData.length,
      viewportByKeyRef.current,
      viewportKey,
      transferViewportFromKey,
      transferViewportToken,
      lastAppliedTransferTokenRef,
      onViewportSnapshotChange
    );
    refreshCrosshairGeometry();
    hideCrosshairAxisLabel();
    setOverlayVersion((previous) => previous + 1);
    viewportCaptureResumeFrameRef.current = window.requestAnimationFrame(() => {
      viewportCaptureResumeFrameRef.current = 0;
      suppressViewportCaptureRef.current = false;
    });
  }, [
    candleData,
    latestClosedClose,
    onViewportSnapshotChange,
    priceDisplay,
    transferViewportFromKey,
    transferViewportToken,
    viewportKey,
    volumeData
  ]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) {
      return;
    }
    const next = new Map<string, ISeriesApi<"Line">>();
    for (const line of indicators) {
      if (visibleIndicators[line.id] === false) {
        continue;
      }
      const existing = indicatorSeriesRef.current.get(line.id);
      const series =
        existing ||
        chart.addSeries(LineSeries, {
          color: line.color,
          lineWidth: 1.5,
          lastValueVisible: false,
          priceLineVisible: false,
          crosshairMarkerVisible: false
        });
      series.setData(
        line.points.map((point) => ({
          time: toSeriesTime(point.ts),
          value: point.value
        }))
      );
      next.set(line.id, series);
    }
    for (const [id, series] of indicatorSeriesRef.current.entries()) {
      if (next.has(id)) {
        continue;
      }
      chart.removeSeries(series);
    }
    indicatorSeriesRef.current = next;
  }, [indicators, visibleIndicators]);

  const selectionBox = useMemo(
    () => resolveSelectionBox(chartRef.current, candleSeriesRef.current, selection),
    [selection, candleData, overlayVersion]
  );
  const draftSelectionBox = useMemo(
    () => resolveDraftSelectionBox(selectionDraft),
    [selectionDraft]
  );
  const eventMarkers = useMemo(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!chart || !candleSeries) {
      return [];
    }
    return buildTradingViewEventMarkers(
      chart,
      candleSeries,
      candles,
      events,
      chartMetricsRef.current.height,
      priceDisplay.formatter
    );
  }, [candles, events, overlayVersion, priceDisplay]);
  const positionLevelOverlays = useMemo(() => {
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) {
      return [];
    }
    return buildPositionLevelOverlays(
      candleSeries,
      positionLevels,
      chartMetricsRef.current.width,
      chartMetricsRef.current.height,
      priceDisplayRef.current.axisWidth,
      priceDisplayRef.current.formatter,
      draggingLevel
    );
  }, [draggingLevel, overlayVersion, positionLevels]);

  const activeSelection = useMemo(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (selectionDraft && chart && candleSeries) {
      return buildSelectionFromDraft(chart, candleSeries, candles, selectionDraft, chartMetricsRef.current);
    }
    return selection;
  }, [candles, selection, selectionDraft, overlayVersion]);

  const activeSelectionBox = selectionDraft ? draftSelectionBox : selectionBox;
  const selectionStatsCard = useMemo(
    () => buildSelectionStatsCard(activeSelection, activeSelectionBox, candles),
    [activeSelection, activeSelectionBox, candles]
  );

  useEffect(() => {
    if (!hoveredMarker) {
      return;
    }
    if (!eventMarkers.some((item) => item.id === hoveredMarker.markerID)) {
      setHoveredMarker(null);
    }
  }, [eventMarkers, hoveredMarker]);

  useEffect(() => {
    const shell = shellRef.current;
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!shell || !chart || !candleSeries) {
      return;
    }

    const clearSelection = () => {
      isSelectingRef.current = false;
      selectionHasMovedRef.current = false;
      selectionDraftRef.current = null;
      setSelectionDraft(null);
      setSelection(null);
      setContextMenu(null);
    };

    const finishDraftSelection = () => {
      const draft = selectionDraftRef.current;
      isSelectingRef.current = false;
      selectionHasMovedRef.current = false;
      selectionDraftRef.current = null;
      if (!draft) {
        setSelectionDraft(null);
        return;
      }
      setSelectionDraft(null);
      const nextSelection = buildSelectionFromDraft(chart, candleSeries, candlesRef.current, draft, chartMetricsRef.current);
      setSelection(nextSelection);
      setContextMenu(null);
      if (nextSelection) {
        ignoreSelectionClearUntilRef.current = Date.now() + 250;
      }
    };

    const isContextMenuTarget = (target: EventTarget | null): boolean => {
      return target instanceof Element && target.closest(".tv-chart-context-menu") !== null;
    };

    const isLegendTarget = (target: EventTarget | null): boolean => {
      return target instanceof Element && target.closest(".tv-chart-legend") !== null;
    };

    const isPositionLevelTarget = (target: EventTarget | null): boolean => {
      return target instanceof Element && target.closest(".tv-position-level-item") !== null;
    };

    const pointFromMouseEvent = (event: MouseEvent | PointerEvent): { x: number; y: number } | null => {
      const point = resolveClientPoint(event, shell, chartMetricsRef.current);
      if (!point) {
        return null;
      }
      return point;
    };

    const handleMouseDown = (event: MouseEvent) => {
      if (event.button !== 0 || !event.shiftKey) {
        return;
      }
      if (isContextMenuTarget(event.target) || isLegendTarget(event.target) || isPositionLevelTarget(event.target)) {
        return;
      }
      if (selectionDraftRef.current) {
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      const point = pointFromMouseEvent(event);
      if (!point) {
        return;
      }
      const nextDraft = {
        startX: point.x,
        startY: point.y,
        endX: point.x,
        endY: point.y
      };
      isSelectingRef.current = true;
      selectionHasMovedRef.current = false;
      selectionDraftRef.current = nextDraft;
      setSelectionDraft(nextDraft);
      setContextMenu(null);
      event.preventDefault();
      event.stopPropagation();
    };

    const handleWindowMouseMove = (event: MouseEvent) => {
      const draft = selectionDraftRef.current;
      if (!draft) {
        return;
      }
      const point = pointFromMouseEvent(event);
      if (!point) {
        return;
      }
      const nextDraft = {
        ...draft,
        endX: point.x,
        endY: point.y
      };
      if (
        !selectionHasMovedRef.current &&
        (Math.abs(nextDraft.endX - nextDraft.startX) >= 2 || Math.abs(nextDraft.endY - nextDraft.startY) >= 2)
      ) {
        selectionHasMovedRef.current = true;
      }
      selectionDraftRef.current = nextDraft;
      setSelectionDraft(nextDraft);
      event.preventDefault();
    };

    const handleWindowMouseUp = (event: MouseEvent) => {
      if (!selectionDraftRef.current || event.button !== 0) {
        return;
      }
      isSelectingRef.current = false;
      event.preventDefault();
    };

    const handleDragStart = (event: DragEvent) => {
      if (!selectionDraftRef.current && !event.shiftKey) {
        return;
      }
      event.preventDefault();
    };

    const handleClick = (event: MouseEvent) => {
      if (selectionDraftRef.current) {
        event.preventDefault();
        event.stopPropagation();
        if (selectionHasMovedRef.current) {
          finishDraftSelection();
        }
        return;
      }
      if (isSelectingRef.current) {
        return;
      }
      const point = pointFromMouseEvent(event);
      if (!point) {
        return;
      }
      const pickedPrice = candleSeries.coordinateToPrice(point.y);
      if (event.button === 0 && !isPositionLevelTarget(event.target) && Number.isFinite(pickedPrice) && pickedPrice > 0) {
        onPricePick?.(Number(pickedPrice));
      }
      if (!selection) {
        return;
      }
      if (Date.now() < ignoreSelectionClearUntilRef.current) {
        return;
      }
      if (isContextMenuTarget(event.target) || isPositionLevelTarget(event.target)) {
        return;
      }
      if (isPointInsideSelection(point.x, point.y, selectionBox)) {
        return;
      }
      clearSelection();
    };

    const handleContextMenu = (event: MouseEvent) => {
      if (selectionDraftRef.current) {
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      if (isSelectingRef.current) {
        event.preventDefault();
        return;
      }
      if (!selection) {
        return;
      }
      if (Date.now() < ignoreSelectionClearUntilRef.current) {
        event.preventDefault();
        return;
      }
      if (isContextMenuTarget(event.target) || isPositionLevelTarget(event.target)) {
        event.preventDefault();
        return;
      }
      const point = pointFromMouseEvent(event);
      if (!point) {
        return;
      }
      const inside = isPointInsideSelection(point.x, point.y, selectionBox);
      if (!inside) {
        event.preventDefault();
        clearSelection();
        return;
      }
      event.preventDefault();
      setContextMenu({
        left: clampValue(point.x, 8, Math.max(8, chartMetricsRef.current.plotWidth - 144)),
        top: clampValue(point.y, 8, Math.max(8, chartMetricsRef.current.height - 44))
      });
    };

    shell.addEventListener("mousedown", handleMouseDown, true);
    shell.addEventListener("dragstart", handleDragStart, true);
    shell.addEventListener("click", handleClick, true);
    shell.addEventListener("contextmenu", handleContextMenu, true);
    window.addEventListener("mousemove", handleWindowMouseMove, true);
    window.addEventListener("mouseup", handleWindowMouseUp, true);
    return () => {
      shell.removeEventListener("mousedown", handleMouseDown, true);
      shell.removeEventListener("dragstart", handleDragStart, true);
      shell.removeEventListener("click", handleClick, true);
      shell.removeEventListener("contextmenu", handleContextMenu, true);
      window.removeEventListener("mousemove", handleWindowMouseMove, true);
      window.removeEventListener("mouseup", handleWindowMouseUp, true);
    };
  }, [onPricePick, selection, selectionBox]);

  useEffect(() => {
    const shell = shellRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!shell || !candleSeries) {
      return;
    }

    const handleMouseDown = (event: MouseEvent) => {
      if (event.button !== 0) {
        return;
      }
      const target = event.target instanceof Element ? event.target.closest<HTMLElement>(".tv-position-level-item") : null;
      if (!target) {
        return;
      }
      const kind = (target.dataset.kind || "").trim().toLowerCase();
      if (kind !== "tp" && kind !== "sl") {
        return;
      }
      const positionID = Number(target.dataset.positionId || 0);
      const side = (target.dataset.side || "").trim().toLowerCase() === "short" ? "short" : "long";
      const price = Number(target.dataset.price || 0);
      if (!Number.isFinite(positionID) || positionID < 0 || !Number.isFinite(price) || price <= 0) {
        return;
      }
      const nextDragState: PositionLevelDragState = {
        positionID,
        side,
        kind,
        price,
        originalPrice: price
      };
      draggingLevelRef.current = nextDragState;
      setDraggingLevel(nextDragState);
      event.preventDefault();
      event.stopPropagation();
    };

    const handleWindowMouseMove = (event: MouseEvent) => {
      const activeDrag = draggingLevelRef.current;
      if (!activeDrag) {
        return;
      }
      const point = resolveClientPoint(event, shell, chartMetricsRef.current);
      if (!point) {
        return;
      }
      const price = candleSeries.coordinateToPrice(point.y);
      if (!Number.isFinite(price) || price <= 0) {
        return;
      }
      const nextDragState: PositionLevelDragState = {
        ...activeDrag,
        price: Number(price)
      };
      draggingLevelRef.current = nextDragState;
      setDraggingLevel(nextDragState);
      event.preventDefault();
    };

    const handleWindowMouseUp = (event: MouseEvent) => {
      const activeDrag = draggingLevelRef.current;
      if (!activeDrag || event.button !== 0) {
        return;
      }
      draggingLevelRef.current = null;
      setDraggingLevel(null);
      if (Math.abs(activeDrag.price - activeDrag.originalPrice) > activeDrag.originalPrice * 0.000001) {
        onMovePositionLevel?.({
          positionID: activeDrag.positionID,
          side: activeDrag.side,
          kind: activeDrag.kind,
          price: activeDrag.price
        });
      }
      event.preventDefault();
    };

    shell.addEventListener("mousedown", handleMouseDown, true);
    window.addEventListener("mousemove", handleWindowMouseMove, true);
    window.addEventListener("mouseup", handleWindowMouseUp, true);
    return () => {
      shell.removeEventListener("mousedown", handleMouseDown, true);
      window.removeEventListener("mousemove", handleWindowMouseMove, true);
      window.removeEventListener("mouseup", handleWindowMouseUp, true);
    };
  }, [onMovePositionLevel]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const bridge: CandlesChartBenchmarkBridge = {
      version: 1,
      getSnapshot: () =>
        buildCandlesChartBenchmarkSnapshot(
          chartRef.current,
          candleCountRef.current,
          chartMetricsRef.current,
          viewportKeyRef.current,
          crosshairPointXRef.current,
          crosshairPointYRef.current
        ),
      resetView: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        scheduleFitAllData(chart);
      },
      zoomIn: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        zoomChartFromKeyboard(chart, chartMetricsRef.current.plotWidth, 1);
      },
      zoomOut: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        zoomChartFromKeyboard(chart, chartMetricsRef.current.plotWidth, -1);
      },
      panLeft: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        panChart(chart, -1);
      },
      panRight: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        panChart(chart, 1);
      }
    };
    window.__GOBOT_TV_BENCH__ = bridge;
    return () => {
      if (window.__GOBOT_TV_BENCH__ === bridge) {
        delete window.__GOBOT_TV_BENCH__;
      }
    };
  }, []);

  useImperativeHandle(
    ref,
    () => ({
      resetView: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        scheduleFitAllData(chart);
      },
      zoomIn: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        zoomChartFromKeyboard(chart, chartMetricsRef.current.plotWidth, 1);
      },
      zoomOut: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        zoomChartFromKeyboard(chart, chartMetricsRef.current.plotWidth, -1);
      },
      panLeft: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        panChart(chart, -1);
      },
      panRight: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        panChart(chart, 1);
      },
      getVisibleLogicalRange: () => {
        const chart = chartRef.current;
        if (!chart) {
          return null;
        }
        return readVisibleLogicalRange(chart);
      },
      focusLogicalRange: (from: number, to: number) => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        focusChartLogicalRange(chart, from, to);
      },
      focusCrosshairAtTime: (timeMS: number, price?: number | null) => {
        const chart = chartRef.current;
        const candleSeries = candleSeriesRef.current;
        const chartCandles = candlesRef.current;
        if (!chart || !candleSeries || !Array.isArray(chartCandles) || chartCandles.length === 0) {
          return;
        }
        const candleIndex = findNearestCandleIndexByTime(chartCandles, timeMS);
        if (candleIndex < 0) {
          return;
        }
        const candle = chartCandles[candleIndex];
        const resolvedPrice = Number.isFinite(price) && (price || 0) > 0 ? Number(price) : candle.close;
        chart.setCrosshairPosition(resolvedPrice, toSeriesTime(candle.ts), candleSeries);
      }
    }),
    []
  );

  const showEmpty = !loading && candles.length === 0;

  return (
    <div ref={shellRef} className="tv-chart-shell">
      <div ref={containerRef} className="tv-chart-canvas" />
      {selectionBox ? (
        <div
          className={`tv-chart-selection-box ${selection?.selectionDirection === "up" ? "is-up" : "is-down"}`}
          style={{
            left: `${selectionBox.left}px`,
            top: `${selectionBox.top}px`,
            width: `${selectionBox.width}px`,
            height: `${selectionBox.height}px`
          }}
        >
          <div className="tv-chart-selection-guide tv-chart-selection-guide-horizontal" />
          <div className="tv-chart-selection-guide tv-chart-selection-guide-vertical" />
        </div>
      ) : null}
      {draftSelectionBox ? (
        <div
          className={`tv-chart-selection-box ${draftSelectionBox.selectionDirection === "up" ? "is-up" : "is-down"} is-draft`}
          style={{
            left: `${draftSelectionBox.left}px`,
            top: `${draftSelectionBox.top}px`,
            width: `${draftSelectionBox.width}px`,
            height: `${draftSelectionBox.height}px`
          }}
        >
          <div className="tv-chart-selection-guide tv-chart-selection-guide-horizontal" />
          <div className="tv-chart-selection-guide tv-chart-selection-guide-vertical" />
        </div>
      ) : null}
      {selectionStatsCard && activeSelectionBox && activeSelection ? (
        <div
          className={`tv-chart-selection-card ${activeSelection.selectionDirection === "up" ? "is-up" : "is-down"}`}
          style={{
            left: `${activeSelectionBox.left + activeSelectionBox.width * 0.5}px`,
            top: selectionStatsCard.placeAbove
              ? `${Math.max(8, activeSelectionBox.top - 12)}px`
              : `${Math.min(chartMetricsRef.current.height - 8, activeSelectionBox.top + 14)}px`,
            transform: selectionStatsCard.placeAbove ? "translate(-50%, -100%)" : "translate(-50%, 0)"
          }}
        >
          <div className="tv-chart-selection-card-line">{selectionStatsCard.deltaText}</div>
          <div className="tv-chart-selection-card-line">{selectionStatsCard.barsText}</div>
          <div className="tv-chart-selection-card-line">{selectionStatsCard.volumeText}</div>
        </div>
      ) : null}
      {positionLevelOverlays.length > 0 ? (
        <div className="tv-position-level-overlay" aria-hidden="true">
          {positionLevelOverlays.map((level) => {
            const style: CSSProperties & Record<"--tv-position-level-axis-offset" | "--tv-position-level-line-length", string> = {
              top: `${level.y}px`,
              "--tv-position-level-axis-offset": `${level.axisOffset}px`,
              "--tv-position-level-line-length": `${level.lineLength}px`
            };
            return (
              <div
                key={level.id}
                className={`tv-position-level-item is-${level.kind} ${level.side === "short" ? "is-short" : ""} ${
                  level.draggable ? "is-draggable" : ""
                }`}
                style={style}
                data-kind={level.kind}
                data-position-id={String(level.positionID)}
                data-side={level.side}
                data-price={String(level.price)}
              >
                <div className="tv-position-level-label">{level.label}</div>
                <span className="tv-position-level-line" />
              </div>
            );
          })}
        </div>
      ) : null}
      {eventMarkers.length > 0 ? (
        <div className="tv-chart-event-layer" aria-hidden="true">
          {eventMarkers.map((marker) => (
            marker.variant === "arrow" ? (
              <div
                key={marker.id}
                className={`tv-chart-arrow ${marker.colorClass}`}
                style={
                  {
                    left: `${marker.left}px`,
                    top: `${marker.top}px`,
                    "--tv-arrow-line-length": `${marker.arrowLength ?? 34}px`
                  } as CSSProperties & Record<"--tv-arrow-line-length", string>
                }
                title={marker.title}
                onMouseEnter={(event) => {
                  setHoveredMarker(buildMarkerTooltipState(marker, event.clientX, event.clientY, shellRef.current));
                }}
                onMouseMove={(event) => {
                  setHoveredMarker(buildMarkerTooltipState(marker, event.clientX, event.clientY, shellRef.current));
                }}
                onMouseLeave={() => {
                  setHoveredMarker((current) => (current?.markerID === marker.id ? null : current));
                }}
              >
                <span className="tv-chart-arrow-label">{marker.label}</span>
                <span className="tv-chart-arrow-line" />
                <span className="tv-chart-arrow-head" />
              </div>
            ) : (
              <div
                key={marker.id}
                className={`tv-chart-event-marker ${marker.colorClass}`}
                style={{
                  left: `${marker.left}px`,
                  top: `${marker.top}px`
                }}
                title={marker.title}
                onMouseEnter={(event) => {
                  setHoveredMarker(buildMarkerTooltipState(marker, event.clientX, event.clientY, shellRef.current));
                }}
                onMouseMove={(event) => {
                  setHoveredMarker(buildMarkerTooltipState(marker, event.clientX, event.clientY, shellRef.current));
                }}
                onMouseLeave={() => {
                  setHoveredMarker((current) => (current?.markerID === marker.id ? null : current));
                }}
              >
                {marker.label}
              </div>
            )
          ))}
        </div>
      ) : null}
      {hoveredMarker ? (
        <div
          className="tv-marker-tooltip"
          style={{
            left: `${hoveredMarker.left}px`,
            top: `${hoveredMarker.top}px`
          }}
        >
          <div className="tv-marker-tooltip-title">{hoveredMarker.title}</div>
          <div className="tv-marker-tooltip-value">{priceDisplayRef.current.formatter(hoveredMarker.price)}</div>
          {hoveredMarker.rows.length > 0 ? (
            <div className="tv-marker-tooltip-rows">
              {hoveredMarker.rows.map((row) => (
                <div key={`${hoveredMarker.markerID}-${row.label}`} className="tv-marker-tooltip-row">
                  <span className="tv-marker-tooltip-key">{row.label}</span>
                  <span className="tv-marker-tooltip-data">{row.value}</span>
                </div>
              ))}
            </div>
          ) : null}
          {hoveredMarker.clampDirection ? (
            <div className="tv-marker-tooltip-note">
              {hoveredMarker.clampDirection === "top" ? "超出当前显示范围（顶部）" : "超出当前显示范围（底部）"}
            </div>
          ) : null}
        </div>
      ) : null}
      {contextMenu && selection ? (
        <div
          className="tv-chart-context-menu"
          style={{
            left: `${contextMenu.left}px`,
            top: `${contextMenu.top}px`
          }}
        >
          <button
            type="button"
            onClick={() => {
              setContextMenu(null);
              onRunBacktestArea?.(selection);
            }}
          >
            back-test
          </button>
        </div>
      ) : null}
      <div className="tv-crosshair-axis-overlay">
        <div ref={crosshairAxisLabelRef} className="tv-crosshair-axis-label" hidden>
          <span ref={crosshairAxisPriceRef}>--</span>
          <span ref={crosshairAxisPercentRef}>--</span>
        </div>
      </div>
      {indicatorLegend.length > 0 ? (
        <div className="tv-chart-legend">
          {indicatorLegend.map((item) => (
            <div key={item.id} className={`tv-chart-legend-row ${item.visible ? "" : "is-hidden"}`}>
              <span className="tv-chart-legend-text" style={{ color: item.color }}>
                {item.label} {item.value}
              </span>
              <button
                type="button"
                className="tv-chart-legend-toggle"
                onClick={() => onToggleIndicator(item.id)}
                aria-label={item.visible ? `隐藏 ${item.label}` : `显示 ${item.label}`}
                title={item.visible ? "隐藏指标" : "显示指标"}
              >
                <EyeIcon hidden={!item.visible} />
              </button>
            </div>
          ))}
        </div>
      ) : null}
      {loading ? <div className="tv-chart-overlay">加载 K 线中...</div> : null}
      {showEmpty ? <div className="tv-chart-overlay">{emptyText}</div> : null}
    </div>
  );

  function handleCrosshairMove(param: unknown): void {
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries || candleCountRef.current <= 0) {
      hideCrosshairAxisLabel();
      return;
    }
    const eventParam = param as { point?: { x?: number; y?: number } } | null;
    const point = eventParam?.point || null;
    if (!point) {
      hideCrosshairAxisLabel();
      return;
    }
    const pointX = Number(point.x);
    const pointY = Number(point.y);
    if (!Number.isFinite(pointX) || !Number.isFinite(pointY)) {
      hideCrosshairAxisLabel();
      return;
    }

    let metrics = chartMetricsRef.current;
    if (metrics.width <= 0 || metrics.height <= 0) {
      refreshCrosshairGeometry();
      metrics = chartMetricsRef.current;
    }
    if (pointX < 0 || pointX > metrics.plotWidth || pointY < 0 || pointY > metrics.height) {
      hideCrosshairAxisLabel();
      return;
    }

    const roundedPointX = Math.round(pointX);
    const roundedPointY = Math.round(pointY);
    if (
      crosshairVisibleRef.current &&
      crosshairPointXRef.current === roundedPointX &&
      crosshairPointYRef.current === roundedPointY
    ) {
      return;
    }

    const price = candleSeries.coordinateToPrice(pointY);
    const latestClose = latestClosedCloseRef.current;
    if (!Number.isFinite(price) || !Number.isFinite(latestClose) || latestClose <= 0) {
      hideCrosshairAxisLabel();
      return;
    }

    const pct = ((Number(price) - latestClose) / latestClose) * 100;
    const translateY = clampNumber(
      Math.round(pointY - CROSSHAIR_AXIS_LABEL_HEIGHT * 0.5),
      4,
      Math.max(4, metrics.height - CROSSHAIR_AXIS_LABEL_HEIGHT - 4)
    );
    const priceText = priceDisplayRef.current.formatter(Number(price));
    const percentText = formatSignedPercent(pct);
    const label = crosshairAxisLabelRef.current;
    const priceNode = crosshairAxisPriceRef.current;
    const percentNode = crosshairAxisPercentRef.current;
    if (!label || !priceNode || !percentNode) {
      return;
    }

    if (crosshairTranslateYRef.current !== translateY) {
      crosshairTranslateYRef.current = translateY;
      label.style.transform = `translate3d(0, ${translateY}px, 0)`;
    }
    if (crosshairPriceTextRef.current !== priceText) {
      crosshairPriceTextRef.current = priceText;
      priceNode.textContent = priceText;
    }
    if (crosshairPercentTextRef.current !== percentText) {
      crosshairPercentTextRef.current = percentText;
      percentNode.textContent = percentText;
      percentNode.className = "";
    }
    crosshairPointXRef.current = roundedPointX;
    crosshairPointYRef.current = roundedPointY;
    if (!crosshairVisibleRef.current) {
      crosshairVisibleRef.current = true;
      label.hidden = false;
    }
  }

  function hideCrosshairAxisLabel(): void {
    const label = crosshairAxisLabelRef.current;
    if (label && crosshairVisibleRef.current) {
      crosshairVisibleRef.current = false;
      label.hidden = true;
    }
    crosshairPointXRef.current = Number.NaN;
    crosshairPointYRef.current = Number.NaN;
    crosshairTranslateYRef.current = Number.NaN;
  }

  function refreshCrosshairGeometry(): void {
    const container = containerRef.current;
    const label = crosshairAxisLabelRef.current;
    if (!container) {
      return;
    }
    const width = container.clientWidth;
    const height = container.clientHeight;
    const axisWidth = priceDisplayRef.current.axisWidth > 0 ? priceDisplayRef.current.axisWidth : PRICE_AXIS_MIN_WIDTH;
    chartMetricsRef.current = {
      width,
      height,
      plotWidth: Math.max(0, width - axisWidth)
    };
    if (label && crosshairWidthRef.current !== axisWidth) {
      crosshairWidthRef.current = axisWidth;
      label.style.width = `${axisWidth}px`;
    }
  }
});

function zoomChartFromKeyboard(chart: IChartApi, plotWidth: number, direction: 1 | -1): void {
  const currentBarSpacing = getCurrentImpliedBarSpacing(chart, plotWidth);
  if (currentBarSpacing == null) {
    return;
  }
  const step = computeKeyboardBarSpacingDelta(currentBarSpacing);
  zoomChartWithBarSpacingDelta(chart, plotWidth, step * direction);
}

function zoomChartFromWheelDelta(chart: IChartApi, plotWidth: number, deltaY: number): void {
  const currentBarSpacing = getCurrentImpliedBarSpacing(chart, plotWidth);
  if (currentBarSpacing == null) {
    return;
  }
  const barSpacingDelta = wheelDeltaToBarSpacingDelta(deltaY, currentBarSpacing);
  zoomChartWithBarSpacingDelta(chart, plotWidth, barSpacingDelta);
}

function zoomChartWithBarSpacingDelta(chart: IChartApi, plotWidth: number, barSpacingDelta: number): void {
  if (!Number.isFinite(plotWidth) || plotWidth <= 0 || !Number.isFinite(barSpacingDelta) || barSpacingDelta === 0) {
    return;
  }
  const timeScale = chart.timeScale();
  const current = readVisibleLogicalRange(chart);
  if (!current) {
    return;
  }
  const span = Math.max(MIN_VISIBLE_LOGICAL_SPAN, current.to - current.from);
  const currentBarSpacing = visibleSpanToImpliedBarSpacing(plotWidth, span);
  const nextBarSpacing = clampNumber(
    currentBarSpacing + barSpacingDelta,
    MIN_IMPLIED_BAR_SPACING,
    MAX_IMPLIED_BAR_SPACING
  );
  if (Math.abs(nextBarSpacing - currentBarSpacing) < 0.000001) {
    return;
  }
  const nextSpan = impliedBarSpacingToVisibleSpan(plotWidth, nextBarSpacing);
  const to = current.to;
  const from = to - nextSpan;
  timeScale.setVisibleLogicalRange({
    from,
    to
  });
}

function getCurrentImpliedBarSpacing(chart: IChartApi, plotWidth: number): number | null {
  if (!Number.isFinite(plotWidth) || plotWidth <= 0) {
    return null;
  }
  const current = readVisibleLogicalRange(chart);
  if (!current) {
    return null;
  }
  const span = Math.max(MIN_VISIBLE_LOGICAL_SPAN, current.to - current.from);
  return visibleSpanToImpliedBarSpacing(plotWidth, span);
}

function buildMarkerTooltipState(
  marker: TradingViewEventMarker,
  clientX: number,
  clientY: number,
  bodyElement: HTMLDivElement | null
): MarkerTooltipState {
  const tooltipWidth = marker.tooltipRows.length >= 6 ? 304 : 276;
  const tooltipHeight = 56 + marker.tooltipRows.length * 20 + (marker.clampDirection ? 24 : 0);
  const fallback = {
    markerID: marker.id,
    price: marker.price,
    clampDirection: marker.clampDirection,
    title: marker.tooltipTitle,
    rows: marker.tooltipRows,
    left: marker.left + 14,
    top: Math.max(8, marker.top - tooltipHeight)
  };
  if (!bodyElement) {
    return fallback;
  }
  const rect = bodyElement.getBoundingClientRect();
  const localX = clientX - rect.left;
  const localY = clientY - rect.top;
  return {
    markerID: marker.id,
    price: marker.price,
    clampDirection: marker.clampDirection,
    title: marker.tooltipTitle,
    rows: marker.tooltipRows,
    left: clampNumber(localX + 14, 8, Math.max(8, rect.width - tooltipWidth - 8)),
    top: clampNumber(localY - tooltipHeight - 10, 8, Math.max(8, rect.height - tooltipHeight - 8))
  };
}

function readVisibleLogicalRange(chart: IChartApi): { from: number; to: number } | null {
  const range = chart.timeScale().getVisibleLogicalRange();
  if (!range || !Number.isFinite(range.from) || !Number.isFinite(range.to)) {
    return null;
  }
  return {
    from: range.from,
    to: range.to
  };
}

function resolveClientPoint(
  event: MouseEvent | PointerEvent,
  container: HTMLElement,
  metrics: { plotWidth: number; height: number }
): { x: number; y: number } | null {
  const rect = container.getBoundingClientRect();
  if (!Number.isFinite(rect.left) || !Number.isFinite(rect.top)) {
    return null;
  }
  const rawX = event.clientX - rect.left;
  const rawY = event.clientY - rect.top;
  if (!Number.isFinite(rawX) || !Number.isFinite(rawY)) {
    return null;
  }
  return {
    x: clampValue(rawX, 0, metrics.plotWidth),
    y: clampValue(rawY, 0, metrics.height)
  };
}

function buildSelectionFromDraft(
  chart: IChartApi,
  candleSeries: ISeriesApi<"Candlestick">,
  candles: TradingViewCandle[],
  draft: ChartSelectionDraft,
  metrics: { plotWidth: number; height: number }
): ChartRangeSelection | null {
  if (!draft || candles.length === 0) {
    return null;
  }
  const left = Math.min(draft.startX, draft.endX);
  const right = Math.max(draft.startX, draft.endX);
  const top = Math.min(draft.startY, draft.endY);
  const bottom = Math.max(draft.startY, draft.endY);
  if (right - left < 6 || bottom - top < 6 || metrics.plotWidth <= 0 || metrics.height <= 0) {
    return null;
  }
  const timeScale = chart.timeScale();
  const fromLogical = timeScale.coordinateToLogical(left);
  const toLogical = timeScale.coordinateToLogical(right);
  if (!Number.isFinite(fromLogical) || !Number.isFinite(toLogical)) {
    return null;
  }
  const startIndex = clampNumber(Math.round(Math.min(fromLogical, toLogical)), 0, candles.length - 1);
  const endIndex = clampNumber(Math.round(Math.max(fromLogical, toLogical)), 0, candles.length - 1);
  const startTS = candles[startIndex]?.ts;
  const endTS = candles[endIndex]?.ts;
  const startPrice = candleSeries.coordinateToPrice(clampValue(draft.startY, 0, metrics.height));
  const endPrice = candleSeries.coordinateToPrice(clampValue(draft.endY, 0, metrics.height));
  if (!Number.isFinite(startTS) || !Number.isFinite(endTS) || !Number.isFinite(startPrice) || !Number.isFinite(endPrice)) {
    return null;
  }
  return {
    rangeStartMS: Math.min(startTS, endTS),
    rangeEndMS: Math.max(startTS, endTS),
    priceLow: Math.min(startPrice, endPrice),
    priceHigh: Math.max(startPrice, endPrice),
    selectionDirection: endPrice >= startPrice ? "up" : "down"
  };
}

function resolveSelectionBox(
  chart: IChartApi | null,
  candleSeries: ISeriesApi<"Candlestick"> | null,
  selection: ChartRangeSelection | null
): { left: number; top: number; width: number; height: number } | null {
  if (!chart || !candleSeries || !selection) {
    return null;
  }
  const timeScale = chart.timeScale();
  const startX = timeScale.timeToCoordinate(toSeriesTime(selection.rangeStartMS));
  const endX = timeScale.timeToCoordinate(toSeriesTime(selection.rangeEndMS));
  const highY = candleSeries.priceToCoordinate(selection.priceHigh);
  const lowY = candleSeries.priceToCoordinate(selection.priceLow);
  if (!Number.isFinite(startX) || !Number.isFinite(endX) || !Number.isFinite(highY) || !Number.isFinite(lowY)) {
    return null;
  }
  return {
    left: Math.min(startX, endX),
    top: Math.min(highY, lowY),
    width: Math.max(6, Math.abs(endX - startX)),
    height: Math.max(6, Math.abs(lowY - highY))
  };
}

function resolveDraftSelectionBox(
  draft: ChartSelectionDraft | null
): ({ left: number; top: number; width: number; height: number; selectionDirection: "up" | "down" }) | null {
  if (!draft) {
    return null;
  }
  const left = Math.min(draft.startX, draft.endX);
  const right = Math.max(draft.startX, draft.endX);
  const top = Math.min(draft.startY, draft.endY);
  const bottom = Math.max(draft.startY, draft.endY);
  if (right - left < 2 || bottom - top < 2) {
    return null;
  }
  return {
    left,
    top,
    width: right - left,
    height: bottom - top,
    selectionDirection: draft.endY <= draft.startY ? "up" : "down"
  };
}

function buildSelectionStatsCard(
  selection: ChartRangeSelection | null,
  selectionBox: { left: number; top: number; width: number; height: number } | null,
  candles: TradingViewCandle[]
): SelectionStatsCard | null {
  if (!selection || !selectionBox || candles.length === 0) {
    return null;
  }
  const selectedCandles = candles.filter(
    (candle) => candle.ts >= selection.rangeStartMS && candle.ts <= selection.rangeEndMS
  );
  if (selectedCandles.length === 0) {
    return null;
  }

  const decimals = inferPriceDecimals(selectedCandles);
  const delta = Math.max(0, selection.priceHigh - selection.priceLow);
  const signedDelta = selection.selectionDirection === "down" ? -delta : delta;
  const percentBase = selection.selectionDirection === "down" ? selection.priceHigh : selection.priceLow;
  const percent = percentBase > 0 ? (signedDelta / percentBase) * 100 : 0;
  const pointStep = Math.pow(10, -decimals);
  const pointCount = pointStep > 0 ? Math.round(signedDelta / pointStep) : 0;
  const durationMS = selectedCandles.length * inferCandleIntervalMS(candles, selectedCandles);
  const volume = selectedCandles.reduce((sum, candle) => sum + (Number.isFinite(candle.volume) ? candle.volume : 0), 0);

  return {
    deltaText: `${formatSignedPriceDelta(signedDelta, decimals)} (${formatSignedPercent(percent)}) ${formatSignedInteger(pointCount)}`,
    barsText: `${selectedCandles.length} bars, ${formatSelectionDuration(durationMS)}`,
    volumeText: `Vol ${formatCompactVolume(volume)}`,
    placeAbove: selectionBox.top >= 84
  };
}

function buildPositionLevelOverlays(
  candleSeries: ISeriesApi<"Candlestick">,
  positionLevels: CandlesChartPositionLevel[],
  chartWidth: number,
  chartHeight: number,
  axisWidth: number,
  priceFormatter: (value: number) => string,
  draggingLevel: PositionLevelDragState | null
): PositionLevelOverlay[] {
  if (!Array.isArray(positionLevels) || positionLevels.length === 0 || chartWidth <= 0 || chartHeight <= 0) {
    return [];
  }
  const plotWidth = Math.max(36, chartWidth - axisWidth - 6);
  const lineLength = Math.max(28, Math.round(plotWidth * 0.1));
  const axisOffset = Math.max(2, axisWidth + 2);
  const clampTop = 8;
  const clampBottom = Math.max(clampTop, chartHeight - 8);
  const overlays: PositionLevelOverlay[] = [];

  const appendOverlay = (
    positionID: number,
    side: "long" | "short",
    kind: "entry" | "tp" | "sl",
    price?: number
  ) => {
    const actualPrice =
      draggingLevel &&
      draggingLevel.positionID === positionID &&
      draggingLevel.kind === kind &&
      kind !== "entry"
        ? draggingLevel.price
        : price;
    if (!Number.isFinite(actualPrice) || !actualPrice || actualPrice <= 0) {
      return;
    }
    const coordinate = candleSeries.priceToCoordinate(actualPrice);
    if (!Number.isFinite(coordinate)) {
      return;
    }
    overlays.push({
      id: `tv-position-level-${positionID}-${kind}`,
      positionID,
      side,
      kind,
      price: actualPrice,
      y: clampNumber(Number(coordinate), clampTop, clampBottom),
      axisOffset,
      lineLength,
      label: `${kind.toUpperCase()} ${priceFormatter(actualPrice)}`,
      draggable: kind === "tp" || kind === "sl"
    });
  };

  for (const level of positionLevels) {
    if (!level || level.positionID < 0 || !(level.entryPrice > 0)) {
      continue;
    }
    appendOverlay(level.positionID, level.side, "entry", level.entryPrice);
    appendOverlay(level.positionID, level.side, "tp", level.takeProfitPrice);
    appendOverlay(level.positionID, level.side, "sl", level.stopLossPrice);
  }
  return overlays;
}

function isPointInsideSelection(
  x: number,
  y: number,
  selectionBox: { left: number; top: number; width: number; height: number } | null
): boolean {
  if (!selectionBox) {
    return false;
  }
  return (
    x >= selectionBox.left &&
    x <= selectionBox.left + selectionBox.width &&
    y >= selectionBox.top &&
    y <= selectionBox.top + selectionBox.height
  );
}

function clampValue(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function findNearestCandleIndexByTime(candles: TradingViewCandle[], targetMS: number): number {
  if (!Array.isArray(candles) || candles.length === 0 || !Number.isFinite(targetMS)) {
    return -1;
  }
  let nearestIndex = 0;
  let nearestDistance = Math.abs(candles[0].ts - targetMS);
  for (let index = 1; index < candles.length; index += 1) {
    const distance = Math.abs(candles[index].ts - targetMS);
    if (distance < nearestDistance) {
      nearestDistance = distance;
      nearestIndex = index;
    }
  }
  return nearestIndex;
}

function normalizeWheelDelta(event: WheelEvent): number {
  switch (event.deltaMode) {
    case 1:
      return event.deltaY * 16;
    case 2:
      return event.deltaY * 120;
    default:
      return event.deltaY;
  }
}

function wheelDeltaToBarSpacingDelta(deltaY: number, currentBarSpacing: number): number {
  if (!Number.isFinite(deltaY) || deltaY === 0) {
    return 0;
  }
  if (!Number.isFinite(currentBarSpacing) || currentBarSpacing <= 0) {
    return 0;
  }
  const spacingDelta = computeWheelBarSpacingDelta(Math.abs(deltaY), currentBarSpacing);
  return deltaY > 0 ? -spacingDelta : spacingDelta;
}

function computeKeyboardBarSpacingDelta(currentBarSpacing: number): number {
  const targetRatio = interpolateZoomInTargetRatio(
    currentBarSpacing,
    KEYBOARD_ZOOM_TARGET_RATIO_MIN,
    KEYBOARD_ZOOM_TARGET_RATIO_MAX,
    KEYBOARD_ZOOM_TARGET_RATIO_SPACING_RANGE
  );
  return targetSpanRatioToBarSpacingDelta(currentBarSpacing, targetRatio, MAX_KEYBOARD_BAR_SPACING_DELTA);
}

function computeWheelBarSpacingDelta(absDeltaY: number, currentBarSpacing: number): number {
  const inputStrength = clampNumber(Math.abs(absDeltaY) / 88, 0, 1);
  const minRatio = lerp(
    WHEEL_ZOOM_TARGET_RATIO_LIGHT_MIN,
    WHEEL_ZOOM_TARGET_RATIO_STRONG_MIN,
    inputStrength
  );
  const maxRatio = lerp(
    WHEEL_ZOOM_TARGET_RATIO_LIGHT_MAX,
    WHEEL_ZOOM_TARGET_RATIO_STRONG_MAX,
    inputStrength
  );
  const targetRatio = interpolateZoomInTargetRatio(
    currentBarSpacing,
    minRatio,
    maxRatio,
    WHEEL_ZOOM_TARGET_RATIO_SPACING_RANGE
  );
  return targetSpanRatioToBarSpacingDelta(currentBarSpacing, targetRatio, MAX_WHEEL_BAR_SPACING_DELTA);
}

function interpolateZoomInTargetRatio(
  currentBarSpacing: number,
  minRatio: number,
  maxRatio: number,
  spacingRange: number
): number {
  const normalizedSpacing = clampNumber(
    (currentBarSpacing - MIN_IMPLIED_BAR_SPACING) / spacingRange,
    0,
    1
  );
  const easedSpacing = Math.pow(normalizedSpacing, 0.82);
  return lerp(minRatio, maxRatio, easedSpacing);
}

function targetSpanRatioToBarSpacingDelta(
  currentBarSpacing: number,
  targetRatio: number,
  maxDelta: number
): number {
  const safeTargetRatio = clampNumber(targetRatio, 0.5, 0.99);
  const rawDelta = currentBarSpacing * (1 / safeTargetRatio - 1);
  return clampNumber(rawDelta, MIN_COMPUTED_BAR_SPACING_DELTA, maxDelta);
}

function lerp(from: number, to: number, factor: number): number {
  return from + (to - from) * factor;
}

function visibleSpanToImpliedBarSpacing(plotWidth: number, visibleSpan: number): number {
  if (!Number.isFinite(plotWidth) || plotWidth <= 0 || !Number.isFinite(visibleSpan) || visibleSpan <= 0) {
    return MIN_IMPLIED_BAR_SPACING;
  }
  return clampNumber(plotWidth / visibleSpan, MIN_IMPLIED_BAR_SPACING, MAX_IMPLIED_BAR_SPACING);
}

function impliedBarSpacingToVisibleSpan(plotWidth: number, barSpacing: number): number {
  if (!Number.isFinite(plotWidth) || plotWidth <= 0 || !Number.isFinite(barSpacing) || barSpacing <= 0) {
    return MIN_VISIBLE_LOGICAL_SPAN;
  }
  return Math.max(MIN_VISIBLE_LOGICAL_SPAN, plotWidth / barSpacing);
}

function panChart(chart: IChartApi, bars: number): void {
  if (!Number.isFinite(bars) || bars === 0) {
    return;
  }
  const timeScale = chart.timeScale();
  const current = timeScale.getVisibleLogicalRange();
  if (!current || !Number.isFinite(current.from) || !Number.isFinite(current.to)) {
    return;
  }
  timeScale.setVisibleLogicalRange({
    from: current.from + bars,
    to: current.to + bars
  });
}

function focusChartLogicalRange(chart: IChartApi, from: number, to: number): void {
  if (!Number.isFinite(from) || !Number.isFinite(to) || to < from) {
    return;
  }
  chart.timeScale().setVisibleLogicalRange({ from, to });
}

function scheduleFitAllData(chart: IChartApi): void {
  const apply = () => {
    chart.timeScale().fitContent();
  };
  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(apply);
  });
}

function applyViewport(
  chart: IChartApi,
  candleCount: number,
  viewportStore: Map<string, ChartViewportSnapshot>,
  viewportKey: string,
  transferViewportFromKey?: string,
  transferViewportToken?: string,
  lastAppliedTransferTokenRef?: MutableRefObject<string>,
  onViewportSnapshotChange?: (key: string, snapshot: ChartViewportSnapshot) => void
): void {
  if (!viewportKey || candleCount <= 0) {
    return;
  }
  if (
    transferViewportFromKey &&
    transferViewportFromKey !== viewportKey &&
    viewportStore.has(transferViewportFromKey)
  ) {
    const source = viewportStore.get(transferViewportFromKey);
    const shouldTransfer =
      source &&
      transferViewportToken &&
      lastAppliedTransferTokenRef &&
      lastAppliedTransferTokenRef.current !== transferViewportToken;
    if (source && shouldTransfer) {
      const transferredSnapshot = {
        bars: source.bars,
        latestOffset: source.latestOffset,
        updatedAtMS: Date.now()
      };
      viewportStore.set(viewportKey, transferredSnapshot);
      onViewportSnapshotChange?.(viewportKey, transferredSnapshot);
      lastAppliedTransferTokenRef.current = transferViewportToken;
    }
  }
  const cached = viewportStore.get(viewportKey);
  if (cached && Number.isFinite(cached.bars) && Number.isFinite(cached.latestOffset)) {
    const to = candleCount - 1 - cached.latestOffset;
    const from = to - cached.bars;
    chart.timeScale().setVisibleLogicalRange({ from, to });
    return;
  }
  const bars = Math.min(Math.max(80, candleCount), 260);
  const to = candleCount - 1 + 2;
  const from = to - bars;
  chart.timeScale().setVisibleLogicalRange({ from, to });
  const defaultSnapshot = {
    bars,
    latestOffset: candleCount - 1 - to,
    updatedAtMS: Date.now()
  };
  viewportStore.set(viewportKey, defaultSnapshot);
  onViewportSnapshotChange?.(viewportKey, defaultSnapshot);
}

function resolveLatestClosedClose(candles: TradingViewCandle[]): number {
  if (candles.length === 0) {
    return Number.NaN;
  }
  const latest = candles[candles.length - 1];
  if (latest.close > 0) {
    return latest.close;
  }
  return Number.NaN;
}

function buildDynamicPriceDisplay(
  bars: Array<{
    open: number;
    high: number;
    low: number;
    close: number;
  }>
): PriceDisplay {
  if (!Array.isArray(bars) || bars.length === 0) {
    return {
      key: "empty",
      axisWidth: PRICE_AXIS_MIN_WIDTH,
      formatter: (value: number) =>
        Number.isFinite(value)
          ? value.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 2 })
          : "--"
    };
  }
  const values: number[] = [];
  let maxAbs = 0;
  let minPositiveAbs = Number.POSITIVE_INFINITY;
  bars.forEach((bar) => {
    values.push(bar.open, bar.high, bar.low, bar.close);
  });
  values.forEach((value) => {
    if (!Number.isFinite(value)) {
      return;
    }
    const abs = Math.abs(value);
    if (abs > maxAbs) {
      maxAbs = abs;
    }
    if (abs > 0 && abs < minPositiveAbs) {
      minPositiveAbs = abs;
    }
  });
  if (!Number.isFinite(maxAbs) || maxAbs <= 0) {
    return {
      key: "zero",
      axisWidth: PRICE_AXIS_MIN_WIDTH,
      formatter: (value: number) =>
        Number.isFinite(value)
          ? value.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 2 })
          : "--"
    };
  }
  const minPositive = Number.isFinite(minPositiveAbs) ? minPositiveAbs : maxAbs;
  const leadingZeros = countFractionLeadingZeros(minPositive);
  const useScientific = maxAbs < 1 && leadingZeros >= SCIENTIFIC_LEADING_ZERO_THRESHOLD;
  const plainPrecision = maxAbs >= 1000 ? 2 : maxAbs >= 1 ? 4 : clampNumber(leadingZeros + 3, 4, 10);
  const scientificDigits = clampNumber(plainPrecision - leadingZeros + 1, 2, 5);
  const formatter = useScientific
    ? (value: number) => formatScientificWithZeroInteger(value, scientificDigits)
    : (value: number) =>
        Number.isFinite(value)
          ? value.toLocaleString("en-US", {
              minimumFractionDigits: 0,
              maximumFractionDigits: plainPrecision
            })
          : "--";
  let maxLabelWidth = measurePriceLabelWidth(formatter(0));
  values.forEach((value) => {
    if (!Number.isFinite(value)) {
      return;
    }
    maxLabelWidth = Math.max(maxLabelWidth, measurePriceLabelWidth(formatter(value)));
  });
  return {
    key: `${useScientific ? "sci" : "plain"}|w:${maxLabelWidth}`,
    axisWidth: clampNumber(Math.ceil(maxLabelWidth + 14), PRICE_AXIS_MIN_WIDTH, PRICE_AXIS_MAX_WIDTH),
    formatter
  };
}

function buildCandlesChartBenchmarkSnapshot(
  chart: IChartApi | null,
  candleCount: number,
  metrics: { width: number; height: number; plotWidth: number },
  viewportKey: string,
  crosshairX: number,
  crosshairY: number
): CandlesChartBenchmarkSnapshot {
  const range = chart?.timeScale().getVisibleLogicalRange();
  const logicalFrom = range && Number.isFinite(range.from) ? range.from : null;
  const logicalTo = range && Number.isFinite(range.to) ? range.to : null;
  const latestIndex = candleCount > 0 ? candleCount - 1 : null;
  const visibleSpan =
    logicalFrom != null && logicalTo != null ? Math.max(0, logicalTo - logicalFrom) : null;
  const latestOffsetBars =
    latestIndex != null && logicalTo != null ? latestIndex - logicalTo : null;
  const rightPaddingBars =
    latestIndex != null && logicalTo != null ? logicalTo - latestIndex : null;
  return {
    ready: Boolean(chart) && candleCount > 0 && logicalFrom != null && logicalTo != null,
    timestampMS: Date.now(),
    viewportKey,
    candleCount,
    chartWidth: metrics.width,
    chartHeight: metrics.height,
    plotWidth: metrics.plotWidth,
    axisWidth: Math.max(0, metrics.width - metrics.plotWidth),
    logicalFrom,
    logicalTo,
    visibleSpan,
    latestIndex,
    latestOffsetBars,
    rightPaddingBars,
    crosshairX: Number.isFinite(crosshairX) ? crosshairX : null,
    crosshairY: Number.isFinite(crosshairY) ? crosshairY : null
  };
}

declare global {
  interface Window {
    __GOBOT_TV_BENCH__?: CandlesChartBenchmarkBridge;
  }
}

function resolvePriceAxisMeasureContext(): CanvasRenderingContext2D | null {
  if (resolvePriceAxisMeasureContext.ctx !== undefined) {
    return resolvePriceAxisMeasureContext.ctx;
  }
  if (typeof document === "undefined") {
    resolvePriceAxisMeasureContext.ctx = null;
    return resolvePriceAxisMeasureContext.ctx;
  }
  const canvas = document.createElement("canvas");
  resolvePriceAxisMeasureContext.ctx = canvas.getContext("2d");
  return resolvePriceAxisMeasureContext.ctx;
}

function measurePriceLabelWidth(text: string): number {
  const ctx = resolvePriceAxisMeasureContext();
  if (!ctx) {
    return text.length * 7;
  }
  ctx.font = PRICE_AXIS_FONT;
  return ctx.measureText(text).width;
}

function countFractionLeadingZeros(value: number): number {
  if (!Number.isFinite(value) || value <= 0 || value >= 1) {
    return 0;
  }
  const fixed = value.toFixed(18);
  const dot = fixed.indexOf(".");
  if (dot < 0) {
    return 0;
  }
  let count = 0;
  for (let i = dot + 1; i < fixed.length; i += 1) {
    if (fixed[i] !== "0") {
      break;
    }
    count += 1;
  }
  return count;
}

function inferPriceDecimals(candles: TradingViewCandle[]): number {
  let maxDecimals = 0;
  for (const candle of candles) {
    maxDecimals = Math.max(
      maxDecimals,
      countPriceDecimals(candle.open),
      countPriceDecimals(candle.high),
      countPriceDecimals(candle.low),
      countPriceDecimals(candle.close)
    );
  }
  return clampNumber(maxDecimals, 0, 8);
}

function countPriceDecimals(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  const normalized = value.toFixed(10).replace(/0+$/, "").replace(/\.$/, "");
  const dot = normalized.indexOf(".");
  if (dot < 0) {
    return 0;
  }
  return normalized.length - dot - 1;
}

function inferCandleIntervalMS(allCandles: TradingViewCandle[], selectedCandles: TradingViewCandle[]): number {
  const source = selectedCandles.length >= 2 ? selectedCandles : allCandles;
  for (let index = 1; index < source.length; index += 1) {
    const diff = source[index].ts - source[index - 1].ts;
    if (Number.isFinite(diff) && diff > 0) {
      return diff;
    }
  }
  return 60_000;
}

function formatSignedPriceDelta(value: number, decimals: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${Math.abs(value).toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals
  })}`;
}

function formatSignedInteger(value: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${Math.abs(Math.trunc(value)).toLocaleString("en-US")}`;
}

function formatSelectionDuration(durationMS: number): string {
  if (!Number.isFinite(durationMS) || durationMS <= 0) {
    return "0m";
  }
  const totalMinutes = Math.max(1, Math.round(durationMS / 60_000));
  const days = Math.floor(totalMinutes / 1_440);
  const hours = Math.floor((totalMinutes % 1_440) / 60);
  const minutes = totalMinutes % 60;
  const parts: string[] = [];
  if (days > 0) {
    parts.push(`${days}d`);
  }
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0 || parts.length === 0) {
    parts.push(`${minutes}m`);
  }
  return parts.join(" ");
}

function formatCompactVolume(value: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  const abs = Math.abs(value);
  const units = [
    { threshold: 1_000_000_000_000, suffix: "T" },
    { threshold: 1_000_000_000, suffix: "B" },
    { threshold: 1_000_000, suffix: "M" },
    { threshold: 1_000, suffix: "K" }
  ];
  for (const unit of units) {
    if (abs >= unit.threshold) {
      return `${(value / unit.threshold).toLocaleString("en-US", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2
      })}${unit.suffix}`;
    }
  }
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2
  });
}

function formatScientificWithZeroInteger(value: number, digits: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  if (value === 0) {
    return "0";
  }
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  const exponent = Math.floor(Math.log10(abs));
  const displayExponent = exponent + 1;
  const mantissa = abs / Math.pow(10, displayExponent);
  const suffix = displayExponent >= 0 ? `+${displayExponent}` : `${displayExponent}`;
  return `${sign}${mantissa.toFixed(digits)}e${suffix}`;
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function formatCrosshairTimeLabel(time: Time): string {
  const date = resolveSeriesTimeDate(time);
  if (!date) {
    return "--";
  }
  const year = date.getFullYear();
  const month = pad2(date.getMonth() + 1);
  const day = pad2(date.getDate());
  const hour = pad2(date.getHours());
  const minute = pad2(date.getMinutes());
  return `${year}/${month}/${day} ${hour}:${minute}`;
}

function formatAxisTimeLabel(time: Time): string {
  const date = resolveSeriesTimeDate(time);
  if (!date) {
    return "--";
  }
  const hour = date.getHours();
  const minute = date.getMinutes();
  if (hour === 0 && minute === 0) {
    return `${pad2(date.getMonth() + 1)}/${pad2(date.getDate())}`;
  }
  return `${pad2(hour)}:${pad2(minute)}`;
}

function resolveSeriesTimeDate(time: Time): Date | null {
  if (typeof time === "number" && Number.isFinite(time)) {
    return new Date(time * 1000);
  }
  if (typeof time === "string") {
    const parsed = Date.parse(time);
    if (Number.isFinite(parsed)) {
      return new Date(parsed);
    }
    return null;
  }
  if (time && typeof time === "object" && "year" in time && "month" in time && "day" in time) {
    const year = Number(time.year);
    const month = Number(time.month);
    const day = Number(time.day);
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
      return null;
    }
    return new Date(year, month - 1, day);
  }
  return null;
}

function pad2(value: number): string {
  if (!Number.isFinite(value)) {
    return "00";
  }
  return value < 10 ? `0${value}` : String(value);
}

function formatSignedPercent(value: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function EyeIcon(props: { hidden: boolean }): JSX.Element {
  if (props.hidden) {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M3.5 4.5 19.5 20.5"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
          fill="none"
        />
        <path
          d="M10.1 6.3a10.7 10.7 0 0 1 1.9-.2c5.1 0 8.8 4.1 9.8 5.9-.4.7-1.2 1.8-2.4 2.9m-3 1.9a8.9 8.9 0 0 1-4.4 1.2c-5 0-8.7-4.1-9.7-5.9a15.7 15.7 0 0 1 3.6-3.9"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
          strokeLinejoin="round"
          fill="none"
        />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M2.2 12c1-1.8 4.7-5.9 9.8-5.9s8.8 4.1 9.8 5.9c-1 1.8-4.7 5.9-9.8 5.9S3.2 13.8 2.2 12Z"
        stroke="currentColor"
        strokeWidth="1.7"
        strokeLinejoin="round"
        fill="none"
      />
      <circle cx="12" cy="12" r="2.7" fill="currentColor" />
    </svg>
  );
}
