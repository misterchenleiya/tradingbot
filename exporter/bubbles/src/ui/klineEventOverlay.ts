import type { IChartApi, ISeriesApi, Time } from "lightweight-charts"
import type { BubbleCandleBar, BubbleCandleEvent, BubbleCandlePosition } from "../app/types"

export type BubbleMarkerKind = "ENTRY" | "EXIT" | "TP" | "SL"

type RawOverlayMarker = {
  id: string
  kind: BubbleMarkerKind
  x: number
  rawY: number
  candleTS: number
  eventAtMS: number
  price: number
}

export type BubbleOverlayMarker = {
  id: string
  kind: BubbleMarkerKind
  x: number
  y: number
  size: number
  arrowLength: number
  price: number
  clampDirection: "top" | "bottom" | null
  clampGuideTop: number
  clampGuideHeight: number
}

export type BubbleMarkerTooltipState = {
  markerID: string
  kind: BubbleMarkerKind
  price: number
  clampDirection: "top" | "bottom" | null
  left: number
  top: number
}

const MAX_TPSL_MARKERS_PER_CANDLE = 24
const MAX_TOTAL_MARKERS = 1800
const MAX_MARKER_SCAN_EVENTS = 6000

function normalizePositionSide(side?: string): "long" | "short" {
  const normalized = (side || "").trim().toLowerCase()
  if (normalized === "short" || normalized === "sell") return "short"
  return "long"
}

function normalizeEventToken(value: string): string {
  return value.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_")
}

function readFirstNumber(detail: Record<string, unknown> | undefined, keys: string[]): number | null {
  if (!detail) return null
  const lowered = new Map<string, unknown>()
  for (const [key, value] of Object.entries(detail)) {
    lowered.set(key.trim().toLowerCase(), value)
  }
  for (const key of keys) {
    if (!lowered.has(key)) continue
    const value = lowered.get(key)
    if (typeof value === "number" && Number.isFinite(value)) return value
    if (typeof value === "string") {
      const parsed = Number(value)
      if (Number.isFinite(parsed)) return parsed
    }
  }
  return null
}

function readFirstString(detail: Record<string, unknown> | undefined, keys: string[]): string {
  if (!detail) return ""
  const lowered = new Map<string, unknown>()
  for (const [key, value] of Object.entries(detail)) {
    lowered.set(key.trim().toLowerCase(), value)
  }
  for (const key of keys) {
    if (!lowered.has(key)) continue
    const value = lowered.get(key)
    if (typeof value === "string") {
      const normalized = value.trim()
      if (normalized) return normalized
    }
  }
  return ""
}

function addKindsByTypeToken(target: Set<BubbleMarkerKind>, token: string): void {
  if (!token) return
  switch (token) {
    case "TRAILING_TP":
    case "TP":
    case "TAKE_PROFIT":
      target.add("TP")
      return
    case "TRAILING_STOP":
    case "TRAILING_SL":
    case "SL":
    case "STOP_LOSS":
      target.add("SL")
      return
    case "TRAILING_TP_SL":
    case "TP_SL":
    case "TAKE_PROFIT_STOP_LOSS":
      target.add("TP")
      target.add("SL")
      return
    default:
  }
}

function addKindsByMarkerToken(target: Set<BubbleMarkerKind>, markerToken: string): void {
  if (!markerToken) return
  if (markerToken === "TP" || markerToken.startsWith("TP_") || markerToken.includes("TAKE_PROFIT")) {
    target.add("TP")
  }
  if (markerToken === "SL" || markerToken.startsWith("SL_") || markerToken.includes("STOP_LOSS")) {
    target.add("SL")
  }
}

function classifyEventKinds(event: BubbleCandleEvent): BubbleMarkerKind[] {
  const typeToken = normalizeEventToken(event.type || "")
  if (typeToken === "ENTRY") return ["ENTRY"]
  if (typeToken === "EXIT") return ["EXIT"]

  const kinds = new Set<BubbleMarkerKind>()
  addKindsByTypeToken(kinds, typeToken)
  addKindsByMarkerToken(kinds, normalizeEventToken(readFirstString(event.detail, ["marker"])))
  addKindsByTypeToken(
    kinds,
    normalizeEventToken(readFirstString(event.detail, ["event_type", "order_type", "action"]))
  )
  if (kinds.size === 0) return []
  return Array.from(kinds)
}

function shouldRenderEventOnTimeframe(event: BubbleCandleEvent, activeTimeframe: string, kinds: BubbleMarkerKind[]): boolean {
  if (kinds.includes("TP") || kinds.includes("SL")) {
    // TP/SL 与 visual-history 一致：跨周期显示。
    return true
  }
  if (!activeTimeframe) return true
  const timeframe = readFirstString(event.detail, ["timeframe"]).trim().toLowerCase()
  if (!timeframe) return true
  return timeframe === activeTimeframe
}

function readMarkerPrice(detail: Record<string, unknown> | undefined, kind: "TP" | "SL"): number | null {
  const directKeys =
    kind === "TP" ? ["tp_price", "take_profit_price", "tp"] : ["sl_price", "stop_loss_price", "sl"]
  return readFirstNumber(detail, directKeys)
}

export function resolveBubbleLatestMarkerPrice(events: BubbleCandleEvent[], kind: "TP" | "SL"): number | null {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const event = events[i]
    const kinds = classifyEventKinds(event)
    if (!kinds.includes(kind)) continue
    const value = readMarkerPrice(event.detail, kind)
    if (value != null && value > 0) return value
  }
  return null
}

function resolveMarkerPrice(
  kind: BubbleMarkerKind,
  event: BubbleCandleEvent,
  candle: BubbleCandleBar,
  position: BubbleCandlePosition | undefined,
  side: "long" | "short"
): number {
  if (kind === "TP") {
    return readMarkerPrice(event.detail, "TP") ?? (side === "long" ? candle.high : candle.low)
  }
  if (kind === "SL") {
    return readMarkerPrice(event.detail, "SL") ?? (side === "long" ? candle.low : candle.high)
  }
  if (kind === "ENTRY") {
    return (
      readFirstNumber(event.detail, ["entry_price", "price", "fill_price", "avg_px"]) ??
      (typeof position?.entryPrice === "number" && Number.isFinite(position.entryPrice) ? position.entryPrice : null) ??
      candle.close
    )
  }
  return (
    readFirstNumber(event.detail, ["exit_price", "price", "fill_price", "close_avg_px"]) ??
    (typeof position?.exitPrice === "number" && Number.isFinite(position.exitPrice) ? position.exitPrice : null) ??
    candle.close
  )
}

function clampValue(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

function estimateArrowLineLength(kind: BubbleMarkerKind): number {
  const text = kind === "ENTRY" ? "ENTRY" : kind === "EXIT" ? "EXIT" : kind
  return Math.max(26, Math.round(text.length * 7 + 6))
}

function tpSlAnchor(kind: BubbleMarkerKind, side: "long" | "short"): "above" | "below" {
  if (kind === "TP") {
    return side === "long" ? "above" : "below"
  }
  return side === "long" ? "below" : "above"
}

function buildClampGuide(
  markerY: number,
  markerSize: number,
  clampDirection: "top" | "bottom" | null
): { guideTop: number; guideHeight: number } {
  if (!clampDirection) return { guideTop: 0, guideHeight: 0 }
  const guideLength = 26
  const radius = markerSize * 0.5
  if (clampDirection === "top") {
    return { guideTop: markerY + radius, guideHeight: guideLength }
  }
  return { guideTop: markerY - radius - guideLength, guideHeight: guideLength }
}

function computeCandleWidth(chart: IChartApi, candles: BubbleCandleBar[]): number {
  if (candles.length <= 1) return 12
  const timeScale = chart.timeScale()
  let minDiff = Number.POSITIVE_INFINITY
  for (let i = 1; i < candles.length; i += 1) {
    const prev = timeScale.timeToCoordinate(Math.floor(candles[i - 1].ts / 1000) as Time)
    const next = timeScale.timeToCoordinate(Math.floor(candles[i].ts / 1000) as Time)
    if (!Number.isFinite(prev) || !Number.isFinite(next)) continue
    const diff = Math.abs(Number(next) - Number(prev))
    if (diff > 0.1 && diff < minDiff) minDiff = diff
  }
  if (!Number.isFinite(minDiff)) return 12
  return minDiff * 0.78
}

function findNearestCandle(ts: number, candles: BubbleCandleBar[]): BubbleCandleBar | null {
  if (!candles.length) return null
  let left = 0
  let right = candles.length - 1
  while (left <= right) {
    const mid = Math.floor((left + right) / 2)
    const value = candles[mid].ts
    if (value === ts) return candles[mid]
    if (value < ts) left = mid + 1
    else right = mid - 1
  }
  const prev = right >= 0 ? candles[right] : undefined
  const next = left < candles.length ? candles[left] : undefined
  if (!prev) return next || null
  if (!next) return prev
  return Math.abs(prev.ts - ts) <= Math.abs(next.ts - ts) ? prev : next
}

function layoutTpSlMarkers(
  group: RawOverlayMarker[],
  side: "long" | "short",
  markerSize: number,
  clampTop: number,
  clampBottom: number
): BubbleOverlayMarker[] {
  if (group.length === 0) return []
  const minGap = markerSize + 3
  const anchor = tpSlAnchor(group[0].kind, side)
  const sorted = [...group]
    .sort((a, b) => {
      if (a.eventAtMS !== b.eventAtMS) return b.eventAtMS - a.eventAtMS
      return b.id.localeCompare(a.id)
    })
    .slice(0, MAX_TPSL_MARKERS_PER_CANDLE)
  const placed: BubbleOverlayMarker[] = []
  let previousY: number | null = null

  for (const item of sorted) {
    let y = clampValue(item.rawY, clampTop, clampBottom)
    if (previousY != null) {
      if (anchor === "above") y = Math.max(y, previousY + minGap)
      else y = Math.min(y, previousY - minGap)
    }
    y = clampValue(y, clampTop, clampBottom)
    previousY = y
    const clampDirection = item.rawY < clampTop ? "top" : item.rawY > clampBottom ? "bottom" : null
    const { guideTop, guideHeight } = buildClampGuide(y, markerSize, clampDirection)
    placed.push({
      id: item.id,
      kind: item.kind,
      x: item.x,
      y,
      size: markerSize,
      arrowLength: estimateArrowLineLength(item.kind),
      price: item.price,
      clampDirection,
      clampGuideTop: guideTop,
      clampGuideHeight: guideHeight
    })
  }
  return placed
}

export function buildBubbleOverlayMarkers(
  chart: IChartApi,
  candleSeries: ISeriesApi<"Candlestick">,
  candles: BubbleCandleBar[],
  events: BubbleCandleEvent[],
  position: BubbleCandlePosition | undefined,
  activeTimeframe: string,
  chartHeight: number
): BubbleOverlayMarker[] {
  const timeScale = chart.timeScale()
  const markerByCandleKind = new Map<string, RawOverlayMarker>()
  const eventPool = events.length > MAX_MARKER_SCAN_EVENTS ? events.slice(events.length - MAX_MARKER_SCAN_EVENTS) : events
  const sortedEvents = [...eventPool].sort((a, b) => a.eventAtMs - b.eventAtMs)
  const side = normalizePositionSide(position?.positionSide)
  const normalizedActiveTimeframe = activeTimeframe.trim().toLowerCase()

  for (const event of sortedEvents) {
    const kinds = classifyEventKinds(event)
    if (kinds.length === 0) continue
    if (!shouldRenderEventOnTimeframe(event, normalizedActiveTimeframe, kinds)) continue
    const nearest = findNearestCandle(event.eventAtMs, candles)
    if (!nearest) continue
    const eventTime = Math.floor(nearest.ts / 1000) as Time
    const x = timeScale.timeToCoordinate(eventTime)
    if (!Number.isFinite(x)) continue

    for (const kind of kinds) {
      if (position?.isOpen && kind === "EXIT") continue
      const markerPrice = resolveMarkerPrice(kind, event, nearest, position, side)
      const rawY = candleSeries.priceToCoordinate(markerPrice)
      if (!Number.isFinite(rawY)) continue
      const markerKey = `${kind}|${nearest.ts}`
      const candidate: RawOverlayMarker = {
        id: `${event.id}-${kind}`,
        kind,
        x: Number(x),
        rawY: Number(rawY),
        candleTS: nearest.ts,
        eventAtMS: event.eventAtMs,
        price: markerPrice
      }
      const prev = markerByCandleKind.get(markerKey)
      if (prev && prev.eventAtMS > candidate.eventAtMS) continue
      if (!prev && markerByCandleKind.size >= MAX_TOTAL_MARKERS) break
      markerByCandleKind.set(markerKey, candidate)
    }
    if (markerByCandleKind.size >= MAX_TOTAL_MARKERS) break
  }

  const rawMarkers = Array.from(markerByCandleKind.values()).sort((a, b) => {
    if (a.eventAtMS !== b.eventAtMS) return a.eventAtMS - b.eventAtMS
    return a.id.localeCompare(b.id)
  })
  if (rawMarkers.length === 0 || chartHeight <= 0) return []

  const candleWidth = computeCandleWidth(chart, candles)
  const circleSize = Math.max(10, Math.min(32, candleWidth))
  const candleHalfWidth = Math.max(3, candleWidth * 0.5)
  const markerRadius = circleSize * 0.5
  const clampTop = markerRadius + 4
  const clampBottom = Math.max(clampTop, chartHeight - markerRadius - 4)
  const out: BubbleOverlayMarker[] = []

  const groupMap = new Map<string, RawOverlayMarker[]>()
  for (const item of rawMarkers) {
    if (item.kind === "TP" || item.kind === "SL") {
      const key = `${item.kind}-${item.candleTS}`
      const group = groupMap.get(key) || []
      group.push(item)
      groupMap.set(key, group)
      continue
    }
    out.push({
      id: item.id,
      kind: item.kind,
      x: item.kind === "ENTRY" ? item.x - candleHalfWidth : item.x + candleHalfWidth,
      y: clampValue(item.rawY, clampTop, clampBottom),
      size: circleSize,
      arrowLength: estimateArrowLineLength(item.kind),
      price: item.price,
      clampDirection: null,
      clampGuideTop: 0,
      clampGuideHeight: 0
    })
  }
  for (const group of groupMap.values()) {
    out.push(...layoutTpSlMarkers(group, side, circleSize, clampTop, clampBottom))
  }
  return out
}

export function buildBubbleMarkerTooltipState(
  marker: BubbleOverlayMarker,
  clientX: number,
  clientY: number,
  bodyElement: HTMLDivElement | null
): BubbleMarkerTooltipState {
  const fallback = {
    markerID: marker.id,
    kind: marker.kind,
    price: marker.price,
    clampDirection: marker.clampDirection,
    left: marker.x + 14,
    top: Math.max(8, marker.y - 56)
  }
  if (!bodyElement) return fallback
  const rect = bodyElement.getBoundingClientRect()
  const tooltipWidth = 220
  const tooltipHeight = marker.clampDirection ? 78 : 58
  const localX = clientX - rect.left
  const localY = clientY - rect.top
  return {
    markerID: marker.id,
    kind: marker.kind,
    price: marker.price,
    clampDirection: marker.clampDirection,
    left: clampValue(localX + 14, 8, Math.max(8, rect.width - tooltipWidth - 8)),
    top: clampValue(localY - tooltipHeight - 10, 8, Math.max(8, rect.height - tooltipHeight - 8))
  }
}

export function formatBubbleMarkerPrice(price: number): string {
  const abs = Math.abs(price)
  let digits = 6
  if (abs >= 1000) digits = 2
  else if (abs >= 1) digits = 4
  const fixed = price.toFixed(digits)
  return fixed.replace(/(\.\d*?[1-9])0+$/u, "$1").replace(/\.0+$/u, "")
}
