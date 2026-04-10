import type { Candle } from "../types";

export interface ParsedIndicator {
  id: string;
  kind: "EMA" | "SMA" | "OTHER";
  label: string;
  period?: number;
}

export function calculateEMA(values: number[], period: number): Array<number | null> {
  if (!Array.isArray(values) || values.length === 0 || period <= 0) {
    return [];
  }
  const result: Array<number | null> = new Array(values.length).fill(null);
  const multiplier = 2 / (period + 1);

  let seed = 0;
  for (let i = 0; i < values.length; i += 1) {
    seed += values[i];
    if (i === period - 1) {
      const sma = seed / period;
      result[i] = sma;
      let prev = sma;
      for (let j = i + 1; j < values.length; j += 1) {
        prev = (values[j] - prev) * multiplier + prev;
        result[j] = prev;
      }
      break;
    }
  }
  return result;
}

export function calculateSMA(values: number[], period: number): Array<number | null> {
  if (!Array.isArray(values) || values.length === 0 || period <= 0) {
    return [];
  }
  const result: Array<number | null> = new Array(values.length).fill(null);
  let sum = 0;
  for (let i = 0; i < values.length; i += 1) {
    sum += values[i];
    if (i >= period) {
      sum -= values[i - period];
    }
    if (i >= period - 1) {
      result[i] = sum / period;
    }
  }
  return result;
}

export function buildEMAFromCandles(candles: Candle[], period: number): Array<{ time: number; value: number }> {
  const closes = candles.map((item) => item.close);
  const ema = calculateEMA(closes, period);
  const out: Array<{ time: number; value: number }> = [];
  for (let i = 0; i < ema.length; i += 1) {
    const value = ema[i];
    if (value == null || !Number.isFinite(value)) {
      continue;
    }
    out.push({
      time: Math.floor(candles[i].ts / 1000),
      value
    });
  }
  return out;
}

export function buildSMAFromCandles(candles: Candle[], period: number): Array<{ time: number; value: number }> {
  const closes = candles.map((item) => item.close);
  const sma = calculateSMA(closes, period);
  const out: Array<{ time: number; value: number }> = [];
  for (let i = 0; i < sma.length; i += 1) {
    const value = sma[i];
    if (value == null || !Number.isFinite(value)) {
      continue;
    }
    out.push({
      time: Math.floor(candles[i].ts / 1000),
      value
    });
  }
  return out;
}

export function pickEMASettings(indicatorMap: Record<string, string[]>, timeframe: string): number[] {
  const defaults = [5, 20, 60];
  const source = indicatorMap[timeframe] || [];
  const values = new Set<number>();

  for (const name of source) {
    const match = name.match(/(\d{1,5})/);
    if (!match) {
      continue;
    }
    const period = Number(match[1]);
    if (!Number.isFinite(period) || period <= 0) {
      continue;
    }
    values.add(period);
  }

  if (values.size === 0) {
    return defaults;
  }
  return Array.from(values).sort((a, b) => a - b);
}

export function parseIndicatorsForTimeframe(
  indicatorMap: Record<string, string[]>,
  timeframe: string
): ParsedIndicator[] {
  const normalizedTF = timeframe.trim().toLowerCase();
  if (!normalizedTF) {
    return [];
  }
  const source = findIndicatorItems(indicatorMap, normalizedTF);
  const out: ParsedIndicator[] = [];
  const dedupe = new Set<string>();

  for (const raw of source) {
    const parsed = parseIndicatorToken(raw);
    if (!parsed) {
      continue;
    }
    if (dedupe.has(parsed.id)) {
      continue;
    }
    dedupe.add(parsed.id);
    out.push(parsed);
  }

  if (out.length === 0) {
    const fallback = parseGlobalIndicatorMap(indicatorMap);
    for (const item of fallback) {
      if (dedupe.has(item.id)) {
        continue;
      }
      dedupe.add(item.id);
      out.push(item);
    }
  }

  return out.sort(indicatorSort);
}

function findIndicatorItems(indicatorMap: Record<string, string[]>, normalizedTF: string): string[] {
  const direct = indicatorMap[normalizedTF];
  if (Array.isArray(direct)) {
    return direct;
  }
  for (const [key, values] of Object.entries(indicatorMap)) {
    if (key.trim().toLowerCase() === normalizedTF && Array.isArray(values)) {
      return values;
    }
  }
  return [];
}

function parseIndicatorToken(raw: string): ParsedIndicator | null {
  const token = raw.trim();
  if (!token) {
    return null;
  }
  const upper = token.toUpperCase();

  const emaMatch = upper.match(/EMA\D*(\d{1,5})/);
  if (emaMatch) {
    const period = Number(emaMatch[1]);
    if (Number.isFinite(period) && period > 0) {
      return { id: `EMA:${period}`, kind: "EMA", period, label: `EMA ${period}` };
    }
  }

  const smaMatch = upper.match(/SMA\D*(\d{1,5})/);
  if (smaMatch) {
    const period = Number(smaMatch[1]);
    if (Number.isFinite(period) && period > 0) {
      return { id: `SMA:${period}`, kind: "SMA", period, label: `SMA ${period}` };
    }
  }

  if (upper.includes("EMA")) {
    return { id: `EMA:${upper}`, kind: "EMA", label: upper.replace(/\s+/g, " ") };
  }
  if (upper.includes("SMA")) {
    return { id: `SMA:${upper}`, kind: "SMA", label: upper.replace(/\s+/g, " ") };
  }
  return { id: `OTHER:${upper}`, kind: "OTHER", label: upper.replace(/\s+/g, " ") };
}

function parseGlobalIndicatorMap(indicatorMap: Record<string, string[]>): ParsedIndicator[] {
  const out: ParsedIndicator[] = [];
  for (const [rawKey, values] of Object.entries(indicatorMap)) {
    const key = rawKey.trim();
    if (!key) {
      continue;
    }
    // timeframe-key map (e.g. "15m": ["EMA5","EMA20"]) handled by findIndicatorItems
    if (/^\d+[mhdw]$/i.test(key)) {
      continue;
    }
    if (!Array.isArray(values) || values.length === 0) {
      continue;
    }
    const upperKey = key.toUpperCase();
    for (const value of values) {
      const period = parseNumericPeriod(value);
      if (period != null) {
        out.push(parseIndicatorToken(`${upperKey} ${period}`) || { id: `${upperKey}:${period}`, kind: "OTHER", label: `${upperKey} ${period}`, period });
        continue;
      }
      const parsed = parseIndicatorToken(`${upperKey} ${String(value).trim()}`);
      if (parsed) {
        out.push(parsed);
      }
    }
  }
  return out;
}

function parseNumericPeriod(raw: string): number | null {
  const value = Number(String(raw).trim());
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }
  return Math.floor(value);
}

function indicatorSort(a: ParsedIndicator, b: ParsedIndicator): number {
  const weight = (item: ParsedIndicator): number => {
    switch (item.kind) {
      case "EMA":
        return 0;
      case "SMA":
        return 1;
      default:
        return 2;
    }
  };
  const left = weight(a);
  const right = weight(b);
  if (left !== right) {
    return left - right;
  }
  const leftPeriod = a.period ?? Number.MAX_SAFE_INTEGER;
  const rightPeriod = b.period ?? Number.MAX_SAFE_INTEGER;
  if (leftPeriod !== rightPeriod) {
    return leftPeriod - rightPeriod;
  }
  return a.label.localeCompare(b.label);
}
