import type { AccountSnapshot, BubbleDatum, PositionSnapshot } from "./types";
import {
  normalizeAccount,
  normalizePosition,
  normalizeSnapshot
} from "../datasource/adapters/normalize";

const CACHE_VERSION = 1;
const CACHE_KEY = `bubbles.page.cache.v${CACHE_VERSION}`;
const MAX_SIGNAL_ROWS = 3000;
const MAX_POSITION_ROWS = 500;
const MAX_HISTORY_ROWS = 1500;

type UnknownRecord = Record<string, unknown>;

export type CachedPageData = {
  savedAt: number;
  signals: BubbleDatum[];
  accountSnapshot?: AccountSnapshot;
  positionSnapshot?: PositionSnapshot;
  historySnapshot?: PositionSnapshot;
};

type PersistedPayloadV1 = {
  version: number;
  savedAt: number;
  signals: BubbleDatum[];
  accountSnapshot?: AccountSnapshot;
  positionSnapshot?: PositionSnapshot;
  historySnapshot?: PositionSnapshot;
};

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function trimPositionSnapshot(
  snapshot: PositionSnapshot | undefined,
  maxRows: number
): PositionSnapshot | undefined {
  if (!snapshot) return undefined;
  const positions = snapshot.positions.slice(0, maxRows);
  const count = Number.isFinite(snapshot.count)
    ? Math.max(0, Math.trunc(snapshot.count))
    : positions.length;
  const fetchedAt = Number.isFinite(snapshot.fetchedAt)
    ? Math.trunc(snapshot.fetchedAt)
    : Date.now();
  return {
    ...snapshot,
    count,
    positions,
    fetchedAt
  };
}

export function readCachedPageData(): CachedPageData | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(CACHE_KEY);
    if (!raw) return null;

    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) return null;
    if (parsed.version !== CACHE_VERSION) return null;

    const signals = normalizeSnapshot(parsed.signals).slice(0, MAX_SIGNAL_ROWS);
    const accountSnapshot = normalizeAccount(parsed.accountSnapshot) || undefined;
    const positionSnapshot = trimPositionSnapshot(
      normalizePosition(parsed.positionSnapshot) || undefined,
      MAX_POSITION_ROWS
    );
    const historySnapshot = trimPositionSnapshot(
      normalizePosition(parsed.historySnapshot) || undefined,
      MAX_HISTORY_ROWS
    );

    if (
      signals.length === 0 &&
      !accountSnapshot &&
      !positionSnapshot &&
      !historySnapshot
    ) {
      return null;
    }

    const savedAt = Number.isFinite(parsed.savedAt)
      ? Math.trunc(Number(parsed.savedAt))
      : Date.now();

    return {
      savedAt,
      signals,
      accountSnapshot,
      positionSnapshot,
      historySnapshot
    };
  } catch (error) {
    console.warn("[cache] read page cache failed", error);
    return null;
  }
}

export function writeCachedPageData(payload: Omit<CachedPageData, "savedAt">): void {
  if (typeof window === "undefined") return;
  try {
    const persisted: PersistedPayloadV1 = {
      version: CACHE_VERSION,
      savedAt: Date.now(),
      signals: payload.signals.slice(0, MAX_SIGNAL_ROWS),
      accountSnapshot: payload.accountSnapshot,
      positionSnapshot: trimPositionSnapshot(payload.positionSnapshot, MAX_POSITION_ROWS),
      historySnapshot: trimPositionSnapshot(payload.historySnapshot, MAX_HISTORY_ROWS)
    };
    window.localStorage.setItem(CACHE_KEY, JSON.stringify(persisted));
  } catch (error) {
    console.warn("[cache] write page cache failed", error);
  }
}
