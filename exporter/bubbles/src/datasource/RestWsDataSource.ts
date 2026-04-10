import type { IDataSource, DataSourceHandlers } from "./IDataSource";
import type {
  AccountSnapshot,
  BackendStatus,
  BubbleCandlesFetchRequest,
  BubbleCandlesSnapshot,
  BubbleDatum,
  PositionSnapshot,
  TrendGroupsSnapshot
} from "../app/types";
import {
  normalizeAccount,
  normalizeBackendStatus,
  normalizeCandlesSnapshot,
  normalizeGroups,
  normalizePosition,
  normalizePong,
  normalizeRemovedIds,
  normalizeSnapshot,
  normalizeUpdates
} from "./adapters/normalize";

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOwn(source: UnknownRecord, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(source, key);
}

const PING_INTERVAL_MS = 1000;
const STATUS_REFRESH_INTERVAL_MS = 5000;
const INDICATOR_REFRESH_INTERVAL_ACTIVE_MS = 1000;
const INDICATOR_REFRESH_INTERVAL_IDLE_MS = 3000;
const PONG_TIMEOUT_CYCLES = 5;
const AUTO_RECONNECT_TIMEOUT_CYCLES = 15;
const MAX_PENDING_PINGS = 64;
const CANDLES_FETCH_TIMEOUT_MS = 15000;
const LIGHT_SNAPSHOT_STREAMS = ["signals", "groups"] as const;
const INDICATOR_STREAMS = ["account", "position"] as const;
type WsHistoryRange = "today" | "24h" | "7d";

type PendingCandlesFetch = {
  resolve: (snapshot: BubbleCandlesSnapshot | undefined) => void;
  reject: (reason?: string) => void;
  timer: number;
};

export class RestWsDataSource implements IDataSource {
  private ws: WebSocket | null = null;
  private staleCheckTimer: number | null = null;
  private reconnectTimer: number | null = null;
  private pingTimer: number | null = null;
  private forcedReconnectInFlight = false;
  private stopped = false;
  private reconnectAttempts = 0;
  private pingSequence = 0;
  private fetchSequence = 0;
  private lastPongAt: number | null = null;
  private lastKnownRttMs: number | undefined = undefined;
  private lastDataEventAt: number | null = null;
  private lastIndicatorRefreshAt: number | null = null;
  private lastStatusRefreshAt: number | null = null;
  private restFallbackInFlight = false;
  private pendingPings = new Map<string, number>();
  private snapshotMap = new Map<string, BubbleDatum>();
  private lastSnapshotSignature: string | null = null;
  private lastAccountSignature: string | null = null;
  private lastPositionSignature: string | null = null;
  private lastHistorySignature: string | null = null;
  private lastGroupsSignature: string | null = null;
  private lastBackendStatusSignature: string | null = null;
  private lastKnownPositionCount = 0;
  private historyRange: WsHistoryRange = "today";
  private pendingCandlesFetches = new Map<string, PendingCandlesFetch>();

  constructor(
    private readonly restUrl: string,
    private readonly wsUrl: string,
    private readonly statusUrl: string,
    private readonly accountUrl: string,
    private readonly positionUrl: string,
    private readonly historyUrl: string,
    private readonly groupsUrl: string,
    private readonly dataCheckIntervalMs: number,
    private readonly handlers: DataSourceHandlers
  ) {}

  start(): void {
    this.stopped = false;
    this.historyRange = "today";
    this.lastSnapshotSignature = null;
    this.lastAccountSignature = null;
    this.lastPositionSignature = null;
    this.lastHistorySignature = null;
    this.lastGroupsSignature = null;
    this.lastBackendStatusSignature = null;
    this.lastStatusRefreshAt = null;
    this.lastKnownPositionCount = 0;
    this.startDataCheck();
    this.connectWs();
  }

  stop(): void {
    this.stopped = true;
    this.forcedReconnectInFlight = false;
    this.stopHeartbeat();
    this.rejectAllPendingCandlesFetches("WS stopped");
    this.cleanupWs();
    if (this.staleCheckTimer !== null) {
      window.clearInterval(this.staleCheckTimer);
      this.staleCheckTimer = null;
    }
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private async fetchSignalsViaRest(): Promise<boolean> {
    if (!this.restUrl) {
      this.handlers.onStatus?.({ restStatus: "idle" });
      return false;
    }
    try {
      this.handlers.onStatus?.({ restStatus: "loading" });
      const response = await fetch(this.restUrl);
      if (!response.ok) {
        throw new Error(`REST ${response.status}`);
      }
      const payload = await response.json();
      const data = normalizeSnapshot(payload);
      const now = Date.now();
      if (this.replaceSnapshot(data)) {
        this.lastDataEventAt = now;
        this.handlers.onStatus?.({
          restStatus: "ok",
          lastSnapshotAt: now,
          lastUpdateAt: now,
          errorMessage: undefined
        });
      }
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : "REST error";
      this.handlers.onStatus?.({ restStatus: "error", errorMessage: message });
      return false;
    }
  }

  private async fetchStatusViaRest(): Promise<void> {
    if (!this.statusUrl) return;
    try {
      const response = await fetch(this.statusUrl);
      if (!response.ok) {
        throw new Error(`STATUS ${response.status}`);
      }
      const payload = await response.json();
      const status = normalizeBackendStatus(payload);
      if (status) {
        this.emitBackendStatus(status, Date.now());
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "status error";
      this.handlers.onStatus?.({ errorMessage: message });
    }
  }

  private async fetchAccountViaRest(): Promise<boolean> {
    if (!this.accountUrl) {
      this.handlers.onAccount?.(undefined);
      this.handlers.onStatus?.({ accountStatus: "idle" });
      return false;
    }
    try {
      this.handlers.onStatus?.({ accountStatus: "loading" });
      const response = await fetch(this.accountUrl);
      if (!response.ok) {
        throw new Error(`ACCOUNT ${response.status}`);
      }
      const payload = await response.json();
      const account = normalizeAccount(payload);
      if (!account) {
        throw new Error("ACCOUNT invalid payload");
      }
      const now = Date.now();
      this.emitAccountSnapshot(account, now);
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : "account error";
      this.handlers.onAccount?.(undefined);
      this.handlers.onStatus?.({
        accountStatus: "idle",
        errorMessage: message
      });
      return false;
    }
  }

  private async fetchPositionViaRest(): Promise<boolean> {
    if (!this.positionUrl) {
      this.handlers.onPosition?.(undefined);
      this.handlers.onStatus?.({ positionStatus: "idle" });
      return false;
    }
    try {
      this.handlers.onStatus?.({ positionStatus: "loading" });
      const response = await fetch(this.positionUrl);
      if (!response.ok) {
        throw new Error(`POSITION ${response.status}`);
      }
      const payload = await response.json();
      const position = normalizePosition(payload);
      if (!position) {
        throw new Error("POSITION invalid payload");
      }
      const now = Date.now();
      this.emitPositionSnapshot(position, now);
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : "position error";
      this.handlers.onPosition?.(undefined);
      this.handlers.onStatus?.({
        positionStatus: "idle",
        errorMessage: message
      });
      return false;
    }
  }

  private async fetchHistoryViaRest(): Promise<boolean> {
    if (!this.historyUrl) {
      this.handlers.onHistory?.(undefined);
      return false;
    }
    try {
      const response = await fetch(this.historyUrl);
      if (!response.ok) {
        throw new Error(`HISTORY ${response.status}`);
      }
      const payload = await response.json();
      const history = normalizePosition(payload);
      if (!history) {
        throw new Error("HISTORY invalid payload");
      }
      this.emitHistorySnapshot(history, Date.now());
      return true;
    } catch (error) {
      this.handlers.onHistory?.(undefined);
      const message = error instanceof Error ? error.message : "history error";
      this.handlers.onStatus?.({ errorMessage: message });
      return false;
    }
  }

  private async fetchGroupsViaRest(): Promise<boolean> {
    if (!this.groupsUrl) {
      this.handlers.onGroups?.(undefined);
      return false;
    }
    try {
      const response = await fetch(this.groupsUrl);
      if (!response.ok) {
        throw new Error(`GROUPS ${response.status}`);
      }
      const payload = await response.json();
      const groups = normalizeGroups(payload);
      if (!groups) {
        throw new Error("GROUPS invalid payload");
      }
      this.emitGroupsSnapshot(groups, Date.now());
      return true;
    } catch (error) {
      this.handlers.onStatus?.({
        errorMessage: error instanceof Error ? error.message : "groups error"
      });
      return false;
    }
  }

  private async fallbackToRest(reason: string): Promise<void> {
    if (this.restFallbackInFlight) return;
    this.restFallbackInFlight = true;
    try {
      await this.fetchStatusViaRest();
      const [signalsOk, accountOk, positionOk, historyOk, groupsOk] = await Promise.all([
        this.fetchSignalsViaRest(),
        this.fetchAccountViaRest(),
        this.fetchPositionViaRest(),
        this.fetchHistoryViaRest(),
        this.fetchGroupsViaRest()
      ]);
      if (!signalsOk && !accountOk && !positionOk && !historyOk && !groupsOk) {
        this.handlers.onStatus?.({ errorMessage: reason });
      }
    } finally {
      this.restFallbackInFlight = false;
    }
  }

  private connectWs(): void {
    if (!this.wsUrl) {
      this.stopHeartbeat();
      this.handlers.onStatus?.({ wsStatus: "idle", wsConnectedAt: undefined });
      void this.fallbackToRest("WS URL not configured, fallback to REST");
      return;
    }

    this.handlers.onStatus?.({ wsStatus: "connecting", wsConnectedAt: undefined });
    try {
      this.ws = new WebSocket(this.wsUrl);
    } catch (error) {
      this.handlers.onStatus?.({ wsStatus: "error", wsConnectedAt: undefined });
      void this.fallbackToRest("WS connect failed, fallback to REST");
      this.scheduleReconnect();
      return;
    }

    if (!this.ws) return;

    this.ws.onopen = () => {
      const connectedAt = Date.now();
      this.reconnectAttempts = 0;
      this.lastDataEventAt = null;
      this.lastIndicatorRefreshAt = null;
      this.lastStatusRefreshAt = connectedAt;
      this.handlers.onStatus?.({
        wsStatus: "open",
        wsConnectedAt: connectedAt,
        restStatus: "ok",
        accountStatus: "idle",
        positionStatus: "idle",
        heartbeatStale: false,
        heartbeatMissedCycles: 0,
        errorMessage: undefined
      });
      // 每次 WS 建连后都主动调用一次 /status（用户要求）。
      void this.fetchStatusViaRest();
      void this.fetchGroupsViaRest();
      const snapshotFetchSent = this.sendWsFetchSnapshot();
      const statusFetchSent = this.sendWsFetchStatus();
      const indicatorsFetchSent = this.sendWsFetchIndicators();
      const historyFetchSent = this.sendWsFetchHistory(this.historyRange);
      if (!snapshotFetchSent || !statusFetchSent) {
        void this.fallbackToRest("WS initial fetch failed on open, fallback to REST");
      }
      if (!indicatorsFetchSent) {
        void this.fallbackToRest("WS initial indicators fetch failed, fallback to REST");
      }
      if (!historyFetchSent) {
        void this.fallbackToRest("WS initial history fetch failed, fallback to REST");
      }
      this.startHeartbeat();
    };

    this.ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data as string);
        if (this.handleWsMessage(payload)) {
          return;
        }
        const updates = normalizeUpdates(payload);
        if (updates.length > 0) {
          const now = Date.now();
          this.lastDataEventAt = now;
          this.handlers.onUpdate(updates);
          this.handlers.onStatus?.({ restStatus: "ok", lastUpdateAt: now });
        }
      } catch (error) {
        this.handlers.onStatus?.({ errorMessage: "WS parse error" });
      }
    };

    this.ws.onerror = () => {
      this.handlers.onStatus?.({ wsStatus: "error", wsConnectedAt: undefined });
    };

    this.ws.onclose = (event) => {
      this.stopHeartbeat();
      this.rejectAllPendingCandlesFetches(`WS closed(code=${event.code})`);
      const reason = event.reason ? `, reason=${event.reason}` : "";
      const closeMessage = `WS closed(code=${event.code}${reason})`;
      this.handlers.onStatus?.({
        wsStatus: "closed",
        wsConnectedAt: undefined,
        heartbeatStale: false,
        heartbeatMissedCycles: 0,
        errorMessage: closeMessage
      });
      if (!this.stopped) {
        void this.fallbackToRest(`${closeMessage}, fallback to REST`);
        this.scheduleReconnect();
      }
    };
  }

  private handleSnapshotFrame(raw: UnknownRecord): boolean {
    const now = Date.now();
    let handled = false;

    const hasSignals = hasOwn(raw, "signals") || hasOwn(raw, "data");
    if (hasSignals) {
      const source = hasOwn(raw, "signals") ? raw.signals : raw.data;
      const data = normalizeSnapshot(source);
      if (this.replaceSnapshot(data)) {
        this.handlers.onStatus?.({
          restStatus: "ok",
          lastSnapshotAt: now,
          lastUpdateAt: now,
          errorMessage: undefined
        });
        handled = true;
      }
    }

    if (hasOwn(raw, "account") && this.pushAccountFromWs(raw.account, now)) {
      handled = true;
    }
    if (hasOwn(raw, "groups") && this.pushGroupsFromWs(raw.groups, now)) {
      handled = true;
    }
    if (hasOwn(raw, "position") && this.pushPositionFromWs(raw.position, now)) {
      handled = true;
    }
    if (hasOwn(raw, "history") && this.pushHistoryFromWs(raw.history, now)) {
      handled = true;
    }

    if (handled) {
      this.lastDataEventAt = now;
    }
    return handled;
  }

  private handleDiffFrame(raw: UnknownRecord): boolean {
    const now = Date.now();
    let handled = false;

    const hasSignals =
      hasOwn(raw, "signals") || hasOwn(raw, "added") || hasOwn(raw, "updated") || hasOwn(raw, "removed");
    if (hasSignals) {
      const signalSource = isRecord(raw.signals) ? raw.signals : raw;
      const added = normalizeSnapshot(signalSource.added);
      const updated = normalizeSnapshot(signalSource.updated);
      for (const item of added) {
        this.snapshotMap.set(item.id, item);
      }
      for (const item of updated) {
        this.snapshotMap.set(item.id, item);
      }
      const removedIds = normalizeRemovedIds(signalSource.removed);
      for (const id of removedIds) {
        this.snapshotMap.delete(id);
      }
      let emitted = false;
      if (added.length > 0 || updated.length > 0 || removedIds.length > 0) {
        emitted = this.emitSnapshot();
      }
      if (emitted) {
        this.handlers.onStatus?.({
          restStatus: "ok",
          lastUpdateAt: now,
          errorMessage: undefined
        });
        handled = true;
      }
    }

    if (hasOwn(raw, "account") && this.pushAccountFromWs(raw.account, now)) {
      handled = true;
    }
    if (hasOwn(raw, "groups") && this.pushGroupsFromWs(raw.groups, now)) {
      handled = true;
    }
    if (hasOwn(raw, "position") && this.pushPositionFromWs(raw.position, now)) {
      handled = true;
    }
    if (hasOwn(raw, "history") && this.pushHistoryFromWs(raw.history, now)) {
      handled = true;
    }

    if (handled) {
      this.lastDataEventAt = now;
    }
    return handled;
  }

  private pushAccountFromWs(payload: unknown, now: number): boolean {
    const account = normalizeAccount(payload);
    if (!account) return false;
    return this.emitAccountSnapshot(account, now);
  }

  private pushPositionFromWs(payload: unknown, now: number): boolean {
    const position = normalizePosition(payload);
    if (!position) return false;
    return this.emitPositionSnapshot(position, now);
  }

  private pushGroupsFromWs(payload: unknown, now: number): boolean {
    const groups = normalizeGroups(payload);
    if (!groups) return false;
    return this.emitGroupsSnapshot(groups, now);
  }

  private pushHistoryFromWs(payload: unknown, now: number): boolean {
    const history = normalizePosition(payload);
    if (!history) return false;
    return this.emitHistorySnapshot(history, now);
  }

  private pushBackendStatusFromWs(payload: unknown, now: number): boolean {
    const status = normalizeBackendStatus(payload);
    const statusChanged = status ? this.emitBackendStatus(status, now) : false;
    return statusChanged;
  }

  private handleWsMessage(payload: unknown): boolean {
    if (!isRecord(payload)) {
      return false;
    }
    const raw = payload;
    const type = raw.type;
    if (typeof type !== "string") {
      return false;
    }

    if (type === "snapshot") {
      return this.handleSnapshotFrame(raw);
    }

    if (type === "diff") {
      return this.handleDiffFrame(raw);
    }

    if (type === "status") {
      const now = Date.now();
      const statusPayload = isRecord(raw.data) ? raw.data : raw;
      const pushed = this.pushBackendStatusFromWs(statusPayload, now);
      if (pushed) {
        this.lastDataEventAt = now;
      }
      return pushed;
    }

    if (type === "candles") {
      const requestId = typeof raw.request_id === "string" ? raw.request_id : "";
      if (!requestId) return true;
      const pending = this.pendingCandlesFetches.get(requestId);
      if (!pending) return true;
      this.pendingCandlesFetches.delete(requestId);
      window.clearTimeout(pending.timer);
      pending.resolve(normalizeCandlesSnapshot(raw) || undefined);
      return true;
    }

    if (type === "error") {
      const message = typeof raw.message === "string" ? raw.message : "WS message error";
      const requestId = typeof raw.request_id === "string" ? raw.request_id : "";
      if (requestId) {
        const pending = this.pendingCandlesFetches.get(requestId);
        if (pending) {
          this.pendingCandlesFetches.delete(requestId);
          window.clearTimeout(pending.timer);
          pending.reject(message);
          return true;
        }
      }
      // error 帧属于业务层错误（例如局部数据拉取失败），不代表 WS 链路中断。
      this.handlers.onStatus?.({ errorMessage: message });
      return true;
    }

    if (type === "pong") {
      const now = Date.now();
      this.lastPongAt = now;
      const requestId = typeof raw.request_id === "string" ? raw.request_id : undefined;
      let rttMs: number | undefined;
      if (requestId) {
        const sentAt = this.pendingPings.get(requestId);
        if (typeof sentAt === "number") {
          rttMs = Math.max(0, now - sentAt);
        }
        this.pendingPings.delete(requestId);
      }

      const pong = normalizePong(raw);
      if (pong) {
        pong.receivedAt = now;
        if (!pong.requestId && requestId) {
          pong.requestId = requestId;
        }
        if (rttMs !== undefined) {
          pong.rttMs = rttMs;
          this.lastKnownRttMs = rttMs;
        } else if (this.lastKnownRttMs !== undefined) {
          pong.rttMs = this.lastKnownRttMs;
        }
      }

      this.handlers.onStatus?.({
        lastPongAt: now,
        heartbeatMissedCycles: 0,
        heartbeatStale: false,
        lastPong: pong || undefined
      });
      return true;
    }

    return false;
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer);
    }
    const delay = Math.min(30000, 1000 * Math.pow(2, this.reconnectAttempts));
    this.reconnectAttempts += 1;
    this.reconnectTimer = window.setTimeout(() => {
      this.cleanupWs();
      this.connectWs();
    }, delay);
  }

  private cleanupWs(): void {
    this.stopHeartbeat();
    this.rejectAllPendingCandlesFetches("WS disconnected");
    if (!this.ws) return;
    this.ws.onopen = null;
    this.ws.onclose = null;
    this.ws.onerror = null;
    this.ws.onmessage = null;
    this.ws.close();
    this.ws = null;
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.lastPongAt = Date.now();
    this.sendPing();
    this.pingTimer = window.setInterval(() => {
      this.sendPing();
    }, PING_INTERVAL_MS);
  }

  private stopHeartbeat(): void {
    if (this.pingTimer !== null) {
      window.clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    this.pendingPings.clear();
    this.lastPongAt = null;
    this.lastIndicatorRefreshAt = null;
    this.lastStatusRefreshAt = null;
  }

  private buildFetchSnapshotMessage(): string {
    const requestId = `fetch-snapshot-${Date.now()}-${this.fetchSequence}`;
    this.fetchSequence += 1;
    return JSON.stringify({
      type: "fetch",
      request_id: requestId,
      target: "snapshot",
      subscription: {
        streams: [...LIGHT_SNAPSHOT_STREAMS],
      }
    });
  }

  private buildFetchStatusMessage(): string {
    const requestId = `fetch-status-${Date.now()}-${this.fetchSequence}`;
    this.fetchSequence += 1;
    return JSON.stringify({
      type: "fetch",
      request_id: requestId,
      target: "status"
    });
  }

  private buildFetchIndicatorsMessage(): string {
    const requestId = `fetch-indicators-${Date.now()}-${this.fetchSequence}`;
    this.fetchSequence += 1;
    return JSON.stringify({
      type: "fetch",
      request_id: requestId,
      target: "snapshot",
      subscription: {
        streams: [...INDICATOR_STREAMS]
      }
    });
  }

  private buildFetchHistoryMessage(range: WsHistoryRange): string {
    const requestId = `fetch-history-${Date.now()}-${this.fetchSequence}`;
    this.fetchSequence += 1;
    return JSON.stringify({
      type: "fetch",
      request_id: requestId,
      target: "history",
      history_range: range
    });
  }

  private buildFetchCandlesMessage(request: BubbleCandlesFetchRequest): {
    requestId: string;
    command: string;
  } {
    const requestId = `fetch-candles-${Date.now()}-${this.fetchSequence}`;
    this.fetchSequence += 1;
    const requests = Array.isArray(request.requests)
      ? request.requests.map((item) => ({
          exchange: (item.exchange || "").trim().toLowerCase(),
          symbol: (item.symbol || "").trim().toUpperCase(),
          timeframes: Array.isArray(item.timeframes)
            ? item.timeframes
                .map((timeframe) => (timeframe || "").trim().toLowerCase())
                .filter((timeframe) => timeframe.length > 0)
            : [],
          limit:
            typeof item.limit === "number" && Number.isFinite(item.limit)
              ? Math.max(1, Math.trunc(item.limit))
              : undefined,
          position: item.position
            ? {
                position_id:
                  typeof item.position.positionId === "number" && Number.isFinite(item.position.positionId)
                    ? Math.trunc(item.position.positionId)
                    : undefined,
                position_key: (item.position.positionKey || "").trim() || undefined,
                position_side: (item.position.positionSide || "").trim().toLowerCase() || undefined,
                margin_mode: (item.position.marginMode || "").trim().toLowerCase() || undefined,
                entry_time: (item.position.entryTime || "").trim() || undefined,
                strategy_name: (item.position.strategyName || "").trim() || undefined,
                strategy_version: (item.position.strategyVersion || "").trim() || undefined
              }
            : undefined
        }))
      : [];
    const eventLimit =
      typeof request.eventLimit === "number" && Number.isFinite(request.eventLimit)
        ? Math.max(1, Math.trunc(request.eventLimit))
        : undefined;
    const command = JSON.stringify({
      type: "fetch",
      request_id: requestId,
      target: "candles",
      candles_fetch: {
        requests,
        closed_only: request.closedOnly !== false,
        include_events: request.includeEvents === true,
        event_limit: eventLimit
      }
    });
    return { requestId, command };
  }

  private sendWsCommand(command: string, errorLabel: string): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return false;
    }
    try {
      this.ws.send(command);
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : `${errorLabel} send failed`;
      this.handlers.onStatus?.({ wsStatus: "error", errorMessage: message });
      this.forceReconnect(`WS ${errorLabel} failed: ${message}`);
      return false;
    }
  }

  private sendWsFetchSnapshot(): boolean {
    return this.sendWsCommand(this.buildFetchSnapshotMessage(), "fetch snapshot");
  }

  private sendWsFetchStatus(): boolean {
    return this.sendWsCommand(this.buildFetchStatusMessage(), "fetch status");
  }

  private sendWsFetchIndicators(): boolean {
    return this.sendWsCommand(this.buildFetchIndicatorsMessage(), "fetch indicators");
  }

  private sendWsFetchHistory(range: WsHistoryRange): boolean {
    return this.sendWsCommand(this.buildFetchHistoryMessage(range), "fetch history");
  }

  async fetchCandles(request: BubbleCandlesFetchRequest): Promise<BubbleCandlesSnapshot | undefined> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this.handlers.onStatus?.({ errorMessage: "WS not connected, candles fetch skipped" });
      return undefined;
    }
    const { requestId, command } = this.buildFetchCandlesMessage(request);
    return new Promise((resolve) => {
      const timer = window.setTimeout(() => {
        const pending = this.pendingCandlesFetches.get(requestId);
        if (!pending) return;
        this.pendingCandlesFetches.delete(requestId);
        pending.reject("candles fetch timeout");
      }, CANDLES_FETCH_TIMEOUT_MS);
      this.pendingCandlesFetches.set(requestId, {
        timer,
        resolve,
        reject: (reason) => {
          this.handlers.onStatus?.({
            errorMessage: reason || "candles fetch failed"
          });
          resolve(undefined);
        }
      });
      if (!this.sendWsCommand(command, "fetch candles")) {
        const pending = this.pendingCandlesFetches.get(requestId);
        if (!pending) return;
        this.pendingCandlesFetches.delete(requestId);
        window.clearTimeout(pending.timer);
        pending.reject("WS fetch candles send failed");
      }
    });
  }

  private refreshIndicatorsViaWs(now: number): void {
    const interval =
      this.lastKnownPositionCount > 0
        ? INDICATOR_REFRESH_INTERVAL_ACTIVE_MS
        : INDICATOR_REFRESH_INTERVAL_IDLE_MS;
    if (
      this.lastIndicatorRefreshAt !== null &&
      now - this.lastIndicatorRefreshAt < interval
    ) {
      return;
    }
    if (this.sendWsFetchIndicators()) {
      this.lastIndicatorRefreshAt = now;
      return;
    }
    void this.fallbackToRest("WS indicator fetch failed, fallback to REST");
  }

  private refreshStatusViaWs(now: number): void {
    if (
      this.lastStatusRefreshAt !== null &&
      now - this.lastStatusRefreshAt < STATUS_REFRESH_INTERVAL_MS
    ) {
      return;
    }
    if (this.sendWsFetchStatus()) {
      this.lastStatusRefreshAt = now;
      return;
    }
    void this.fallbackToRest("WS status fetch failed, fallback to REST");
  }

  private sendPing(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    const sentAt = Date.now();
    const requestId = `ping-${sentAt}-${this.pingSequence}`;
    this.pingSequence += 1;

    try {
      this.ws.send(JSON.stringify({ type: "ping", request_id: requestId }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "ping send failed";
      this.handlers.onStatus?.({ wsStatus: "error", errorMessage: message });
      this.forceReconnect(`WS ping failed: ${message}`);
      return;
    }

    this.pendingPings.set(requestId, sentAt);
    if (this.pendingPings.size > MAX_PENDING_PINGS) {
      const oldest = this.pendingPings.keys().next();
      if (!oldest.done) {
        this.pendingPings.delete(oldest.value);
      }
    }

    const missedCycles = this.computeMissedPongCycles(sentAt);
    this.handlers.onStatus?.({
      lastPingAt: sentAt,
      heartbeatMissedCycles: missedCycles,
      heartbeatStale: missedCycles > PONG_TIMEOUT_CYCLES
    });

    this.refreshIndicatorsViaWs(sentAt);
    this.refreshStatusViaWs(sentAt);

    if (missedCycles >= AUTO_RECONNECT_TIMEOUT_CYCLES) {
      this.forceReconnect(`WS heartbeat timeout(${missedCycles}s)`);
    }
  }

  private computeMissedPongCycles(now: number): number {
    if (this.lastPongAt === null) return 0;
    return Math.max(0, Math.floor((now - this.lastPongAt) / PING_INTERVAL_MS));
  }

  private startDataCheck(): void {
    if (this.staleCheckTimer !== null) return;
    const interval = Math.max(1000, this.dataCheckIntervalMs || 15000);
    this.staleCheckTimer = window.setInterval(() => {
      void this.checkDataFreshness(interval);
    }, interval);
  }

  private async checkDataFreshness(intervalMs: number): Promise<void> {
    if (this.stopped) return;

    const wsOpen = this.ws && this.ws.readyState === WebSocket.OPEN;
    if (wsOpen) {
      const now = Date.now();
      const stale = this.lastDataEventAt === null || now - this.lastDataEventAt >= intervalMs;
      if (!stale) return;
      const snapshotSent = this.sendWsFetchSnapshot();
      const statusSent = this.sendWsFetchStatus();
      const historySent = this.sendWsFetchHistory(this.historyRange);
      if (snapshotSent && statusSent && historySent) return;
      await this.fallbackToRest("WS fetch failed, fallback to REST");
      return;
    }

    await this.fallbackToRest("WS unavailable, fallback to REST");
  }

  private replaceSnapshot(data: BubbleDatum[]): boolean {
    this.snapshotMap = new Map<string, BubbleDatum>();
    for (const item of data) {
      this.snapshotMap.set(item.id, item);
    }
    return this.emitSnapshot();
  }

  private emitSnapshot(): boolean {
    const list = Array.from(this.snapshotMap.values());
    list.sort((a, b) => {
      const rankDiff = a.rank - b.rank;
      if (rankDiff !== 0) return rankDiff;
      return a.id.localeCompare(b.id);
    });
    const signature = this.buildSignalSnapshotSignature(list);
    if (this.lastSnapshotSignature !== null && signature === this.lastSnapshotSignature) {
      return false;
    }
    this.lastSnapshotSignature = signature;
    this.handlers.onSnapshot(list);
    return true;
  }

  private emitAccountSnapshot(account: AccountSnapshot, now: number): boolean {
    const signature = this.buildAccountSignature(account);
    if (this.lastAccountSignature !== null && signature === this.lastAccountSignature) return false;
    this.lastAccountSignature = signature;
    this.handlers.onAccount?.(account);
    this.handlers.onStatus?.({
      accountStatus: "ok",
      lastAccountAt: now,
      errorMessage: undefined
    });
    this.lastDataEventAt = now;
    return true;
  }

  private emitPositionSnapshot(position: PositionSnapshot, now: number): boolean {
    const signature = this.buildPositionSignature(position);
    if (this.lastPositionSignature !== null && signature === this.lastPositionSignature) return false;
    this.lastPositionSignature = signature;
    this.lastKnownPositionCount =
      typeof position.count === "number" && Number.isFinite(position.count)
        ? Math.max(0, Math.trunc(position.count))
        : position.positions.length;
    this.handlers.onPosition?.(position);
    this.handlers.onStatus?.({
      positionStatus: "ok",
      lastPositionAt: now,
      errorMessage: undefined
    });
    this.lastDataEventAt = now;
    return true;
  }

  private emitHistorySnapshot(history: PositionSnapshot, now: number): boolean {
    const signature = this.buildPositionSignature(history);
    if (this.lastHistorySignature !== null && signature === this.lastHistorySignature) return false;
    this.lastHistorySignature = signature;
    this.handlers.onHistory?.(history);
    this.handlers.onStatus?.({
      lastUpdateAt: now,
      errorMessage: undefined
    });
    this.lastDataEventAt = now;
    return true;
  }

  private emitGroupsSnapshot(groups: TrendGroupsSnapshot, now: number): boolean {
    const signature = this.buildGroupsSignature(groups);
    if (this.lastGroupsSignature !== null && signature === this.lastGroupsSignature) return false;
    this.lastGroupsSignature = signature;
    this.handlers.onGroups?.(groups);
    this.handlers.onStatus?.({
      lastUpdateAt: now,
      errorMessage: undefined
    });
    this.lastDataEventAt = now;
    return true;
  }

  private emitBackendStatus(status: BackendStatus, now: number): boolean {
    const signature = this.buildBackendStatusSignature(status);
    if (this.lastBackendStatusSignature !== null && signature === this.lastBackendStatusSignature) return false;
    this.lastBackendStatusSignature = signature;
    this.handlers.onBackendStatus?.(status);
    this.handlers.onStatus?.({
      lastUpdateAt: now,
      errorMessage: undefined
    });
    this.lastDataEventAt = now;
    return true;
  }

  private signatureInit(): { a: number; b: number } {
    return { a: 2166136261, b: 5381 };
  }

  private signatureMixInt(state: { a: number; b: number }, value: number): void {
    const normalized = value | 0;
    state.a ^= normalized;
    state.a = Math.imul(state.a, 16777619);
    state.b = (Math.imul(state.b, 33) + normalized) | 0;
  }

  private signatureMixNumber(state: { a: number; b: number }, value: number | undefined): void {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      this.signatureMixInt(state, -1);
      return;
    }
    const intPart = Math.trunc(value);
    const fracPart = Math.round((value - intPart) * 1e8);
    const intHigh = Math.trunc(intPart / 0x100000000);
    const intLow = intPart >>> 0;
    this.signatureMixInt(state, intHigh);
    this.signatureMixInt(state, intLow);
    this.signatureMixInt(state, fracPart);
  }

  private signatureMixString(state: { a: number; b: number }, value: string | undefined): void {
    const normalized = (value || "").trim();
    for (let i = 0; i < normalized.length; i += 1) {
      const code = normalized.charCodeAt(i);
      state.a ^= code;
      state.a = Math.imul(state.a, 16777619);
      state.b = (Math.imul(state.b, 33) + code) | 0;
    }
    this.signatureMixInt(state, 0x1f);
  }

  private signatureFinalize(state: { a: number; b: number }, prefix: number): string {
    return `${prefix}:${(state.a >>> 0).toString(16)}:${(state.b >>> 0).toString(16)}`;
  }

  private buildSignalSnapshotSignature(list: BubbleDatum[]): string {
    const state = this.signatureInit();
    this.signatureMixInt(state, list.length);
    for (const item of list) {
      this.signatureMixString(state, item.id);
      this.signatureMixString(state, item.symbol);
      this.signatureMixString(state, item.exchange);
      this.signatureMixString(state, item.timeframe);
      this.signatureMixString(state, item.groupId);
      this.signatureMixString(state, item.strategy);
      this.signatureMixString(state, item.strategyVersion);
      this.signatureMixNumber(state, item.rank);
      this.signatureMixNumber(state, item.price);
      this.signatureMixNumber(state, item.marketCap);
      this.signatureMixNumber(state, item.volume24h);
      this.signatureMixNumber(state, item.change24h);
      this.signatureMixNumber(state, item.change7d);
      this.signatureMixNumber(state, item.side);
      this.signatureMixNumber(state, item.highSide);
      this.signatureMixNumber(state, item.midSide);
      this.signatureMixNumber(state, item.action);
      this.signatureMixNumber(state, item.entry);
      this.signatureMixNumber(state, item.exit);
      this.signatureMixNumber(state, item.sl);
      this.signatureMixNumber(state, item.tp);
      this.signatureMixNumber(state, item.trendingTimestamp);
      this.signatureMixNumber(state, item.triggerTimestamp);
      if (Array.isArray(item.ohlcv)) {
        this.signatureMixInt(state, item.ohlcv.length);
        for (const bar of item.ohlcv) {
          this.signatureMixNumber(state, bar.ts);
          this.signatureMixNumber(state, bar.open);
          this.signatureMixNumber(state, bar.high);
          this.signatureMixNumber(state, bar.low);
          this.signatureMixNumber(state, bar.close);
          this.signatureMixNumber(state, bar.volume);
        }
      } else {
        this.signatureMixInt(state, 0);
      }
    }
    return this.signatureFinalize(state, list.length);
  }

  private buildAccountSignature(account: AccountSnapshot): string {
    const state = this.signatureInit();
    this.signatureMixString(state, account.exchange);
    this.signatureMixString(state, account.currency);
    this.signatureMixNumber(state, account.fundingUsdt);
    this.signatureMixNumber(state, account.tradingUsdt);
    this.signatureMixNumber(state, account.totalUsdt);
    this.signatureMixNumber(state, account.perTradeUsdt);
    this.signatureMixNumber(state, account.dailyProfitUsdt);
    this.signatureMixNumber(state, account.updatedAtMs);
    return this.signatureFinalize(state, 1);
  }

  private comparePositionItems(left: PositionSnapshot["positions"][number], right: PositionSnapshot["positions"][number]): number {
    const exchangeDiff = (left.exchange || "").localeCompare(right.exchange || "");
    if (exchangeDiff !== 0) return exchangeDiff;
    const symbolDiff = (left.symbol || "").localeCompare(right.symbol || "");
    if (symbolDiff !== 0) return symbolDiff;
    const groupDiff = (left.groupId || "").localeCompare(right.groupId || "");
    if (groupDiff !== 0) return groupDiff;
    const timeframeDiff = (left.timeframe || "").localeCompare(right.timeframe || "");
    if (timeframeDiff !== 0) return timeframeDiff;
    const sideDiff = (left.positionSide || "").localeCompare(right.positionSide || "");
    if (sideDiff !== 0) return sideDiff;
    const strategyDiff = (left.strategyName || "").localeCompare(right.strategyName || "");
    if (strategyDiff !== 0) return strategyDiff;
    const versionDiff = (left.strategyVersion || "").localeCompare(right.strategyVersion || "");
    if (versionDiff !== 0) return versionDiff;
    const entryTimeDiff = (left.entryTime || "").localeCompare(right.entryTime || "");
    if (entryTimeDiff !== 0) return entryTimeDiff;
    return (left.updatedTime || "").localeCompare(right.updatedTime || "");
  }

  private buildPositionSignature(snapshot: PositionSnapshot): string {
    const state = this.signatureInit();
    const items = [...snapshot.positions].sort((left, right) => this.comparePositionItems(left, right));
    this.signatureMixNumber(state, snapshot.count);
    this.signatureMixInt(state, items.length);
    for (const item of items) {
      this.signatureMixNumber(state, item.positionId);
      this.signatureMixString(state, item.exchange);
      this.signatureMixString(state, item.symbol);
      this.signatureMixString(state, item.groupId);
      this.signatureMixString(state, item.timeframe);
      this.signatureMixString(state, item.positionSide);
      this.signatureMixString(state, item.marginMode);
      this.signatureMixNumber(state, item.leverageMultiplier);
      this.signatureMixNumber(state, item.marginAmount);
      this.signatureMixNumber(state, item.entryPrice);
      this.signatureMixNumber(state, item.exitPrice);
      this.signatureMixNumber(state, item.entryQuantity);
      this.signatureMixNumber(state, item.currentPrice);
      this.signatureMixNumber(state, item.takeProfitPrice);
      this.signatureMixNumber(state, item.stopLossPrice);
      this.signatureMixNumber(state, item.unrealizedProfitAmount);
      this.signatureMixNumber(state, item.unrealizedProfitRate);
      this.signatureMixNumber(state, item.profitAmount);
      this.signatureMixNumber(state, item.profitRate);
      this.signatureMixNumber(state, item.maxFloatingProfitAmount);
      this.signatureMixNumber(state, item.maxFloatingProfitRate);
      this.signatureMixNumber(state, item.maxFloatingLossAmount);
      this.signatureMixNumber(state, item.maxFloatingLossRate);
      this.signatureMixString(state, item.entryTime);
      this.signatureMixString(state, item.updatedTime);
      this.signatureMixString(state, item.exitTime);
      this.signatureMixString(state, item.holdingTime);
      this.signatureMixString(state, item.status);
      this.signatureMixString(state, item.strategyName);
      this.signatureMixString(state, item.strategyVersion);
    }
    return this.signatureFinalize(state, items.length);
  }

  private buildBackendStatusSignature(status: BackendStatus): string {
    const state = this.signatureInit();
    const modules = [...status.modules].sort((left, right) => {
      const nameDiff = (left.name || "").localeCompare(right.name || "");
      if (nameDiff !== 0) return nameDiff;
      const stateDiff = (left.state || "").localeCompare(right.state || "");
      if (stateDiff !== 0) return stateDiff;
      return (left.updatedAt || "").localeCompare(right.updatedAt || "");
    });
    this.signatureMixString(state, status.singletonUuid);
    this.signatureMixString(state, status.versionTag);
    this.signatureMixString(state, status.versionCommit);
    this.signatureMixString(state, status.buildTime);
    this.signatureMixNumber(state, status.cache.exchangeCount);
    this.signatureMixNumber(state, status.cache.symbolCount);
    this.signatureMixNumber(state, status.cache.timeframeCount);
    this.signatureMixNumber(state, status.cache.signalCount);
    this.signatureMixInt(state, modules.length);
    for (const module of modules) {
      this.signatureMixString(state, module.name);
      this.signatureMixString(state, module.state);
      this.signatureMixString(state, module.updatedAt);
    }
    return this.signatureFinalize(state, modules.length);
  }

  private buildGroupsSignature(snapshot: TrendGroupsSnapshot): string {
    const state = this.signatureInit();
    this.signatureMixString(state, snapshot.mode);
    this.signatureMixNumber(state, snapshot.groupsTotal);
    this.signatureMixNumber(state, snapshot.groupsActive);
    this.signatureMixInt(state, snapshot.enabled ? 1 : 0);
    const groups = [...snapshot.groups].sort((left, right) => left.groupId.localeCompare(right.groupId));
    this.signatureMixInt(state, groups.length);
    for (const group of groups) {
      this.signatureMixString(state, group.groupId);
      this.signatureMixString(state, group.strategy);
      this.signatureMixString(state, group.primaryTimeframe);
      this.signatureMixString(state, group.side);
      this.signatureMixNumber(state, group.anchorTrendingTimestampMs);
      this.signatureMixString(state, group.state);
      this.signatureMixString(state, group.lockStage);
      this.signatureMixString(state, group.selectedCandidateKey);
      this.signatureMixNumber(state, group.entryCount);
      this.signatureMixInt(state, group.candidates.length);
      for (const candidate of group.candidates) {
        const entries = Object.entries(candidate).sort(([left], [right]) => left.localeCompare(right));
        this.signatureMixInt(state, entries.length);
        for (const [key, value] of entries) {
          this.signatureMixString(state, key);
          if (typeof value === "number") {
            this.signatureMixNumber(state, value);
          } else if (typeof value === "boolean") {
            this.signatureMixInt(state, value ? 1 : 0);
          } else if (typeof value === "string") {
            this.signatureMixString(state, value);
          } else if (value === null || value === undefined) {
            this.signatureMixString(state, "");
          } else {
            this.signatureMixString(state, JSON.stringify(value));
          }
        }
      }
    }
    return this.signatureFinalize(state, groups.length);
  }

  private forceReconnect(reason: string): void {
    if (this.stopped || this.forcedReconnectInFlight) return;
    this.forcedReconnectInFlight = true;

    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    this.handlers.onStatus?.({
      wsStatus: "connecting",
      heartbeatStale: true,
      errorMessage: `${reason}, reconnecting`
    });

    this.cleanupWs();
    this.reconnectAttempts = 0;
    this.connectWs();
    this.forcedReconnectInFlight = false;
  }

  private rejectAllPendingCandlesFetches(reason: string): void {
    if (this.pendingCandlesFetches.size === 0) return;
    for (const [requestId, pending] of this.pendingCandlesFetches) {
      window.clearTimeout(pending.timer);
      pending.reject(reason || `candles request ${requestId} canceled`);
    }
    this.pendingCandlesFetches.clear();
  }

  loadMoreHistory(): void {
    const nextRange: WsHistoryRange =
      this.historyRange === "today" ? "24h" : this.historyRange === "24h" ? "7d" : "7d";
    this.historyRange = nextRange;
    if (this.sendWsFetchHistory(nextRange)) {
      return;
    }
    void this.fetchHistoryViaRest();
  }
}
