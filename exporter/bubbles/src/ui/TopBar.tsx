import { KeyboardEvent, memo, useEffect, useMemo, useRef, useState } from "react";
import { useAppStore } from "../app/store";
import { BUBBLE_SHORTCUT_HELP_ROWS } from "../shortcuts";

function formatAmount(value?: number, digits = 2): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "--";
  return value.toFixed(digits);
}

function exchangeLabel(value: string): string {
  const normalized = normalizeExchange(value);
  if (normalized === "binance") return "Binance";
  if (normalized === "okx") return "OKX";
  if (normalized === "bitget") return "Bitget";
  return value.toUpperCase();
}

function normalizeExchange(value?: string): string {
  return (value || "").trim().toLowerCase();
}

function shouldIgnoreTopbarShortcut(event: globalThis.KeyboardEvent): boolean {
  const target = event.target as HTMLElement | null;
  if (!target) return false;
  const tag = target.tagName.toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") return true;
  return target.isContentEditable;
}

function formatPositionBadgeCount(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "";
  if (value > 99) return "99+";
  return String(Math.floor(value));
}

function cloneSetMap(source: Map<string, Set<string>>): Map<string, Set<string>> {
  const next = new Map<string, Set<string>>();
  for (const [key, values] of source) {
    next.set(key, new Set(values));
  }
  return next;
}

function setEquals(left: Set<string> | undefined, right: Set<string> | undefined): boolean {
  if (!left && !right) return true;
  if (!left || !right) return false;
  if (left.size !== right.size) return false;
  for (const value of left) {
    if (!right.has(value)) return false;
  }
  return true;
}

function setMapEquals(left: Map<string, Set<string>>, right: Map<string, Set<string>>): boolean {
  if (left.size !== right.size) return false;
  for (const [key, values] of left) {
    if (!setEquals(values, right.get(key))) return false;
  }
  return true;
}

function isArmedAction(action?: number): boolean {
  return typeof action === "number" && Number.isFinite(action) && (Math.trunc(action) & 4) !== 0;
}

function formatConnectionDuration(durationMs: number): string {
  const safeDurationMs = Math.max(0, Math.floor(durationMs));
  const totalSeconds = Math.floor(safeDurationMs / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (days > 0) {
    if (hours > 0) return `${days}d${hours}h`;
    return `${days}d`;
  }
  if (hours > 0) {
    if (minutes > 0) return `${hours}h${minutes}m`;
    return `${hours}h`;
  }
  if (minutes > 0) {
    if (seconds > 0) return `${minutes}m${seconds}s`;
    return `${minutes}m`;
  }
  return `${seconds}s`;
}

export const TopBar = memo(function TopBar() {
  const timeframeTabs = useAppStore((s) => s.timeframeTabs);
  const checkedTimeframes = useAppStore((s) => s.checkedTimeframes);
  const toggleTimeframeChecked = useAppStore((s) => s.toggleTimeframeChecked);
  const exchangeTabs = useAppStore((s) => s.exchangeTabs);
  const checkedExchanges = useAppStore((s) => s.checkedExchanges);
  const toggleExchangeChecked = useAppStore((s) => s.toggleExchangeChecked);
  const searchQuery = useAppStore((s) => s.searchQuery);
  const setSearchQuery = useAppStore((s) => s.setSearchQuery);
  const clearSearchQuery = useAppStore((s) => s.clearSearchQuery);
  const allDataList = useAppStore((s) => s.allDataList);
  const signalCount = useAppStore((s) => s.allDataList.length);
  const dataSourceStatus = useAppStore((s) => s.dataSourceStatus);
  const backendStatus = useAppStore((s) => s.backendStatus);
  const accountSnapshot = useAppStore((s) => s.accountSnapshot);
  const positionSnapshot = useAppStore((s) => s.positionSnapshot);
  const selectedBubbleId = useAppStore((s) => s.selectedBubbleId);
  const selectedGroupId = useAppStore((s) => s.selectedGroupId);
  const positionCount = positionSnapshot?.count ?? 0;
  const checkedSet = new Set(checkedTimeframes);
  const checkedExchangeSet = new Set(checkedExchanges);
  const heartbeatStale = dataSourceStatus.heartbeatStale === true;
  const heartbeatMissedCycles = dataSourceStatus.heartbeatMissedCycles ?? 0;
  const pongStatus = dataSourceStatus.lastPong;
  const wsConnectedAt = dataSourceStatus.wsConnectedAt;
  const searchInputRef = useRef<HTMLInputElement | null>(null);
  const [showShortcutModal, setShowShortcutModal] = useState(false);

  const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key !== "Escape") return;
    event.preventDefault();
    clearSearchQuery();
    searchInputRef.current?.blur();
  };

  const connectionDurationText = useMemo(() => {
    if (dataSourceStatus.mode !== "restws") return "";
    if (dataSourceStatus.wsStatus !== "open") return "";
    if (typeof wsConnectedAt !== "number" || !Number.isFinite(wsConnectedAt)) return "";
    const nowMs =
      typeof dataSourceStatus.lastPingAt === "number" && Number.isFinite(dataSourceStatus.lastPingAt)
        ? dataSourceStatus.lastPingAt
        : Date.now();
    return `(${formatConnectionDuration(nowMs - wsConnectedAt)})`;
  }, [dataSourceStatus.lastPingAt, dataSourceStatus.mode, dataSourceStatus.wsStatus, wsConnectedAt]);

  const connectionText = useMemo(() => {
    if (dataSourceStatus.mode === "mock") {
      return "Mock 数据源";
    }
    if (dataSourceStatus.wsStatus === "open") {
      const statusText = heartbeatStale ? `WS 心跳超时(${heartbeatMissedCycles}s)` : "WS 已连接";
      return connectionDurationText ? `${statusText} ${connectionDurationText}` : statusText;
    }
    if (dataSourceStatus.wsStatus === "connecting") {
      return "WS 连接中";
    }
    if (dataSourceStatus.wsStatus === "closed") {
      return "WS 已断开";
    }
    if (dataSourceStatus.wsStatus === "error") {
      return "WS 异常";
    }
    return "等待连接";
  }, [connectionDurationText, dataSourceStatus.mode, dataSourceStatus.wsStatus, heartbeatStale, heartbeatMissedCycles]);

  const positionCountsByExchange = useMemo(() => {
    const counts = new Map<string, number>();
    const positions = positionSnapshot?.positions;
    if (!Array.isArray(positions) || positions.length === 0) return counts;
    for (const item of positions) {
      const key = normalizeExchange(item.exchange);
      if (!key) continue;
      counts.set(key, (counts.get(key) || 0) + 1);
    }
    return counts;
  }, [positionSnapshot?.positions]);

  const armedCountsByTimeframe = useMemo(() => {
    const counts = new Map<string, Set<string>>();
    if (!Array.isArray(allDataList) || allDataList.length === 0) return counts;
    const selectedExchangeSet = new Set(checkedExchanges.map((exchange) => normalizeExchange(exchange)));
    for (const item of allDataList) {
      const timeframe = (item.timeframe || "").trim();
      const exchange = normalizeExchange(item.exchange);
      if (!timeframe || !exchange) continue;
      if (!selectedExchangeSet.has(exchange)) continue;
      if (!isArmedAction(item.action)) continue;
      const ids = counts.get(timeframe) || new Set<string>();
      ids.add(item.id);
      counts.set(timeframe, ids);
    }
    return counts;
  }, [allDataList, checkedExchanges]);
  const [armedSeenByTimeframe, setArmedSeenByTimeframe] = useState<Map<string, Set<string>>>(new Map());

  useEffect(() => {
    const selectedTimeframe = checkedTimeframes[0]?.trim() || "";
    setArmedSeenByTimeframe((previous) => {
      const next = cloneSetMap(previous);
      const timeframes = new Set<string>([
        ...Array.from(previous.keys()),
        ...Array.from(armedCountsByTimeframe.keys())
      ]);
      for (const timeframe of timeframes) {
        const currentIds = armedCountsByTimeframe.get(timeframe);
        if (!currentIds || currentIds.size === 0) {
          next.delete(timeframe);
          continue;
        }
        const seenIds = next.get(timeframe);
        if (seenIds && seenIds.size > 0) {
          const retained = new Set<string>();
          for (const id of seenIds) {
            if (currentIds.has(id)) retained.add(id);
          }
          if (retained.size > 0) {
            next.set(timeframe, retained);
          } else {
            next.delete(timeframe);
          }
        }
      }
      if (selectedTimeframe) {
        const currentIds = armedCountsByTimeframe.get(selectedTimeframe);
        if (currentIds && currentIds.size > 0) {
          next.set(selectedTimeframe, new Set(currentIds));
        } else {
          next.delete(selectedTimeframe);
        }
      }
      if (setMapEquals(previous, next)) return previous;
      return next;
    });
  }, [armedCountsByTimeframe, checkedTimeframes]);

  const statusTone = useMemo(() => {
    if (dataSourceStatus.mode === "mock") return "idle";
    if (dataSourceStatus.wsStatus === "open") {
      return heartbeatStale ? "warn" : "ok";
    }
    if (dataSourceStatus.wsStatus === "connecting") {
      return "warn";
    }
    // idle / closed / error 统一视为未连接。
    return "error";
  }, [dataSourceStatus.mode, dataSourceStatus.wsStatus, heartbeatStale]);

  const tooltipGroups = useMemo(() => {
    const runtimeHuman = backendStatus?.runtimeHuman || pongStatus?.runtimeHuman || "--";
    const uuid = backendStatus?.singletonUuid || pongStatus?.singletonUuid || "--";
    const rtt = typeof pongStatus?.rttMs === "number" ? `${pongStatus.rttMs} ms` : "--";
    return [
      [
        { label: "连接状态", value: connectionText },
        { label: "心跳状态", value: heartbeatStale ? "超时" : "正常" },
        { label: "RTT", value: rtt }
      ],
      [
        { label: "Tag", value: backendStatus?.versionTag || "--" },
        { label: "Commit", value: backendStatus?.versionCommit || "--" },
        { label: "Build time", value: backendStatus?.buildTime || "--" },
        { label: "UUID", value: uuid },
        { label: "运行时长", value: runtimeHuman }
      ],
      [
        { label: "交易所", value: accountSnapshot?.exchange || "--" },
        { label: "资金帐户", value: `${formatAmount(accountSnapshot?.fundingUsdt)} USDT` },
        { label: "交易帐户", value: `${formatAmount(accountSnapshot?.tradingUsdt)} USDT` },
        { label: "交易收益", value: `${formatAmount(accountSnapshot?.dailyProfitUsdt)} USDT` },
        { label: "单笔资金", value: `${formatAmount(accountSnapshot?.perTradeUsdt)} USDT` }
      ],
      [
        { label: "持仓数量", value: String(positionCount) },
        { label: "信号数量", value: String(signalCount) }
      ]
    ];
  }, [accountSnapshot, backendStatus, connectionText, heartbeatStale, pongStatus, positionCount, signalCount]);

  const exchangeDisabled = (value: string) => {
    const target = exchangeTabs.find((tab) => tab.value === value);
    return !target || !target.active;
  };

  const activeTimeframes = useMemo(
    () => timeframeTabs.filter((tab) => tab.active).map((tab) => tab.value),
    [timeframeTabs]
  );

  useEffect(() => {
    const onKeyDown = (event: globalThis.KeyboardEvent) => {
      if (showShortcutModal && event.key === "Escape") {
        event.preventDefault();
        setShowShortcutModal(false);
        return;
      }
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      if (
        event.key === "?" &&
        !shouldIgnoreTopbarShortcut(event) &&
        !selectedBubbleId &&
        !selectedGroupId
      ) {
        event.preventDefault();
        setShowShortcutModal(true);
        return;
      }
      if (
        event.shiftKey &&
        (event.key === "S" || event.key === "s") &&
        !shouldIgnoreTopbarShortcut(event) &&
        !selectedBubbleId &&
        !selectedGroupId
      ) {
        event.preventDefault();
        searchInputRef.current?.focus();
        searchInputRef.current?.select();
        return;
      }
      if (event.shiftKey) return;
      if (shouldIgnoreTopbarShortcut(event)) return;
      if (selectedBubbleId || selectedGroupId) return;
      if (event.key !== "[" && event.key !== "]") return;
      if (activeTimeframes.length <= 1) return;

      const selectedTimeframe = checkedTimeframes[0] || activeTimeframes[0];
      const currentIndex = Math.max(0, activeTimeframes.indexOf(selectedTimeframe));
      const nextIndex =
        event.key === "["
          ? (currentIndex - 1 + activeTimeframes.length) % activeTimeframes.length
          : (currentIndex + 1) % activeTimeframes.length;
      const nextTimeframe = activeTimeframes[nextIndex];
      if (!nextTimeframe || nextTimeframe === selectedTimeframe) return;

      event.preventDefault();
      toggleTimeframeChecked(nextTimeframe);
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [activeTimeframes, checkedTimeframes, selectedBubbleId, selectedGroupId, showShortcutModal, toggleTimeframeChecked]);

  return (
    <>
      <div className="topbar">
        <div className="topbar__group topbar__group--left">
          <div className="topbar__title">Bubbles</div>
          <div className="timeframe-tabs" role="group" aria-label="时间标签筛选">
            {timeframeTabs.length === 0 ? (
              <div className="timeframe-tab timeframe-tab--disabled">暂无信号</div>
            ) : (
              timeframeTabs.map((tab) => {
                const selected = checkedSet.has(tab.value);
                const currentArmedIds = armedCountsByTimeframe.get(tab.value);
                const seenArmedIds = armedSeenByTimeframe.get(tab.value);
                const unreadCount =
                  selected || !currentArmedIds
                    ? 0
                    : Array.from(currentArmedIds).filter((id) => !seenArmedIds?.has(id)).length;
                const armedBadgeText = formatPositionBadgeCount(unreadCount);
                return (
                  <label
                    key={tab.value}
                    className={`timeframe-tab ${selected ? "timeframe-tab--active" : ""} ${!tab.active ? "timeframe-tab--disabled" : ""}`}
                  >
                    <input
                      type="radio"
                      className="timeframe-tab__checkbox"
                      name="timeframe-filter"
                      disabled={!tab.active}
                      checked={selected}
                      onChange={() => toggleTimeframeChecked(tab.value)}
                    />
                    <span>{tab.value}</span>
                    {armedBadgeText ? (
                      <span
                        className="timeframe-tab__position-count timeframe-tab__position-count--armed"
                        aria-hidden="true"
                      >
                        {armedBadgeText}
                      </span>
                    ) : null}
                  </label>
                );
              })
            )}

            {exchangeTabs.length > 0 ? (
              <>
                <span className="timeframe-tabs__spacer" aria-hidden="true" />
                {exchangeTabs.map((exchange) => {
                  const disabled = exchangeDisabled(exchange.value);
                  const active = checkedExchangeSet.has(exchange.value);
                  const positionBadgeCount = positionCountsByExchange.get(normalizeExchange(exchange.value)) || 0;
                  const positionBadgeText = formatPositionBadgeCount(positionBadgeCount);
                  return (
                    <label
                      key={exchange.value}
                      className={`timeframe-tab timeframe-tab--exchange ${active ? "timeframe-tab--active" : ""} ${disabled ? "timeframe-tab--disabled" : ""}`}
                    >
                      <input
                        type="checkbox"
                        className="timeframe-tab__checkbox"
                        disabled={disabled}
                        checked={active}
                        onChange={() => toggleExchangeChecked(exchange.value)}
                      />
                      <span>{exchangeLabel(exchange.value)}</span>
                      {positionBadgeText ? (
                        <span className="timeframe-tab__position-count" aria-hidden="true">
                          {positionBadgeText}
                        </span>
                      ) : null}
                    </label>
                  );
                })}
              </>
            ) : null}
          </div>
        </div>
        <div className="topbar__group topbar__group--right">
          <div className="search topbar__search">
            <input
              ref={searchInputRef}
              type="text"
              placeholder="输入 BTC / ETH 等关键词"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              onKeyDown={handleKeyDown}
            />
          </div>
          <button
            type="button"
            className="topbar-help-btn"
            onClick={() => setShowShortcutModal(true)}
            title="快捷键帮助"
            aria-label="快捷键帮助"
          >
            ?
          </button>
          <div
            className={`status-indicator status-indicator--${statusTone}`}
            role="status"
            tabIndex={0}
            aria-label={`后端连接状态：${connectionText}`}
          >
            <span className="status-indicator__dot" />
            <div className="status-tooltip" role="tooltip">
              {tooltipGroups.map((group, groupIndex) => (
                <div key={`status-group-${groupIndex}`}>
                  {group.map((row) => (
                    <div key={row.label}>
                      {row.label}：{row.value}
                    </div>
                  ))}
                  {groupIndex < tooltipGroups.length - 1 ? (
                    <div className="status-tooltip__divider" aria-hidden="true" />
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {showShortcutModal ? (
        <div className="bubbles-shortcut-modal-backdrop" onClick={() => setShowShortcutModal(false)}>
          <div className="bubbles-shortcut-modal" onClick={(event) => event.stopPropagation()}>
            <header className="bubbles-shortcut-modal-header">
              <span>Keyboard Shortcuts & Help</span>
              <button
                type="button"
                className="bubbles-shortcut-close-btn"
                onClick={() => setShowShortcutModal(false)}
              >
                x
              </button>
            </header>
            <div className="bubbles-shortcut-modal-body custom-scrollbar">
              <section className="bubbles-shortcut-section">
                <div className="bubbles-shortcut-section-title">Keyboard Shortcuts</div>
                <table className="bubbles-shortcut-table">
                  <thead>
                    <tr>
                      <th>功能</th>
                      <th>快捷键</th>
                      <th>说明</th>
                    </tr>
                  </thead>
                  <tbody>
                    {BUBBLE_SHORTCUT_HELP_ROWS.map((row) => (
                      <tr key={`${row.action}-${row.key}`}>
                        <td>{row.action}</td>
                        <td>{row.key}</td>
                        <td>{row.description}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>
              <section className="bubbles-shortcut-section">
                <div className="bubbles-shortcut-section-title">说明</div>
                <div className="bubbles-shortcut-notes">
                  <div>1. 帮助按钮位于搜索框和状态指示灯之间，和 visual-history 的帮助入口保持一致。</div>
                  <div>2. 交易所筛选当前仍是多选；搜索只在“当前时间周期 + 已选交易所”结果集内生效。</div>
                  <div>3. 输入框、可编辑区域和详情菜单交互中不会响应 `?` 全局帮助快捷键。</div>
                  <div>4. 帮助窗口打开后，按 `Esc` 会优先关闭帮助窗口。</div>
                </div>
              </section>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
});
