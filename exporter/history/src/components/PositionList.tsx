import { useEffect, useMemo, useRef, useState } from "react";
import type { HistoryPosition, PositionFilterOptions, PositionRunOption } from "../types";

const MOBILE_LAYOUT_QUERY = "(max-width: 980px)";

interface PositionListProps {
  positions: HistoryPosition[];
  selectedPosition: HistoryPosition | null;
  selectedDate: string;
  selectedRunID: string;
  selectedStrategy: string;
  selectedVersion: string;
  selectedExchange: string;
  selectedSymbol: string;
  filterOptions: PositionFilterOptions;
  loading: boolean;
  chartLoading: boolean;
  loadedPositionUID: string;
  hasMore: boolean;
  onDateChange: (date: string) => void;
  onRunIDChange: (runID: string) => void;
  onStrategyChange: (strategy: string) => void;
  onVersionChange: (version: string) => void;
  onExchangeChange: (exchange: string) => void;
  onSymbolChange: (symbol: string) => void;
  onSelectPosition: (position: HistoryPosition) => void;
  onLoadChart: (position: HistoryPosition) => void;
  onLoadMore: () => void;
  onRefresh: () => void;
}

export function PositionList(props: PositionListProps): JSX.Element {
  const {
    positions,
    selectedPosition,
    selectedDate,
    selectedRunID,
    selectedStrategy,
    selectedVersion,
    selectedExchange,
    selectedSymbol,
    filterOptions,
    loading,
    chartLoading,
    loadedPositionUID,
    hasMore,
    onDateChange,
    onRunIDChange,
    onStrategyChange,
    onVersionChange,
    onExchangeChange,
    onSymbolChange,
    onSelectPosition,
    onLoadChart,
    onLoadMore,
    onRefresh
  } = props;

  const summary = useMemo(() => {
    let pnl = 0;
    let win = 0;
    let count = 0;
    for (const item of positions) {
      pnl += item.realized_pnl;
      count += 1;
      if (item.realized_pnl > 0) {
        win += 1;
      }
    }
    const winRate = count > 0 ? (win / count) * 100 : 0;
    return { pnl, winRate, count };
  }, [positions]);

  const [dateInputText, setDateInputText] = useState<string>(displayDateTimeInputValue(selectedDate, "00:00:00"));
  const [timeInputText, setTimeInputText] = useState<string>("00:00:00");
  const [pickerMode, setPickerMode] = useState<"date" | "time" | null>(null);
  const [datePickerValue, setDatePickerValue] = useState<string>(selectedDate);
  const [timePickerValue, setTimePickerValue] = useState<string>("00:00:00");
  const [isMobileLayout, setIsMobileLayout] = useState<boolean>(() => isMobileViewport());
  const [mobileFiltersExpanded, setMobileFiltersExpanded] = useState<boolean>(() => !isMobileViewport());
  const dateInputRef = useRef<HTMLInputElement | null>(null);
  const dateControlRef = useRef<HTMLDivElement | null>(null);
  const datePickerRef = useRef<HTMLInputElement | null>(null);
  const timePickerRef = useRef<HTMLInputElement | null>(null);
  const activeFilterCount = countActiveFilters(
    selectedRunID,
    selectedStrategy,
    selectedVersion,
    selectedExchange,
    selectedSymbol
  );

  useEffect(() => {
    const normalizedDate = parseDateInputText(selectedDate) || "";
    const normalizedTime = parseTimeInputText(timeInputText) || "00:00:00";
    setDateInputText(displayDateTimeInputValue(normalizedDate, normalizedTime));
    setDatePickerValue(normalizedDate);
    setTimePickerValue(normalizedTime);
  }, [selectedDate, timeInputText]);

  useEffect(() => {
    if (!pickerMode) {
      return;
    }
    const onPointerDown = (event: MouseEvent) => {
      const root = dateControlRef.current;
      if (!root) {
        return;
      }
      if (root.contains(event.target as Node)) {
        return;
      }
      setPickerMode(null);
    };
    document.addEventListener("mousedown", onPointerDown);
    return () => document.removeEventListener("mousedown", onPointerDown);
  }, [pickerMode]);

  useEffect(() => {
    if (pickerMode === "date" && datePickerRef.current) {
      const input = datePickerRef.current;
      window.requestAnimationFrame(() => {
        input.focus({ preventScroll: true });
        openNativePicker(input);
      });
      return;
    }
    if (pickerMode === "time" && timePickerRef.current) {
      const input = timePickerRef.current;
      window.requestAnimationFrame(() => {
        input.focus({ preventScroll: true });
        openNativePicker(input);
      });
    }
  }, [pickerMode]);

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
      return;
    }
    const media = window.matchMedia(MOBILE_LAYOUT_QUERY);
    const sync = (): void => {
      setIsMobileLayout(media.matches);
    };
    sync();
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", sync);
      return () => media.removeEventListener("change", sync);
    }
    media.addListener(sync);
    return () => media.removeListener(sync);
  }, []);

  useEffect(() => {
    if (!isMobileLayout) {
      setMobileFiltersExpanded(true);
    }
  }, [isMobileLayout]);

  return (
    <aside className={isMobileLayout ? "vh-position-panel is-mobile-layout" : "vh-position-panel"}>
      <header className={isMobileLayout ? "vh-position-header is-sticky" : "vh-position-header"}>
        <div className="vh-position-title-row">
          <span className="vh-badge-dot" />
          <h2>Historical Positions</h2>
          <span className="vh-count-pill">{positions.length}</span>
        </div>
        <div className="vh-position-filter-row">
          <div className="vh-date-input-wrap" ref={dateControlRef}>
            <input
              ref={dateInputRef}
              type="text"
              inputMode="numeric"
              placeholder="yyyy/mm/dd hh:mm:ss"
              value={dateInputText}
              readOnly
              onFocus={(event) => {
                const segment = resolveDateTimeSegment(event.currentTarget.selectionStart || 0);
                selectDateTimeSegment(event.currentTarget, segment);
              }}
              onClick={(event) => {
                const segment = resolveDateTimeSegment(event.currentTarget.selectionStart || 0);
                selectDateTimeSegment(event.currentTarget, segment);
                if (isTimeSegment(segment)) {
                  setPickerMode("time");
                } else {
                  setPickerMode("date");
                }
              }}
              onBlur={(event) => {
                const root = dateControlRef.current;
                const nextFocus = event.relatedTarget;
                if (root && nextFocus instanceof Node && root.contains(nextFocus)) {
                  return;
                }
                const parsed = parseDateTimeInputText(dateInputText);
                if (parsed) {
                  const display = displayDateTimeInputValue(parsed.date, parsed.time);
                  setDateInputText(display);
                  setDatePickerValue(parsed.date);
                  setTimeInputText(parsed.time);
                  setTimePickerValue(parsed.time);
                  if (parsed.date !== selectedDate) {
                    onDateChange(parsed.date);
                  }
                  return;
                }
                setDateInputText(displayDateTimeInputValue(selectedDate, timeInputText || "00:00:00"));
              }}
              onKeyDown={(event) => {
                const input = event.currentTarget;
                if (event.key === "Backspace" || event.key === "Delete") {
                  const segment = resolveDateTimeSegment(input.selectionStart || 0);
                  event.preventDefault();
                  const nextText = replaceSegmentWithPlaceholder(dateInputText, segment);
                  setDateInputText(nextText);
                  window.requestAnimationFrame(() => {
                    selectDateTimeSegment(input, segment);
                  });
                  return;
                }
                if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
                  const current = resolveDateTimeSegment(input.selectionStart || 0);
                  const next = event.key === "ArrowLeft" ? prevDateTimeSegment(current) : nextDateTimeSegment(current);
                  event.preventDefault();
                  selectDateTimeSegment(input, next);
                  return;
                }
                if (/^\d$/.test(event.key)) {
                  const segment = resolveDateTimeSegment(input.selectionStart || 0);
                  const nextState = writeDigitIntoSegment(dateInputText, segment, event.key);
                  event.preventDefault();
                  setDateInputText(nextState.text);
                  const parsed = parseDateTimeInputText(nextState.text);
                  if (parsed) {
                    setDatePickerValue(parsed.date);
                    setTimeInputText(parsed.time);
                    setTimePickerValue(parsed.time);
                    onDateChange(parsed.date);
                  }
                  window.requestAnimationFrame(() => {
                    selectDateTimeRange(input, nextState.selectionStart, nextState.selectionEnd);
                  });
                  return;
                }
                if (event.key === "/" || event.key === ":" || event.key === " ") {
                  const current = resolveDateTimeSegment(input.selectionStart || 0);
                  event.preventDefault();
                  selectDateTimeSegment(input, nextDateTimeSegment(current));
                  return;
                }
                if (event.key === "Enter") {
                  const segment = resolveDateTimeSegment(input.selectionStart || 0);
                  event.preventDefault();
                  setPickerMode(isTimeSegment(segment) ? "time" : "date");
                }
              }}
              onPaste={(event) => {
                const pasted = event.clipboardData.getData("text");
                const parsed = parseDateTimeInputText(pasted);
                if (!parsed) {
                  return;
                }
                event.preventDefault();
                setDateInputText(displayDateTimeInputValue(parsed.date, parsed.time));
                setDatePickerValue(parsed.date);
                setTimeInputText(parsed.time);
                setTimePickerValue(parsed.time);
                onDateChange(parsed.date);
                window.requestAnimationFrame(() => {
                  if (!dateInputRef.current) {
                    return;
                  }
                  selectDateTimeSegment(dateInputRef.current, "second");
                });
              }}
            />
            {pickerMode === "date" ? (
              <div className="vh-datetime-popover">
                <input
                  ref={datePickerRef}
                  type="date"
                  value={datePickerValue}
                  autoFocus
                  onBlur={(event) => {
                    const root = dateControlRef.current;
                    const nextFocus = event.relatedTarget;
                    if (root && nextFocus instanceof Node && root.contains(nextFocus)) {
                      return;
                    }
                    setPickerMode(null);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Escape") {
                      event.preventDefault();
                      setPickerMode(null);
                    }
                  }}
                  onChange={(event) => {
                    const nextDate = parseDateInputText(event.target.value);
                    if (!nextDate) {
                      return;
                    }
                    const currentTime = parseTimeInputText(timeInputText) || "00:00:00";
                    setDatePickerValue(nextDate);
                    setDateInputText(displayDateTimeInputValue(nextDate, currentTime));
                    onDateChange(nextDate);
                    setPickerMode(null);
                    window.requestAnimationFrame(() => {
                      if (!dateInputRef.current) {
                        return;
                      }
                      dateInputRef.current.focus();
                      selectDateTimeSegment(dateInputRef.current, "day");
                    });
                  }}
                />
              </div>
            ) : null}
            {pickerMode === "time" ? (
              <div className="vh-datetime-popover">
                <input
                  ref={timePickerRef}
                  type="time"
                  step={1}
                  value={timePickerValue}
                  autoFocus
                  onBlur={(event) => {
                    const root = dateControlRef.current;
                    const nextFocus = event.relatedTarget;
                    if (root && nextFocus instanceof Node && root.contains(nextFocus)) {
                      return;
                    }
                    setPickerMode(null);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Escape") {
                      event.preventDefault();
                      setPickerMode(null);
                    }
                  }}
                  onChange={(event) => {
                    const nextTime = parseTimeInputText(event.target.value);
                    if (!nextTime) {
                      return;
                    }
                    const nextDate = parseDateInputText(dateInputText) || parseDateInputText(selectedDate);
                    if (!nextDate) {
                      return;
                    }
                    setTimePickerValue(nextTime);
                    setTimeInputText(nextTime);
                    setDateInputText(displayDateTimeInputValue(nextDate, nextTime));
                    setPickerMode(null);
                    window.requestAnimationFrame(() => {
                      if (!dateInputRef.current) {
                        return;
                      }
                      dateInputRef.current.focus();
                      selectDateTimeSegment(dateInputRef.current, "second");
                    });
                  }}
                />
              </div>
            ) : null}
          </div>
          <button className="vh-icon-btn" type="button" onClick={onRefresh} disabled={loading}>
            ↻
          </button>
        </div>
        {isMobileLayout ? (
          <button
            className={mobileFiltersExpanded ? "vh-mobile-filter-toggle is-expanded" : "vh-mobile-filter-toggle"}
            type="button"
            onClick={() => setMobileFiltersExpanded((prev) => !prev)}
            aria-expanded={mobileFiltersExpanded}
          >
            <span className="vh-mobile-filter-toggle-label">Filters</span>
            {activeFilterCount > 0 ? <span className="vh-mobile-filter-toggle-pill">{activeFilterCount}</span> : null}
            <span className="vh-mobile-filter-toggle-icon">{mobileFiltersExpanded ? "▾" : "▸"}</span>
          </button>
        ) : null}
        <div
          className={
            isMobileLayout && !mobileFiltersExpanded
              ? "vh-position-filter-stack is-collapsed"
              : "vh-position-filter-stack"
          }
        >
          <div className="vh-filter-field">
            <label htmlFor="vh-position-run-id">Run ID</label>
            <select id="vh-position-run-id" value={selectedRunID} onChange={(event) => onRunIDChange(event.target.value)}>
              <option value="">All</option>
              {buildRunSelectOptions(filterOptions.run_options, filterOptions.run_ids, selectedRunID).map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </div>
          <div className="vh-position-filter-row vh-position-filter-row-two-columns">
            <div className="vh-filter-field">
              <label htmlFor="vh-position-strategy">Strategy</label>
              <select id="vh-position-strategy" value={selectedStrategy} onChange={(event) => onStrategyChange(event.target.value)}>
                <option value="">All</option>
                {buildSelectOptions(filterOptions.strategies, selectedStrategy).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div className="vh-filter-field">
              <label htmlFor="vh-position-version">Version</label>
              <select id="vh-position-version" value={selectedVersion} onChange={(event) => onVersionChange(event.target.value)}>
                <option value="">All</option>
                {buildSelectOptions(filterOptions.versions, selectedVersion).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
          </div>
          <div className="vh-position-filter-row vh-position-filter-row-two-columns">
            <div className="vh-filter-field">
              <label htmlFor="vh-position-exchange">Exchange</label>
              <select id="vh-position-exchange" value={selectedExchange} onChange={(event) => onExchangeChange(event.target.value)}>
                <option value="">All</option>
                {buildSelectOptions(filterOptions.exchanges, selectedExchange).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div className="vh-filter-field">
              <label htmlFor="vh-position-symbol">Symbol</label>
              <select id="vh-position-symbol" value={selectedSymbol} onChange={(event) => onSymbolChange(event.target.value)}>
                <option value="">All</option>
                {buildSelectOptions(filterOptions.symbols, selectedSymbol).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </div>
      </header>

      <section className="vh-position-summary">
        <div>
          <span>PnL</span>
          <strong className={summary.pnl >= 0 ? "text-green" : "text-red"}>{formatSigned(summary.pnl)} USDT</strong>
        </div>
        <div>
          <span>胜率</span>
          <strong>{summary.winRate.toFixed(1)}%</strong>
        </div>
        <div>
          <span>COUNT</span>
          <strong>{summary.count}</strong>
        </div>
      </section>

      <div className="vh-position-list custom-scrollbar">
        {loading ? <div className="vh-loading-row">加载仓位中...</div> : null}
        {!loading && positions.length === 0 ? <div className="vh-loading-row">当前时间范围无历史仓位</div> : null}

        {positions.map((item) => {
          const isSelected = positionUIDOf(selectedPosition) === positionUIDOf(item);
          const isLoaded = loadedPositionUID === positionUIDOf(item);
          const sideLabel = item.position_side.toUpperCase();
          return (
            <article
              key={positionUIDOf(item) || item.position_key}
              className={isSelected ? "vh-position-card is-selected" : "vh-position-card"}
              onClick={() => onSelectPosition(item)}
            >
              <div className="vh-position-row1">
                <strong>{item.symbol}</strong>
                <span className={item.position_side.toLowerCase() === "long" ? "side-pill long" : "side-pill short"}>{sideLabel}</span>
                <span className="leverage-pill">{item.leverage.toFixed(0)}x</span>
              </div>
              <div className="vh-position-row2">
                <span className="vh-exchange-text">{item.exchange}</span>
                <span className="vh-strategy-text">{formatStrategyLabel(item.strategy_name, item.strategy_version)}</span>
                <span className={item.realized_pnl >= 0 ? "text-green vh-pnl-text" : "text-red vh-pnl-text"}>
                  {formatPnLWithRatio(item.realized_pnl, item.pnl_ratio)}
                </span>
              </div>
              <div className="vh-position-row3">
                <span className="vh-range-text">
                  {formatDateTimeRange(item.open_time_ms, item.close_time_ms)}
                </span>
                <span className="vh-duration-text">
                  {formatHoldDuration(item.open_time_ms, item.close_time_ms)}
                </span>
              </div>

              {isSelected ? (
                <div className="vh-position-expanded">
                  <div className="vh-position-grid">
                    <div>
                      <span>开仓价</span>
                      <strong>{item.entry_price.toFixed(2)}</strong>
                    </div>
                    <div>
                      <span>平仓价</span>
                      <strong>{item.exit_price.toFixed(2)}</strong>
                    </div>
                    <div>
                      <span>数量</span>
                      <strong>{item.quantity.toFixed(4)}</strong>
                    </div>
                    <div>
                      <span>周期</span>
                      <strong>{item.timeframes.join(" / ")}</strong>
                    </div>
                  </div>
                  <button
                    type="button"
                    className={isLoaded ? "vh-load-btn is-loaded" : "vh-load-btn"}
                    disabled={chartLoading}
                    onClick={(event) => {
                      event.stopPropagation();
                      onLoadChart(item);
                    }}
                  >
                    {chartLoading ? "加载中..." : isLoaded ? "已加载 K 线" : "加载 K 线"}
                  </button>
                </div>
              ) : null}
            </article>
          );
        })}

        {hasMore ? (
          <button className="vh-more-btn" type="button" onClick={onLoadMore}>
            加载更早仓位
          </button>
        ) : null}
      </div>
    </aside>
  );
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

function formatDateTime(ts: number): string {
  if (!ts) return "--";
  const d = new Date(ts);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  return `${yyyy}/${mm}/${dd} ${hh}:${mi}`;
}

function formatDateTimeRange(openTS: number, closeTS: number): string {
  return `${formatDateTime(openTS)} - ${formatDateTime(closeTS)}`;
}

function formatHoldDuration(openTS: number, closeTS: number): string {
  if (!openTS) {
    return "--";
  }
  const endTS = closeTS > 0 ? closeTS : openTS;
  if (endTS < openTS) {
    return "--";
  }
  let seconds = Math.floor((endTS - openTS) / 1000);
  if (seconds < 0) {
    seconds = 0;
  }
  const day = Math.floor(seconds / 86400);
  seconds %= 86400;
  const hour = Math.floor(seconds / 3600);
  seconds %= 3600;
  const minute = Math.floor(seconds / 60);
  const second = seconds % 60;

  if (day > 0) {
    return `${day}d${hour}h${minute}m${second}s`;
  }
  if (hour > 0) {
    return `${hour}h${minute}m${second}s`;
  }
  if (minute > 0) {
    return `${minute}m${second}s`;
  }
  return `${second}s`;
}

function formatSigned(value: number): string {
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(3)}`;
}

function formatSignedPercent(value: number): string {
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatPnLWithRatio(pnl: number, pnlRatio: number): string {
  return `${formatSigned(pnl)} (${formatSignedPercent(pnlRatio * 100)})`;
}

function formatStrategyLabel(name: string, version: string): string {
  const strategyName = name.trim();
  const strategyVersion = version.trim();
  if (!strategyName && !strategyVersion) {
    return "--";
  }
  if (!strategyVersion) {
    return strategyName;
  }
  if (!strategyName) {
    return `-- (${strategyVersion})`;
  }
  return `${strategyName} (${strategyVersion})`;
}

type DateTimeSegment = "year" | "month" | "day" | "hour" | "minute" | "second";

type DateTimeSegmentMeta = {
  start: number;
  end: number;
  placeholder: string;
};

const DATE_PLACEHOLDER = "yyyy/mm/dd";
const TIME_PLACEHOLDER = "hh:mm:ss";
const DATE_TIME_PLACEHOLDER = `${DATE_PLACEHOLDER} ${TIME_PLACEHOLDER}`;

const DATE_TIME_SEGMENTS: Record<DateTimeSegment, DateTimeSegmentMeta> = {
  year: { start: 0, end: 4, placeholder: "yyyy" },
  month: { start: 5, end: 7, placeholder: "mm" },
  day: { start: 8, end: 10, placeholder: "dd" },
  hour: { start: 11, end: 13, placeholder: "hh" },
  minute: { start: 14, end: 16, placeholder: "mm" },
  second: { start: 17, end: 19, placeholder: "ss" }
};

function normalizeDateInputYear(value: string): string {
  const trimmed = value.trim();
  const matched = /^(\d+)-(\d{2})-(\d{2})$/.exec(trimmed);
  if (!matched) {
    return trimmed;
  }
  const year = matched[1];
  const month = matched[2];
  const day = matched[3];
  if (year.length <= 4) {
    return `${year}-${month}-${day}`;
  }
  return `${year.slice(0, 4)}-${month}-${day}`;
}

function parseDateInputText(value: string): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  const dateToken = normalized.split(" ")[0]?.trim().replace(/-/g, "/") || "";
  if (!dateToken) {
    return "";
  }
  if (/^\d{8}$/.test(dateToken)) {
    const y = dateToken.slice(0, 4);
    const m = dateToken.slice(4, 6);
    const d = dateToken.slice(6, 8);
    return normalizeDateInputYear(`${y}-${m}-${d}`);
  }
  const matched = /^(\d{4})\/(\d{2})\/(\d{2})$/.exec(dateToken);
  if (!matched) {
    return "";
  }
  const [, yearRaw, monthRaw, dayRaw] = matched;
  const year = String(yearRaw).slice(0, 4);
  const month = String(monthRaw).padStart(2, "0");
  const day = String(dayRaw).padStart(2, "0");
  const monthInt = Number(month);
  const dayInt = Number(day);
  if (!Number.isFinite(monthInt) || monthInt < 1 || monthInt > 12) {
    return "";
  }
  if (!Number.isFinite(dayInt) || dayInt < 1 || dayInt > 31) {
    return "";
  }
  return normalizeDateInputYear(`${year}-${month}-${day}`);
}

function parseTimeInputText(value: string): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  const parts = normalized.split(" ");
  const token = (parts.length >= 2 ? parts.slice(1).join(" ").trim() : normalized).trim();
  if (!token) {
    return "";
  }
  const matched = /^(\d{1,2})(?::(\d{1,2}))?(?::(\d{1,2}))?$/.exec(token);
  if (!matched) {
    return "";
  }
  const hour = Number(matched[1]);
  const minute = matched[2] == null ? 0 : Number(matched[2]);
  const second = matched[3] == null ? 0 : Number(matched[3]);
  if (!Number.isFinite(hour) || hour < 0 || hour > 23) {
    return "";
  }
  if (!Number.isFinite(minute) || minute < 0 || minute > 59) {
    return "";
  }
  if (!Number.isFinite(second) || second < 0 || second > 59) {
    return "";
  }
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}`;
}

function parseDateTimeInputText(value: string): { date: string; time: string } | null {
  const date = parseDateInputText(value);
  if (!date) {
    return null;
  }
  const time = parseTimeInputText(value) || "00:00:00";
  return { date, time };
}

function displayDateTimeInputValue(date: string, time: string): string {
  const normalizedDate = parseDateInputText(date);
  const normalizedTime = parseTimeInputText(time) || "00:00:00";
  const datePart = normalizedDate ? normalizedDate.replace(/-/g, "/") : DATE_PLACEHOLDER;
  return `${datePart} ${normalizedTime}`;
}

function countActiveFilters(
  selectedRunID: string,
  selectedStrategy: string,
  selectedVersion: string,
  selectedExchange: string,
  selectedSymbol: string
): number {
  return [
    selectedRunID,
    selectedStrategy,
    selectedVersion,
    selectedExchange,
    selectedSymbol
  ].filter((item) => item.trim().length > 0).length;
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

function resolveDateTimeSegment(caret: number): DateTimeSegment {
  if (caret <= 4) {
    return "year";
  }
  if (caret <= 7) {
    return "month";
  }
  if (caret <= 10) {
    return "day";
  }
  if (caret <= 13) {
    return "hour";
  }
  if (caret <= 16) {
    return "minute";
  }
  return "second";
}

function selectDateTimeSegment(input: HTMLInputElement, segment: DateTimeSegment): void {
  const range = dateTimeSegmentRange(segment);
  selectDateTimeRange(input, range[0], range[1]);
}

function selectDateTimeRange(input: HTMLInputElement, start: number, end: number): void {
  window.requestAnimationFrame(() => {
    try {
      input.setSelectionRange(start, end);
    } catch {
      // no-op
    }
  });
}

function dateTimeSegmentRange(segment: DateTimeSegment): [number, number] {
  const meta = DATE_TIME_SEGMENTS[segment];
  return [meta.start, meta.end];
}

function isTimeSegment(segment: DateTimeSegment): boolean {
  return segment === "hour" || segment === "minute" || segment === "second";
}

function nextDateTimeSegment(segment: DateTimeSegment): DateTimeSegment {
  switch (segment) {
    case "year":
      return "month";
    case "month":
      return "day";
    case "day":
      return "hour";
    case "hour":
      return "minute";
    case "minute":
      return "second";
    case "second":
      return "second";
    default:
      return "year";
  }
}

function prevDateTimeSegment(segment: DateTimeSegment): DateTimeSegment {
  switch (segment) {
    case "second":
      return "minute";
    case "minute":
      return "hour";
    case "hour":
      return "day";
    case "day":
      return "month";
    case "month":
      return "year";
    case "year":
      return "year";
    default:
      return "year";
  }
}

function replaceSegmentWithPlaceholder(value: string, segment: DateTimeSegment): string {
  return replaceDateTimeSegment(value, segment, DATE_TIME_SEGMENTS[segment].placeholder);
}

function writeDigitIntoSegment(
  value: string,
  segment: DateTimeSegment,
  digit: string
): { text: string; selectionStart: number; selectionEnd: number } {
  const meta = DATE_TIME_SEGMENTS[segment];
  const currentText = normalizeDisplayDateTimeText(value);
  const currentSegmentValue = currentText.slice(meta.start, meta.end);
  const chars = currentSegmentValue.split("");
  const placeholder = meta.placeholder;
  const firstPlaceholderIndex = chars.findIndex((char, index) => char === placeholder[index]);
  if (firstPlaceholderIndex >= 0) {
    chars[firstPlaceholderIndex] = digit;
  } else {
    chars.splice(0, chars.length, ...(digit + placeholder.slice(1)).split(""));
  }
  const nextSegmentValue = chars.join("");
  const nextText = replaceDateTimeSegment(currentText, segment, nextSegmentValue);
  const nextPlaceholderIndex = chars.findIndex((char, index) => char === placeholder[index]);
  if (nextPlaceholderIndex >= 0) {
    return {
      text: nextText,
      selectionStart: meta.start + nextPlaceholderIndex,
      selectionEnd: meta.end
    };
  }
  const nextSegment = nextDateTimeSegment(segment);
  const nextRange = dateTimeSegmentRange(nextSegment);
  return {
    text: nextText,
    selectionStart: nextRange[0],
    selectionEnd: nextRange[1]
  };
}

function replaceDateTimeSegment(value: string, segment: DateTimeSegment, nextSegmentValue: string): string {
  const meta = DATE_TIME_SEGMENTS[segment];
  const current = normalizeDisplayDateTimeText(value);
  return `${current.slice(0, meta.start)}${nextSegmentValue}${current.slice(meta.end)}`;
}

function normalizeDisplayDateTimeText(value: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    return DATE_TIME_PLACEHOLDER;
  }
  const raw = value.trim();
  const year = padSegment(extractSegment(raw, "year"), "year");
  const month = padSegment(extractSegment(raw, "month"), "month");
  const day = padSegment(extractSegment(raw, "day"), "day");
  const hour = padSegment(extractSegment(raw, "hour"), "hour");
  const minute = padSegment(extractSegment(raw, "minute"), "minute");
  const second = padSegment(extractSegment(raw, "second"), "second");
  return `${year}/${month}/${day} ${hour}:${minute}:${second}`;
}

function extractSegment(value: string, segment: DateTimeSegment): string {
  const normalized = value.replace(/-/g, "/");
  const [datePartRaw = "", timePartRaw = ""] = normalized.split(" ");
  const dateParts = datePartRaw.split("/");
  const timeParts = timePartRaw.split(":");
  switch (segment) {
    case "year":
      return dateParts[0] || "";
    case "month":
      return dateParts[1] || "";
    case "day":
      return dateParts[2] || "";
    case "hour":
      return timeParts[0] || "";
    case "minute":
      return timeParts[1] || "";
    case "second":
      return timeParts[2] || "";
    default:
      return "";
  }
}

function padSegment(value: string, segment: DateTimeSegment): string {
  const meta = DATE_TIME_SEGMENTS[segment];
  const digits = value.replace(/\D/g, "").slice(0, meta.placeholder.length);
  return `${digits}${meta.placeholder.slice(digits.length)}`;
}

function openNativePicker(input: HTMLInputElement): void {
  if (typeof input.showPicker === "function") {
    try {
      input.showPicker();
    } catch {
      // no-op
    }
  }
}

function buildSelectOptions(options: string[], selectedValue: string): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  const normalizedSelected = selectedValue.trim();
  if (normalizedSelected) {
    seen.add(normalizedSelected);
    out.push(normalizedSelected);
  }
  for (const item of options) {
    const normalized = item.trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

function buildRunSelectOptions(options: PositionRunOption[], legacyOptions: string[], selectedValue: string): PositionRunOption[] {
  const seen = new Set<string>();
  const out: PositionRunOption[] = [];
  for (const item of options) {
    const normalizedValue = item.value.trim();
    if (!normalizedValue || seen.has(normalizedValue)) {
      continue;
    }
    seen.add(normalizedValue);
    out.push({
      value: normalizedValue,
      label: item.label.trim() || normalizedValue,
      singleton_id: item.singleton_id
    });
  }
  for (const item of legacyOptions) {
    const normalizedValue = item.trim();
    if (!normalizedValue || seen.has(normalizedValue)) {
      continue;
    }
    seen.add(normalizedValue);
    out.push({
      value: normalizedValue,
      label: normalizedValue
    });
  }
  const normalizedSelected = selectedValue.trim();
  if (normalizedSelected && !seen.has(normalizedSelected)) {
    out.push({
      value: normalizedSelected,
      label: normalizedSelected
    });
  }
  return out;
}
