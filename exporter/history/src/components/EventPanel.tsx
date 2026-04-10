import { useEffect, useMemo, useState } from "react";
import type { HistoryEvent } from "../types";

interface EventPanelProps {
  events: HistoryEvent[];
  expanded: boolean;
  onToggle: () => void;
}

export function EventPanel(props: EventPanelProps): JSX.Element {
  const { events, expanded, onToggle } = props;
  const [filter, setFilter] = useState<string>("ALL");
  const [visibleCount, setVisibleCount] = useState<number>(200);

  const filtered = useMemo(() => {
    if (filter === "ALL") return events;
    return events.filter((item) => item.type === filter);
  }, [events, filter]);

  useEffect(() => {
    setVisibleCount(200);
  }, [filter, events]);

  const visibleEvents = useMemo(() => {
    if (filtered.length <= visibleCount) {
      return filtered;
    }
    return filtered.slice(filtered.length - visibleCount);
  }, [filtered, visibleCount]);

  const eventTypes = useMemo(() => {
    const set = new Set<string>();
    for (const item of events) {
      set.add(item.type);
    }
    return ["ALL", ...Array.from(set).sort()];
  }, [events]);

  return (
    <section className={expanded ? "vh-event-panel" : "vh-event-panel is-collapsed"}>
      <button className="vh-event-toggle" type="button" onClick={onToggle}>
        <span className="vh-badge-dot" />
        <span>Event Panel</span>
        <span className="vh-count-pill">{events.length}</span>
        <span className="vh-event-toggle-icon">{expanded ? "▾" : "▸"}</span>
      </button>

      {expanded ? (
        <>
          <header className="vh-event-header">
            <span>Position Events</span>
            <select value={filter} onChange={(event) => setFilter(event.target.value)}>
              {eventTypes.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </header>

          <div className="vh-event-list custom-scrollbar">
            {filtered.length === 0 ? <div className="vh-loading-row">当前无事件</div> : null}
            {filtered.length > 0 ? (
              <div className="vh-event-table">
                {visibleEvents.map((event) => {
                  const summaryText = event.summary || "-";
                  const metaText = formatEventMeta(event);
                  return (
                    <article key={event.id} className={`vh-event-row level-${event.level}`}>
                      <span className="vh-event-cell vh-event-cell-time">{formatTime(event.event_at_ms)}</span>
                      <span className="vh-event-cell vh-event-cell-type">
                        <span className="vh-event-type-pill">{event.type}</span>
                      </span>
                      <span className="vh-event-cell vh-event-cell-title" title={event.title}>
                        {event.title}
                      </span>
                      <span className="vh-event-cell vh-event-cell-summary" title={summaryText}>
                        {summaryText}
                      </span>
                      <span className="vh-event-cell vh-event-cell-meta" title={metaText}>
                        {metaText}
                      </span>
                    </article>
                  );
                })}
              </div>
            ) : null}
            {visibleEvents.length < filtered.length ? (
              <button
                className="vh-more-btn"
                type="button"
                onClick={() => setVisibleCount((prev) => Math.min(filtered.length, prev + 200))}
              >
                加载更多事件（剩余 {filtered.length - visibleEvents.length}）
              </button>
            ) : null}
          </div>
        </>
      ) : null}
    </section>
  );
}

function formatTime(ts: number): string {
  if (!ts) return "--";
  const date = new Date(ts);
  const hh = String(date.getHours()).padStart(2, "0");
  const mm = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

const DETAIL_KEY_PRIORITY = [
  "timeframe",
  "action",
  "high_side",
  "mid_side",
  "entry_price",
  "sl_price",
  "tp_price",
  "price",
  "result_status",
  "order_type",
  "position_side",
  "quantity",
  "realized_pnl",
];

function formatEventMeta(event: HistoryEvent): string {
  const detail = event.detail;
  if (!detail) return "-";
  const allowed = Object.entries(detail).filter(([key, value]) => {
    if (/_json$/i.test(key)) {
      return false;
    }
    if (value == null) {
      return false;
    }
    if (typeof value === "object") {
      return false;
    }
    if (typeof value === "string" && (value.length === 0 || value.length > 120)) {
      return false;
    }
    return true;
  });
  const picked: Array<[string, unknown]> = [];
  const used = new Set<string>();
  for (const key of DETAIL_KEY_PRIORITY) {
    const found = allowed.find(([entryKey]) => entryKey === key);
    if (!found || used.has(found[0])) {
      continue;
    }
    picked.push(found);
    used.add(found[0]);
    if (picked.length >= 3) {
      break;
    }
  }
  for (const entry of allowed) {
    if (used.has(entry[0])) {
      continue;
    }
    picked.push(entry);
    if (picked.length >= 3) {
      break;
    }
  }
  if (picked.length === 0) {
    return "-";
  }
  return picked.map(([key, value]) => `${key}=${String(value)}`).join(" · ");
}
