import type { CSSProperties, ReactNode } from "react";
import { memo, useEffect, useMemo, useState } from "react";
import type { PositionItem, TrendGroupCandidate, TrendGroupItem } from "../app/types";
import { useAppStore } from "../app/store";

type DetailRow = {
  label: string;
  value: string;
};

type PositionSummaryCell = {
  key: string;
  label: string;
  value: ReactNode;
  tone?: "positive" | "negative" | "neutral";
};

const KNOWN_QUOTES = ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "GBP", "JPY"] as const;
const CANDIDATE_KEY_SEPARATOR = "|";
const KNOWN_CANDIDATE_COLUMN_ORDER = [
  "candidateKey",
  "candidateState",
  "isSelected",
  "priorityScore",
  "hasOpenPosition"
] as const;

const CANDIDATE_COLUMN_LABELS: Record<string, string> = {
  candidateKey: "候选键",
  candidateState: "候选状态",
  isSelected: "已选",
  priorityScore: "评分",
  hasOpenPosition: "有持仓"
};

function formatValue(value: string | number | undefined): string {
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "--";
  if (typeof value === "string") return value.trim() || "--";
  return "--";
}

function formatNumeric(value?: number, maxFractionDigits = 6): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1000) {
    return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }
  if (abs >= 1) {
    return value.toLocaleString("en-US", { maximumFractionDigits: Math.max(2, maxFractionDigits) });
  }
  return value.toLocaleString("en-US", { maximumFractionDigits });
}

function formatStableFloatingAmount(value?: number): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1000) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  }
  if (abs >= 1) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 4,
      maximumFractionDigits: 4
    });
  }
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 6,
    maximumFractionDigits: 6
  });
}

function normalizeRatePercent(value?: number): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return Math.abs(value) <= 1 ? value * 100 : value;
}

function formatRatePercent(value?: number): string {
  const normalized = normalizeRatePercent(value);
  if (typeof normalized !== "number") return "--";
  return `${normalized.toFixed(2)}%`;
}

function formatFloatingAmountWithRate(amount?: number, rate?: number): string {
  const normalizedRate = normalizeRatePercent(rate);
  if ((typeof amount !== "number" || !Number.isFinite(amount)) && typeof normalizedRate !== "number") {
    return "--";
  }
  return `${formatStableFloatingAmount(amount)} (${formatRatePercent(rate)})`;
}

function resolveSignedTone(value?: number): "positive" | "negative" | "neutral" {
  if (typeof value !== "number" || !Number.isFinite(value)) return "neutral";
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "neutral";
}

function resolveAmountRateTone(amount?: number, rate?: number): "positive" | "negative" | "neutral" {
  if (typeof amount === "number" && Number.isFinite(amount)) {
    return resolveSignedTone(amount);
  }
  return resolveSignedTone(normalizeRatePercent(rate));
}

function formatLeverageMultiplier(value?: number): string {
  const normalized = formatNumeric(value, 4);
  if (normalized === "--") return "--";
  return `${normalized}x`;
}

function normalizeExchange(value?: string): string {
  return value?.trim().toLowerCase() || "";
}

function parsePositionTimeMs(value?: string): number | undefined {
  if (!value) return undefined;
  const raw = value.trim();
  if (!raw) return undefined;

  if (/^\d{10,13}$/.test(raw)) {
    const ts = Number(raw);
    if (Number.isFinite(ts)) {
      return raw.length <= 10 ? ts * 1000 : ts;
    }
  }

  const utcMatch = raw.match(
    /^(\d{4})[/-](\d{2})[/-](\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (utcMatch) {
    const [, yyyy, mm, dd, hh = "0", mi = "0", ss = "0"] = utcMatch;
    const ts = Date.UTC(
      Number(yyyy),
      Number(mm) - 1,
      Number(dd),
      Number(hh),
      Number(mi),
      Number(ss)
    );
    return Number.isNaN(ts) ? undefined : ts;
  }

  const direct = Date.parse(raw);
  if (!Number.isNaN(direct)) return direct;

  const normalized = raw.replace(" ", "T");
  const normalizedParsed = Date.parse(normalized);
  if (!Number.isNaN(normalizedParsed)) return normalizedParsed;

  return undefined;
}

function formatHoldingDuration(entryTime?: string, nowMs = Date.now()): string {
  const entryMs = parsePositionTimeMs(entryTime);
  if (typeof entryMs !== "number") return "--";
  const durationMs = Math.max(0, nowMs - entryMs);
  const totalSeconds = Math.floor(durationMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${hours}h${minutes}m${seconds}s`;
}

function resolvePositionSideLongShort(item: PositionItem): "LONG" | "SHORT" | "--" {
  const side = (item.positionSide || "").trim().toLowerCase();
  if (side === "long" || side === "buy") return "LONG";
  if (side === "short" || side === "sell") return "SHORT";
  return "--";
}

function buildSymbolVariants(symbol?: string): Set<string> {
  const out = new Set<string>();
  const raw = symbol?.trim().toUpperCase() || "";
  if (!raw) return out;

  const noSuffix = raw.replace(/\.P$/i, "");
  out.add(noSuffix);

  const compact = noSuffix.replace(/[^A-Z0-9]/g, "");
  if (compact) out.add(compact);

  const splitParts = noSuffix.split(/[/:_-]+/).filter(Boolean);
  if (splitParts.length >= 2) {
    const base = splitParts[0];
    const quote = splitParts[1];
    out.add(`${base}/${quote}`);
    out.add(`${base}-${quote}`);
    out.add(`${base}${quote}`);
  } else {
    const matchedQuote = KNOWN_QUOTES.find((quote) => compact.endsWith(quote) && compact.length > quote.length);
    if (matchedQuote) {
      const base = compact.slice(0, compact.length - matchedQuote.length);
      out.add(`${base}/${matchedQuote}`);
      out.add(`${base}-${matchedQuote}`);
      out.add(`${base}${matchedQuote}`);
    }
  }

  return out;
}

function symbolsLikelyMatch(left?: string, right?: string): boolean {
  const leftVariants = buildSymbolVariants(left);
  const rightVariants = buildSymbolVariants(right);
  if (leftVariants.size === 0 || rightVariants.size === 0) return false;
  for (const value of leftVariants) {
    if (rightVariants.has(value)) return true;
  }
  return false;
}

function parseCandidateKey(value?: unknown): { exchange: string; symbol: string } | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const [exchange, symbol] = trimmed.split(CANDIDATE_KEY_SEPARATOR);
  if (!exchange || !symbol) return null;
  return {
    exchange: normalizeExchange(exchange),
    symbol: symbol.trim().toUpperCase()
  };
}

function formatCandidateKey(value?: unknown): string {
  const parsed = parseCandidateKey(value);
  if (!parsed) {
    return typeof value === "string" && value.trim() ? value.trim() : "--";
  }
  return `${parsed.exchange.toUpperCase()} | ${parsed.symbol}`;
}

function translateGroupSide(value?: string): string {
  const normalized = (value || "").trim().toLowerCase();
  if (normalized === "long") return "LONG";
  if (normalized === "short") return "SHORT";
  return formatValue(value);
}

function translateGroupState(value?: string): string {
  return formatValue(value);
}

function translateLockStage(value?: string): string {
  return formatValue(value);
}

function translateCandidateState(value?: unknown): string {
  return formatValue(typeof value === "string" ? value : undefined);
}

function shouldDisplayCandidate(candidate: TrendGroupCandidate): boolean {
  const state =
    typeof candidate.candidateState === "string" ? candidate.candidateState.trim().toLowerCase() : "";
  return state !== "inactive";
}

function getCandidatePriorityScore(candidate: TrendGroupCandidate): number {
  const value = candidate.priorityScore;
  return typeof value === "number" && Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
}

function comparePositionRecency(left: PositionItem, right: PositionItem): number {
  const leftTs = parsePositionTimeMs(left.updatedTime) ?? parsePositionTimeMs(left.entryTime) ?? 0;
  const rightTs = parsePositionTimeMs(right.updatedTime) ?? parsePositionTimeMs(right.entryTime) ?? 0;
  return rightTs - leftTs;
}

function resolveMatchedPosition(group: TrendGroupItem, positions: PositionItem[]): PositionItem | undefined {
  if (!positions.length) return undefined;

  const normalizedGroupId = group.groupId.trim().toLowerCase();
  const selectedCandidate = parseCandidateKey(group.selectedCandidateKey);
  const openCandidateKeys = new Set(
    group.candidates
      .filter((item) => item.hasOpenPosition === true)
      .map((item) => item.candidateKey)
      .filter((item): item is string => typeof item === "string" && item.trim().length > 0)
      .map((item) => item.trim().toLowerCase())
  );
  const candidatePairs = group.candidates
    .map((item) => ({
      raw: typeof item.candidateKey === "string" ? item.candidateKey.trim().toLowerCase() : "",
      parsed: parseCandidateKey(item.candidateKey)
    }))
    .filter((item) => item.parsed !== null) as Array<{ raw: string; parsed: { exchange: string; symbol: string } }>;

  let best: PositionItem | undefined;
  let bestScore = -1;

  for (const item of positions) {
    let score = -1;
    const itemGroupId = (item.groupId || "").trim().toLowerCase();
    const itemExchange = normalizeExchange(item.exchange);
    const itemSymbol = (item.symbol || "").trim().toUpperCase();
    const candidateRawKey = `${itemExchange}|${itemSymbol}`;

    if (normalizedGroupId && itemGroupId && normalizedGroupId === itemGroupId) {
      score = 100;
    } else if (
      selectedCandidate &&
      itemExchange === selectedCandidate.exchange &&
      symbolsLikelyMatch(itemSymbol, selectedCandidate.symbol)
    ) {
      score = 80;
    } else if (openCandidateKeys.has(candidateRawKey.toLowerCase())) {
      score = 70;
    } else if (
      candidatePairs.some(
        (candidate) => candidate.parsed.exchange === itemExchange && symbolsLikelyMatch(itemSymbol, candidate.parsed.symbol)
      )
    ) {
      score = 60;
    }

    if (score < 0) continue;
    if (!best || score > bestScore) {
      best = item;
      bestScore = score;
      continue;
    }
    if (score === bestScore && comparePositionRecency(item, best) < 0) {
      best = item;
    }
  }

  return best;
}

function deriveCandidateColumns(candidates: TrendGroupCandidate[]): string[] {
  const columnSet = new Set<string>();
  for (const candidate of candidates) {
    for (const key of Object.keys(candidate)) {
      columnSet.add(key);
    }
  }

  const columns: string[] = [];
  for (const key of KNOWN_CANDIDATE_COLUMN_ORDER) {
    if (columnSet.has(key)) {
      columns.push(key);
      columnSet.delete(key);
    }
  }

  const extras = Array.from(columnSet).sort((left, right) => left.localeCompare(right));
  return [...columns, ...extras];
}

function formatCandidateHeader(key: string): string {
  return CANDIDATE_COLUMN_LABELS[key] || key;
}

function formatCandidateValue(candidate: TrendGroupCandidate, key: string): string {
  const value = candidate[key];
  if (key === "candidateKey") return formatCandidateKey(value);
  if (key === "candidateState") return translateCandidateState(value);
  if (key === "isSelected" || key === "hasOpenPosition") {
    if (typeof value === "boolean") return value ? "是" : "否";
    return "--";
  }
  if (key === "priorityScore") {
    return typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "--";
  }
  if (typeof value === "number") return formatNumeric(value);
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "string") return value.trim() || "--";
  if (value === null || value === undefined) return "--";
  return JSON.stringify(value);
}

export const GroupDetailsModal = memo(function GroupDetailsModal() {
  const selectedGroupId = useAppStore((s) => s.selectedGroupId);
  const groupsSnapshot = useAppStore((s) => s.groupsSnapshot);
  const positionSnapshot = useAppStore((s) => s.positionSnapshot);
  const clearSelectedGroupId = useAppStore((s) => s.clearSelectedGroupId);
  const [topOffset, setTopOffset] = useState(84);
  const [positionNowMs, setPositionNowMs] = useState(() => Date.now());

  const selectedGroup = useMemo(() => {
    if (!selectedGroupId || !groupsSnapshot) return undefined;
    const normalized = selectedGroupId.trim().toLowerCase();
    return groupsSnapshot.groups.find((item) => item.groupId.trim().toLowerCase() === normalized);
  }, [groupsSnapshot, selectedGroupId]);
  const isLoadingGroup = Boolean(selectedGroupId) && !selectedGroup;

  useEffect(() => {
    if (!selectedGroup?.candidates.some((item) => item.hasOpenPosition === true)) return undefined;
    const timer = window.setInterval(() => setPositionNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [selectedGroup]);

  useEffect(() => {
    if (!selectedGroupId) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      clearSelectedGroupId();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [clearSelectedGroupId, selectedGroupId]);

  useEffect(() => {
    if (!selectedGroupId) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as HTMLElement | null;
      if (!target) return;
      if (target.closest(".details-modal")) return;
      if (target.closest(".canvas-wrap")) return;

      const topbar = target.closest(".topbar");
      if (topbar) {
        const interactive = target.closest(
          ".timeframe-tab, .topbar__search, .status-indicator, input, label, button, a"
        );
        if (interactive) return;
        clearSelectedGroupId();
      }
    };

    window.addEventListener("pointerdown", onPointerDown, true);
    return () => window.removeEventListener("pointerdown", onPointerDown, true);
  }, [clearSelectedGroupId, selectedGroupId]);

  useEffect(() => {
    const topbar = document.querySelector<HTMLElement>(".topbar");
    if (!topbar) return;
    const viewport = window.visualViewport;

    const updateTopOffset = () => {
      const viewportHeight = viewport?.height || window.innerHeight;
      const bottom = topbar.getBoundingClientRect().bottom;
      const next = Math.max(12, Math.min(viewportHeight - 120, Math.ceil(bottom + 12)));
      setTopOffset(next);
    };

    updateTopOffset();
    const observer = new ResizeObserver(() => updateTopOffset());
    observer.observe(topbar);
    window.addEventListener("resize", updateTopOffset);
    viewport?.addEventListener("resize", updateTopOffset);
    viewport?.addEventListener("scroll", updateTopOffset);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", updateTopOffset);
      viewport?.removeEventListener("resize", updateTopOffset);
      viewport?.removeEventListener("scroll", updateTopOffset);
    };
  }, []);

  const modalStyle = useMemo(
    () => ({ "--details-modal-top": `${topOffset}px` }) as CSSProperties,
    [topOffset]
  );

  const matchedPosition = useMemo(() => {
    if (!selectedGroup || !positionSnapshot?.positions?.length) return undefined;
    return resolveMatchedPosition(selectedGroup, positionSnapshot.positions);
  }, [positionSnapshot?.positions, selectedGroup]);

  const positionSummaryCells = useMemo<PositionSummaryCell[]>(() => {
    if (!matchedPosition) return [];
    return [
      { key: "exchange", label: "交易所", value: formatValue(matchedPosition.exchange) },
      { key: "symbol", label: "交易对", value: formatValue(matchedPosition.symbol) },
      {
        key: "side",
        label: "方向",
        value: resolvePositionSideLongShort(matchedPosition)
      },
      {
        key: "leverage",
        label: "杠杆",
        value: formatLeverageMultiplier(matchedPosition.leverageMultiplier)
      },
      {
        key: "floating",
        label: "浮动收益",
        value: formatFloatingAmountWithRate(
          matchedPosition.unrealizedProfitAmount,
          matchedPosition.unrealizedProfitRate
        ),
        tone: resolveAmountRateTone(
          matchedPosition.unrealizedProfitAmount,
          matchedPosition.unrealizedProfitRate
        )
      },
      {
        key: "holding",
        label: "持仓时长",
        value: formatHoldingDuration(matchedPosition.entryTime, positionNowMs)
      }
    ];
  }, [matchedPosition, positionNowMs]);

  const detailRows = useMemo<DetailRow[]>(() => {
    if (!selectedGroup) return [];
    return [
      { label: "策略", value: formatValue(selectedGroup.strategy) },
      { label: "周期", value: formatValue(selectedGroup.primaryTimeframe) },
      { label: "方向", value: translateGroupSide(selectedGroup.side) },
      { label: "状态", value: translateGroupState(selectedGroup.state) },
      { label: "锁定阶段", value: translateLockStage(selectedGroup.lockStage) },
      { label: "已选候选", value: formatCandidateKey(selectedGroup.selectedCandidateKey) },
      { label: "开仓次数", value: formatValue(selectedGroup.entryCount) }
    ];
  }, [selectedGroup]);

  const sortedCandidates = useMemo(() => {
    const candidates = selectedGroup?.candidates || [];
    return candidates
      .filter(shouldDisplayCandidate)
      .map((candidate, index) => ({ candidate, index }))
      .sort((left, right) => {
        const scoreDelta = getCandidatePriorityScore(right.candidate) - getCandidatePriorityScore(left.candidate);
        if (scoreDelta !== 0) return scoreDelta;
        return left.index - right.index;
      })
      .map((item) => item.candidate);
  }, [selectedGroup?.candidates]);

  const candidateColumns = useMemo(() => deriveCandidateColumns(sortedCandidates), [sortedCandidates]);

  if (!selectedGroupId) return null;

  return (
    <section
      className="details-modal"
      style={modalStyle}
      role="dialog"
      aria-modal="true"
      aria-label="趋势组详情"
    >
      <header className="details-modal__header">
        <div>
          <div className="details-modal__title">{selectedGroup?.groupId || selectedGroupId}</div>
        </div>
        <button
          type="button"
          className="details-modal__close"
          onClick={clearSelectedGroupId}
          aria-label="关闭趋势组详情"
        >
          x
        </button>
      </header>
      <div className="details-modal__body">
        <div className="details-modal__content">
          {isLoadingGroup ? (
            <section className="details-modal__loading" aria-label="趋势组详情加载中">
              <div className="details-modal__loading-title">正在加载趋势组详情</div>
              <div className="details-modal__loading-text">已选中当前趋势组，等待 /groups 数据返回。</div>
            </section>
          ) : null}
          {!isLoadingGroup && matchedPosition ? (
            <section className="details-modal__position" aria-label="持仓信息">
              <div className="details-modal__position-title">持仓信息</div>
              <div className="details-modal__position-grid">
                {positionSummaryCells.map((cell) => (
                  <div key={cell.key} className="details-modal__position-item">
                    <span className="details-modal__position-item-label">{cell.label}</span>
                    <span
                      className={`details-modal__position-item-value ${
                        cell.tone ? `position-rate position-rate--${cell.tone}` : ""
                      }`}
                    >
                      {cell.value}
                    </span>
                  </div>
                ))}
              </div>
            </section>
          ) : null}
          {!isLoadingGroup ? detailRows.map((row) => (
            <div key={row.label} className="details-modal__row">
              <span>{row.label}</span>
              <span>{row.value}</span>
            </div>
          )) : null}
          {!isLoadingGroup ? <section className="group-details__candidates" aria-label="候选列表">
            <div className="group-details__candidate-drawer">
              {sortedCandidates.length > 0 ? (
                <div className="group-details__candidate-table-wrap">
                  <table className="group-details__candidate-table">
                    <thead>
                      <tr>
                        {candidateColumns.map((column) => (
                          <th key={column} className="group-details__candidate-cell group-details__candidate-cell--header">
                            {formatCandidateHeader(column)}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {sortedCandidates.map((candidate, index) => (
                        <tr key={`${selectedGroup.groupId}-candidate-${index}`}>
                          {candidateColumns.map((column) => (
                            <td key={column} className="group-details__candidate-cell">
                              {formatCandidateValue(candidate, column)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="group-details__candidate-empty">暂无候选数据</div>
              )}
            </div>
          </section> : null}
        </div>
      </div>
    </section>
  );
});
