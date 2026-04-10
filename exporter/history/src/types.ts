export interface HistoryPosition {
  position_uid: string;
  position_key: string;
  id: number;
  display_state: string;
  exchange: string;
  symbol: string;
  inst_id: string;
  position_side: string;
  margin_mode: string;
  leverage: number;
  margin: number;
  entry_price: number;
  exit_price: number;
  take_profit_price: number;
  stop_loss_price: number;
  quantity: number;
  notional_usd: number;
  realized_pnl: number;
  pnl_ratio: number;
  fee: number;
  funding_fee: number;
  open_time_ms: number;
  close_time_ms: number;
  open_update_time_ms: number;
  updated_at_ms: number;
  max_floating_loss_amount: number;
  max_floating_profit_amount: number;
  state: string;
  singleton_id?: number;
  run_id?: string;
  strategy_name: string;
  strategy_version: string;
  timeframes: string[];
  indicators: Record<string, string[]>;
}

export interface PositionRunOption {
  value: string;
  label: string;
  singleton_id?: number;
}

export interface PositionFilterOptions {
  run_ids: string[];
  run_options: PositionRunOption[];
  strategies: string[];
  versions: string[];
  exchanges: string[];
  symbols: string[];
}

export interface PositionsResponse {
  date: string;
  count: number;
  has_more: boolean;
  next_before_ms?: number;
  filter_options: PositionFilterOptions;
  positions: HistoryPosition[];
}

export interface HistoryEvent {
  id: string;
  source: string;
  type: string;
  level: "info" | "warning" | "success" | "error";
  event_at_ms: number;
  title: string;
  summary: string;
  detail?: Record<string, unknown>;
}

export interface EventsResponse {
  position_id: number;
  count: number;
  total?: number;
  truncated?: boolean;
  events: HistoryEvent[];
}

export interface Candle {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface TimeframeCandles {
  count: number;
  lookback_bars: number;
  expected_start_ms: number;
  expected_end_ms: number;
  candles: Candle[];
}

export interface CandlesResponse {
  position_id: number;
  position_key?: string;
  open_time_ms: number;
  close_time_ms: number;
  timeframes: Record<string, TimeframeCandles>;
  integrity: IntegrityResponse;
}

export interface LoadCandleItem {
  timeframe: string;
  bars: number;
  lookback_bars: number;
  fetch_start_ms: number;
  fetch_end_ms: number;
}

export interface LoadCandlesResponse {
  position_id: number;
  loaded: LoadCandleItem[];
}

export interface IntegrityGap {
  kind: string;
  start_ts: number;
  end_ts: number;
  bars: number;
}

export interface IntegrityTimeframe {
  timeframe: string;
  expected_start_ms: number;
  expected_end_ms: number;
  expected_bars: number;
  actual_bars: number;
  complete: boolean;
  continuous: boolean;
  gaps?: IntegrityGap[];
}

export interface IntegrityResponse {
  position_id: number;
  position_key?: string;
  events: {
    main_db_available: boolean;
    signals: number;
    orders: number;
  };
  candles: {
    total_rows: number;
    timeframes: Array<{ timeframe: string; rows: number; first_ts?: number; last_ts?: number }>;
  };
  summary: {
    incomplete_timeframes: number;
    missing_bars: number;
    discontinuities: number;
  };
  check: {
    has_events: boolean;
    has_candles: boolean;
    has_discontinuity: boolean;
    ok: boolean;
  };
  timeframes: IntegrityTimeframe[];
}
