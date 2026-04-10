export interface TradingViewExchange {
  name: string;
  display_name: string;
  active: boolean;
  runtime_state?: string;
  runtime_message?: string;
  active_symbol_count: number;
}

export interface TradingViewSymbol {
  exchange: string;
  symbol: string;
  display_symbol: string;
  base?: string;
  quote?: string;
  market_type?: string;
  active_sync: boolean;
  dynamic?: boolean;
  ws_subscribed?: boolean;
  last_ws_at_ms?: number;
  is_held: boolean;
  position_side?: string;
  leverage_multiplier?: number;
  margin_amount?: number;
  last_price: number;
  change_24h_pct: number;
  high_24h: number;
  low_24h: number;
  turnover_24h: number;
  unrealized_profit_amount?: number;
  unrealized_profit_rate?: number;
}

export interface TradingViewPosition {
  position_id: number;
  exchange: string;
  symbol: string;
  position_side: string;
  margin_mode: string;
  leverage_multiplier: number;
  margin_amount: number;
  entry_price: number;
  entry_quantity: number;
  entry_value: number;
  entry_time: string;
  take_profit_price: number;
  stop_loss_price: number;
  current_price: number;
  unrealized_profit_amount: number;
  unrealized_profit_rate: number;
  max_floating_profit_amount: number;
  max_floating_profit_rate: number;
  max_floating_loss_amount: number;
  max_floating_loss_rate: number;
  profit_amount: number;
  profit_rate: number;
  holding_duration_ms?: number;
  updated_time: string;
  status: string;
  strategy_name?: string;
  strategy_version?: string;
  strategy_timeframes?: string[];
  combo_key?: string;
}

export interface TradingViewHistoryPosition {
  position_id?: number;
  exchange: string;
  symbol: string;
  position_side: string;
  margin_mode: string;
  leverage_multiplier: number;
  margin_amount: number;
  entry_price: number;
  entry_quantity: number;
  entry_value: number;
  entry_time: string;
  current_price: number;
  exit_price: number;
  exit_quantity: number;
  exit_value: number;
  exit_time: string;
  profit_amount: number;
  profit_rate: number;
  close_status?: string;
  updated_time: string;
  status: string;
  strategy_name?: string;
  strategy_version?: string;
  strategy_timeframes?: string[];
  combo_key?: string;
}

export interface TradingViewFunds {
  exchange: string;
  currency: string;
  total_equity_usdt: number;
  floating_profit_usdt: number;
  margin_in_use_usdt: number;
  funding_usdt: number;
  trading_usdt: number;
  per_trade_usdt: number;
  daily_profit_usdt: number;
  closed_profit_rate: number;
  floating_profit_rate: number;
  total_profit_rate: number;
  updated_at_ms: number;
}

export interface TradingViewRealtimeAccount {
  exchange: string;
  currency: string;
  funding_usdt: number;
  trading_usdt: number;
  total_usdt: number;
  per_trade_usdt: number;
  daily_profit_usdt: number;
  closed_profit_rate: number;
  floating_profit_rate: number;
  total_profit_rate: number;
  updated_at_ms: number;
}

export interface TradingViewOrder {
  id: number;
  exchange: string;
  symbol: string;
  display_symbol: string;
  action: string;
  order_type?: string;
  position_side?: string;
  leverage_multiplier?: number;
  price?: number;
  size?: number;
  take_profit_price?: number;
  stop_loss_price?: number;
  result_status?: string;
  error_message?: string;
  started_at_ms?: number;
  updated_at_ms?: number;
}

export interface TradingViewTradeRequest {
  action: string;
  exchange: string;
  symbol: string;
  side: string;
  timeframe?: string;
  order_type?: string;
  amount?: number;
  entry?: number;
  tp?: number;
  sl?: number;
  strategy?: string;
}

export interface TradingViewTradeResponse {
  position_found: boolean;
  position?: TradingViewPosition;
  decision?: Record<string, unknown>;
  executed: boolean;
  manual_order?: TradingViewOrder;
  risk_error?: string;
  execution_error?: string;
}

export interface TradingViewPositionDelegateRequest {
  exchange: string;
  symbol: string;
  side: string;
  strategy_name: string;
  trade_timeframes: string[];
}

export interface TradingViewPositionDelegateResponse {
  delegated: boolean;
}

export interface TradingViewStrategyOption {
  strategy_name: string;
  trade_timeframes: string[];
  combo_key: string;
  display_label: string;
}

export interface TradingViewRuntimeResponse {
  exchanges: TradingViewExchange[];
  mode: string;
  selected_exchange: string;
  default_symbol: string;
  default_timeframe: string;
  bootstrap_complete: boolean;
  timeframes: string[];
  symbols: TradingViewSymbol[];
  orders: TradingViewOrder[];
  positions: TradingViewPosition[];
  history_positions: TradingViewHistoryPosition[];
  funds: TradingViewFunds;
  strategy_options: TradingViewStrategyOption[];
  read_only: boolean;
}

export interface TradingViewExporterStatusVersion {
  tag: string;
  commit: string;
  build_time: string;
}

export interface TradingViewExporterStatusRuntime {
  seconds: number;
  human: string;
}

export interface TradingViewExporterStatusModule {
  name: string;
  state: string;
  message?: string;
  updated_at?: string;
}

export interface TradingViewExporterStatusResponse {
  version: TradingViewExporterStatusVersion;
  runtime: TradingViewExporterStatusRuntime;
  modules?: Record<string, TradingViewExporterStatusModule>;
}

export interface TradingViewBacktestTask {
  id: number;
  status: string;
  exchange: string;
  symbol: string;
  display_symbol: string;
  position_side?: string;
  leverage_multiplier?: number;
  open_price?: number;
  close_price?: number;
  realized_profit_rate?: number;
  open_time_ms?: number;
  close_time_ms?: number;
  holding_duration_ms?: number;
  chart_timeframe: string;
  trade_timeframes: string[];
  range_start_ms: number;
  range_end_ms: number;
  price_low?: number;
  price_high?: number;
  selection_direction?: string;
  source: string;
  history_bars: number;
  singleton_id?: number;
  singleton_uuid?: string;
  pid?: number;
  error_message?: string;
  created_at_ms: number;
  started_at_ms?: number;
  finished_at_ms?: number;
  updated_at_ms: number;
}

export interface TradingViewBacktestTasksResponse {
  date: string;
  count: number;
  has_more_days: boolean;
  tasks: TradingViewBacktestTask[];
}

export interface TradingViewBacktestTaskResponse {
  task: TradingViewBacktestTask;
}

export interface TradingViewEventEntry {
  id: string;
  source: string;
  type: string;
  level: string;
  event_at_ms: number;
  title: string;
  summary: string;
  detail?: Record<string, unknown>;
}

export interface TradingViewBacktestOverlayResponse {
  task: TradingViewBacktestTask;
  count: number;
  total?: number;
  truncated?: boolean;
  events: TradingViewEventEntry[];
  positions?: TradingViewHistoryPosition[];
}

export interface TradingViewPositionEventsResponse {
  position_id: number;
  position_key: string;
  is_open: boolean;
  count: number;
  total?: number;
  truncated?: boolean;
  events: TradingViewEventEntry[];
}

export interface TradingViewCandle {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface TradingViewIndicatorPoint {
  ts: number;
  value: number;
}

export interface TradingViewIndicatorLine {
  id: string;
  label: string;
  color: string;
  legend_color?: string;
  points: TradingViewIndicatorPoint[];
}

export interface TradingViewCandlesResponse {
  exchange: string;
  symbol: string;
  display_symbol: string;
  market_type?: string;
  timeframe: string;
  candles: TradingViewCandle[];
  indicators: TradingViewIndicatorLine[];
}

export interface TradingViewRealtimePositionResponse {
  count: number;
  positions: TradingViewPosition[];
}

export interface TradingViewRealtimeSymbolsResponse {
  exchange: string;
  symbols: TradingViewSymbol[];
}

export interface TradingViewRealtimeMessage {
  type: string;
  request_id?: string;
  subscription?: {
    streams?: string[];
  };
  account?: TradingViewRealtimeAccount;
  position?: TradingViewRealtimePositionResponse;
  symbols?: TradingViewRealtimeSymbolsResponse;
  candles?: TradingViewCandlesResponse;
}
