import type {
  AccountSnapshot,
  BackendStatus,
  BubbleCandlesFetchRequest,
  BubbleCandlesSnapshot,
  BubbleDatum,
  BubbleUpdate,
  DataSourceStatus,
  PositionSnapshot,
  TrendGroupsSnapshot
} from "../app/types";

export type SnapshotHandler = (data: BubbleDatum[]) => void;
export type UpdateHandler = (updates: BubbleUpdate[]) => void;
export type StatusHandler = (status: Partial<DataSourceStatus>) => void;
export type BackendStatusHandler = (status: BackendStatus) => void;
export type AccountHandler = (account?: AccountSnapshot) => void;
export type PositionHandler = (position?: PositionSnapshot) => void;
export type HistoryHandler = (history?: PositionSnapshot) => void;
export type GroupsHandler = (groups?: TrendGroupsSnapshot) => void;

export type DataSourceHandlers = {
  onSnapshot: SnapshotHandler;
  onUpdate: UpdateHandler;
  onStatus?: StatusHandler;
  onBackendStatus?: BackendStatusHandler;
  onAccount?: AccountHandler;
  onPosition?: PositionHandler;
  onHistory?: HistoryHandler;
  onGroups?: GroupsHandler;
};

export interface IDataSource {
  start: () => void;
  stop: () => void;
  loadMoreHistory: () => void;
  fetchCandles: (request: BubbleCandlesFetchRequest) => Promise<BubbleCandlesSnapshot | undefined>;
}
