import type { TrendType } from "./types";

export const TrendTypeOrder: TrendType[] = [
  "bullish",
  "bullishPullback",
  "bearish",
  "bearishPullback",
  "range"
];

export const TrendTypeLabels: Record<TrendType, string> = {
  bullish: "多头",
  bullishPullback: "多头回调",
  bearish: "空头",
  bearishPullback: "空头回调",
  range: "震荡",
  none: "无趋势"
};

export function sideToTrendType(side: number): TrendType {
  switch (side) {
    case 1:
      return "bullish";
    case 8:
      return "bullishPullback";
    case -1:
      return "bearish";
    case -8:
      return "bearishPullback";
    case 255:
    case -255:
      return "range";
    default:
      return "none";
  }
}

export type ActionType = "armed" | "open" | "riskUpdate" | "partialClose" | "closeAll" | "none" | "mixed";

export function resolveActionType(action: number): ActionType {
  const bits: ActionType[] = [];
  if ((action & 4) !== 0) bits.push("armed");
  if ((action & 8) !== 0) bits.push("open");
  if ((action & 16) !== 0) bits.push("riskUpdate");
  if ((action & 32) !== 0) bits.push("partialClose");
  if ((action & 64) !== 0) bits.push("closeAll");
  if (bits.length === 0) return "none";
  if (bits.length > 1) return "mixed";
  return bits[0];
}

export const ActionTypeLabels: Record<ActionType, string> = {
  armed: "准备开仓",
  open: "开仓",
  riskUpdate: "更新止盈止损",
  partialClose: "部分平仓",
  closeAll: "全部平仓",
  none: "无动作",
  mixed: "组合动作"
};

export function actionWeight(action: number): number {
  if (action === 0) return 0;
  let score = 0;
  if ((action & 8) !== 0) score += 4;
  if ((action & 16) !== 0) score += 3;
  if ((action & 32) !== 0) score += 2;
  if ((action & 64) !== 0) score += 1;
  return score;
}
