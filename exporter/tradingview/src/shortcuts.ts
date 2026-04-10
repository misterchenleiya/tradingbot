export interface ShortcutHelpRow {
  action: string;
  winLinux: string;
  mac: string;
}

export interface ConnectionStatusHelpRow {
  color: "connected" | "slow" | "disconnected" | "warmup";
  label: string;
  description: string;
}

export type EventLegendPreviewKind =
  | "ENTRY"
  | "EXIT"
  | "EXECUTION"
  | "TP"
  | "SL"
  | "ARMED"
  | "TREND_N"
  | "HIGH_H"
  | "MID_M"
  | "R_2R"
  | "R_4R";

export interface EventLegendRow {
  kind: EventLegendPreviewKind;
  label: string;
  description: string;
}

export const SHORTCUT_HELP_ROWS: ShortcutHelpRow[] = [
  {
    action: "显示或隐藏快捷键帮助",
    winLinux: "?",
    mac: "?"
  },
  {
    action: "重置 K 线视图",
    winLinux: "Alt + R",
    mac: "Option + R"
  },
  {
    action: "放大 K 线区域",
    winLinux: "Ctrl + ↑",
    mac: "Command + ↑"
  },
  {
    action: "缩小 K 线区域",
    winLinux: "Ctrl + ↓",
    mac: "Command + ↓"
  },
  {
    action: "向左平移 1 根 K 线",
    winLinux: "←",
    mac: "←"
  },
  {
    action: "向右平移 1 根 K 线",
    winLinux: "→",
    mac: "→"
  },
  {
    action: "切换图表最大化",
    winLinux: "Shift + F",
    mac: "Shift + F"
  },
  {
    action: "切换周期",
    winLinux: "1..9",
    mac: "1..9"
  }
];

export const CONNECTION_STATUS_HELP_ROWS: ConnectionStatusHelpRow[] = [
  {
    color: "warmup",
    label: "白色",
    description: "WS 链接正常，但当前系统仍处于 warmup / 启动预热阶段。"
  },
  {
    color: "connected",
    label: "绿色",
    description: "当前页面与 gobot 后端的 WS 链接正常。"
  },
  {
    color: "slow",
    label: "黄色",
    description: "WS 链接正常，但当前往返延迟大于 100ms。"
  },
  {
    color: "disconnected",
    label: "红色",
    description: "当前页面与 gobot 后端的 WS 链接中断。"
  }
];

export const EVENT_LEGEND_ROWS: EventLegendRow[] = [
  {
    kind: "ENTRY",
    label: "ENTRY",
    description: "绿色右箭头，指向开仓 K 线左边缘。"
  },
  {
    kind: "EXIT",
    label: "EXIT",
    description: "红色左箭头，指向平仓 K 线右边缘。"
  },
  {
    kind: "EXECUTION",
    label: "EXECUTION",
    description: "蓝色圆角徽标，表示执行层事件；update 类执行固定落在对应 K 线中性位置，不再冒充开平仓价格。"
  },
  {
    kind: "TP",
    label: "TP",
    description: "绿色实心圆，按真实止盈价格定位。"
  },
  {
    kind: "SL",
    label: "SL",
    description: "红色空心圆，按真实止损价格定位。"
  },
  {
    kind: "ARMED",
    label: "ARMED",
    description: "黄色实心圆，表示小周期进入 armed。"
  },
  {
    kind: "TREND_N",
    label: "TREND",
    description: "绿色空心圆，表示趋势检测完成。"
  },
  {
    kind: "HIGH_H",
    label: "HIGH",
    description: "高周期状态变化。绿色=1，红色=-1，灰色=255/-255。"
  },
  {
    kind: "MID_M",
    label: "MID",
    description: "中周期状态变化。绿色=1，红色=-1，灰色=255/-255。"
  },
  {
    kind: "R_2R",
    label: "2R",
    description: "保本保护事件，定位到生效后的 SL 价格。"
  },
  {
    kind: "R_4R",
    label: "4R",
    description: "部分平仓保护事件，定位到生效后的 SL 价格。"
  }
];

export function shouldIgnoreShortcut(event: KeyboardEvent): boolean {
  const target = event.target as HTMLElement | null;
  if (!target) {
    return false;
  }
  const tag = target.tagName.toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") {
    return true;
  }
  if (target.isContentEditable) {
    return true;
  }
  return false;
}

export function getDigitIndex(key: string): number | null {
  if (!/^[1-9]$/.test(key)) {
    return null;
  }
  return Number(key) - 1;
}
