export interface BubbleShortcutHelpRow {
  action: string;
  key: string;
  description: string;
}

export const BUBBLE_SHORTCUT_HELP_ROWS: BubbleShortcutHelpRow[] = [
  {
    action: "打开帮助",
    key: "?（Shift + /）",
    description: "打开当前 bubbles 帮助窗口。"
  },
  {
    action: "进入搜索模式",
    key: "Shift + S",
    description: "聚焦顶部搜索框，仅匹配当前时间周期和已选交易所。"
  },
  {
    action: "退出搜索模式",
    key: "Esc",
    description: "搜索框聚焦时清空关键词并退出焦点。"
  },
  {
    action: "切换时间周期",
    key: "[ / ]",
    description: "在当前可用时间周期之间前后切换。"
  },
  {
    action: "K线窗口切换周期",
    key: "1..9",
    description: "仅在普通气泡菜单 K 线窗口中生效。"
  },
  {
    action: "关闭帮助/弹窗",
    key: "Esc",
    description: "帮助窗口打开时优先关闭帮助窗口；其他弹窗保持现有关闭语义。"
  }
];
