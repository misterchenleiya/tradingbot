export interface ShortcutHelpRow {
  action: string;
  winLinux: string;
  mac: string;
}

export const SHORTCUT_HELP_ROWS: ShortcutHelpRow[] = [
  {
    action: "Toggle help window",
    winLinux: "?",
    mac: "?"
  },
  {
    action: "Reset chart view",
    winLinux: "Alt + R",
    mac: "Option + R"
  },
  {
    action: "Zoom in",
    winLinux: "Ctrl + Up",
    mac: "Command + Up"
  },
  {
    action: "Zoom out",
    winLinux: "Ctrl + Down",
    mac: "Command + Down"
  },
  {
    action: "Pan chart by 1 bar (left)",
    winLinux: "Left Arrow",
    mac: "Left Arrow"
  },
  {
    action: "Pan chart by 1 bar (right)",
    winLinux: "Right Arrow",
    mac: "Right Arrow"
  },
  {
    action: "Toggle maximize chart",
    winLinux: "Shift + F",
    mac: "Shift + F"
  },
  {
    action: "Switch timeframe",
    winLinux: "1..9",
    mac: "1..9"
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
