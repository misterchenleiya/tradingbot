import { UIEvent as ReactUIEvent, useEffect, useRef } from "react";
import { TopBar } from "../ui/TopBar";
import { CanvasScene } from "../canvas/CanvasScene";
import { BubbleDetailsModal } from "../ui/BubbleDetailsModal";
import { GroupDetailsModal } from "../ui/GroupDetailsModal";
import { SignalsTable } from "../ui/SignalsTable";
import type { BackendStatus } from "./types";
import { useAppStore } from "./store";

const MAIN_SCROLL_RATIO_STORAGE_KEY = "bubbles.layout.mainScrollRatio.v1";
const VERSION_RELOAD_SIGNATURE_STORAGE_KEY = "bubbles.versionReloadSignature.v1";

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function readStoredScrollRatio(): number {
  if (typeof window === "undefined") return 0;
  try {
    const raw = window.localStorage.getItem(MAIN_SCROLL_RATIO_STORAGE_KEY);
    if (raw === null) return 0;
    return clamp01(Number(raw));
  } catch (error) {
    console.warn("[layout] read main scroll ratio failed", error);
    return 0;
  }
}

function writeStoredScrollRatio(ratio: number): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(MAIN_SCROLL_RATIO_STORAGE_KEY, String(clamp01(ratio)));
  } catch (error) {
    console.warn("[layout] save main scroll ratio failed", error);
  }
}

function normalizeVersionToken(value?: string): string {
  return (value || "").trim();
}

function buildVersionFingerprint(status?: BackendStatus): string {
  const versionTag = normalizeVersionToken(status?.versionTag);
  const versionCommit = normalizeVersionToken(status?.versionCommit);
  const buildTime = normalizeVersionToken(status?.buildTime);
  if (!versionTag && !versionCommit && !buildTime) {
    return "";
  }
  return `${versionTag}|${versionCommit}|${buildTime}`;
}

function readReloadedVersionFingerprint(): string {
  if (typeof window === "undefined") return "";
  try {
    return window.sessionStorage.getItem(VERSION_RELOAD_SIGNATURE_STORAGE_KEY) || "";
  } catch (error) {
    console.warn("[version] read reloaded fingerprint failed", error);
    return "";
  }
}

function writeReloadedVersionFingerprint(fingerprint: string): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(VERSION_RELOAD_SIGNATURE_STORAGE_KEY, fingerprint);
  } catch (error) {
    console.warn("[version] save reloaded fingerprint failed", error);
  }
}

export function App() {
  const mainRef = useRef<HTMLDivElement | null>(null);
  const saveTimerRef = useRef<number | null>(null);
  const scrollbarTimerRef = useRef<number | null>(null);
  const pendingRestoreRatioRef = useRef<number>(readStoredScrollRatio());
  const hasRestoredRef = useRef(false);
  const baselineVersionFingerprintRef = useRef<string>("");
  const lastWsConnectedAtRef = useRef<number | null>(null);
  const pendingReconnectAtRef = useRef<number | null>(null);
  const pendingHiddenReloadFingerprintRef = useRef<string>("");
  const mainStale = useAppStore((s) => {
    const wsUnavailable =
      s.dataSourceStatus.mode === "restws" &&
      (s.dataSourceStatus.wsStatus !== "open" || s.dataSourceStatus.heartbeatStale === true);
    return !s.hasLiveSnapshot || wsUnavailable;
  });
  const visibleRowCount = useAppStore((s) => s.dataList.length);
  const backendStatus = useAppStore((s) => s.backendStatus);
  const wsStatus = useAppStore((s) => s.dataSourceStatus.wsStatus);
  const wsConnectedAt = useAppStore((s) => s.dataSourceStatus.wsConnectedAt);
  const versionFingerprint = buildVersionFingerprint(backendStatus);

  useEffect(
    () => () => {
      if (saveTimerRef.current !== null) {
        window.clearTimeout(saveTimerRef.current);
      }
      if (scrollbarTimerRef.current !== null) {
        window.clearTimeout(scrollbarTimerRef.current);
      }
    },
    []
  );

  useEffect(() => {
    if (!versionFingerprint) return;
    if (!baselineVersionFingerprintRef.current) {
      baselineVersionFingerprintRef.current = versionFingerprint;
    }
  }, [versionFingerprint]);

  useEffect(() => {
    if (wsStatus !== "open") return;
    if (typeof wsConnectedAt !== "number" || !Number.isFinite(wsConnectedAt)) return;
    if (lastWsConnectedAtRef.current === null) {
      lastWsConnectedAtRef.current = wsConnectedAt;
      pendingReconnectAtRef.current = null;
      return;
    }
    if (lastWsConnectedAtRef.current === wsConnectedAt) return;
    lastWsConnectedAtRef.current = wsConnectedAt;
    pendingReconnectAtRef.current = wsConnectedAt;
  }, [wsConnectedAt, wsStatus]);

  useEffect(() => {
    if (wsStatus !== "open") return;
    if (!versionFingerprint) return;
    if (!baselineVersionFingerprintRef.current) {
      baselineVersionFingerprintRef.current = versionFingerprint;
      pendingReconnectAtRef.current = null;
      return;
    }
    if (pendingReconnectAtRef.current === null) return;
    pendingReconnectAtRef.current = null;
    if (baselineVersionFingerprintRef.current === versionFingerprint) {
      return;
    }
    const reloadedFingerprint = readReloadedVersionFingerprint();
    if (reloadedFingerprint === versionFingerprint) {
      baselineVersionFingerprintRef.current = versionFingerprint;
      return;
    }
    const reloadToVersion = () => {
      writeReloadedVersionFingerprint(versionFingerprint);
      window.location.reload();
    };
    if (document.visibilityState === "visible") {
      reloadToVersion();
      return;
    }
    pendingHiddenReloadFingerprintRef.current = versionFingerprint;
    const handleVisibilityChange = () => {
      if (document.visibilityState !== "visible") return;
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      if (pendingHiddenReloadFingerprintRef.current !== versionFingerprint) return;
      pendingHiddenReloadFingerprintRef.current = "";
      reloadToVersion();
    };
    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [versionFingerprint, wsConnectedAt, wsStatus]);

  useEffect(() => {
    const container = mainRef.current;
    if (!container || hasRestoredRef.current) return;
    const rafId = window.requestAnimationFrame(() => {
      const maxScrollTop = Math.max(0, container.scrollHeight - container.clientHeight);
      const ratio = clamp01(pendingRestoreRatioRef.current);
      if (maxScrollTop <= 0) {
        if (ratio === 0) {
          hasRestoredRef.current = true;
        }
        return;
      }
      container.scrollTop = maxScrollTop * ratio;
      hasRestoredRef.current = true;
    });
    return () => window.cancelAnimationFrame(rafId);
  }, [visibleRowCount]);

  const handleMainScroll = (event: ReactUIEvent<HTMLDivElement>) => {
    const container = event.currentTarget;
    if (!container.classList.contains("main--scrolling")) {
      container.classList.add("main--scrolling");
    }
    if (scrollbarTimerRef.current !== null) {
      window.clearTimeout(scrollbarTimerRef.current);
    }
    scrollbarTimerRef.current = window.setTimeout(() => {
      container.classList.remove("main--scrolling");
    }, 260);

    const maxScrollTop = Math.max(0, container.scrollHeight - container.clientHeight);
    const ratio = maxScrollTop <= 0 ? 0 : container.scrollTop / maxScrollTop;
    pendingRestoreRatioRef.current = clamp01(ratio);
    if (saveTimerRef.current !== null) {
      window.clearTimeout(saveTimerRef.current);
    }
    saveTimerRef.current = window.setTimeout(() => {
      writeStoredScrollRatio(pendingRestoreRatioRef.current);
    }, 120);
  };

  return (
    <div className="app">
      <TopBar />
      <div className={`main main--scroll ${mainStale ? "main--stale" : ""}`} ref={mainRef} onScroll={handleMainScroll}>
        <div className="main__canvas">
          <CanvasScene />
        </div>
        <div className="main__list">
          <SignalsTable />
        </div>
      </div>
      <BubbleDetailsModal />
      <GroupDetailsModal />
    </div>
  );
}
