import { MouseEvent as ReactMouseEvent, memo, useEffect, useMemo, useRef } from "react";
import { Engine } from "./physics/Engine";
import { CanvasRenderer, resolveViewportTransform, toWorldPoint } from "./renderer/CanvasRenderer";
import { loadBubblePresets } from "./renderer/bubblePresets";
import { ImageCache } from "../utils/imageCache";
import { AppConfig } from "../app/config";
import { readCachedPageData, writeCachedPageData } from "../app/cache";
import { useAppStore } from "../app/store";
import { MockDataSource } from "../datasource/MockDataSource";
import { RestWsDataSource } from "../datasource/RestWsDataSource";
import type { IDataSource } from "../datasource/IDataSource";
import { generateRandomBubbles } from "../data/mock/generator";
import type { PositionItem } from "../app/types";

const LAYOUT_WORLD_WIDTH = 1600;
const LAYOUT_WORLD_HEIGHT = 900;
const LAYOUT_WORLD_AREA = LAYOUT_WORLD_WIDTH * LAYOUT_WORLD_HEIGHT;
const LAYOUT_BASE_RADIUS = 30;
const LAYOUT_RADIUS_MIN = LAYOUT_BASE_RADIUS * 0.5;
const LAYOUT_RADIUS_MAX = LAYOUT_BASE_RADIUS * 5;
const CACHE_WRITE_THROTTLE_MS = 3000;

function normalizeSymbolKey(symbol: string): string {
  return symbol.trim().toUpperCase();
}

function simplifySymbolKey(symbol: string): string {
  const raw = normalizeSymbolKey(symbol);
  if (!raw) return "";
  const parts = raw.split(/[/:_-]/).filter(Boolean);
  let base = parts.length > 1 ? parts[0] : raw;
  base = base.replace(/USDT(?:\.P|P)?$/i, "");
  base = base.replace(/\.P$/i, "");
  return base || raw;
}

function buildPositionRateMap(positions: PositionItem[]): Map<string, number> {
  const out = new Map<string, number>();
  for (const item of positions) {
    if (typeof item.unrealizedProfitRate !== "number" || !Number.isFinite(item.unrealizedProfitRate)) continue;
    const rawKey = normalizeSymbolKey(item.symbol || "");
    if (!rawKey) continue;
    out.set(rawKey, item.unrealizedProfitRate);
    const simplified = simplifySymbolKey(rawKey);
    if (simplified) {
      out.set(simplified, item.unrealizedProfitRate);
    }
  }
  return out;
}

function resolveAdaptiveWorldSize(
  viewportWidth: number,
  viewportHeight: number
): { width: number; height: number } {
  const safeWidth = Math.max(1, viewportWidth);
  const safeHeight = Math.max(1, viewportHeight);
  const aspect = safeWidth / safeHeight;
  const clampedAspect = Math.min(4, Math.max(0.35, aspect));
  const worldWidth = Math.sqrt(LAYOUT_WORLD_AREA * clampedAspect);
  const worldHeight = LAYOUT_WORLD_AREA / worldWidth;
  return { width: worldWidth, height: worldHeight };
}

function CanvasSceneRuntime() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const canvasContextRef = useRef<CanvasRenderingContext2D | null>(null);
  const engineRef = useRef<Engine | null>(null);
  const rendererRef = useRef<CanvasRenderer | null>(null);
  const imageCacheRef = useRef(new ImageCache());
  const colorMetricRef = useRef(AppConfig.initialColorMetric);
  const positionRateBySymbolRef = useRef<Map<string, number>>(new Map());
  const worldSizeRef = useRef({ width: LAYOUT_WORLD_WIDTH, height: LAYOUT_WORLD_HEIGHT });
  const viewportRef = useRef({ width: 0, height: 0, dpr: 1 });
  const forceFastPhysicsUntilRef = useRef(0);
  const forceFastRenderUntilRef = useRef(0);

  const bubbleCount = useAppStore((s) => s.bubbleCount);
  const sizeMetric = useAppStore((s) => s.sizeMetric);
  const colorMetric = useAppStore((s) => s.colorMetric);
  const visibleDataList = useAppStore((s) => s.dataList);
  const visibleDataListRef = useRef(visibleDataList);
  const positionSnapshot = useAppStore((s) => s.positionSnapshot);
  const renderSnapshotVersion = useAppStore((s) => s.renderSnapshotVersion);
  const renderPatchVersion = useAppStore((s) => s.renderPatchVersion);
  const renderPatches = useAppStore((s) => s.renderPatches);
  const renderPatchesRef = useRef(renderPatches);
  const setStats = useAppStore((s) => s.setStats);
  const selectedBubbleId = useAppStore((s) => s.selectedBubbleId);
  const setSelectedBubbleId = useAppStore((s) => s.setSelectedBubbleId);
  const clearSelectedBubbleId = useAppStore((s) => s.clearSelectedBubbleId);
  const selectedGroupId = useAppStore((s) => s.selectedGroupId);
  const setSelectedGroupId = useAppStore((s) => s.setSelectedGroupId);
  const clearSelectedGroupId = useAppStore((s) => s.clearSelectedGroupId);
  const positionRateBySymbol = useMemo(
    () => buildPositionRateMap(positionSnapshot?.positions || []),
    [positionSnapshot?.positions]
  );

  useEffect(() => {
    colorMetricRef.current = colorMetric;
  }, [colorMetric]);

  useEffect(() => {
    positionRateBySymbolRef.current = positionRateBySymbol;
  }, [positionRateBySymbol]);

  useEffect(() => {
    visibleDataListRef.current = visibleDataList;
  }, [visibleDataList]);

  useEffect(() => {
    renderPatchesRef.current = renderPatches;
  }, [renderPatches]);

  useEffect(() => {
    const engine = new Engine();
    engineRef.current = engine;
    const renderer = new CanvasRenderer(imageCacheRef.current);
    rendererRef.current = renderer;

    let rafId = 0;
    let cancelled = false;
    let lastTime = performance.now();
    let lastFpsTime = lastTime;
    let frames = 0;
    let accumulatorMs = 0;
    const physicsStepFastMs = 1000 / 60;
    const physicsStepSlowMs = 1000 / 30;
    const maxCatchUpSteps = 5;
    const activityFastThreshold = 0.34;
    const activitySlowThreshold = 0.22;
    let physicsStepMs = physicsStepFastMs;
    const drawIntervalFastMs = 1000 / 60;
    const drawIntervalMidMs = 1000 / 45;
    const drawIntervalSlowMs = 1000 / 30;
    let drawIntervalMs = drawIntervalFastMs;
    let nextDrawAt = lastTime;

    const loop = (now: number) => {
      const canvas = canvasRef.current;
      const renderer = rendererRef.current;
      const engineInstance = engineRef.current;
      if (!canvas || !renderer || !engineInstance) {
        rafId = requestAnimationFrame(loop);
        return;
      }

      let ctx = canvasContextRef.current;
      if (!ctx || ctx.canvas !== canvas) {
        ctx = canvas.getContext("2d");
        if (ctx) {
          canvasContextRef.current = ctx;
        }
      }
      if (!ctx) {
        rafId = requestAnimationFrame(loop);
        return;
      }

      const dtMs = Math.min(100, now - lastTime);
      lastTime = now;
      accumulatorMs += dtMs;

      const activityScore = engineInstance.getActivityScore();
      if (now < forceFastPhysicsUntilRef.current) {
        physicsStepMs = physicsStepFastMs;
      } else if (physicsStepMs === physicsStepFastMs && activityScore < activitySlowThreshold) {
        physicsStepMs = physicsStepSlowMs;
      } else if (physicsStepMs === physicsStepSlowMs && activityScore > activityFastThreshold) {
        physicsStepMs = physicsStepFastMs;
      }
      const prevDrawIntervalMs = drawIntervalMs;
      if (now < forceFastRenderUntilRef.current) {
        drawIntervalMs = drawIntervalFastMs;
      } else if (activityScore > 0.44) {
        drawIntervalMs = drawIntervalFastMs;
      } else if (activityScore > 0.2) {
        drawIntervalMs = drawIntervalMidMs;
      } else {
        drawIntervalMs = drawIntervalSlowMs;
      }
      if (drawIntervalMs !== prevDrawIntervalMs) {
        nextDrawAt = now + drawIntervalMs;
      }

      let physicsSteps = 0;
      while (accumulatorMs >= physicsStepMs && physicsSteps < maxCatchUpSteps) {
        engineInstance.tick(physicsStepMs / 1000);
        accumulatorMs -= physicsStepMs;
        physicsSteps += 1;
      }
      if (physicsSteps >= maxCatchUpSteps) {
        accumulatorMs = Math.min(accumulatorMs, physicsStepMs * 1.5);
      }

      if (now < nextDrawAt) {
        rafId = requestAnimationFrame(loop);
        return;
      }
      while (nextDrawAt <= now) {
        nextDrawAt += drawIntervalMs;
      }

      const { width, height, dpr } = viewportRef.current;
      if (width <= 0 || height <= 0) {
        rafId = requestAnimationFrame(loop);
        return;
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      renderer.draw(ctx, {
        width,
        height,
        dpr,
        nowMs: now,
        worldWidth: worldSizeRef.current.width,
        worldHeight: worldSizeRef.current.height,
        groups: engineInstance.getGroupBubbles(),
        bubbles: engineInstance.getBubbles(),
        colorMetric: colorMetricRef.current,
        positionRateBySymbol: positionRateBySymbolRef.current
      });

      frames += 1;
      if (now - lastFpsTime > 500) {
        const fps = frames / ((now - lastFpsTime) / 1000);
        setStats({
          fps: Math.round(fps),
          dtMs: Math.round(dtMs),
          bubbleCount: engineInstance.getBubbles().length
        });
        frames = 0;
        lastFpsTime = now;
      }

      rafId = requestAnimationFrame(loop);
    };

    const bootstrap = async () => {
      const presetRuntime = await loadBubblePresets();
      if (cancelled) return;
      renderer.setBubblePresetRuntime(presetRuntime);
      rafId = requestAnimationFrame(loop);
    };

    void bootstrap();

    return () => {
      cancelled = true;
      cancelAnimationFrame(rafId);
      canvasContextRef.current = null;
    };
  }, [setStats]);

  useEffect(() => {
    const engine = engineRef.current;
    if (!engine) return;
    engine.setMetrics(sizeMetric, colorMetric);
  }, [sizeMetric, colorMetric]);

  useEffect(() => {
    engineRef.current?.setMaxBubbles(bubbleCount);
  }, [bubbleCount]);

  useEffect(() => {
    const container = containerRef.current;
    const canvas = canvasRef.current;
    const engine = engineRef.current;
    if (!container || !canvas || !engine) return;
    if (!canvasContextRef.current || canvasContextRef.current.canvas !== canvas) {
      canvasContextRef.current = canvas.getContext("2d");
    }

    const resize = () => {
      const rect = container.getBoundingClientRect();
      const rawDpr = window.devicePixelRatio || 1;
      const maxRenderDpr =
        Number.isFinite(AppConfig.maxRenderDpr) && AppConfig.maxRenderDpr > 0
          ? AppConfig.maxRenderDpr
          : 1.5;
      const dpr = Math.max(1, Math.min(rawDpr, maxRenderDpr));
      canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      canvas.height = Math.max(1, Math.floor(rect.height * dpr));
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
      viewportRef.current = { width: rect.width, height: rect.height, dpr };
      const worldSize = resolveAdaptiveWorldSize(rect.width, rect.height);
      worldSizeRef.current = worldSize;
      engine.setViewport(worldSize.width, worldSize.height);
      engine.setRadiusRange(LAYOUT_RADIUS_MIN, LAYOUT_RADIUS_MAX);
      engine.setBaseRadius(LAYOUT_BASE_RADIUS);
    };

    const observer = new ResizeObserver(resize);
    observer.observe(container);
    resize();
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const engine = engineRef.current;
    if (!engine) return;
    const snapshot = visibleDataListRef.current;
    engine.setSnapshot(snapshot);
    forceFastPhysicsUntilRef.current = performance.now() + 1600;
    forceFastRenderUntilRef.current = performance.now() + 1600;
    imageCacheRef.current.prefetch(
      snapshot.map((item) => item.logoUrl).filter(Boolean) as string[]
    );
  }, [renderSnapshotVersion]);

  useEffect(() => {
    const engine = engineRef.current;
    const patches = renderPatchesRef.current;
    if (!engine || patches.length === 0) return;
    engine.applyUpdates(patches);
    forceFastPhysicsUntilRef.current = performance.now() + 1200;
    forceFastRenderUntilRef.current = performance.now() + 1200;
    imageCacheRef.current.prefetch(
      patches.map((item) => item.logoUrl).filter(Boolean) as string[]
    );
  }, [renderPatchVersion]);

  const handleCanvasMouseDown = (event: ReactMouseEvent<HTMLCanvasElement>) => {
    if (event.button !== 0) return;
    const canvas = canvasRef.current;
    const engine = engineRef.current;
    if (!canvas || !engine) return;
    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const worldWidth = worldSizeRef.current.width;
    const worldHeight = worldSizeRef.current.height;
    const transform = resolveViewportTransform(
      rect.width,
      rect.height,
      worldWidth,
      worldHeight
    );
    const worldPoint = toWorldPoint(
      x,
      y,
      worldWidth,
      worldHeight,
      transform
    );
    if (!worldPoint) {
      // 点击到画布中的留白区域（非世界坐标可映射区域）也视为“空白点击关闭”。
      clearSelectedBubbleId();
      clearSelectedGroupId();
      return;
    }
    const picked = engine.pick(worldPoint.x, worldPoint.y);
    if (picked) {
      forceFastRenderUntilRef.current = performance.now() + 1200;
      forceFastPhysicsUntilRef.current = performance.now() + 1200;
      if (picked.id === selectedBubbleId) {
        clearSelectedBubbleId();
        return;
      }
      setSelectedBubbleId(picked.id);
      return;
    }
    const pickedGroup = engine.pickGroup(worldPoint.x, worldPoint.y);
    if (!pickedGroup) {
      clearSelectedBubbleId();
      clearSelectedGroupId();
      return;
    }
    forceFastRenderUntilRef.current = performance.now() + 1200;
    forceFastPhysicsUntilRef.current = performance.now() + 1200;
    if (pickedGroup.groupId === selectedGroupId) {
      clearSelectedGroupId();
      return;
    }
    setSelectedGroupId(pickedGroup.groupId);
  };

  return (
    <div className="canvas-wrap" ref={containerRef}>
      <canvas ref={canvasRef} onMouseDown={handleCanvasMouseDown} />
    </div>
  );
}

function CanvasSceneDataController() {
  const cacheTimerRef = useRef<number | null>(null);
  const cacheIdleRef = useRef<number | null>(null);
  const pendingCachePayloadRef = useRef<{
    signals: ReturnType<typeof useAppStore.getState>["allDataList"];
    accountSnapshot: ReturnType<typeof useAppStore.getState>["accountSnapshot"];
    positionSnapshot: ReturnType<typeof useAppStore.getState>["positionSnapshot"];
    historySnapshot: ReturnType<typeof useAppStore.getState>["historySnapshot"];
  } | null>(null);

  const hasLiveSnapshot = useAppStore((s) => s.hasLiveSnapshot);
  const allDataList = useAppStore((s) => s.allDataList);
  const accountSnapshot = useAppStore((s) => s.accountSnapshot);
  const positionSnapshot = useAppStore((s) => s.positionSnapshot);
  const historySnapshot = useAppStore((s) => s.historySnapshot);
  const setSnapshot = useAppStore((s) => s.setSnapshot);
  const applyUpdates = useAppStore((s) => s.applyUpdates);
  const setDataSourceStatus = useAppStore((s) => s.setDataSourceStatus);
  const setBackendStatus = useAppStore((s) => s.setBackendStatus);
  const setAccountSnapshot = useAppStore((s) => s.setAccountSnapshot);
  const setPositionSnapshot = useAppStore((s) => s.setPositionSnapshot);
  const setHistorySnapshot = useAppStore((s) => s.setHistorySnapshot);
  const setGroupsSnapshot = useAppStore((s) => s.setGroupsSnapshot);
  const setHistoryLoadMoreHandler = useAppStore((s) => s.setHistoryLoadMoreHandler);
  const setCandleFetchHandler = useAppStore((s) => s.setCandleFetchHandler);
  const setHasLiveSnapshot = useAppStore((s) => s.setHasLiveSnapshot);

  useEffect(
    () => () => {
      if (cacheTimerRef.current !== null) {
        window.clearTimeout(cacheTimerRef.current);
      }
      if (cacheIdleRef.current !== null && typeof window.cancelIdleCallback === "function") {
        window.cancelIdleCallback(cacheIdleRef.current);
      }
    },
    []
  );

  useEffect(() => {
    const hasAnyData =
      allDataList.length > 0 ||
      accountSnapshot !== undefined ||
      positionSnapshot !== undefined ||
      historySnapshot !== undefined;
    if (!hasAnyData && !hasLiveSnapshot) {
      return;
    }

    pendingCachePayloadRef.current = {
      signals: allDataList,
      accountSnapshot,
      positionSnapshot,
      historySnapshot
    };

    if (cacheTimerRef.current !== null || cacheIdleRef.current !== null) {
      return;
    }

    cacheTimerRef.current = window.setTimeout(() => {
      cacheTimerRef.current = null;
      const flush = () => {
        cacheIdleRef.current = null;
        const payload = pendingCachePayloadRef.current;
        if (!payload) return;
        writeCachedPageData(payload);
      };

      if (typeof window.requestIdleCallback === "function") {
        cacheIdleRef.current = window.requestIdleCallback(flush, {
          timeout: 1200
        });
        return;
      }

      flush();
    }, CACHE_WRITE_THROTTLE_MS);
  }, [accountSnapshot, allDataList, hasLiveSnapshot, historySnapshot, positionSnapshot]);

  useEffect(() => {
    setHasLiveSnapshot(false);
    const cached = readCachedPageData();
    if (cached) {
      setSnapshot(cached.signals);
      setAccountSnapshot(cached.accountSnapshot);
      setPositionSnapshot(cached.positionSnapshot);
      setHistorySnapshot(cached.historySnapshot);
    }

    const mockData = generateRandomBubbles(AppConfig.maxBubbleCount, 20260127);
    const dataSource: IDataSource =
      AppConfig.dataSource === "restws"
        ? new RestWsDataSource(
            AppConfig.restUrl,
            AppConfig.wsUrl,
            AppConfig.statusUrl,
            AppConfig.accountUrl,
            AppConfig.positionUrl,
            AppConfig.historyUrl,
            AppConfig.groupsUrl,
            AppConfig.dataStaleCheckIntervalMs,
            {
              onSnapshot: (data) => {
                setSnapshot(data);
                setHasLiveSnapshot(true);
                setDataSourceStatus({ mode: "restws", lastSnapshotAt: Date.now() });
              },
              onUpdate: (updates) => {
                applyUpdates(updates);
                setDataSourceStatus({ lastUpdateAt: Date.now() });
              },
              onStatus: (status) => setDataSourceStatus(status),
              onBackendStatus: (status) => setBackendStatus(status),
              onAccount: (account) => setAccountSnapshot(account),
              onGroups: (groups) => setGroupsSnapshot(groups),
              onPosition: (position) => setPositionSnapshot(position),
              onHistory: (history) => setHistorySnapshot(history)
            }
          )
        : new MockDataSource(mockData, AppConfig.mockUpdateIntervalMs, {
            onSnapshot: (data) => {
              setSnapshot(data);
              setHasLiveSnapshot(true);
              setDataSourceStatus({ mode: "mock", lastSnapshotAt: Date.now(), restStatus: "ok" });
            },
            onUpdate: (updates) => {
              applyUpdates(updates);
              setDataSourceStatus({ lastUpdateAt: Date.now() });
            }
          });

    let started = false;
    const startTimer = window.setTimeout(() => {
      started = true;
      setHistoryLoadMoreHandler(() => dataSource.loadMoreHistory());
      setCandleFetchHandler(dataSource.fetchCandles.bind(dataSource));
      dataSource.start();
    }, 0);

    return () => {
      window.clearTimeout(startTimer);
      setHistoryLoadMoreHandler(undefined);
      setCandleFetchHandler(undefined);
      setHasLiveSnapshot(false);
      if (started) {
        dataSource.stop();
      }
    };
  }, [applyUpdates, setAccountSnapshot, setBackendStatus, setCandleFetchHandler, setDataSourceStatus, setGroupsSnapshot, setHasLiveSnapshot, setHistoryLoadMoreHandler, setHistorySnapshot, setPositionSnapshot, setSnapshot]);

  return null;
}

export const CanvasScene = memo(function CanvasScene() {
  return (
    <>
      <CanvasSceneDataController />
      <CanvasSceneRuntime />
    </>
  );
});
