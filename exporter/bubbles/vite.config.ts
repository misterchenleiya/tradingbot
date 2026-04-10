import { createLogger, defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

const viteLogger = createLogger();
const viteLoggerError = viteLogger.error;
viteLogger.error = (msg, options) => {
  if (
    typeof msg === "string" &&
    msg.includes("ws proxy socket error:") &&
    msg.includes("write EPIPE")
  ) {
    return;
  }
  viteLoggerError(msg, options);
};

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const normalizeBase = (value: string): string => {
    let base = value.trim();
    if (base.length === 0) {
      return "/";
    }
    if (!base.startsWith("/")) {
      base = `/${base}`;
    }
    if (!base.endsWith("/")) {
      base = `${base}/`;
    }
    return base;
  };
  const appBase = normalizeBase(
    env.VITE_PUBLIC_BASE || (mode === "production" ? "/bubbles/" : "/")
  );
  const rawBase = env.VITE_GOBOT_BASE_URL || "http://127.0.0.1:8081";
  const base = rawBase.replace(/\/+$/, "");
  let target = base;
  let pathPrefix = "";
  try {
    const parsed = new URL(base);
    target = parsed.origin;
    pathPrefix = parsed.pathname === "/" ? "" : parsed.pathname.replace(/\/+$/, "");
  } catch {
    target = "http://127.0.0.1:8081";
    pathPrefix = "";
  }

  const withPrefix = (path: string) => `${pathPrefix}${path}`;

  return {
    base: appBase,
    customLogger: viteLogger,
    plugins: [react()],
    server: {
      proxy: {
        "/signals": {
          target,
          changeOrigin: true,
          rewrite: () => withPrefix("/signals")
        },
        "/status": {
          target,
          changeOrigin: true,
          rewrite: () => withPrefix("/status")
        },
        "/account": {
          target,
          changeOrigin: true,
          rewrite: () => withPrefix("/account")
        },
        "/positions": {
          target,
          changeOrigin: true,
          rewrite: () => withPrefix("/positions")
        },
        "/position": {
          target,
          changeOrigin: true,
          rewrite: () => withPrefix("/position")
        },
        "/ws/stream": {
          target,
          changeOrigin: true,
          rewriteWsOrigin: true,
          ws: true,
          rewrite: () => withPrefix("/ws/stream")
        }
      }
    },
    test: {
      environment: "jsdom",
      globals: true
    }
  };
});
