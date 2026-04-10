import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const appBase = mode === "production" ? "/tradingview/" : "/";
  return {
    base: appBase,
    plugins: [react()],
    server: {
      proxy: {
        "/tradingview/api": {
          target: "http://127.0.0.1:8081",
          changeOrigin: true
        }
      }
    }
  };
});
