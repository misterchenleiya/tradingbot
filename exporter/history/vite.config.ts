import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const appBase = mode === "production" ? "/visual-history/" : "/";
  return {
    base: appBase,
    plugins: [react()],
    server: {
      proxy: {
        "/visual-history/api": {
          target: "http://127.0.0.1:8081",
          changeOrigin: true
        }
      }
    }
  };
});
