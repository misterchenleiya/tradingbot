import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import "./styles.css";

interface TradingViewErrorBoundaryState {
  error: Error | null;
}

class TradingViewErrorBoundary extends React.Component<React.PropsWithChildren, TradingViewErrorBoundaryState> {
  state: TradingViewErrorBoundaryState = {
    error: null
  };

  static getDerivedStateFromError(error: Error): TradingViewErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    console.error("tradingview render failed", error, errorInfo);
    if (typeof window !== "undefined") {
      (window as Window & { __TRADINGVIEW_BOOT_ERROR__?: string }).__TRADINGVIEW_BOOT_ERROR__ = `${error.name}: ${error.message}`;
    }
  }

  private handleReload = (): void => {
    if (typeof window !== "undefined") {
      window.location.reload();
    }
  };

  private handleResetLocalState = (): void => {
    if (typeof window === "undefined") {
      return;
    }
    try {
      window.localStorage.removeItem("tradingview.ui-state.v1");
      window.localStorage.removeItem("tradingview.ui-state.v2");
    } catch (error) {
      console.warn("failed to clear tradingview ui state", error);
    }
    window.location.reload();
  };

  render(): React.ReactNode {
    if (!this.state.error) {
      return this.props.children;
    }
    return (
      <div className="tv-boot-error">
        <div className="tv-boot-error-card">
          <div className="tv-boot-error-title">TradingView 页面渲染失败</div>
          <div className="tv-boot-error-message">
            {this.state.error.name}: {this.state.error.message}
          </div>
          <div className="tv-boot-error-actions">
            <button type="button" onClick={this.handleReload}>
              重新加载
            </button>
            <button type="button" onClick={this.handleResetLocalState}>
              清空本地缓存并重试
            </button>
          </div>
        </div>
      </div>
    );
  }
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <TradingViewErrorBoundary>
      <App />
    </TradingViewErrorBoundary>
  </React.StrictMode>
);
