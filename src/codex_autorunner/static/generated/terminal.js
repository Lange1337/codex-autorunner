// GENERATED FILE - do not edit directly. Source: static_src/
import { TerminalManager } from "./terminalManager.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
import { refreshAgentControls } from "./agentControls.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
import { subscribe } from "./bus.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
import { isRepoHealthy } from "./health.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
let terminalManager = null;
let terminalHealthRefreshInitialized = false;
export function getTerminalManager() {
    return terminalManager;
}
export function initTerminal() {
    if (terminalManager) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        if (typeof terminalManager.fit === "function") {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            terminalManager.fit();
        }
        return;
    }
    terminalManager = new TerminalManager();
    terminalManager.init();
    initTerminalHealthRefresh();
    // Ensure terminal is resized to fit container after initialization
    if (terminalManager) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        if (typeof terminalManager.fit === "function") {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            terminalManager.fit();
        }
    }
}
function initTerminalHealthRefresh() {
    if (terminalHealthRefreshInitialized)
        return;
    terminalHealthRefreshInitialized = true;
    subscribe("repo:health", (payload) => {
        const status = payload?.status || "";
        if (status !== "ok" && status !== "degraded")
            return;
        if (!isRepoHealthy())
            return;
        void refreshAgentControls({ reason: "background" });
    });
}
