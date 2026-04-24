// GENERATED FILE - do not edit directly. Source: static_src/
import { loadHubBootstrapCache, saveHubBootstrapCache } from "./hubCache.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
import { renderRepos as _renderRepos, renderAgentWorkspaces as _renderAgentWorkspaces } from "./hubRepoCards.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
import { applyHubPanelState, toggleHubPanel, initInteractionHarness, setHubChannelEntries as setHubChannelEntriesAction, getHubChannelEntries, getPinnedParentRepoIds } from "./hubActions.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
import { attachHandlersAndControls, bootstrapHubData } from "./hubActions.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
export { loadHubBootstrapCache, saveHubBootstrapCache } from "./hubCache.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
export { applyHubPanelState, toggleHubPanel, initInteractionHarness } from "./hubActions.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
export function initHub() {
    attachHandlersAndControls();
    bootstrapHubData();
}
function renderRepos(repos) {
    _renderRepos(repos, getHubChannelEntries(), getPinnedParentRepoIds());
}
function renderAgentWorkspaces(workspaces) {
    _renderAgentWorkspaces(workspaces, getHubChannelEntries());
}
export const __hubTest = {
    renderRepos,
    renderAgentWorkspaces,
    applyHubPanelState,
    toggleHubPanel,
    loadHubBootstrapCache,
    saveHubBootstrapCache,
    initInteractionHarness,
    setHubChannelEntries(entries) {
        setHubChannelEntriesAction(entries);
    },
};
