// GENERATED FILE - do not edit directly. Source: static_src/
import { ticketChat } from "./ticketChatActions.js?v=672f0b14d26680ec4c346e9b1c1cd1ec3bab19c20c06c2593893e8ee4601afcd";
// This module now delegates to docChatCore for rendering and event parsing.
export function applyTicketEvent(payload) {
    ticketChat.applyAppEvent(payload);
}
export function renderTicketEvents() {
    ticketChat.renderEvents();
}
export function renderTicketMessages() {
    ticketChat.renderMessages();
}
export function initTicketChatEvents() {
    // Toggle already wired in docChatCore constructor.
    return;
}
