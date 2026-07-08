// recon-collab-rt.js — real-time collaboration lazy chunk (Workbench Phase 2 spike, place#112).
//
// Connects a WorkbenchProject to the Hocuspocus WebSocket service and syncs the `decisions` map as a
// Yjs shared type (Pass-1 spike scope — proves the round-trip; Pass 2 widens to rows/matches/…).
// Loaded on demand from reconciliation.js only for team (non-personal) server projects; solo/personal
// projects keep the Phase-1 REST sync and never load this chunk (bundle stays lean by default).

import * as Y from 'yjs';
import { HocuspocusProvider } from '@hocuspocus/provider';

let provider = null;
let ydoc = null;
let decisionsMap = null;

export function isConnected() { return !!(provider && provider.status === 'connected'); }

// opts: { serverId, token, wsUrl, onRemoteDecisions(decisionsObj), onStatus(status) }
export function connect(opts) {
  disconnect();
  ydoc = new Y.Doc();
  provider = new HocuspocusProvider({
    url: opts.wsUrl,
    name: opts.serverId,
    token: opts.token,
    document: ydoc,
    onStatus: ({ status }) => { if (opts.onStatus) opts.onStatus(status); },
    onAuthenticationFailed: () => { if (opts.onStatus) opts.onStatus('unauthorized'); },
  });
  decisionsMap = ydoc.getMap('decisions');
  // React only to REMOTE changes (transaction.local is false when applied from a peer update).
  decisionsMap.observe((event, transaction) => {
    if (transaction.local) return;
    if (opts.onRemoteDecisions) opts.onRemoteDecisions(decisionsMap.toJSON());
  });
  return { provider, ydoc };
}

// Mirror the local `decisions` object into the shared map: set changed keys, delete removed ones.
// One transaction (origin 'local') so the observer above ignores it as our own write.
export function syncLocalDecisions(decisions) {
  if (!decisionsMap || !ydoc) return;
  const src = decisions || {};
  ydoc.transact(() => {
    const seen = new Set();
    for (const [k, v] of Object.entries(src)) {
      seen.add(k);
      if (JSON.stringify(decisionsMap.get(k)) !== JSON.stringify(v)) decisionsMap.set(k, v);
    }
    for (const k of Array.from(decisionsMap.keys())) if (!seen.has(k)) decisionsMap.delete(k);
  }, 'local');
}

export function currentDecisions() { return decisionsMap ? decisionsMap.toJSON() : {}; }

export function disconnect() {
  if (provider) { try { provider.destroy(); } catch (_) { /* ignore */ } }
  provider = null;
  ydoc = null;
  decisionsMap = null;
}
