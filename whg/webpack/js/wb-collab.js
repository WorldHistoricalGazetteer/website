// wb-collab.js — shared "Collaborate & share" pane for Workbench doc-type editors.
//
// Reuses the place#112 collaboration substrate wholesale (plan §7): Team-owned WorkbenchProjects,
// read-only share links, and (Phase 2) Hocuspocus real-time co-editing — all doc-type-agnostic and
// already built for Map your Data. This module just renders that story as a Step pane the new
// editors mount. It shares the editor's `serverBridge` instance, so once a project is saved here the
// editor's autosave pushes to the same server project.

import { el, esc, copyToClipboard } from './wb-shell.js';
import { listTeams, createTeam, listMembers, addMember, shareProject, unshareProject, collabToken } from './recon-sync.js';

const ROLE_LABEL = { owner: 'owner', editor: 'editor', viewer: 'viewer' };

// mountCollab({ bridge, getSnapshot, getTitle, container, onSaved }):
//   bridge      — the editor's serverBridge (shared id/version)
//   getSnapshot — () => current snapshot object
//   getTitle    — () => current title string
//   container   — the pane body element to render into
//   onSaved     — optional callback(projectId) after a save happens here
export function mountCollab({ bridge, getSnapshot, getTitle, container, onSaved }) {
  let teams = [];          // [{id,title,role,member_count}]
  let selectedTeam = '';   // '' = personal (private)
  let rt = null;           // realtime availability: null unknown, true/false resolved

  const setBusy = (node, on) => { if (node) node.disabled = on; };

  async function refreshTeams() {
    const r = await listTeams();
    teams = (r.ok && r.data && r.data.teams) || [];
  }

  async function detectRealtime() {
    if (!bridge.id) { rt = null; return; }
    const r = await collabToken(bridge.id);
    rt = r.status === 200;   // 501 → not configured (async sync), 403 → not member
  }

  async function save() {
    // Create the server project in the chosen team (or personal). If already saved, just push.
    if (bridge.id) { await bridge.push(getSnapshot()); }
    else { await bridge.create(getSnapshot(), getTitle(), selectedTeam || null); }
    if (bridge.id && onSaved) onSaved(bridge.id);
    await detectRealtime();
    render();
  }

  async function doShare(on) {
    if (!bridge.id) return;
    await (on ? shareProject(bridge.id) : unshareProject(bridge.id));
    render();
  }

  function render() {
    if (!container) return;
    const saved = !!bridge.id;
    const teamOpts = ['<option value="">My workbench (private — just me)</option>']
      .concat(teams.map((t) => `<option value="${t.id}" ${String(selectedTeam) === String(t.id) ? 'selected' : ''}>${esc(t.title)} (${t.member_count} member${t.member_count === 1 ? '' : 's'})</option>`))
      .concat('<option value="__new">＋ Create a new team…</option>').join('');

    container.innerHTML = `
      <p class="text-muted small mb-2">Collaborate with a team, or share a read-only link. Your project is
        saved to WHG only when you choose — nothing is shared until then.</p>
      <div class="mb-3">
        <label class="form-label small mb-1">Who can edit this?</label>
        <select id="wb-collab-team" class="form-select form-select-sm mb-2">${teamOpts}</select>
        <div id="wb-collab-newteam" class="input-group input-group-sm mb-2 d-none">
          <input id="wb-collab-newteam-title" class="form-control" placeholder="New team name">
          <button id="wb-collab-newteam-btn" class="btn btn-outline-primary" type="button">Create</button>
        </div>
        <button id="wb-collab-save" class="btn btn-sm btn-primary" type="button">
          ${saved ? 'Update saved project' : 'Save to collaborate'}</button>
        <span id="wb-collab-savemsg" class="small ms-2"></span>
      </div>
      ${saved ? renderShareAndInvite() : '<p class="text-muted small mb-0">Save first to invite people or create a share link.</p>'}
    `;
    wire();
    if (saved) loadMembers();
  }

  function renderShareAndInvite() {
    const rtNote = rt === true
      ? '<span class="badge bg-success">Real-time co-editing on</span>'
      : rt === false ? '<span class="badge bg-secondary">Changes sync when saved</span>' : '';
    return `
      <hr>
      <div class="mb-3">
        <label class="form-label small mb-1">Invite people to this team ${rtNote}</label>
        <div class="input-group input-group-sm mb-1">
          <input id="wb-collab-invite" class="form-control" placeholder="username">
          <select id="wb-collab-role" class="form-select" style="max-width:8rem;">
            <option value="editor">Editor</option><option value="viewer">Viewer</option>
          </select>
          <button id="wb-collab-invite-btn" class="btn btn-outline-primary" type="button">Invite</button>
        </div>
        <div id="wb-collab-invite-msg" class="small"></div>
        <ul id="wb-collab-members" class="list-unstyled small mb-0 mt-1"></ul>
      </div>
      <div>
        <label class="form-label small mb-1">Read-only share link</label>
        <div id="wb-collab-share"></div>
      </div>`;
  }

  function renderShareLink() {
    const box = el('wb-collab-share'); if (!box) return;
    // The bridge doesn't track share state; offer both actions and let the server be authoritative.
    box.innerHTML = `
      <div class="input-group input-group-sm">
        <input id="wb-collab-link" class="form-control" readonly placeholder="No link yet — create one">
        <button id="wb-collab-mklink" class="btn btn-outline-secondary" type="button">Create link</button>
        <button id="wb-collab-copy" class="btn btn-outline-secondary" type="button">Copy</button>
        <button id="wb-collab-revoke" class="btn btn-outline-danger" type="button">Revoke</button>
      </div>`;
    el('wb-collab-mklink').addEventListener('click', async () => {
      const r = await shareProject(bridge.id);
      if (r.ok && r.data && r.data.url) el('wb-collab-link').value = r.data.url;
    });
    el('wb-collab-copy').addEventListener('click', async () => {
      const v = el('wb-collab-link').value; if (v) await copyToClipboard(v);
    });
    el('wb-collab-revoke').addEventListener('click', async () => {
      await unshareProject(bridge.id); el('wb-collab-link').value = '';
    });
  }

  async function loadMembers() {
    if (!selectedTeam || selectedTeam === '__new') { return; }
    const r = await listMembers(selectedTeam);
    const ul = el('wb-collab-members');
    if (ul && r.ok && r.data) {
      ul.innerHTML = (r.data.members || []).map((m) =>
        `<li><i class="fas fa-user fa-fw me-1 text-muted"></i>${esc(m.name || m.username)} <span class="text-muted">(${ROLE_LABEL[m.role] || m.role})</span></li>`).join('');
    }
    renderShareLink();
  }

  function wire() {
    const sel = el('wb-collab-team');
    if (sel) sel.addEventListener('change', () => {
      if (sel.value === '__new') { el('wb-collab-newteam').classList.remove('d-none'); }
      else { el('wb-collab-newteam').classList.add('d-none'); selectedTeam = sel.value; }
    });
    const newBtn = el('wb-collab-newteam-btn');
    if (newBtn) newBtn.addEventListener('click', async () => {
      const title = (el('wb-collab-newteam-title').value || '').trim();
      if (!title) return;
      setBusy(newBtn, true);
      const r = await createTeam(title);
      setBusy(newBtn, false);
      if (r.ok && r.data && r.data.id) { await refreshTeams(); selectedTeam = String(r.data.id); render(); }
    });
    const saveBtn = el('wb-collab-save');
    if (saveBtn) saveBtn.addEventListener('click', async () => {
      setBusy(saveBtn, true);
      el('wb-collab-savemsg').textContent = 'Saving…';
      await save();
      const m = el('wb-collab-savemsg'); if (m) { m.textContent = 'Saved.'; m.className = 'small ms-2 text-success'; }
    });
    const inviteBtn = el('wb-collab-invite-btn');
    if (inviteBtn) inviteBtn.addEventListener('click', async () => {
      if (!selectedTeam || selectedTeam === '__new') {
        el('wb-collab-invite-msg').innerHTML = '<span class="text-warning">Save this project to a team first.</span>'; return;
      }
      const who = (el('wb-collab-invite').value || '').trim();
      const role = el('wb-collab-role').value;
      if (!who) return;
      const r = await addMember(selectedTeam, who, role);
      const msg = el('wb-collab-invite-msg');
      if (r.ok) { msg.innerHTML = `<span class="text-success">Added ${esc(who)}.</span>`; el('wb-collab-invite').value = ''; loadMembers(); }
      else { msg.innerHTML = `<span class="text-danger">${esc((r.data && r.data.error) || 'Could not add that user.')}</span>`; }
    });
  }

  // init
  (async () => { await refreshTeams(); if (bridge.id) await detectRealtime(); render(); })();

  return { render };
}
