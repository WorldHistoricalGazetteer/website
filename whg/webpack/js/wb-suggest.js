// wb-suggest.js — shared, framework-free client for community record corrections
// (plan-record-suggestions §5). Exposes window.WHGSuggest so it can be driven from any surface
// (place detail, portal source-boxes, dataset-places detail pane) without importing it into those
// bundles. Two jobs:
//   1. a delegated click handler that turns any [data-wb-suggest][data-place-id] element into
//      "open this record in the Workbench to correct/suggest" (checkout → redirect to the editor);
//   2. mountInsets(root): fill [data-wb-suggest-inset][data-place-id] containers with the pending
//      suggestions for that record (count for everyone; details for staff/owner/proposer).

import '../css/wb-suggest.css';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

function csrf() {
  var input = document.querySelector('input[name=csrfmiddlewaretoken]');
  if (input && input.value) return input.value;
  var m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
  return m ? decodeURIComponent(m[1]) : '';
}

function esc(s) {
  return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// Check out a record → editor. Any beta user may; the editor shows Publish (owners/staff) or Submit
// suggestion (everyone else) based on the server's can_apply.
function openInWorkbench(placeId, el) {
  if (el) { el.dataset.busy = '1'; el.style.pointerEvents = 'none'; el.style.opacity = '.6'; }
  return fetch('/reconciliation/checkout/place/' + placeId + '/', {
    method: 'POST', credentials: 'same-origin',
    headers: { 'X-CSRFToken': csrf(), 'Content-Type': 'application/json' }, body: '{}'
  }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); })
    .then(function (res) {
      if (res.ok && res.d && res.d.id) { window.location.href = '/workbench/record/?project=' + res.d.id; }
      else {
        alert((res.d && res.d.error) || 'Could not open this record in the Workbench.');
        if (el) { el.dataset.busy = ''; el.style.pointerEvents = ''; el.style.opacity = ''; }
      }
    }).catch(function () {
      alert('Could not reach the Workbench — please try again.');
      if (el) { el.dataset.busy = ''; el.style.pointerEvents = ''; el.style.opacity = ''; }
    });
}

function renderInset(box, data) {
  var n = data.count || 0;
  if (!n) { box.innerHTML = ''; box.classList.remove('wb-inset--has'); return; }
  box.classList.add('wb-inset--has');
  var items = data.items || [];
  var head = '<span class="wb-inset-badge">' + n + ' correction' + (n !== 1 ? 's' : '') + ' proposed</span>';
  if (!items.length) {
    // public: count only, no content (plan §1e)
    box.innerHTML = '<div class="wb-inset">' + head + ' <span class="wb-inset-muted">— awaiting review</span></div>';
    return;
  }
  var rows = items.map(function (it) {
    var fields = (it.changed_fields || []).map(function (f) { return '<span class="wb-inset-chip">' + esc(f) + '</span>'; }).join(' ');
    var why = it.rationale ? ' <span class="wb-inset-muted">— “' + esc(it.rationale) + '”</span>' : '';
    var rev = data.can_review ? ' <a href="/workbench/suggestions/" class="wb-inset-link">review →</a>' : '';
    return '<li>' + fields + ' <span class="wb-inset-muted">by ' + esc(it.proposer) + '</span>' + why + rev + '</li>';
  }).join('');
  box.innerHTML = '<div class="wb-inset">' + head +
    '<ul class="wb-inset-list">' + rows + '</ul></div>';
}

function mountInsets(root) {
  var scope = root || document;
  scope.querySelectorAll('[data-wb-suggest-inset][data-place-id]').forEach(function (box) {
    if (box.dataset.wbLoaded) return;
    box.dataset.wbLoaded = '1';
    fetch('/reconciliation/suggestions/for-place/' + box.dataset.placeId + '/', { credentials: 'same-origin' })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) { if (d) renderInset(box, d); })
      .catch(function () { /* silent */ });
  });
}

// Delegated: any click on a [data-wb-suggest][data-place-id] opens the record in the Workbench.
document.addEventListener('click', function (e) {
  var t = e.target.closest && e.target.closest('[data-wb-suggest][data-place-id]');
  if (!t || t.dataset.busy) return;
  e.preventDefault();
  openInWorkbench(t.dataset.placeId, t);
});

window.WHGSuggest = {
  // Surfaces that inject the affordance into dynamic DOM (portal, ds_places) read this to decide
  // whether to render the button. Set from the page via window.WHG_CAN_SUGGEST (a beta flag).
  canSuggest: !!window.WHG_CAN_SUGGEST,
  open: openInWorkbench,
  mountInsets: mountInsets,
  // Injected button markup surfaces can drop into dynamic DOM (e.g. portal source-boxes).
  buttonHTML: function (placeId, label) {
    return '<a href="#" class="wb-suggest-btn" data-wb-suggest data-place-id="' + placeId + '">' +
      '<i class="fas fa-pen-to-square"></i> ' + esc(label || 'Suggest a correction') + '</a>';
  },
  insetHTML: function (placeId) {
    return '<span class="wb-inset-wrap" data-wb-suggest-inset data-place-id="' + placeId + '"></span>';
  }
};

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { mountInsets(document); });
else mountInsets(document);
