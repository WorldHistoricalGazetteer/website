// Relaxable search-constraint chips (place#208), as Map your Data's hand search draws them
// (reconciliation.js searchConstraintsHTML): ticked = applied, unticked = relaxed. Shared module
// so the contributor feed (place#338) shows the same control with the same markup and classes.
import '../css/constraintChips.css';

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// parts: [{id, label, title}]; relaxed: a Set of relaxed part ids.
export function constraintChipsHTML(parts, relaxed) {
  if (!parts || !parts.length) {
    return '<div class="recon-search-cx small mt-1 text-muted">'
      + '<i class="fas fa-globe me-1"></i>Nothing narrows this search: it is worldwide, across every source.</div>';
  }
  const on = parts.filter((c) => !relaxed.has(c.id)).length;
  const chips = parts.map((c) => {
    const applied = !relaxed.has(c.id);
    return `<label class="recon-cx${applied ? '' : ' recon-cx--off'}" title="${esc(c.title)}">`
      + `<input type="checkbox" class="recon-cx-cb" data-cx="${esc(c.id)}"${applied ? ' checked' : ''}> ${esc(c.label)}</label>`;
  }).join('');
  const all = on
    ? '<button type="button" class="btn btn-sm btn-link p-0 ms-1" data-cx-all="relax">relax all</button>'
    : '<button type="button" class="btn btn-sm btn-link p-0 ms-1" data-cx-all="apply">re-apply all</button>';
  return '<div class="recon-search-cx small mt-1"><span class="text-muted me-1" '
    + 'title="The same limits the suggestions were found under. Untick one to widen the search.">Search limited to:</span>'
    + `${chips}${all}</div>`;
}

// Wire a container's chips to `relaxed`; `getParts()` returns the chips now shown; `onChange()`
// re-runs the search.
export function bindConstraintChips(container, getParts, relaxed, onChange) {
  container.addEventListener('change', (e) => {
    const cb = e.target.closest('.recon-cx-cb');
    if (!cb) return;
    if (cb.checked) relaxed.delete(cb.dataset.cx); else relaxed.add(cb.dataset.cx);
    onChange();
  });
  container.addEventListener('click', (e) => {
    const b = e.target.closest('[data-cx-all]');
    if (!b) return;
    if (b.dataset.cxAll === 'relax') getParts().forEach((p) => relaxed.add(p.id)); else relaxed.clear();
    onChange();
  });
}

export { esc };
