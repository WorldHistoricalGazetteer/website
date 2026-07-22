// wb-aat-modal.js — one shared "pick Getty AAT place type(s)" modal for the
// Workbench record editors (place#134). Built lazily on first use, appended to
// <body>, and reused across every record editor on the page (wb-dataset mounts
// many). Wraps the shared TypeTreeWidget (search + browse + tri-state selection)
// and reports the chosen concepts back through onApply as [{id:'aat:<id>', text}].
//
// Keeps the Workbench in step with Atlas + Map-your-Data: one AAT dictionary,
// one type-tree widget, one IndexedDB vocab cache.

import TypeTreeWidget from './typeTreeWidget.js';
import { loadAatVocab } from './aatVocab.js';
import { esc } from './wb-shell.js';

let modalEl = null;
let widget = null;
let bsModal = null;
let currentApply = null;

function build() {
  loadAatVocab();   // warm the shared vocab cache (labels + tooltips)
  modalEl = document.createElement('div');
  modalEl.className = 'modal fade wb-aat-modal';
  modalEl.tabIndex = -1;
  modalEl.setAttribute('aria-hidden', 'true');
  modalEl.innerHTML = `
    <div class="modal-dialog modal-dialog-scrollable">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title"><i class="fas fa-shapes me-2 text-secondary"></i>Place types</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <div class="modal-body">
          <p class="small text-muted mb-2">Choose one or more Getty AAT place types — search or browse the hierarchy. Selected types are stored as <code>aat:&lt;id&gt;</code> with their label.</p>
          <div class="wb-aat-tree" style="max-height:52vh;overflow:auto;border:1px solid var(--bs-border-color,#dee2e6);border-radius:.4rem;padding:.4rem .5rem;"></div>
          <div class="form-text small mt-2">Place types from the Getty <a href="https://www.getty.edu/research/tools/vocabularies/aat/" target="_blank" rel="noopener">Art &amp; Architecture Thesaurus (AAT)&reg;</a>, &copy; J.&nbsp;Paul Getty Trust, under the ODC Attribution License.</div>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-outline-secondary btn-sm" data-bs-dismiss="modal">Cancel</button>
          <button type="button" class="btn btn-primary btn-sm wb-aat-apply">Apply</button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(modalEl);
  widget = new TypeTreeWidget(modalEl.querySelector('.wb-aat-tree'), {});
  widget.init();
  modalEl.querySelector('.wb-aat-apply').addEventListener('click', () => {
    if (currentApply) currentApply(widget.getSelectedConcepts());
    if (bsModal) bsModal.hide();
  });
  bsModal = (window.bootstrap && window.bootstrap.Modal) ? new window.bootstrap.Modal(modalEl) : null;
}

/**
 * Open the shared AAT picker.
 * @param {Object}   opts
 * @param {Array}    [opts.selected] - current selection ([{id, text}] or 'aat:<id>' strings)
 * @param {string}   [opts.title]    - optional heading (e.g. the record's name)
 * @param {Function} opts.onApply    - called with [{id:'aat:<id>', text}] when the user clicks Apply
 */
export function openAatPicker(opts = {}) {
  if (!modalEl) build();
  currentApply = opts.onApply || null;
  const titleEl = modalEl.querySelector('.modal-title');
  if (titleEl) titleEl.innerHTML = `<i class="fas fa-shapes me-2 text-secondary"></i>${esc(opts.title || 'Place types')}`;
  widget.setSelected(opts.selected || []);   // pending until the tree renders, then applied
  if (bsModal) bsModal.show();
}
