/**
 * Licence picker for the legacy collection builders (place#158).
 *
 * The Workbench editors get the picker through their own modules; these two
 * jQuery-era builders had no route to record a licence at all, which is why
 * every legacy collection carries none. This wires the same shared component
 * and vocabulary rather than growing a second, divergent one.
 *
 * The chosen SPDX id goes into the form's hidden `license` input, which
 * CollectionModelForm resolves to the FK on save. Unlike the dataset upload
 * form it is optional: a collection is a selection of records that already
 * carry their own terms, so blocking a save over a missing licence would cost
 * work for no gain.
 */
import { wireLicenseControl } from './licensePicker.js';

export default function wireCollectionLicense() {
  const btn = document.getElementById('coll-license-btn');
  const input = document.getElementById('id_license');
  if (!btn || !input) return null;

  const control = wireLicenseControl({
    button: btn,
    display: document.getElementById('coll-license-display'),
    clearBtn: document.getElementById('coll-license-clear'),
    getChoice: () => (input.value ? { spdx: input.value } : null),
    setChoice: (c) => { input.value = (c && c.spdx) || ''; },
  });
  if (control) control.render();
  return control;
}
