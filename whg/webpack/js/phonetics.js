// phonetics.js — grapheme→IPA rule review UI (place#252).
//
// One entry for two surfaces, each feature-detected rather than gated by a flag:
//
//   * the single-row review form — live IPA validation and a preview of what a
//     proposed correction does to real corpus names;
//   * the matching demo — the Symphonym encoder, run locally, so a contributor
//     can see the problem their correction addresses instead of taking our word
//     for it.
//
// ⚠ The demo is honest about its own limits and the code has to stay that way.
// The encoder reads characters; it does not consult these rules at run time.
// The rules produced the IPA it was TRAINED against, so a correction improves
// the next model's training signal and changes nothing about the score shown
// today. The page says so. Do not "improve" this by implying the score responds
// to the reviewer's edit — the first contributor to check would be right to
// stop trusting the tool.

let Symphonym = null;
const loadSymphonym = async () =>
  (Symphonym || (Symphonym = await import(/* webpackChunkName: "recon-symphonym" */ './recon-symphonym.js')));

const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || '';

// ── Single-row review form ──────────────────────────────────────────────────

function initReviewForm() {
  const ctxEl = document.getElementById('phonetics-context');
  const input = document.getElementById('id_proposed_ipa');
  if (!ctxEl || !input) return;
  const ctx = JSON.parse(ctxEl.textContent || '{}');
  const box = document.getElementById('correction-box');
  const feedback = document.getElementById('ipa-feedback');
  const preview = document.getElementById('ipa-preview');

  const showBox = () => {
    const chosen = document.querySelector('input[name="verdict"]:checked');
    const correcting = chosen && chosen.value === 'correct';
    if (box) box.hidden = !correcting;
    if (correcting && !input.value) input.value = input.dataset.current || '';
  };
  document.querySelectorAll('input[name="verdict"]').forEach((el) => el.addEventListener('change', showBox));
  showBox();

  const render = (data) => {
    if (!feedback) return;
    if (data.errors.length) {
      feedback.innerHTML = data.errors.map((e) => `<div class="text-danger">⚠ ${e.message}</div>`).join('');
    } else if (!data.value) {
      feedback.innerHTML = '<span class="text-muted">Blank — this grapheme would produce nothing, '
        + 'which is a legitimate value.</span>';
    } else {
      feedback.innerHTML = '<span class="text-success">✓ Usable.</span> '
        + data.segments.map((s) => `<span class="seg">${s}</span>`).join('')
        + ` <span class="text-muted font-monospace">${data.codepoints}</span>`;
    }
  };

  async function previewNames() {
    if (!preview || !Array.isArray(ctx.examples) || !ctx.examples.length) return;
    const names = ctx.examples.slice(0, 5).map((e) => e.name).filter(Boolean);
    if (!names.length) return;
    try {
      const res = await fetch(ctx.transcribeUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
        body: JSON.stringify({ ruleset: ctx.ruleset, names, overrides: { [ctx.orth]: input.value } }),
      });
      const data = await res.json();
      preview.innerHTML = '<div class="text-muted mb-1">Effect on corpus names:</div>'
        + data.results.map((r) => `<div><span lang="${ctx.lang}">${r.name}</span>: <code>${r.before.output}</code>`
          + (r.changed ? ` → <code class="text-primary">${r.after.output}</code>`
                       : ' <span class="text-muted">(unchanged)</span>') + '</div>').join('');
    } catch (e) { /* a bonus, never a blocker */ }
  }

  async function validate() {
    try {
      // Advice only. The same checks run server-side on POST, because what must
      // be impossible is *storing* an unusable value, not typing one.
      render(await (await fetch(`${ctx.validateUrl}?ipa=${encodeURIComponent(input.value)}`)).json());
    } catch (e) {
      if (feedback) feedback.innerHTML = '<span class="text-muted">Could not reach the validator; '
        + 'your value is still checked when you submit.</span>';
    }
    previewNames();
  }

  let timer = null;
  input.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(validate, 250); });
  if (input.value) validate();
}

// ── Matching demo ───────────────────────────────────────────────────────────

function cosine(a, b) {
  let dot = 0;
  for (let i = 0; i < a.length; i += 1) dot += a[i] * b[i];
  // embedNames returns int8-quantised vectors of an L2-normalised embedding, so
  // the dot product recovers cosine once the quantisation scale is divided out.
  return dot / (127 * 127);
}

function verdict(score) {
  if (score >= 0.7) return ['success', 'These are matched confidently.'];
  if (score >= 0.45) return ['warning', 'A weak match — it would rank, but not reliably.'];
  return ['danger', 'Effectively no signal. On this script the matcher is at chance, '
                    + 'which is exactly the gap better rules would close.'];
}

function initMatchingDemo() {
  const root = document.getElementById('match-demo');
  const out = document.getElementById('m-out');
  const button = document.getElementById('m-go');
  if (!root || !button) return;

  button.addEventListener('click', async () => {
    const a = document.getElementById('m-a').value.trim();
    const b = document.getElementById('m-b').value.trim();
    if (!a || !b) { out.innerHTML = '<span class="text-muted">Enter both names.</span>'; return; }
    out.innerHTML = '<span class="text-muted">Loading the encoder (about 21 MB, cached after the '
      + 'first run) and embedding locally…</span>';
    try {
      const { embedNames } = await loadSymphonym();
      const lang = root.dataset.lang || 'und';
      // Each name is embedded under its own language tag: the encoder is
      // language-conditioned, and tagging the comparison name with the script
      // under review would measure something nobody will ever ask it.
      const [va, vb] = await Promise.all([embedNames([a], { lang }), embedNames([b], {})]);
      const score = cosine(va, vb);
      const [tone, message] = verdict(score);
      const pct = Math.max(0, Math.min(100, Math.round(score * 100)));
      out.innerHTML = `
        <div class="mb-1"><strong>Similarity ${score.toFixed(3)}</strong>
          <span class="text-${tone}">${message}</span></div>
        <div class="score-bar mb-2"><span style="width:${pct}%"></span></div>
        <div class="text-muted">Computed in your browser by the same encoder WHG matches with.
          Nothing was sent anywhere.</div>`;
      await showTeacherSignal(root, out, a, b);
    } catch (err) {
      out.innerHTML = `<span class="text-danger">The encoder could not be loaded: ${err.message}</span>`;
    }
  });
}

// The other half of the story, and the half the reviewer can actually change:
// what the rules under review turn these names into. Residue is shown because
// that is where a rule set with a hole in it becomes visible.
async function showTeacherSignal(root, out, a, b) {
  try {
    const res = await fetch(root.dataset.transcribeUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf() },
      body: JSON.stringify({ ruleset: root.dataset.ruleset, names: [a] }),
    });
    const data = await res.json();
    const r = data.results && data.results[0];
    if (!r) return;
    const residue = r.before.residue || [];
    out.insertAdjacentHTML('beforeend', `
      <hr class="my-2">
      <div><strong>What these rules make of “${a}”:</strong>
        <code>${r.before.output}</code>
        ${residue.length
          ? `<div class="text-danger">${residue.length} character${residue.length === 1 ? '' : 's'} matched no rule`
            + ` — ${residue.map((c) => `<code>${c}</code>`).join(' ')}. Those are the rows worth your time.</div>`
          : '<div class="text-success">Every character matched a rule.</div>'}
      </div>
      <div class="text-muted mt-1">This IPA is the training signal, not the encoder's input:
        improving it improves the next model rather than the score above.</div>`);
  } catch (e) { /* the score above stands on its own */ }
}

// ── Competence form ─────────────────────────────────────────────────────────

// Narrow the writing-system list to the ones the chosen language is actually
// written in. Offering all 226 when a language uses one is not a choice, it is a
// haystack — and picking the wrong one silently routes you to no work at all.
function initCompetenceForm() {
  const language = document.getElementById('id_language_code');
  const script = document.getElementById('id_script_code');
  if (!language || !script) return;
  const all = Array.from(script.options).map((o) => ({ value: o.value, text: o.text }));

  const narrow = () => {
    const chosen = language.selectedOptions[0];
    const allowed = (chosen?.dataset.scripts || '').split(',').filter(Boolean);
    const keep = allowed.length
      ? all.filter((o) => !o.value || allowed.includes(o.value))
      : all;
    script.innerHTML = '';
    keep.forEach((o) => script.add(new Option(o.text, o.value)));
    // One writing system means there is nothing to choose; say so rather than
    // presenting a select with a single option.
    script.disabled = keep.length <= 2 && allowed.length === 1;
    if (script.disabled) script.value = allowed[0];
  };
  language.addEventListener('change', narrow);
  if (language.value) narrow();
}

// ── Guided tour ─────────────────────────────────────────────────────────────

// Loaded lazily: driver.js and its stylesheet are dead weight for the many page
// views by someone who has already taken the tour.
async function initTour() {
  const button = document.getElementById('phonetics-tour-button');
  const onLandingPage = document.querySelector('.phonetics')?.dataset.phoneticsPage === 'home';
  const tour = await import(/* webpackChunkName: "phonetics-tour" */ './phoneticsTour.js');

  button?.addEventListener('click', () => tour.startPhoneticsTour());
  // Auto-start on a first visit, and only from the landing page: dropping
  // someone into a tour of the whole tool when they arrived at one row is
  // an interruption, not an introduction.
  if (onLandingPage && !tour.hasSeenPhoneticsTour()) {
    setTimeout(() => tour.startPhoneticsTour(), 600);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  initReviewForm();
  initMatchingDemo();
  initCompetenceForm();
  initTour();
});
