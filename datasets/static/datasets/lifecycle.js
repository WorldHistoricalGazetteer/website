// The v4 dataset lifecycle screens (place#334 Phase 3): the owner's panel on the status page
// and the editors' review page. Not a webpack bundle. Every <form class="lifecycle-form">
// posts its fields as JSON to its data-url with the CSRF token; data-confirm asks first;
// on success the page reloads (it is rendered from the new state), on refusal the endpoint's
// own message is shown beside the button.
(function () {
  if (window.__whgLifecycleBound) return;
  window.__whgLifecycleBound = true;

  function csrfToken() {
    var m = document.querySelector('meta[name="csrf-token"]');
    return m ? m.getAttribute('content') : '';
  }

  function messageOf(data, status) {
    if (data && data.error) return data.error;
    if (data && data.errors) {
      return Object.keys(data.errors).map(function (k) {
        return [].concat(data.errors[k]).join(' ');
      }).join(' ');
    }
    return 'Something went wrong (' + status + '). Please try again.';
  }

  document.addEventListener('submit', function (ev) {
    var form = ev.target;
    if (!form.classList || !form.classList.contains('lifecycle-form')) return;
    ev.preventDefault();
    var question = form.getAttribute('data-confirm');
    if (question && !window.confirm(question)) return;
    var body = {};
    new FormData(form).forEach(function (v, k) { body[k] = v; });
    var submitter = ev.submitter;
    if (submitter && submitter.name) body[submitter.name] = submitter.value;
    var errorEl = form.querySelector('.lifecycle-error');
    if (errorEl) errorEl.textContent = '';
    var buttons = form.querySelectorAll('button');
    buttons.forEach(function (b) { b.disabled = true; });
    fetch(form.getAttribute('data-url'), {
      method: 'POST',
      credentials: 'same-origin',
      headers: {'Content-Type': 'application/json', 'X-CSRFToken': csrfToken()},
      body: JSON.stringify(body)
    }).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (data) {
        if (r.ok) { window.location.reload(); return; }
        if (errorEl) errorEl.textContent = messageOf(data, r.status);
        buttons.forEach(function (b) { b.disabled = false; });
      });
    }).catch(function () {
      if (errorEl) errorEl.textContent = 'Could not reach the server. Please try again.';
      buttons.forEach(function (b) { b.disabled = false; });
    });
  });
})();
