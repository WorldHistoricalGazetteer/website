// Create a Bootstrap dialog contact form
import '../css/whg-modal.css';

function initWHGModal() {

    // Create the basic modal structure
    $('body').append(`
	  <div class="modal fade" id="whgModal" tabindex="-1" role="dialog" aria-labelledby="whgModalLabel" aria-hidden="true">
	    <div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable" role="document">
	      <div class="modal-content">
	      </div>
	    </div>
	  </div>
	`);

    $('[data-whg-modal]')
        .attr('data-bs-toggle', 'modal')
        .attr('data-bs-target', '#whgModal')
        .attr('href', '#')
        .addClass('text-decoration-none');

    // A click anywhere inside a trigger (including on an <a href="#"> nested in a
    // <span data-whg-modal>) must not also perform its own navigation: Bootstrap
    // only calls preventDefault() when the element carrying data-bs-toggle is
    // itself an <a>, which is not the case for the many span-wrapped triggers.
    $(document).on('click', '[data-whg-modal]', function (e) {
        e.preventDefault();
    });

    $('#whgModal')
        .on('hidden.bs.modal', function (e) {
            const $content = $('#whgModal .modal-content');

            // Clean up any Turnstile widgets
            $content.find('.cf-turnstile').each(function () {
                const widgetId = $(this).data('widget-id');
                if (widgetId && window.turnstile) {
                    turnstile.remove(widgetId);
                }
                $(this).removeData('widget-id').removeData('initialized');
            });

            // Clear modal content
            $content.html('');
        })
        .on('show.bs.modal', function (e) {
            // Only load when Bootstrap was triggered by a real [data-whg-modal] element.
            // A programmatic .modal('show') (see openWHGModal) has no relatedTarget and has
            // already loaded its content — re-entering here would re-request with an
            // undefined URL, which jQuery resolves to the *current page*, replacing the
            // dialog with a copy of the page behind it.
            const $trigger = $(e.relatedTarget);
            if (!$trigger.length || !$trigger.data('whg-modal')) return;
            loadModalContent($trigger);
        });

    const $trigger = $('#orcidDeniedTrigger');
    if ($trigger.length) {
        setTimeout(function() {
            $trigger[0].click(); // Native click
            console.log('ORCID denied modal triggered');
        }, 1000);
    }

    const MODAL_HEADER = `
        <div class="modal-header">
            <h5 class="modal-title">
                <img alt="WHG" height="38" src="/static/images/whg_logo.svg" width="50">
            </h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
    `;

    /**
     * Some modal sources (the /media/help/*.html and /media/resources/*.html files)
     * are whole HTML documents, complete with <html>/<body> wrappers and long-dead
     * inline scripts, rather than fragments. Injecting those with jQuery's .html()
     * takes its script-executing path and leaves the outcome at the mercy of each
     * engine's fragment parser. Parse such responses out-of-document instead and
     * keep only the body markup; genuine fragments (e.g. the Django-rendered
     * contact form, whose inline scripts are live) are passed through untouched.
     */
    function extractFragment(html) {
        if (typeof html !== 'string' || !/<html[\s>]|<body[\s>]/i.test(html)) return html;
        try {
            const doc = new DOMParser().parseFromString(html, 'text/html');
            if (!doc || !doc.body) return html;
            doc.body.querySelectorAll('script').forEach(s => s.remove());
            return doc.body.innerHTML;
        } catch (err) {
            return html;
        }
    }

    // Bootstrap's data-api opens the dialog the moment the trigger is clicked, so
    // it must never be left blank while the content is in flight: an empty dialog
    // with no header reads as a broken page that has locked the screen, with no
    // obvious way out (place#167). Render the shell — including its working close
    // button — first, then swap in the content.
    function renderShell(bodyHTML) {
        $('#whgModal .modal-content').html(
            MODAL_HEADER + '<div class="modal-body">' + bodyHTML + '</div>'
        );
    }

    function showLoading() {
        renderShell(`
            <div class="text-center text-muted py-4">
                <div class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></div>
                Loading&hellip;
            </div>
        `);
    }

    function showLoadError(url, detail) {
        renderShell(`
            <p class="mb-2">Sorry, this content could not be loaded.</p>
            <p class="small text-muted mb-3">${detail}</p>
            <a class="btn btn-sm btn-outline-secondary" href="${url}" target="_blank" rel="noopener">
                Open it in a new tab
            </a>
        `);
    }

    function loadModalContent(target) {
        const url = target.data('whg-modal');
        if (!url) return;   // never let jQuery default a missing url to the current page
        const modalSubject = target.data('subject');
        showLoading();
        $.ajax({
            url: url,
            method: 'GET',
            success: function (data) {
                // Load the fetched HTML content into the modal
                var $content = $('#whgModal .modal-content');
                $content.html(extractFragment(data));

                if ($content.find('.modal-header').length === 0) {
                    $content
                        .wrapInner('<div class="modal-body"></div>')
                        .prepend(MODAL_HEADER);
                }

                if (modalSubject) {
                    $('#whgModal').find('input[name="subject"]').val(modalSubject);
                }

                // Content might itself have modal requirements
                $content.find('[data-whg-modal]')
                    .attr('href', '#')
                    .addClass('text-decoration-none')
                    .click(function () {
                        loadModalContent($(this));
                    });

                // Initialise Turnstile once content is ready
                initTurnstile();

                // Show the modal
                $('#whgModal').modal('show');
            },
            error: function (xhr, status, error) {
                const detail = url + ' — ' +
                    (xhr && xhr.status ? 'HTTP ' + xhr.status : 'no response') +
                    (status ? ' (' + status + ')' : '');
                showLoadError(url, detail);
                $('#whgModal').modal('show');
                // Record the failure: a beta report of "the dialog came up empty" is
                // only diagnosable if the status that caused it reaches GlitchTip.
                console.error('whg-modal: failed to load ' + detail, error || '');
                if (window.Sentry && typeof window.Sentry.captureException === 'function') {
                    window.Sentry.captureException(new Error('whg-modal load failed: ' + detail));
                }
            }
        });
    }

    // Open a modal for a URL that has no `data-whg-modal` trigger in the page — e.g.
    // the invitation dialog launched from an Atlas map popup, whose markup is
    // re-rendered long after initWHGModal() bound the static triggers (place#155).
    function openWHGModal(url, subject) {
        if (!url) return;
        const $proxy = $('<a>').data('whg-modal', url);
        if (subject) $proxy.data('subject', subject);
        loadModalContent($proxy);
    }
    window.openWHGModal = openWHGModal;

    function initTurnstile() {
        if (window.turnstile) {
            $('.cf-turnstile').each(function () {
                if (!$(this).data('widget-id')) {
                    const widgetId = turnstile.render(this, {
                        sitekey: $(this).data('sitekey')
                    });
                    $(this).data('widget-id', widgetId)
                           .data('initialized', true);
                }
            });
        }
    }

    function validateTurnstile() {
        const responseField = document.querySelector('#whgModal [name="cf-turnstile-response"]');
        return responseField && responseField.value.trim().length > 0;
    }


    // Enable Bootstrap form validation using jQuery
    $('body').on('submit', '#whgModal form', function (event) { // Must delegate from body to account for form refresh on fail
        const $form = $(this);

        // Only require Turnstile if widget exists (i.e. unauthenticated users)
        const hasTurnstile = $form.find('.cf-turnstile').length > 0;
        const turnstileValid = !hasTurnstile || validateTurnstile();

        if (!this.checkValidity() || !turnstileValid) {
            event.preventDefault();
            event.stopPropagation();

            const turnstileContainer = $form.find('.turnstile-container');
            const feedback = turnstileContainer.find('.invalid-feedback');

            if (!turnstileValid) {
                feedback.text('Please verify that you are human.').addClass('d-block');
            } else {
                feedback.text('').removeClass('d-block');
            }
        } else {
            event.preventDefault(); // Prevent default form submission

            var formData = $form.serializeArray();
            formData.push({name: 'page_url', value: window.location.pathname});
            var formDataObject = {};
            formData.forEach(function (item) {
                formDataObject[item.name] = item.value;
            });

            $.ajax({
                url: $form.data('url'),
                method: 'POST',
                data: formDataObject,
                success: function (response, status, xhr) {
                    var contentType = xhr.getResponseHeader("content-type") || "";
                    if (contentType.includes("application/json")) {
                        if (response.success) {
                            // Hide inputs and show #confirmationMessage
                            $('#whgModal .modal-body > div, #whgModal .modal-footer button').toggleClass('d-none');
                        } else {
                            alert('An error occurred while submitting the form.');
                        }
                    } else {
                        // If the response is HTML, update the modal content
                        $form.remove();
                        $('#whgModal .modal-content').html(response);
                    }
                },
                error: function (xhr, status, error) {
                    alert('Sorry, there was an error submitting the form.');
                }
            });
        }
        $(this).addClass('was-validated');
    });

}

export {
    initWHGModal
};