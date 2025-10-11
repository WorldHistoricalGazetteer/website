// profile.js

import {initClipboard} from './utilities'

const {apiTokenUrl, newsToggleUrl, csrfToken, domain} = window.profileConfig;
const tokenSection = document.getElementById('api-token-section');
const tokenCode = document.getElementById('api-token');

function setState(hasToken, tokenValue = null) {
    tokenSection.classList.toggle('has-token', hasToken);
    if (hasToken && tokenValue) {
        tokenCode.textContent = tokenValue;
    }
}

function requestToken(action) {
    return fetch(apiTokenUrl, {
        method: 'POST',
        headers: {'X-CSRFToken': csrfToken, 'X-Requested-With': 'XMLHttpRequest'},
        body: new URLSearchParams({action})
    }).then(r => r.json());
}

document.querySelectorAll(".openrefine").forEach((el, idx) => {
    const btn = document.createElement("i");
    btn.className = "fas fa-gem ms-2 text-primary";
    btn.style.cursor = "pointer";

    // give the source element an id if it doesn't have one
    if (!el.id) {
        el.id = `clippable-${idx}`;
    }
    btn.setAttribute("data-clipboard-target", `#${el.id}`);

    // tooltip title: use element attr or default
    const title = el.dataset.clippableTitle || "Copy OpenRefine Service URL to clipboard";
    btn.setAttribute("data-bs-toggle", "tooltip");
    btn.setAttribute("data-bs-placement", "top");
    btn.setAttribute("title", title);

    // insert after the element
    el.insertAdjacentElement("afterend", btn);
});

// init ClipboardJS
const clipboard = new ClipboardJS(".fa-gem", {
    text: function (trigger) {
        // "trigger" is the <i> button that was clicked
        const targetSelector = trigger.getAttribute("data-clipboard-target");
        const target = document.querySelector(targetSelector);
        console.debug("targetSelector", target);

        if (!target) return "";
        let text = target.textContent;
        return `https://${domain}/reconcile?token=${text.trim()}`;
    }
});

clipboard.on("success", function (e) {
    const icon = e.trigger;
    const tooltip = bootstrap.Tooltip.getInstance(icon);
    if (!tooltip) return;

    // clear highlight
    e.clearSelection();

    // flash colour
    icon.classList.remove("text-secondary");
    icon.classList.add("text-success");

    // change tooltip content
    icon.setAttribute("data-bs-original-title", "Copied!");
    tooltip.show();

    // reset after 2s
    setTimeout(() => {
        const original = icon.dataset.clippableTitle || "Copy OpenRefine Service URL to clipboard";
        icon.setAttribute("data-bs-original-title", original);

        icon.classList.remove("text-success");
        icon.classList.add("text-primary");

        tooltip.hide();
    }, 2000);
});

clipboard.on("error", function () {
    alert("Failed to copy.");
});

initClipboard();

tokenSection.querySelectorAll('#has-token button').forEach(button => {
    button.setAttribute('data-bs-title', 'This will immediately invalidate the existing token');
    button.setAttribute('data-bs-toggle', 'tooltip');
});

document.body.addEventListener('click', function (e) {
    if (e.target.closest('.generate-token')) {
        requestToken('generate').then(data => {
            if (data.token) setState(true, data.token);
            else if (data.error) alert(data.error);
        }).catch(() => alert('Failed to generate token.'));
    }

    if (e.target.closest('.delete-token')) {
        if (!confirm("Are you sure you want to delete your API token?")) return;
        fetch(apiTokenUrl, {
            method: 'POST',
            headers: {'X-CSRFToken': csrfToken, 'X-Requested-With': 'XMLHttpRequest'},
            body: new URLSearchParams({'action': 'delete'})
        })
            .then(resp => resp.json())
            .then(data => {
                if (data.success) setState(false);
                else if (data.error) alert(data.error);
            })
            .catch(() => alert('Failed to delete token.'));
    }
});

const newsCheckbox = document.getElementById('id_news_permitted');
if (newsCheckbox) {
    newsCheckbox.addEventListener('change', function () {
        const formData = new FormData();
        formData.append('news_permitted', newsCheckbox.checked ? 'on' : '');
        formData.append('csrfmiddlewaretoken', csrfToken);

        fetch(newsToggleUrl, {
            method: 'POST',
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
            },
            body: formData
        }).then(response => {
            if (!response.ok) {
                alert('Failed to save news preferences.');
            }
        }).catch(() => {
            alert('Failed to save news preferences.');
        });
    });
}
