// /whg/webpack/js/typeTreeWidget.js
/**
 * Lightweight hierarchical type-tree widget for AAT place types.
 *
 * Loads data lazily from /types/tree/ and /types/tree/<aat_id>/,
 * renders an expandable tree with tri-state checkboxes, and exposes
 * the minimal set of selected AAT identifiers (parent selection implies
 * all descendants — the server expands via expand_type_identifiers).
 *
 * Includes an inline search box that queries /types/tree/search/?q=...
 * and expands the tree to reveal matched nodes.
 *
 * Usage:
 *   import TypeTreeWidget from './typeTreeWidget';
 *   const tree = new TypeTreeWidget('#aat_type_tree', {
 *       onchange: () => { ... },
 *   });
 */

import '../css/typeTreeWidget.css';

const TREE_URL = '/types/tree/';

/**
 * Turn a rejected jqXHR (or a thrown Error) into a one-line description.
 * jQuery rejects with the jqXHR object, which stringifies to "[object Object]" —
 * useless in a GlitchTip report from a beta tester whose tree failed to load.
 */
function describeError(url, err) {
    if (err && typeof err.status !== 'undefined') {
        return `${url} — ${err.status ? 'HTTP ' + err.status : 'no response'}` +
            `${err.statusText ? ' (' + err.statusText + ')' : ''}`;
    }
    return `${url} — ${(err && err.message) || err}`;
}

/** Surface a client-side failure in GlitchTip, when the SDK is loaded. */
function reportError(message) {
    if (window.Sentry && typeof window.Sentry.captureException === 'function') {
        window.Sentry.captureException(new Error(message));
    }
}

/** GeoNames feature-class labels shown as badge tooltips. */
const FCLASS_LABELS = {
    A: 'Administrative divisions',
    H: 'Hydrographic features',
    L: 'Area features',
    P: 'Populated places',
    R: 'Road / railroad features',
    S: 'Spot features',
    T: 'Hypsographic features',
    U: 'Undersea features',
    V: 'Vegetation features',
};

export default class TypeTreeWidget {

    /**
     * @param {string|HTMLElement} container - CSS selector or element
     * @param {Object}  opts
     * @param {Function} opts.onchange - Called whenever selections change
     */
    constructor(container, opts = {}) {
        this.$el = $(container);
        this._onchange = opts.onchange || function () {};
        this._initialised = false;
        this._searchTimer = null;
        // Concepts selected but not (yet) materialised in the lazily-loaded tree —
        // id → label. Lets the widget round-trip a caller-supplied selection
        // (setSelected) and still report it (getSelectedConcepts) before the
        // relevant branch has been expanded. Used by the Map-your-Data + Workbench
        // pickers (place#134); the Atlas filter never sets it.
        this._pending = new Map();
    }

    /* ------------------------------------------------------------ *
     *  Public API
     * ------------------------------------------------------------ */

    /** Lazily initialise: fetch root nodes on first call. */
    async init() {
        if (this._initialised) return;
        this._initialised = true;
        console.log('TypeTreeWidget: initialising, fetching', TREE_URL);
        this.$el.html(
            '<div class="tt-loading"><i class="fas fa-spinner fa-spin"></i> Loading types…</div>'
        );
        try {
            const nodes = await $.getJSON(TREE_URL);
            console.log('TypeTreeWidget: received', nodes.length, 'root nodes');
            this.$el.empty();
            if (!nodes.length) {
                this.$el.html(
                    '<div class="tt-error">No place types found. Run <code>manage.py sync_aat_types</code>.</div>'
                );
                return;
            }

            // --- Search box (sticky at top of scrollable container) ---
            const $searchWrap = $(`
                <div class="tt-search">
                    <div class="tt-search-input-wrap">
                        <input type="text" class="tt-search-input form-control form-control-sm"
                               placeholder="Search place types…" autocomplete="off" spellcheck="false">
                        <span class="tt-search-clear" title="Clear">&times;</span>
                    </div>
                </div>
            `);
            // Results dropdown lives on <body> so it isn't clipped by
            // the overflow:auto tree container.
            this._$searchResults = $('<div class="tt-search-results"></div>')
                .appendTo(document.body);
            this.$el.append($searchWrap);
            this._setupSearch($searchWrap);

            // --- Tree ---
            const $root = $('<ul class="tt-root"></ul>');
            nodes.forEach(n => $root.append(this._renderNode(n)));
            this.$el.append($root);

            // Auto-expand all root-level nodes that have children
            const $rootNodes = $root.children('.tt-node');
            for (let i = 0; i < $rootNodes.length; i++) {
                const $li = $rootNodes.eq(i);
                if ($li.children('.tt-children').length > 0) {
                    await this._toggle($li);
                    // Show immediately without slide animation for initial load
                    $li.children('.tt-children').stop(true, true).show();
                }
            }

            // Apply any selection set before init (setSelected called early).
            this._applyPending(this.$el);

            // Initialise Bootstrap tooltips on all badges rendered so far
            this._initTooltips(this.$el);
        } catch (err) {
            const detail = describeError(TREE_URL, err);
            console.error('TypeTreeWidget: failed to load root nodes — ' + detail);
            reportError('TypeTreeWidget root load failed: ' + detail);
            this.$el.html(
                '<div class="tt-error">Could not load place types.</div>'
            );
        }
    }

    /**
     * Return the minimal set of selected AAT identifier strings.
     *
     * When a parent is fully checked, only the parent is returned
     * (the server calls expand_type_identifiers to include descendants).
     * Partially checked subtrees return only the checked leaves / subtrees.
     */
    getSelectedIdentifiers() {
        if (!this._initialised) return [];
        return this._collectSelected(
            this.$el.find('> .tt-root > .tt-node')
        );
    }

    /** True when the user has made at least one explicit selection. */
    hasSelections() {
        return this.$el.find('.tt-cb:checked').length > 0 ||
            this.$el.find('.tt-cb').filter(function () {
                return this.indeterminate;
            }).length > 0;
    }

    /** Number of individually checked checkboxes (for the badge). */
    selectionCount() {
        return this.$el.find('.tt-cb:checked').length;
    }

    /** Clear every checkbox. */
    clearAll() {
        this._pending = new Map();
        this.$el.find('.tt-cb')
            .prop('checked', false)
            .prop('indeterminate', false);
        this.$el.find('.tt-highlight').removeClass('tt-highlight');
        this._onchange();
    }

    /* ------------------------------------------------------------ *
     *  Selection round-tripping (chip-based pickers — place#134)
     * ------------------------------------------------------------ */

    /**
     * Return the minimal selected set as ``[{id: 'aat:<id>', text}]`` — the same
     * minimal set as getSelectedIdentifiers(), but carrying each concept's label
     * (from the rendered node, or a caller-supplied fallback / pending label).
     * Includes selections not yet rendered in the lazily-loaded tree.
     *
     * @param {Object} [labelFallback] - id → label used when a node isn't in the DOM
     */
    getSelectedConcepts(labelFallback) {
        const out = [];
        const seen = new Set();
        if (this._initialised) {
            const walk = ($nodes) => {
                $nodes.each((_, el) => {
                    const $n = $(el);
                    const $cb = $n.children('.tt-cb');
                    if ($cb.prop('checked')) {
                        const id = $cb.val();
                        if (!seen.has(id)) { seen.add(id); out.push({ id, text: this._labelOf($n, id, labelFallback) }); }
                    } else if ($cb.prop('indeterminate')) {
                        walk($n.children('.tt-children').children('.tt-node'));
                    }
                });
            };
            walk(this.$el.find('> .tt-root > .tt-node'));
        }
        // Selections whose node hasn't been rendered yet (never expanded to).
        this._pending.forEach((text, id) => {
            if (!seen.has(id)) { seen.add(id); out.push({ id, text: text || (labelFallback && labelFallback[id]) || '' }); }
        });
        return out;
    }

    /**
     * Replace the selection with ``concepts`` (``[{id, text}]`` or bare id strings).
     * Any concept whose node is already rendered is checked immediately (cascading
     * to descendants); the rest are held in ``_pending`` and applied as their
     * branch is expanded. Fires onchange.
     */
    setSelected(concepts) {
        this._pending = new Map();
        (concepts || []).forEach((c) => {
            const id = (c && c.id) ? c.id : c;
            if (id) this._pending.set(id, (c && c.text) || '');
        });
        if (this._initialised) {
            this.$el.find('.tt-cb').prop('checked', false).prop('indeterminate', false);
            this._applyPending(this.$el);
        }
        this._onchange();
    }

    /** Deselect one concept by id (``aat:<id>``). Fires onchange. */
    deselect(id) {
        this._pending.delete(id);
        if (this._initialised) {
            const $node = this.$el.find('.tt-node').filter(function () { return $(this).attr('data-id') === id; });
            if ($node.length) {
                $node.children('.tt-cb').prop('checked', false).prop('indeterminate', false);
                $node.find('.tt-cb').prop('checked', false).prop('indeterminate', false);
                this._updateAncestors($node);
            }
        }
        this._onchange();
    }

    /** Apply any pending selections that now have a rendered node within $scope. */
    _applyPending($scope) {
        if (!this._pending.size) return;
        const self = this;
        const touched = [];
        ($scope || this.$el).find('.tt-node').each(function () {
            const $li = $(this);
            const id = $li.attr('data-id');
            if (id && self._pending.has(id)) {
                const $cb = $li.children('.tt-cb');
                if ($cb.length) {
                    $cb.prop('checked', true).prop('indeterminate', false);
                    // Cascade to any already-loaded descendants (later-loaded ones
                    // inherit via the parentChecked branch in _toggle).
                    $li.find('.tt-cb').prop('checked', true).prop('indeterminate', false);
                    self._pending.delete(id);
                    touched.push($li);
                }
            }
        });
        touched.forEach(($li) => this._updateAncestors($li));
    }

    /** Best label for a node: its rendered text, else a fallback map, else ''. */
    _labelOf($n, id, labelFallback) {
        const t = ($n.children('.tt-label').text() || '').trim();
        if (t) return t;
        if (labelFallback && labelFallback[id]) return labelFallback[id];
        return '';
    }

    /* ------------------------------------------------------------ *
     *  Internal: rendering
     * ------------------------------------------------------------ */

    _renderNode(node) {
        const hasChildren = node.children === true;
        const isGuide = !!node.guide;
        const $li = $('<li class="tt-node"></li>')
            .attr('data-id', node.id)
            .attr('data-aat-id', node.aat_id);

        if (isGuide) $li.addClass('tt-guide');

        // Toggle arrow (only when the node can have children)
        const $toggle = hasChildren
            ? $('<span class="tt-toggle"><i class="fas fa-caret-right"></i></span>')
            : $('<span class="tt-toggle tt-leaf"></span>');

        // Checkbox — guide (organisational) nodes are not selectable
        const $cb = isGuide
            ? null
            : $(`<input type="checkbox" class="tt-cb" value="${node.id}">`);

        // Label
        const $label = $(`<span class="tt-label">${this._esc(node.text)}</span>`);

        // fclass badges
        const badges = (node.fclasses || []).map(
            f => {
                const key = f.toUpperCase();
                const label = FCLASS_LABELS[key] || key;
                const tip = `<b>GeoNames class <em>${this._esc(key)}</em></b><br>${this._esc(label)}`;
                return `<span class="tt-badge tt-badge-${f.toLowerCase()}"
                    data-bs-toggle="tooltip" data-bs-html="true"
                    data-bs-title="${tip}">${f}</span>`;
            }
        ).join('');

        if ($cb) {
            $li.append($toggle, $cb, ' ', $label, ' ', badges);
        } else {
            $li.append($toggle, ' ', $label, ' ', badges);
        }

        if (hasChildren) {
            // Start hidden so the first toggle opens correctly
            $li.append('<ul class="tt-children" style="display:none"></ul>');
            $li.data('loaded', false);
        }

        // --- Events ---

        // Expand / collapse
        $toggle.add($label).on('click', (e) => {
            e.preventDefault();
            if (hasChildren) this._toggle($li);
        });

        // Checkbox cascade (only for non-guide nodes)
        if ($cb) {
            $cb.on('change', () => {
                const checked = $cb.prop('checked');
                // Cascade down (only to non-guide checkboxes)
                $li.find('.tt-cb')
                    .prop('checked', checked)
                    .prop('indeterminate', false);
                // Cascade up
                this._updateAncestors($li);
                this._onchange();
            });
        }

        return $li;
    }

    /* ------------------------------------------------------------ *
     *  Internal: expand / collapse with lazy loading
     * ------------------------------------------------------------ */

    async _toggle($li) {
        const $children = $li.children('.tt-children');
        const $icon = $li.children('.tt-toggle').find('i');

        if ($children.is(':visible')) {
            $children.slideUp(120);
            $icon.removeClass('fa-caret-down').addClass('fa-caret-right');
            return;
        }

        // Lazy-load children on first expand
        if (!$li.data('loaded')) {
            const aatId = $li.data('aat-id');
            $icon.removeClass('fa-caret-right').addClass('fa-spinner fa-spin');
            try {
                const nodes = await $.getJSON(`${TREE_URL}${aatId}/`);
                const parentChecked = $li.children('.tt-cb').prop('checked');
                if (nodes.length === 0) {
                    // No children after all — mark as leaf
                    $li.children('.tt-toggle')
                        .addClass('tt-leaf')
                        .find('i').remove();
                    $li.data('loaded', true);
                    $icon.removeClass('fa-spinner fa-spin');
                    return;
                }
                nodes.forEach(n => {
                    const $child = this._renderNode(n);
                    if (parentChecked) {
                        $child.children('.tt-cb').prop('checked', true);
                    }
                    $children.append($child);
                });
                $li.data('loaded', true);
                // Check any pending selections that live in this newly-loaded branch.
                this._applyPending($children);
                // Initialise Bootstrap tooltips on newly loaded children
                this._initTooltips($children);
            } catch (err) {
                const detail = describeError(TREE_URL + aatId + '/', err);
                console.error('TypeTreeWidget: load failed for ' + aatId + ' — ' + detail);
                reportError('TypeTreeWidget branch load failed: ' + detail);
            }
            $icon.removeClass('fa-spinner fa-spin');
        }

        $icon.removeClass('fa-caret-right').addClass('fa-caret-down');
        $children.slideDown(120);
    }

    /** Expand a node if it is collapsed; no-op if already open or a leaf. */
    async _ensureExpanded($li) {
        const $children = $li.children('.tt-children');
        if ($children.length === 0) return;  // leaf
        if ($children.is(':hidden')) {
            await this._toggle($li);
        }
    }

    /* ------------------------------------------------------------ *
     *  Internal: tri-state checkbox propagation
     * ------------------------------------------------------------ */

    _updateAncestors($li) {
        const $parent = $li.parent('.tt-children').closest('.tt-node');
        if ($parent.length === 0) return;

        const $childCbs = $parent.children('.tt-children').children('.tt-node').children('.tt-cb');
        const total = $childCbs.length;
        if (total === 0) return;

        let checked = 0;
        let indeterminate = 0;
        $childCbs.each(function () {
            if (this.checked) checked++;
            if (this.indeterminate) indeterminate++;
        });

        const $pcb = $parent.children('.tt-cb');
        if (checked === total && indeterminate === 0) {
            $pcb.prop({ checked: true, indeterminate: false });
        } else if (checked === 0 && indeterminate === 0) {
            $pcb.prop({ checked: false, indeterminate: false });
        } else {
            $pcb.prop({ checked: false, indeterminate: true });
        }

        // Keep walking up
        this._updateAncestors($parent);
    }

    /* ------------------------------------------------------------ *
     *  Internal: collect the minimal selected-identifier set
     * ------------------------------------------------------------ */

    _collectSelected($nodes) {
        const ids = [];
        $nodes.each((_, el) => {
            const $n = $(el);
            const $cb = $n.children('.tt-cb');
            if ($cb.prop('checked')) {
                // Whole subtree implied → return just this node
                ids.push($cb.val());
            } else if ($cb.prop('indeterminate')) {
                // Partial → recurse into children
                const $childNodes = $n.children('.tt-children').children('.tt-node');
                ids.push(...this._collectSelected($childNodes));
            }
            // unchecked → skip
        });
        return ids;
    }

    /* ------------------------------------------------------------ *
     *  Search
     * ------------------------------------------------------------ */

    _setupSearch($wrap) {
        const $input = $wrap.find('.tt-search-input');
        const $results = this._$searchResults;
        const $clear = $wrap.find('.tt-search-clear');
        let activeIndex = -1;

        const positionDropdown = () => {
            const rect = $input[0].getBoundingClientRect();
            $results.css({
                top:   rect.bottom + 'px',
                left:  rect.left   + 'px',
                width: rect.width  + 'px',
            });
        };

        const hideResults = () => {
            $results.hide();
            activeIndex = -1;
        };

        const highlightActive = () => {
            $results.find('.tt-search-item').removeClass('tt-search-active');
            if (activeIndex >= 0) {
                const $active = $results.find('.tt-search-item').eq(activeIndex);
                $active.addClass('tt-search-active');
                // Scroll into view within the dropdown
                const el = $active[0];
                if (el) el.scrollIntoView({ block: 'nearest' });
            }
        };

        const clearSearch = () => {
            $input.val('');
            hideResults();
            $results.empty();
            $clear.hide();
            this.$el.find('.tt-highlight').removeClass('tt-highlight');
        };

        $input.on('input', () => {
            clearTimeout(this._searchTimer);
            activeIndex = -1;
            const q = $input.val().trim();
            $clear.toggle(q.length > 0);
            if (q.length < 2) {
                hideResults();
                $results.empty();
                return;
            }
            this._searchTimer = setTimeout(() => {
                this._fetchSearchResults(q, $results, positionDropdown);
            }, 300);
        });

        $input.on('keydown', (e) => {
            const $items = $results.find('.tt-search-item');
            const count = $items.length;

            if (e.key === 'Escape') {
                clearSearch();
                return;
            }

            if (!$results.is(':visible') || count === 0) return;

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                activeIndex = Math.min(activeIndex + 1, count - 1);
                highlightActive();
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                activeIndex = Math.max(activeIndex - 1, 0);
                highlightActive();
            } else if (e.key === 'Enter') {
                e.preventDefault();
                if (activeIndex >= 0 && activeIndex < count) {
                    $items.eq(activeIndex).trigger('mousedown');
                }
            }
        });

        $clear.hide().on('click', clearSearch);

        // Close results dropdown on outside click
        $(document).on('mousedown', (e) => {
            if (!$(e.target).closest('.tt-search').length &&
                !$(e.target).closest('.tt-search-results').length) {
                hideResults();
            }
        });

        // Hide when the page scrolls (the fixed dropdown would be misaligned)
        $(window).on('scroll', hideResults);
    }

    async _fetchSearchResults(query, $results, positionDropdown) {
        try {
            const data = await $.getJSON(`${TREE_URL}search/?q=${encodeURIComponent(query)}`);
            $results.empty();
            if (!data.length) {
                $results.html('<div class="tt-search-empty">No matching types</div>');
                positionDropdown();
                $results.show();
                return;
            }
            data.forEach(item => {
                const $item = $(`<div class="tt-search-item">${this._esc(item.text)}</div>`);
                $item.on('mousedown', async (e) => {
                    e.preventDefault();
                    $results.hide();
                    this.$el.find('.tt-highlight').removeClass('tt-highlight');
                    await this._expandToNode(item.aat_id, item.ancestors);
                });
                $results.append($item);
            });
            positionDropdown();
            $results.show();
        } catch (err) {
            console.error('TypeTreeWidget: search failed', err);
        }
    }

    /**
     * Walk the ancestor path, expanding each node in turn, then
     * highlight and scroll to the target node.
     */
    async _expandToNode(targetAatId, ancestors) {
        for (const aat of ancestors) {
            if (aat === targetAatId) break;
            const $node = this.$el.find(`[data-aat-id="${aat}"]`);
            if ($node.length === 0) continue;  // skip nodes not in display tree
            await this._ensureExpanded($node);
        }

        const $target = this.$el.find(`[data-aat-id="${targetAatId}"]`);
        if ($target.length) {
            $target.addClass('tt-highlight');
            // Scroll the tree container so the target is visible
            const container = this.$el.closest('#type_tree_container')[0];
            if (container) {
                const targetEl = $target[0];
                const cRect = container.getBoundingClientRect();
                const tRect = targetEl.getBoundingClientRect();
                if (tRect.top < cRect.top || tRect.bottom > cRect.bottom) {
                    container.scrollTop += (tRect.top - cRect.top) - (cRect.height / 2) + (tRect.height / 2);
                }
            }
        }
    }

    /* ------------------------------------------------------------ *
     *  Helpers
     * ------------------------------------------------------------ */

    _esc(str) {
        const d = document.createElement('div');
        d.textContent = str;
        return d.innerHTML;
    }

    /** Activate Bootstrap 5 tooltips on [data-bs-toggle="tooltip"] inside $container. */
    _initTooltips($container) {
        if (typeof bootstrap === 'undefined' || !bootstrap.Tooltip) return;
        $container.find('[data-bs-toggle="tooltip"]').each(function () {
            if (!bootstrap.Tooltip.getInstance(this)) {
                new bootstrap.Tooltip(this);
            }
        });
    }
}

