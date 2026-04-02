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

const TREE_URL = '/types/tree/';

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
                               placeholder="Browse or search AAT place types…" autocomplete="off" spellcheck="false">
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
        } catch (err) {
            console.error('TypeTreeWidget: failed to load root nodes', err);
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
        this.$el.find('.tt-cb')
            .prop('checked', false)
            .prop('indeterminate', false);
        this.$el.find('.tt-highlight').removeClass('tt-highlight');
        this._onchange();
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
                const title = FCLASS_LABELS[key] || key;
                return `<span class="tt-badge tt-badge-${f.toLowerCase()}" title="${title}">${f}</span>`;
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
            } catch (err) {
                console.error('TypeTreeWidget: load failed for', aatId, err);
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
}

