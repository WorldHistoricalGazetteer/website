// /whg/webpack/js/typeTreeWidget.js
/**
 * Lightweight hierarchical type-tree widget for AAT place types.
 *
 * Loads data lazily from /types/tree/ and /types/tree/<aat_id>/,
 * renders an expandable tree with tri-state checkboxes, and exposes
 * the minimal set of selected AAT identifiers (parent selection implies
 * all descendants — the server expands via expand_type_identifiers).
 *
 * Usage:
 *   import TypeTreeWidget from './typeTreeWidget';
 *   const tree = new TypeTreeWidget('#aat_type_tree', {
 *       onchange: () => { ... },
 *   });
 */

const TREE_URL = '/types/tree/';

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
    }

    /* ------------------------------------------------------------ *
     *  Public API
     * ------------------------------------------------------------ */

    /** Lazily initialise: fetch root nodes on first call. */
    async init() {
        if (this._initialised) return;
        this._initialised = true;
        this.$el.html(
            '<div class="tt-loading"><i class="fas fa-spinner fa-spin"></i> Loading types…</div>'
        );
        try {
            const nodes = await $.getJSON(TREE_URL);
            this.$el.empty();
            const $root = $('<ul class="tt-root"></ul>');
            nodes.forEach(n => $root.append(this._renderNode(n)));
            this.$el.append($root);
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
        this._onchange();
    }

    /* ------------------------------------------------------------ *
     *  Internal: rendering
     * ------------------------------------------------------------ */

    _renderNode(node) {
        const hasChildren = node.children === true;
        const $li = $('<li class="tt-node"></li>')
            .attr('data-id', node.id)
            .attr('data-aat-id', node.aat_id);

        // Toggle arrow (only when the node can have children)
        const $toggle = hasChildren
            ? $('<span class="tt-toggle"><i class="fas fa-caret-right"></i></span>')
            : $('<span class="tt-toggle tt-leaf"></span>');

        // Checkbox
        const $cb = $(`<input type="checkbox" class="tt-cb" value="${node.id}">`);

        // Label
        const $label = $(`<span class="tt-label">${this._esc(node.text)}</span>`);

        // fclass badges
        const badges = (node.fclasses || []).map(
            f => `<span class="tt-badge tt-badge-${f.toLowerCase()}">${f}</span>`
        ).join('');

        $li.append($toggle, $cb, ' ', $label, ' ', badges);

        if (hasChildren) {
            $li.append('<ul class="tt-children"></ul>');
            $li.data('loaded', false);
        }

        // --- Events ---

        // Expand / collapse
        $toggle.add($label).on('click', (e) => {
            e.preventDefault();
            if (hasChildren) this._toggle($li);
        });

        // Checkbox cascade
        $cb.on('change', () => {
            const checked = $cb.prop('checked');
            // Cascade down
            $li.find('.tt-cb')
                .prop('checked', checked)
                .prop('indeterminate', false);
            // Cascade up
            this._updateAncestors($li);
            this._onchange();
        });

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
     *  Helpers
     * ------------------------------------------------------------ */

    _esc(str) {
        const d = document.createElement('div');
        d.textContent = str;
        return d.innerHTML;
    }
}

