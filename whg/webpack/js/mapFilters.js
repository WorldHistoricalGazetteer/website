// mapFilters.js

export function toggleFilters(on, whg_map, table) {
    // Validate dateline values - if either is null/undefined, treat as off
    const fromValue = window.dateline?.fromValue;
    const toValue = window.dateline?.toValue;

    if (on && (fromValue === null || fromValue === undefined || toValue === null || toValue === undefined)) {
        console.warn('toggleFilters: dateline values not initialized, disabling filters', {fromValue, toValue});
        on = false;
    }

    whg_map.layersetObjects.forEach(layerset => {
        // Update the temporal filter for this layerset
        if (on) {
            layerset.setTemporalFilter($('#undated_checkbox').is(':checked') ?
                [
                    'any',
                    [
                        'all',
                        ['!=', 'max', 'null'],
                        ['!=', 'min', 'null'],
                        ['>=', 'max', fromValue],
                        ['<=', 'min', toValue],
                    ],
                    [
                        'any',
                        ['==', 'max', 'null'],
                        ['==', 'min', 'null']
                    ]
                ] :
                [
                    'all',
                    ['!=', 'max', 'null'],
                    ['!=', 'min', 'null'],
                    ['>=', 'max', fromValue],
                    ['<=', 'min', toValue],
                ]); // Store the updated temporal filter
        } else {
            layerset.setTemporalFilter(null);  // Reset the temporal filter
        }
    });
    table.draw();
}
