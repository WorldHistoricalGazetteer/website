import {lch} from 'd3-color';

export function showChooser(type) {
    console.log('showChooser', type);
    let elem = '#' + type + '_chooser';
    $(elem).toggle();
}

export function deepCopy(obj) {
    if (obj === null || typeof obj !== 'object') {
        return obj;
    }

    if (Array.isArray(obj)) {
        return obj.map(deepCopy);
    }

    const result = {};
    for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
            result[key] = deepCopy(obj[key]);
        }
    }
    return result;
}

export function geomsGeoJSON(geomItemsOriginal) { // Convert array of items with .geom to GeoJSON FeatureCollection
    let geomItems = deepCopy(geomItemsOriginal);
    let featureCollection = {
        type: 'FeatureCollection',
        features: [],
    };
    let idCounter = 0;
    for (const item of geomItems) {

        item.geom = item.geom || item.geometry;

        const feature = {
            type: 'Feature',
            geometry: {
                type: 'GeometryCollection',
                geometries: Array.isArray(item.geom) ? item.geom : [item.geom],
            },
            properties: {},
            id: idCounter,
        };
        delete item.geom;
        for (const prop in item) { // Copy all non-standard properties from the original item
            if (!['type', 'geometry', 'properties'].includes(prop)) {
                feature.properties[prop] = item[prop];
            }
        }
        featureCollection.features.push(feature);
        idCounter++;
    }
    return featureCollection;
}

export function largestSubGeometry(geometry) {
    if (getType(geometry) === 'MultiPoint' || getType(geometry) ===
        'MultiLineString' || getType(geometry) === 'MultiPolygon') {
        if (geometry.coordinates && geometry.coordinates.length > 0) {
            // Initialize variables to keep track of the largest bounding box area and its corresponding geometry
            let largestArea = 0;
            let largestGeometry = null;

            for (const subGeometry of geometry.coordinates) {
                const subGeometryType = getType({
                    type: getType(geometry).replace('Multi', ''),
                    coordinates: subGeometry,
                });
                const subGeometryArea = area({
                    type: subGeometryType,
                    coordinates: subGeometry,
                });

                if (subGeometryArea > largestArea) {
                    largestArea = subGeometryArea;
                    largestGeometry = geometry; // Preserve any other attributes
                    largestGeometry.coordinates = subGeometry;
                    largestGeometry.type = subGeometryType;
                }
            }

            if (largestGeometry) {
                return largestGeometry;
            }
        }
    }
    return null; // If the input is not a Multi-geometry or if it's empty, return null.
}

export function representativePoint(geometry) {
    if (getType(geometry) === 'Point') {
        return geometry;
    } else if (getType(geometry) === 'LineString') {
        const midPoint = midpoint(geometry);
        geometry.type = 'Point';
        geometry.coordinates = midPoint.geometry.coordinates;
        return geometry;
    } else if (getType(geometry) === 'Polygon') {
        const centerPoint = centroid(geometry);
        geometry.type = 'Point';
        geometry.coordinates = centerPoint.geometry.coordinates;
        return geometry;
    } else if (getType(geometry) === 'MultiPoint' || getType(geometry) ===
        'MultiLineString' || getType(geometry) === 'MultiPolygon') {
        // For Multi-geometries, use the preceding rules for the largest sub-geometry.
        if (geometry.coordinates && geometry.coordinates.length > 0) {
            return representativePoint(largestSubGeometry(geometry));
        }
    }
    console.log('Failed to generate representative point:', geometry);
    return null; // If the geometry type is not recognized or if it's empty, return null.
}

export function equidistantLCHColors(numColors) {
    const colors = [];
    const hue_default = 60; // Default red-orange
    const hue_avoid = 150; // Avoid greens (150) or reds (30) for colourblindness
    const hue_avoid_tolerance = 40; // Either side of hue-avoid value
    const hueStep = (360 - hue_avoid_tolerance * 2) / numColors;
    for (let i = 0; i < numColors; i++) {
        const hueValue_raw = hue_default + i * hueStep;
        const hueValue_adjust = hueValue_raw > hue_avoid - hue_avoid_tolerance;
        const hueValue_adjusted = hueValue_adjust ?
            hueValue_raw + hue_avoid_tolerance * 2 :
            hueValue_raw;
        const color = lch(50, 70, hueValue_adjusted % 360).rgb();
        const rgbaColor = `rgba(${color.r}, ${color.g}, ${color.b}, ${color.opacity})`;
        colors.push(rgbaColor);
    }
    return colors;
}

export function arrayColors(strings) {
    if (!Array.isArray(strings)) strings = [];
    strings.reverse().unshift('');
    const numColors = strings.length;
    const colors = equidistantLCHColors(numColors);
    let result = [];
    for (let i = 0; i < numColors; i++) {
        result.push(colors[i]);
        result.push(strings[i]);
    }
    return result.reverse();
}

export function colorTable(arrayColors, target, labels = null, multiDataset = false, ds_id = null, whg_map = null) {
    const colorKeyTable = $('<table>').addClass('color-key-table expanded');
    const tableBody = $('<tbody>');

    // Add a header row
    const headerRow = $('<tr>');
    const headerCell = $('<th>')
        .attr('colspan', 2)
        .css({fontSize: '10px'})
        .html('<span id="keyLabel">KEY</span><i id="keySlider" class="fas fa-arrow-circle-right"></i>');
    headerRow.append(headerCell);
    tableBody.append(headerRow);

    function fancyLabel(id, label) {
        return `<a href="/datasets/${id}/places" target= "_blank" data-bs-toggle="tooltip" data-bs-title="<b>Open the <i>${label}</i> dataset in a new tab.</b><br><small>Click the coloured marker on the left to hide/reveal the associated map markers.</small>">${label}</a>`;
    }

    for (let i = 0; i < arrayColors.length - (multiDataset.length > 0 ? 0 : 2); i += 2) {
        const label = i == arrayColors.length - 2 ?
            labels ? '<i>various</i>' : '<i>other</i>' :
            labels ? fancyLabel(arrayColors[i], labels[i / 2]) : arrayColors[i];
        const color = arrayColors[i + 1];
        const row = $('<tr>');
        const colorCell = $('<td>').addClass('color-swatch');
        const colorSwatch = $('<div>')
            .addClass('color-swatch')
            .toggleClass('dataset-selector', labels ? true : false)
            .data('label', labels ? labels[i / 2] : null)
            .data('dataset', labels ? (multiDataset.length > 0 && i === arrayColors.length - (multiDataset.length > 0 ? 0 : 2) - 2 ? multiDataset : arrayColors[i]) : null);
        colorSwatch.css('background-color', color);
        colorCell.append(colorSwatch);
        const labelCell = $('<td>').html(label);
        row.append(colorCell, labelCell);
        tableBody.append(row);
    }

    colorKeyTable.append(tableBody);
    $(target).append(colorKeyTable);

    $('#keySlider')
        .attr('data-bs-toggle', 'tooltip')
        .attr('data-bs-title', 'Click to hide/reveal key.')
        .click(function () {
            colorKeyTable.toggleClass('expanded');
            // $('#colorKeyTable').toggle();
        });

    // For dataset collections add click event to each color swatch
    $('div.dataset-selector').click(function () {
        $(this).closest('tr')
            .toggleClass('disabled')
            .find('a').each(function () {
            // Toggle preventDefault based on disabled class
            if ($(this).closest('tr').hasClass('disabled')) {
                $(this)
                    .tooltip('disable')
                    .off('click')
                    .on('click', function (event) {
                        event.preventDefault();
                    });
            } else {
                $(this)
                    .tooltip('enable')
                    .off('click');
            }
        });
        const visibleDatasets = $('tr:not(.disabled) div.dataset-selector')
            .map((_, el) => $(el).data('dataset'))
            .get();

        whg_map.layersetObjects.forEach(layerset => {
            layerset.setRelationFilter(['in', 'relation', ...visibleDatasets]);
        });

    }).each(function () {
        $(this)
            .attr('data-bs-toggle', 'tooltip')
            .attr('data-bs-title', `${$(this).data('label') ? `<b>${$(this).data('label')}</b><br>` : ''}<i>Click to toggle visibility of this dataset\'s map markers.</i>`);
    });

}

export function initInfoOverlay() {

    var isCollapsed = localStorage.getItem('isCollapsed') === 'true';

    // Initialize checkbox, metadata, and toggle switch, based on isCollapsed
    $('#checkbox').prop('checked', isCollapsed);
    $('#metadata').toggle(!isCollapsed);
    $('#ds_info').toggleClass('info-collapsed', isCollapsed);

    $('#collapseExpand').click(function () {
        $('#metadata').slideToggle(300, function () {
            $('#ds_info').toggleClass('info-collapsed');
        });
    });

    // Update the state when the checkbox is checked
    $('#checkbox').change(function () {
        localStorage.setItem('isCollapsed', $(this).is(':checked'));
    });

}

export function attributionString(data) {
    let attributionParts = [];
    if (!!data && data.attribution) {
        attributionParts.push(data.attribution);
    }
    if (!!data && data.citation) {
        attributionParts.push(data.citation);
    }
    let attribution = '';
    if (attributionParts.length > 0) attribution = attributionParts.join(', ');

    let attributionStringParts = [];
    if (!!data) attributionStringParts.push(
        data.attribution || data.citation || attribution);

    return attributionStringParts.join(' | ');
}

export function initUtils(whg_map) {

    // generic clipboard for modal and non-modal containers
    document.querySelectorAll('.clippy').forEach(element => {
        element.addEventListener('click', function (e) {
            e.preventDefault();

            // Dynamically determine the container based on the trigger element
            // find the closest '.modal' parent but fallback to 'document.body' if not found
            let container = $(this).closest('.modal')[0] || document.body;

            // Now, initialize ClipboardJS
            let clipboard = new ClipboardJS(element, {
                text: function (trigger) {
                    let target = trigger.getAttribute('data-clipboard-target');
                    return target ? $(target).text() : trigger.getAttribute('aria-label');
                },
                container: container,
            }).on('success', function (e) {
                console.log('Content copied to clipboard successfully!');
                e.clearSelection();
                const tooltip = bootstrap.Tooltip.getInstance(e.trigger);
                tooltip.setContent({'.tooltip-inner': 'Copied!'});
                setTimeout(function () { // Hide the tooltip after 2 seconds
                    tooltip.hide();
                    tooltip.setContent({'.tooltip-inner': tooltip._config.title}) // Restore original text
                }, 2000);
            });

            // Manually trigger the copy action
            clipboard.onClick(e);
        });
    });

    // new ClipboardJS('.clippy', {
    // 	text: function(trigger) {
    // 		let target = trigger.getAttribute('data-clipboard-target');
    // 		if (target == null) {
    // 			return trigger.getAttribute('aria-label');
    // 		} else {
    // 			return $(target).text();
    // 		}
    // 	},
    //   container: function(trigger) {
    //       return $(trigger).closest('.modal')[0];
    //   }
    // 	// container: document.getElementById('downloadModal') || document.body
    // }).on('success', function(e) {
    // 	console.log('clipped')
    // 	e.clearSelection();
    // 	var $trigger = $(e.trigger);
    // 	$trigger.tooltip('destroy').attr('title', 'Copied!').tooltip();
    // 	// $trigger.tooltip('dispose').attr('title', 'Copied!').tooltip();
    // 	$trigger.tooltip('show');
    // 	setTimeout(function() {
    // 		$trigger.tooltip('hide');
    // 	}, 2000);
    // });

    // image modal
    $('body').on('click', '.pop, #anno_img', function () {
        let image = $(this).is('img') ? $(this) : $(this).find('img:first');
        let url = image.attr('src');
        let txt = image.attr('alt');
        // let re = /(.png|.jpg|.jpeg|.gif|.tif)/g // TODO: Remove if not required
        console.log('url', url);
        $('#header_text').html(txt);
        $('.imagepreview').attr('src', url);
        $('#imageModal').modal('show');
    });
    // TODO: Figure out why the modal close button doesn't work without this additional code:
    $('#imageModal button.close').click(function () {
        $('#imageModal').modal('hide');
    });

    $('clearlines').click(function () {//TODO: Is this redundant?
        try {
            whg_map.removeLayer('gl_active_poly');
        } catch (error) {
            console.log('Layer ID error.', error);
        }
        //whg_map.removeLayer('outline')
    });

    // for collection description only
    $('.a_more_descrip').click(function () {
        let clicked = $(this);
        clicked.hide();
        clicked.parent().find('#dots_descrip').hide();
        clicked.next().show();
        $('.a_less_descrip').show();
    });
    $('.a_less_descrip').click(function () {
        let clicked = $(this);
        clicked.hide(); // hide 'less' link
        $('.more_descrip').hide(); // hide the extra text again
        $('#dots_descrip').show(); // show dots again
        $('.a_more_descrip').show(); // show more link again
    });
}

export function initClipboard() {
    // Insert clipboard buttons after all .clippable elements
    document.querySelectorAll(".clippable").forEach((el, idx) => {
        const btn = document.createElement("i");
        btn.className = "fas fa-clipboard ms-2 text-secondary";
        btn.style.cursor = "pointer";

        // give the source element an id if it doesn't have one
        if (!el.id) {
            el.id = `clippable-${idx}`;
        }
        btn.setAttribute("data-clipboard-target", `#${el.id}`);

        // tooltip title: use element attr or default
        const title = el.dataset.clippableTitle || "Copy to clipboard";
        btn.setAttribute("data-bs-toggle", "tooltip");
        btn.setAttribute("data-bs-placement", "top");
        btn.setAttribute("title", title);

        // insert after the element
        el.insertAdjacentElement("afterend", btn);
    });

    // init ClipboardJS
    const clipboard = new ClipboardJS(".fa-clipboard");

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
            const original = icon.dataset.clippableTitle || "Copy to clipboard";
            icon.setAttribute("data-bs-original-title", original);

            icon.classList.remove("text-success");
            icon.classList.add("text-secondary");

            tooltip.hide();
        }, 2000);
    });

    clipboard.on("error", function () {
        alert("Failed to copy.");
    });
}

export function minmaxer(timespans) {
    let starts = [], ends = [];

    for (var t in timespans) {
        let start = timespans[t].start ? Object.values(timespans[t].start)[0] : null;
        let end = timespans[t].end ? Object.values(timespans[t].end)[0] : start;
        starts.push(start ?? end);
        ends.push(end ?? start);
    }

    let maxStart = Math.max(...starts);
    let maxEnd = Math.max(...ends);
    return maxStart === maxEnd ? [maxStart] : [maxStart, maxEnd];
}


export function get_ds_list_stats(allFeatures, allExtents = []) {

    let min = Infinity;
    let max = -Infinity;
    let seqMin = Infinity;
    let seqMax = -Infinity;
    for (let i = 0; i < allFeatures.length; i++) {
        // Convert strings to integers
        const featureMin = (/^-?\d+$/.test(allFeatures[i].properties.min)) ?
            parseInt(allFeatures[i].properties.min) :
            false;
        const featureMax = (/^-?\d+$/.test(allFeatures[i].properties.max)) ?
            parseInt(allFeatures[i].properties.max) :
            false;
        const seqValue = (/^-?\d+$/.test(allFeatures[i].properties.seq)) ?
            parseInt(allFeatures[i].properties.seq) :
            false;
        if (featureMin && featureMax) {
            min = Math.min(min, featureMin);
            max = Math.max(max, featureMax);
        }
        if (seqValue) {
            seqMin = Math.min(seqMin, seqValue);
            seqMax = Math.max(seqMax, seqValue);
        }
    }
    if (!isFinite(min)) min = -3000;
    if (!isFinite(max)) max = 2000;

    // Check if all extents are null
    const allExtentsAreNull = allExtents.length === 0 || allExtents.every(extent => extent === null);

    const extent = allExtentsAreNull ? [-180, -90, 180, 90] : bbox({
        'type': 'FeatureCollection',
        'features': [
            ...allFeatures.filter(
                feature => feature.geometry && feature.geometry.coordinates), // Ignore any nullGeometry features
            ...allExtents.map((extent) => ({
                type: 'Feature',
                geometry: {
                    type: 'Polygon',
                    coordinates: [
                        [
                            [extent[0], extent[1]],
                            [extent[2], extent[1]],
                            [extent[2], extent[3]],
                            [extent[0], extent[3]],
                            [extent[0], extent[1]],
                        ],
                    ],
                },
            })),
        ],
    });

    return {
        min: min,
        max: max,
        seqmin: seqMin,
        seqmax: seqMax,
        count: allFeatures.length,
        extent: extent,
    };
}