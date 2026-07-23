// whg_maplibre.js

import Layerset from './layerset';
import { attributionString } from './utilities';
import languages, {getPreferredLanguage, setPreferredLanguage} from './languages.js';

import MapboxDraw from "@mapbox/mapbox-gl-draw";
import '@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css'

import '../css/maplibre-common.css';
import '../css/style-control.css';
import '../css/dateline.css';

maplibregl.Map.prototype.nullCollection = function() {
	return { type: 'FeatureCollection', features: [] }
}

maplibregl.Map.prototype.clearSource = function(sourceId) {
    this.getSource(sourceId).setData(this.nullCollection());
	return this;
};

maplibregl.Map.prototype.eraseSource = function(sourceId) {
	var layersToErase = this.getStyle().layers.filter(layer => {return layer.source === sourceId})
	layersToErase.forEach(layer => {
		this.removeLayer(layer.id);
	});
	if (this.getSource(sourceId)) {
		this.removeSource(sourceId);
	}
	return this;
}

maplibregl.Map.prototype.tileBounds = null;
maplibregl.Map.prototype.newSource = function(ds, fc = null) {
    var map = this;
	if (!!ds.ds_id) { // Standard dataset or collection
		// Check what keys are present in the dataset
		const keys = Object.keys(ds);
		map.addSource(ds.ds_id, {
			'type': 'geojson',
			'data': ds,
			'attribution': attributionString(ds),
		});
	} else if (ds?.metadata?.layers) { // mapdata
		ds.metadata.layers.forEach(layer => {
			map.addSource(`${ds.metadata.ds_type}_${ds.metadata.id}_${layer}`, {
				'type': 'geojson',
				'data': ds[layer],
				'attribution': ds.metadata.attribution,
			});
		});
	} else if (fc) { // Name and FeatureCollection provided
		map.addSource(ds, { 'type': 'geojson', 'data': fc });
	} else { // Only name given, add an empty FeatureCollection
		map.addSource(ds, { 'type': 'geojson', 'data': this.nullCollection() });
	}
	return map;
};

maplibregl.Map.prototype.layersets = [];
maplibregl.Map.prototype.layersetObjects = [];
maplibregl.Map.prototype.newLayerset = function (dc_id, source_id, paintOption, colour, colour_highlight, number, enlarger, relation_colors, initialTemporalFilter = null) {
	this.layersets.push(dc_id);
	console.debug('New layerset', dc_id, source_id, paintOption, colour, colour_highlight, number, enlarger, relation_colors, initialTemporalFilter);
	const layerset = new Layerset(this, dc_id, source_id, paintOption, colour, colour_highlight, number, enlarger, relation_colors, initialTemporalFilter);
	this.layersetObjects.push(layerset);
    return layerset;
};

maplibregl.Map.prototype.highlights = [];
maplibregl.Map.prototype.highlight = function (feature) {
	if (this.getFeatureState({source: feature.source, id: feature.id}) !== ({ highlight: true })) {
    	this.setFeatureState({ source: feature.source, id: feature.id }, { highlight: true });
    	this.highlights.push({ source: feature.source, id: feature.id, geometry: feature.geometry });
  	}
}
maplibregl.Map.prototype.clearHighlights = function () {
	this.highlights.forEach(feature => {
		this.setFeatureState({source: feature.source, id: feature.id}, { highlight: false });
	});
	this.highlights = [];
}

// Polygon-gazetteer coverage footprint (place#140). Polygon gazetteers can't use
// the point `_heat` layer (MapLibre heatmaps consume Point geometry only), so at
// low zoom they looked deceptively sparse. The indexing pipeline now bakes a
// single dissolved footprint polygon (tagged `coverage:1`, present z0–7) into
// each polygon gazetteer's source-layer, plus the real boundaries (z8+). Here we
// paint that footprint as a mottled "heat-like" fill — the polygon analogue of
// the point density heatmap — that cross-fades to the true boundaries on
// zoom-in. Styling is fully client-side: three runtime-generated `fill-pattern`
// sprites (soft radial blobs on a seeded jittered grid) alpha-blend into an
// organic mottle. Point gazetteers carry no `coverage` feature, so these layers
// render nothing for them and their behaviour is unchanged.
//
// NB the sprites live in the map's image atlas, which `setStyle` (basemap swap)
// wipes — so `ensureCoverageSprites` below is re-run after every style load
// (see heroMap.setBasemapStyle), otherwise the re-grafted fill-pattern layers
// reference missing images and the mottle silently vanishes.
const COV_MAXZOOM = 8;      // footprint fades out here; boundaries take over (z8+)
const COV_OVERALL = 0.7;    // GLOBAL opacity multiplier — never 1.0, always basemap bleed-through
// Palette: a BLUE base (matching the z8+ boundary colour and the point-heatmap
// ramp's low end) with warm ORANGE/YELLOW sparkle, so the mottle reads as a
// coverage/heat field yet stays coherent with the blue boundaries and doesn't
// clash with the red road network some basemaps draw. (Earlier a red base +
// red border competed badly with those roads.)
const COV_BORDER = '#2563eb';   // == the `_line` boundary colour (z8+ handoff)
const _COV_C = { BLUE: [79, 143, 214], ORANGE: [253, 141, 60], YELLOW: [255, 237, 160] };
const _covRgba = (c, a) => `rgba(${c[0]},${c[1]},${c[2]},${a})`;
// [key, colour, blobRadius, grid(g×g), jitter, baseOpacity, seed]  bottom→top
const COV_LAYERS = [
	['blue',   _COV_C.BLUE,   24, 3, 9, 0.45, 101],
	['orange', _COV_C.ORANGE, 16, 4, 7, 0.50, 202],
	['yellow', _COV_C.YELLOW, 10, 6, 5, 0.60, 303],
];

// Build one seamless 72px mottle tile: soft radial blobs on a seeded jittered
// grid, wrapped across tile edges so `fill-pattern` repeats without seams. The
// LCG keeps the scatter deterministic (stable mottle across sessions).
function _covSprite(color, radius, grid, jit, seed) {
	const TILE = 72;
	let s = seed >>> 0;
	const rnd = () => { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; };
	const cv = document.createElement('canvas');
	cv.width = cv.height = TILE;
	const x = cv.getContext('2d'), cell = TILE / grid, pts = [];
	for (let i = 0; i < grid; i++) for (let j = 0; j < grid; j++)
		pts.push([(i + 0.5) * cell + (rnd() * 2 - 1) * jit, (j + 0.5) * cell + (rnd() * 2 - 1) * jit]);
	pts.forEach(([px, py]) => {
		for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) {
			const gx = px + dx * TILE, gy = py + dy * TILE, g = x.createRadialGradient(gx, gy, 0, gx, gy, radius);
			g.addColorStop(0, _covRgba(color, 1)); g.addColorStop(1, _covRgba(color, 0));
			x.fillStyle = g; x.beginPath(); x.arc(gx, gy, radius, 0, 7); x.fill();
		}
	});
	return x.getImageData(0, 0, TILE, TILE);
}

// (Re-)register the coverage mottle sprites in the map's image atlas. Idempotent
// (skips any already present) and safe to call after every `setStyle`, which
// clears the atlas — see the place#140 note above. Registered at the default
// pixelRatio 1: passing `{pixelRatio:2}` with a runtime `ImageData` makes
// MapLibre (5.3.1) paint the pattern INVISIBLE.
maplibregl.Map.prototype.ensureCoverageSprites = function () {
	for (const [key, color, r, g, j, , seed] of COV_LAYERS) {
		const imgId = `whg_cov_${key}`;
		if (!this.hasImage(imgId)) this.addImage(imgId, _covSprite(color, r, g, j, seed));
	}
};

// Dynamically load a gazetteer vector tileset (one not in the base whg-context
// style) and add its fill/line/circle shape layers. RETURNS the fetched TileJSON
// so the caller (heroMap.showGazetteer) can read vector_layers + bounds. The
// baseId rule (single vector_layer → `id`; multi → `${id}__${sourceLayer}`) MUST
// stay in sync with heroMap.showGazetteer's interaction wiring.
maplibregl.Map.prototype.loadGazetteerStyle = async function (id) {
	if (!id) return null;
	const tilejson = await fetchJSON(getTilejsonURL(id));
	if (!this.getSource(id)) {
		this.addSource(id, {
			type: 'vector',
			tiles: tilejson.tiles || [],
			minzoom: tilejson.minzoom ?? 0,
			maxzoom: tilejson.maxzoom ?? 14,
			attribution: tilejson.attribution || '',
			bounds: tilejson.bounds || undefined,
		});
	}
	// Place dynamic layers below the lowest label so symbols stay legible.
	const beforeId = this.getStyle().layers.find(l => l.type === 'symbol')?.id;
	const vectorLayers = (tilejson.vector_layers && tilejson.vector_layers.length)
		? tilejson.vector_layers
		: [{ id }];
	// Point-density rendering (place#133): the tiles are pre-clustered to z8
	// (tippecanoe --cluster-maxzoom 8), so at low zoom raw point features are a
	// misleadingly-sparse scatter. Show a density HEATMAP (weighted by the
	// pre-baked sqrt_point_count) below the threshold, and only reveal the
	// individual-point circles once zoomed past it — cross-fading between them.
	const POINT_MINZOOM = 8;   // ~ tiles' --cluster-maxzoom; raw points from here
	const HEAT_MAXZOOM = 9;    // heatmap only at low zoom, faded out by here
	// Register the coverage mottle sprites (shared across all gazetteer
	// source-layers); see the place#140 note above.
	this.ensureCoverageSprites();
	for (const vl of vectorLayers) {
		const sourceLayer = vl.id;
		const baseId = vectorLayers.length > 1 ? `${id}__${sourceLayer}` : id;
		if (!this.getLayer(`${baseId}_heat`)) {
			this.addLayer({
				id: `${baseId}_heat`, type: 'heatmap', source: id, 'source-layer': sourceLayer,
				maxzoom: HEAT_MAXZOOM,
				// Defensive: coverage carries no Point geometry anyway (place#140).
				filter: ['all', ['==', ['geometry-type'], 'Point'], ['!', ['has', 'coverage']]],
				paint: {
					// Weight by the cluster's pre-computed sqrt(point_count) so big
					// clusters don't saturate; un-clustered singletons weigh 1.
					'heatmap-weight': ['coalesce', ['get', 'sqrt_point_count'], 1],
					'heatmap-intensity': ['interpolate', ['linear'], ['zoom'], 0, 1.2, HEAT_MAXZOOM, 3],
					// Wide radius at low zoom so the sparse (heavily-clustered) tile
					// points blend into a density field rather than discrete blobs.
					'heatmap-radius': ['interpolate', ['linear'], ['zoom'], 0, 22, 4, 26, HEAT_MAXZOOM, 40],
					'heatmap-color': ['interpolate', ['linear'], ['heatmap-density'],
						0, 'rgba(0,0,0,0)',
						0.2, 'rgba(103,169,207,0.55)',
						0.4, 'rgb(120,198,121)',
						0.6, 'rgb(255,237,160)',
						0.8, 'rgb(253,141,60)',
						1, 'rgb(224,64,64)'],
					// Fade out as the individual points take over near the threshold.
					'heatmap-opacity': ['interpolate', ['linear'], ['zoom'], HEAT_MAXZOOM - 2, 0.85, HEAT_MAXZOOM, 0],
				},
			}, beforeId);
		}
		if (!this.getLayer(`${baseId}_fill`)) {
			this.addLayer({
				id: `${baseId}_fill`, type: 'fill', source: id, 'source-layer': sourceLayer,
				// Exclude the coverage footprint (place#140) — it has its own
				// `_coverage_*` layers and must not double-draw or become clickable.
				filter: ['all', ['==', ['geometry-type'], 'Polygon'], ['!', ['has', 'coverage']]],
				paint: { 'fill-color': 'rgb(100, 140, 190)', 'fill-opacity': 0.12, 'fill-outline-color': 'rgba(50, 80, 120, 0.35)' },
			}, beforeId);
		}
		if (!this.getLayer(`${baseId}_line`)) {
			this.addLayer({
				id: `${baseId}_line`, type: 'line', source: id, 'source-layer': sourceLayer,
				filter: ['all', ['match', ['geometry-type'], ['Polygon', 'LineString'], true, false], ['!', ['has', 'coverage']]],
				paint: { 'line-color': '#2563eb', 'line-width': 1, 'line-opacity': 0.6 },
			}, beforeId);
		}
		if (!this.getLayer(`${baseId}_circle`)) {
			this.addLayer({
				id: `${baseId}_circle`, type: 'circle', source: id, 'source-layer': sourceLayer,
				minzoom: POINT_MINZOOM,   // raw points only once zoomed in (place#133)
				filter: ['==', ['geometry-type'], 'Point'],
				paint: {
					'circle-radius': 4, 'circle-color': '#e04040',
					'circle-stroke-color': '#fff', 'circle-stroke-width': 1,
					// Fade the points in as the heatmap fades out.
					'circle-opacity': ['interpolate', ['linear'], ['zoom'], POINT_MINZOOM, 0, HEAT_MAXZOOM, 0.85],
					'circle-stroke-opacity': ['interpolate', ['linear'], ['zoom'], POINT_MINZOOM, 0, HEAT_MAXZOOM, 1],
				},
			}, beforeId);
		}
		// place#140 coverage footprint: three mottle `fill-pattern` layers
		// (bottom→top BLUE, ORANGE, YELLOW) + a solid blue border matching the
		// z8+ boundaries, all gated to the low zooms (maxzoom COV_MAXZOOM) and
		// filtered to the single dissolved `coverage:1` feature. They render
		// nothing for point gazetteers (no coverage feature) and are excluded
		// from interaction.
		for (const [key, , , , , op] of COV_LAYERS) {
			const covId = `${baseId}_coverage_${key}`;
			if (!this.getLayer(covId)) {
				this.addLayer({
					id: covId, type: 'fill', source: id, 'source-layer': sourceLayer,
					maxzoom: COV_MAXZOOM,
					filter: ['==', ['get', 'coverage'], 1],
					paint: { 'fill-pattern': `whg_cov_${key}`, 'fill-opacity': op * COV_OVERALL },
				}, beforeId);
			}
		}
		if (!this.getLayer(`${baseId}_coverage_line`)) {
			this.addLayer({
				id: `${baseId}_coverage_line`, type: 'line', source: id, 'source-layer': sourceLayer,
				maxzoom: COV_MAXZOOM,
				filter: ['==', ['get', 'coverage'], 1],
				paint: { 'line-color': COV_BORDER, 'line-width': 1.5, 'line-opacity': 0.8 },
			}, beforeId);
		}
	}
	return tilejson;
};

function getStyleURL(style) {
	return `${process.env.TILEBOSS}/styles/${style}/style.json`; 
}

function getTilejsonURL(style) {
	return `${process.env.TILEBOSS}/data/${style}.json`;
}

async function fetchJSON(jsonURL) {
    try {
        const response = await fetch(jsonURL);
        if (!response.ok) {
            throw new Error(`Failed to fetch JSON. Status: ${response.status}`);
        }
        const resultJSON = await response.json();
        return resultJSON;
    } catch (error) {
        console.error('Error fetching JSON:', error);
        throw error;
    }
}

class acmeStyleControl {

	constructor(mapInstance) {
		this._map = mapInstance;
		this._listContainer = document.getElementById('styleListContainer'); // Hard-coded by template for place_portal page
		this.basemaps = mapInstance.initOptions.basemaps; // e.g. ['natural-earth-1-landcover', 'natural-earth-2-landcover', 'natural-earth-hypsometric-noshade', 'kg-NE2_HR_LC_SR_W', 'kg-NE2_HR_LC_SR_W_DR']
		this.styles = mapInstance.initOptions.styles; // e.g. ['whg-ne-dem']
		this.listDictionary = {};
		this.addNearby = false;
	}

	onAdd() {
		if (!this._listContainer) {
			this._listContainer = document.createElement('div');
			this._listContainer.className = 'maplibregl-ctrl maplibregl-ctrl-group';
			this._listContainer.textContent = 'Basemap';
			this._listContainer.innerHTML =
				'<button type="button" class="style-button" aria-label="Change basemap style" data-bs-title="Change basemap style">' +
				'<span class="maplibregl-ctrl-icon" aria-hidden="true"></span>' +
				'</button>';
			this._listContainer.querySelector('.style-button').addEventListener('click', this._onClick.bind(this));
		}
		else this.addNearby = true;
		
		const currentStyle = this._map.getStyle();

        const promises = [
            ...this.basemaps.map(item => fetchJSON(getTilejsonURL(item))),
            ...this.styles.map(item => fetchJSON(getStyleURL(item))),
        ];

        Promise.all(promises)
            .then(results => {
                [...this.basemaps, ...this.styles].forEach((item, index) => {
                    this.listDictionary[item] = results[index];
                });
                this.createMapStyleList();
                this.buildSwitcher(currentStyle);
            })
            .catch(error => {
                console.error('Failed to fetch JSON:', error);
            });

        return this._listContainer.id == 'styleListContainer' ? document.createElement('div') : this._listContainer;
	}

	buildSwitcher(styleJSON) {
	    const switches = document.createElement('span');
	    switches.id = 'layerSwitches';
    	const checkboxIndex = {};
	    
	    if (styleJSON.metadata && styleJSON.metadata['whg:switchgroups']) {
	        Object.entries(styleJSON.metadata['whg:switchgroups']).forEach(([groupName, group]) => {
	            const groupItem = document.createElement('li');
	            groupItem.className = 'group-item strong-red';
	            groupItem.textContent = groupName;
	
	            if (typeof group === 'object' && group !== null) {
	
		            const itemList = document.createElement('ul');
		            itemList.className = 'variant-list';
		            
	                Object.entries(group).forEach(([key, value]) => {
	                    const listItem = document.createElement('li');
	                    listItem.className = 'variant-item';
	
	                    const checkbox = document.createElement('input');
	                    checkbox.type = 'checkbox';
	                    checkbox.id = key;
	                    checkbox.name = key;
	                    checkbox.checked = value;
	                    checkboxIndex[key] = checkbox;
	
	                    const label = document.createElement('label');
	                    label.htmlFor = key;
	                    label.textContent = key;
	                    label.className = 'layer-label';
	
	                    listItem.appendChild(checkbox);
	                    listItem.appendChild(label);
	                    itemList.appendChild(listItem);
	
	                    checkbox.addEventListener('change', (event) => {
	                        this._onSwitcherChange(event.target);
	                    });
	                });
	                
	            	groupItem.appendChild(itemList);
	                
	            } else {
	
	                const checkbox = document.createElement('input');
	                checkbox.type = 'checkbox';
	                checkbox.id = groupName;
	                checkbox.name = groupName;
	                checkbox.checked = group;
	                checkboxIndex[groupName] = checkbox;
	
	                groupItem.appendChild(checkbox);
	                
	                if (groupName == 'Labels') {
					    const languageSelect = document.createElement('select');
					    languageSelect.id = 'lang';

						// Set to the map's preferred language
						const currentLang = this._map.preferredLanguage || 'local';

						languageSelect.addEventListener('change', (event) => {
							// Update the stored preference when changed, and persist it
							// site-wide (localStorage) so it survives reloads and drives
							// other consumers such as popup Wikipedia links.
							this._map.preferredLanguage = event.target.value;
							setPreferredLanguage(event.target.value);
							styleJSON.layers.forEach(layer => {
								const metadata = layer.metadata;
								if (metadata && metadata['whg:group'] === 'Labels' && layer.source === 'openmaptiles') {
									const textField = event.target.value === 'local'
										? ['coalesce', ['get', 'name:local'], ['get', 'name'], ['get', 'name:en']]
										: ['coalesce', ['get', `name:${event.target.value}`], ['get', 'name'], ['get', 'name:en']];
									this._map.setLayoutProperty(layer.id, 'text-field', textField);
								}
							});
						});
					    
					    Object.entries(languages).forEach(([code, lang]) => {
					        const option = document.createElement('option');
					        option.value = code;
					        option.textContent = lang.local;
							if (code === currentLang) {
								option.selected = true;
							}
					        languageSelect.appendChild(option);
					    });
					
					    groupItem.appendChild(languageSelect);
					}
	
	                checkbox.addEventListener('change', (event) => {
	                    this._onSwitcherChange(event.target);
	                });
	            }
	
	            switches.appendChild(groupItem);
	        });
	    }

	    const horizontalLine = document.createElement('hr');
        horizontalLine.className = 'style-list-divider';
	    switches.appendChild(horizontalLine);

	    const nearbyPlacesControl = document.createElement('li');
	    nearbyPlacesControl.id = 'nearbyPlacesControl';
	    nearbyPlacesControl.classList.add('group-item', 'strong-red');
	    nearbyPlacesControl.textContent = 'Nearby Places';
	
	    const button = document.createElement('div');
	    button.id = 'update_nearby';
	    button.setAttribute('data-bs-title', 'Search again - based on map center');
	    button.innerHTML = '<i class="fas fa-sync-alt"></i><span class="strong-red"></span>';
	    button.style.display = 'none';
	
	    const checkboxItem = document.createElement('input');
	    checkboxItem.id = 'nearby_places';
	    checkboxItem.type = 'checkbox';
	
	    const select = document.createElement('select');
	    select.id = 'radiusSelect';
	    select.setAttribute('data-bs-title', 'Search radius, based on map center');
	    for (let i = 1; i <= 12; i++) {
	        const option = document.createElement('option');
	        option.value = i ** 2;
	        option.textContent = `${i ** 2} km`;
	        select.appendChild(option);
	    }
	    select.value = 16;
	
	    nearbyPlacesControl.appendChild(checkboxItem);
	    nearbyPlacesControl.appendChild(select);
	    nearbyPlacesControl.appendChild(button);
	    
	    switches.appendChild(nearbyPlacesControl); // Event listeners coded in portal.js; hidden with css by default. 
	    
	    styleJSON.layers.forEach(layer => {
	        const metadata = layer.metadata;
	        if (metadata && metadata['whg:group']) {
	            const group = metadata['whg:group'];
	            const checkbox = checkboxIndex[group];
	            if (checkbox) {
		            let layerIds = checkbox.dataset.layers || '';
		            layerIds += (layerIds ? '|' : '') + layer.id;
		            checkbox.dataset.layers = layerIds;
	            }
		    	this._map.setLayoutProperty(layer.id, 'visibility', checkbox.checked ? 'visible' : 'none');
	        }
			if (metadata && metadata['whg:group'] === 'Labels' && layer.source === 'openmaptiles') {
				// Use the stored preferred language instead of hardcoding 'local'
				const lang = this._map.preferredLanguage || 'local';
				const textField = lang === 'local'
					? ['coalesce', ['get', 'name:local'], ['get', 'name'], ['get', 'name:en']]
					: ['coalesce', ['get', `name:${lang}`], ['get', 'name'], ['get', 'name:en']];
				this._map.setLayoutProperty(layer.id, 'text-field', textField);
			}
	    });
	
	    const existingSwitches = document.getElementById('layerSwitches');
	    const mapStyleList = document.getElementById('mapStyleList');
	    if (existingSwitches) {
	        mapStyleList.replaceChild(switches, existingSwitches);
	    } else {
	        mapStyleList.appendChild(switches);
	    }
	}

	_onSwitcherChange(group) {
		group.dataset.layers.split('|').forEach(layerId => {
			this._map.setLayoutProperty(layerId, 'visibility', group.checked ? 'visible' : 'none');
		});
	}

    createMapStyleList() {

        var styleList = document.createElement('ul');
        styleList.id = 'mapStyleList';
        styleList.className = 'maplibre-styles-list';

        const groups = [
            { title: 'Basemaps', items: this.styles, type: 'style' },
/*            { title: 'Basemaps', items: this.basemaps, type: 'basemap' },
            { title: 'Styles', items: this.styles, type: 'style' }*/
        ];

        for (const [index, group] of groups.entries()) {
            const groupItem = document.createElement('li');
            groupItem.textContent = group.title;
            groupItem.className = 'group-item strong-red';
            const itemList = document.createElement('ul');
            itemList.className = 'variant-list';

            for (const [itemIndex, item] of group.items.entries()) {
                const itemElement = document.createElement('li');
                itemElement.className = 'variant-item';
                itemList.appendChild(itemElement);

                const radioInput = document.createElement('input');
                radioInput.type = 'radio';
                radioInput.name = group.type;
                radioInput.dataset.type = group.type;
                radioInput.dataset.value = item;
                radioInput.checked =
                	(group.type === 'style' && item === this.styles[0]) ||
                	(group.type === 'basemap' && this.listDictionary[this.styles[0]].sources.basemap.url.includes(item));
                radioInput.addEventListener('change', (event) => this._onVariantClick(event));
                itemElement.appendChild(radioInput);

                const labelElement = document.createElement('label');
                labelElement.textContent = this.listDictionary[item].name;
                labelElement.className = 'variant-item-label';
                labelElement.dataset.type = group.type;
                labelElement.dataset.value = item;
                labelElement.addEventListener('click', (event) => {
                    radioInput.checked = true;
                    this._onVariantClick(event);
                });
                itemElement.appendChild(labelElement);

            }

            groupItem.appendChild(itemList);
            styleList.appendChild(groupItem);
        }

	    const horizontalLine = document.createElement('hr');
        horizontalLine.className = 'style-list-divider';
	    styleList.appendChild(horizontalLine);

	    const hillshadeGroupItem = document.createElement('li');
	    hillshadeGroupItem.textContent = 'Hillshade';
	    hillshadeGroupItem.className = 'group-item strong-red';
	    const hillshadeCheckbox = document.createElement('input');
	    hillshadeCheckbox.type = 'checkbox';
	    hillshadeCheckbox.id = 'hillshadeCheckbox';
        hillshadeCheckbox.className = 'hillshade-checkbox';

        const sources = this.listDictionary[this.styles[0]].sources || {};
		const hasTerrariumSources = Object.keys(sources).some(source => source.startsWith('terrarium'));
	    if (hasTerrariumSources) {
	        hillshadeCheckbox.checked = true;
	    } else {
	        hillshadeCheckbox.disabled = true;
	    }

	    hillshadeCheckbox.addEventListener('change', (event) => this._toggleHillshadeLayers(event));
	    hillshadeGroupItem.appendChild(hillshadeCheckbox);
	    styleList.appendChild(hillshadeGroupItem);

        this._listContainer.appendChild(styleList);
    }

	onRemove() {
		this._container.parentNode.removeChild(this._container);
		this._map = undefined;
	}

    _onClick() {
        let styleList = document.getElementById('mapStyleList');
        if (styleList) {
            styleList.classList.toggle('show');
        }
        this._listContainer.classList.toggle('opaque');
    }

	_toggleHillshadeLayers(event) {
	    const isChecked = event.target.checked;
	    const layers = this._map.getStyle().layers;
	    for (const layer of layers) {
	        if (layer.type == 'hillshade') {
	            this._map.setLayoutProperty(layer.id, 'visibility', isChecked ? 'visible' : 'none');
	        }
	    }
	}

	_onVariantClick(event) {

	    if (event.target.dataset.type === 'basemap') {

			const resultJSON = this.listDictionary[event.target.dataset.value];
	        this._map.setStyle(this._map.getStyle(), {
	            transformStyle: (previousStyle, nextStyle) => ({
	                ...nextStyle,
	                sources: {
	                    ...previousStyle.sources,
	                    'basemap': resultJSON,
	                }
	            })
	        });

		}
		else {

	        const resultJSON = this.listDictionary[event.target.dataset.value];
			console.log('Switching style', resultJSON);

			this._map.setStyle(resultJSON, {
	            diff: false, // Force rebuild because native diff logic cannot handle this transformation
	            transformStyle: (previousStyle, nextStyle) => {
					console.log('Next style', nextStyle);

					const modifiedSources = {
	                    ...nextStyle.sources,
	                    ...Object.keys(previousStyle.sources).reduce((acc, key) => {
	                        if (!this._map.baseStyle.sources.includes(key)) {
	                            acc[key] = previousStyle.sources[key];
	                        }
	                        return acc;
	                    }, {}),
	                };

	                var modifiedLayers = [
	                    ...nextStyle.layers,
	                    ...previousStyle.layers.filter((layer) => !this._map.baseStyle.layers.includes(layer.id)),
	                ];

	                const hillshadeCheckbox = document.getElementById('hillshadeCheckbox');
	                const isChecked = hillshadeCheckbox ? hillshadeCheckbox.checked : false;
	                for (var layer of modifiedLayers) {
	                    if (layer.type === 'hillshade') {
							layer.layout.visibility = isChecked ? 'visible' : 'none';
	                    }
	                }

			        // Ensure that if present the ecolayers are invisible
			        for (var layer of modifiedLayers) {
			            if (layer.id === 'ecoregions' || layer.id === 'biomes') {
			                layer.layout.visibility = 'none';
			            }
			        }

	                this._map.baseStyle.sources = Object.keys(resultJSON.sources);
	                this._map.baseStyle.layers = resultJSON.layers.map((layer) => layer.id);

					// Set map container background colour to the value in the style metadata
			        const backgroundColor = resultJSON.metadata['whg:backgroundcolour'];
			        if (backgroundColor) {
			            this._map.getContainer().style.backgroundColor = backgroundColor;
			        }

	                const styleName = resultJSON.sources.basemap.url.split('/').pop().replace('.json', '');
				    const basemapInput = document.querySelector(`input[name="basemap"][value="${styleName}"]`);
			        if (basemapInput) {
			            basemapInput.checked = true;
			        }

					return {
	                    ...nextStyle,
	                    sources: modifiedSources,
	                    layers: modifiedLayers,
	                };
	            },
	        }, () => { // Callback function called after setStyle is completed
			    this.buildSwitcher(resultJSON);
			});

		}
	}
}

class CustomTerrainControl {
    constructor(options) {
        this._container = document.createElement('div');
        this._container.className = 'custom-terrain-control maplibregl-ctrl maplibregl-ctrl-group';
        this._terrainSources = [];
        this._currentTerrainSource = null;
        this._tempTerrain = 'temp_terrain';
        this._zoomListener = null;
        this._hillshadeState = null;

        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'maplibregl-ctrl-terrain';
        button.setAttribute('data-bs-title', 'Enable terrain');

        const iconSpan = document.createElement('span');
        iconSpan.className = 'maplibregl-ctrl-icon';
        iconSpan.setAttribute('aria-hidden', 'true');
        button.appendChild(iconSpan);

        button.addEventListener('click', () => this.toggleTerrain(button));
        this._container.appendChild(button);
    }

    onAdd(map) {
        this._map = map;
        return this._container;
    }

    onRemove() {
        this._container.parentNode.removeChild(this._container);
        this._map = undefined;
    }

    toggleTerrain(button) {

        const enablingTerrain = !button.classList.contains('maplibregl-ctrl-terrain-enabled');
        button.classList.toggle('maplibregl-ctrl-terrain-enabled', enablingTerrain);
        button.setAttribute('data-bs-title', enablingTerrain ? 'Disable terrain' : 'Enable terrain');

        const hillshadeCheckbox = document.getElementById('hillshadeCheckbox');
        if (hillshadeCheckbox) {
	        if (enablingTerrain) {
				this._hillshadeState = hillshadeCheckbox.checked; // Record state of hillshadeCheckbox when terrain is enabled
		        if (!hillshadeCheckbox.checked) {
					hillshadeCheckbox.disabled = false;
					hillshadeCheckbox.click();
		        }
				hillshadeCheckbox.disabled = true;
			}
			else {
				hillshadeCheckbox.disabled = false;
				if (!this._hillshadeState) hillshadeCheckbox.click();
			}
		}

        if (enablingTerrain) {
			const mapStyle = this._map.getStyle();

	        this._terrainSources = [];
            for (const source in mapStyle.sources) {
                const demSource = mapStyle.sources[source];
                if (demSource.type === 'raster-dem') {
                    this._terrainSources.push(demSource);
                }
            }

            this._zoomListener = () => this.handleZoomChange();
            this._map.on('zoom', this._zoomListener);
            this.handleZoomChange();
			this._map.setPitch(60);
		}
		else {
			this._map.off('zoom', this._zoomListener);
            this._zoomListener = null;
            this._terrainSources = [];
			if (this._map.getSource(this._tempTerrain)) {
			    this._map.removeSource(this._tempTerrain);
			}
            this._map
            	.setTerrain(null)
				.setPitch(0)
				.resetNorth();
		}

    }

    handleZoomChange() {
        const currentZoom = this._map.getZoom();

        // TODO: Disable terrain for zoom below 8?

        // Find the first terrain source for which the current map zoom is in range
        const selectedTerrainSource = this._terrainSources.find(source => {
            return currentZoom >= source.minzoom && currentZoom <= source.maxzoom;
        });

        if (selectedTerrainSource && selectedTerrainSource !== this._currentTerrainSource) {
            this._map.setTerrain(null);
			if (this._map.getSource(this._tempTerrain)) {
			    this._map.removeSource(this._tempTerrain);
			}
            this._map.addSource(this._tempTerrain, { ...selectedTerrainSource });
            this._currentTerrainSource = selectedTerrainSource;
            this._map.setTerrain({
                source: this._tempTerrain,
                exaggeration: 1.5,
            });
        }
    }
}

function generateMapImage(map, dpi = 300, fileName = 'WHG_Map') {
    // Create and open modal
    const modal = $('<div id="map-download-dialog" class="modal fade" data-bs-backdrop="static" data-bs-keyboard="false"></div>');
    const modalDialog = $('<div class="modal-dialog modal-dialog-centered"></div>');
    const modalContent = $('<div class="modal-content"></div>');
    const modalHeader = $('<div class="modal-header"></div>');
    const modalTitle = $('<h5 class="modal-title" id="map-download-dialog-title">Download Map Image</h5>');
    const modalCloseButton = $('<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>');
    const modalBody = $('<div class="modal-body"></div>');
    const injunctionText = $('<div id="injunction-text" class="injunction-text">The following attribution must be displayed together with any use of this map image:</div>');
    const attributionText = $(`<div id="attribution-text" class="attribution-text">${$('.maplibregl-ctrl-attrib-inner').text()}</div>`);
    const modalFooter = $('<div class="modal-footer"></div>');
    const downloadButton = $('<button type="button" class="btn btn-success" id="download" style="display: none;" disabled>...rendering...</button>');
    const cancelButton = $('<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>');
	const copyAttributionButton = $('<button type="button" class="btn btn-primary" id="copy-attribution">Acknowledge and Copy Attribution</button>');
	const previouslyFocused = document.activeElement;
    modalHeader.append(modalTitle, modalCloseButton);
    modalBody.append(injunctionText, attributionText);
    modalFooter.append(cancelButton, copyAttributionButton, downloadButton);
    modalContent.append(modalHeader, modalBody, modalFooter);
    modalDialog.append(modalContent);
    modal.append(modalDialog);
    $('#map').append(modal);
    $('#map-download-dialog').modal('show');

	const originalCanvas = map.getCanvas();
	const width = originalCanvas.width;
	const height = originalCanvas.height;
	const actualPixelRatio = window.devicePixelRatio;
    const newPixelRatio = dpi / 96;
	
    // TODO: Need to increase zoom as pitch increases - but the following is inadequate
    const pitch = map.getPitch();
    const originalZoom = map.getZoom();
    const pitchFactor = 1 / Math.cos(pitch * Math.PI / 180);
    const adjustedZoom = originalZoom + Math.log2(pitchFactor);

	// Create a hidden container and map renderer
	const originalMapContainer = originalCanvas.parentNode;
	const container = document.createElement('div');
	container.id = 'map-render-container';
	container.style.width = width / actualPixelRatio + 'px';
	container.style.height = height / actualPixelRatio + 'px';
	container.style.position = 'absolute';
	container.style.top = '-9999px';
	container.style.left = '-9999px';
	container.style.zIndex = '-1';
	container.style.opacity = '1';
	originalMapContainer.appendChild(container);
    
	const renderMap = new maplibregl.Map({
		container: container,
		style: map.getStyle(),
		center: map.getCenter(),
		zoom: adjustedZoom,
		bearing: map.getBearing(),
		pitch: pitch,
		interactive: false,
		preserveDrawingBuffer: true,
		fadeDuration: 0,
		attributionControl: false,
		pixelRatio: newPixelRatio, // Set for higher DPI rendering
		transformRequest: map._requestManager._transformRequestFn,
	});

	function waitForRenderComplete(mapInstance, callback) {
		const check = () => {
			const renderStatus = mapInstance.areTilesLoaded() && !mapInstance.isMoving();
			if (renderStatus) {
				callback();
			} else {
				setTimeout(check, 100); // Poll until ready
			}
		};
		check();
	}
	renderMap.once('load', () => {
		waitForRenderComplete(renderMap, () => {
			downloadButton.prop('disabled', false).text('Download');
		});
	});

    $('#map-download-dialog').on('shown.bs.modal', function () {
        new ClipboardJS(copyAttributionButton[0], {
            text: function (trigger) {
                return attributionText.text();
            },
            container: document.getElementById('map-download-dialog')
        }).on('success', function (e) {
            console.log('Attribution text copied to clipboard.');
            downloadButton.show();
        }).on('error', function (e) {
            console.error('Unable to copy attribution text: ', e);
        });
    });

    downloadButton.on('click', function () {
    	this.blur();
		const canvas = renderMap.getCanvas();
		const imageFileName = `${fileName}.png`;
		const a = $('<a>');
		a.prop('href', canvas.toDataURL());
		a.prop('download', imageFileName);
		a[0].click();
		a.remove();
        $('#map-download-dialog').modal('hide');
    });

    cancelButton.on('click', function () {
    	this.blur();
        $('#map-download-dialog').modal('hide');
    });

    $('#map-download-dialog').on('hidden.bs.modal', function () {
		const dialog = this;

		// Delay both focus and DOM cleanup
		setTimeout(() => {
			if (previouslyFocused && typeof previouslyFocused.focus === 'function') {
				previouslyFocused.focus();
			}

			renderMap.remove();
			container.remove();
			$(dialog).remove();
		}, 100);
    });
}

class downloadMapControl {

	constructor(mapInstance) {
		this._map = mapInstance;
	}

    onAdd() {
        this._container = document.createElement('div');
        this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group maplibregl-ctrl-download';

        const downloadButton = document.createElement('button');
        downloadButton.type = 'button';
        downloadButton.className = 'download-map-button';
        downloadButton.setAttribute('aria-label', 'Download map image');
        downloadButton.setAttribute('title', 'Download map image');

        const iconSpan = document.createElement('span');
        iconSpan.className = 'maplibregl-ctrl-icon';
        iconSpan.setAttribute('aria-hidden', 'true');

        downloadButton.appendChild(iconSpan);
        this._container.appendChild(downloadButton);

        downloadButton.addEventListener('click', () => {
            generateMapImage(this._map);
        });

        return this._container;
    }
}

class CustomAttributionControl extends maplibregl.AttributionControl {
    constructor(options) {
        super(options);
        this.autoClose = options.autoClose !== false;
    }

    setLinksTargetBlank(container) {
        const links = container.querySelectorAll('a');
        links.forEach(link => {
            link.setAttribute('target', '_blank');
        });
    }

    // Use MutationObserver to watch for changes and update link targets
    observeContainer(container) {
        const observer = new MutationObserver(mutations => {
            mutations.forEach(mutation => {
                if (mutation.type === 'childList') {
                    this.setLinksTargetBlank(container);
                }
            });
        });

        observer.observe(container, { childList: true, subtree: true });
    }
    
    onAdd(map) {
        const container = super.onAdd(map);
        // Automatically close the AttributionControl if autoClose is enabled
        if (this.autoClose) {
            const attributionButton = container.querySelector('.maplibregl-ctrl-attrib-button');
            if (attributionButton) {
				const delay = 500; // Delay required to ensure that listener is registered
                setTimeout(() => {
	                attributionButton.dispatchEvent(new MouseEvent('click', {
	                    bubbles: true,
	                    cancelable: true,
	                    view: window
	                }));
                    container.style.display = 'block';
                    container.classList.add('fade-in');
                }, delay);
            }
        }
        this.setLinksTargetBlank(container);
        this.observeContainer(container);
        return container;
    }
}

maplibregl.Map.prototype.reset = function (fly = true) {
	if (fly) {
    	this.flyTo({
      		center: this.initOptions.center,
      		zoom: this.initOptions.zoom,
      		speed: 0.5,
    	});
  	} else {
    	this.setCenter(this.initOptions.center);
    	this.setZoom(this.initOptions.zoom);
  	}
  	return this;
};

maplibregl.Map.prototype.fitViewport = function (bbox, maxZoom) {
	// Validate bbox: must be an array of four finite numbers
	if (
		!Array.isArray(bbox) ||
		bbox.length !== 4 ||
		bbox.some(v => typeof v !== "number" || !isFinite(v)) ||
		bbox[1] < -90 || bbox[3] > 90
	) {
		console.warn("fitViewport(): invalid bbox supplied", bbox);
		return this; // Do nothing instead of throwing
	}

	// This function addresses an apparent bug with flyTo and fitBounds in MapLibre/Maptiler,
	// which crash and/or fail to center correctly with large mapPadding values.
	const mapContainer = this.getContainer();
	const mapContainerRect = mapContainer.getBoundingClientRect();
	const mapControls = mapContainer.querySelector('.maplibregl-control-container');
	mapControls.style.height = '100%';
	const mapControlsRect = mapControls.getBoundingClientRect();
	const mapControlsRectMargin = parseFloat(getComputedStyle(mapControls).marginTop);

	const padding = 30; // Apply equal padding on all sides within viewport

	const bounds = [[bbox[0], bbox[1]], [bbox[2], bbox[3]]];
	const sw = this.project(bounds[0]);
	const ne = this.project(bounds[1]);
	let zoom = Math.log2( // Returns Infinity for bbox(Point)
		Math.min(
			(mapControlsRect.width - 2 * padding) / (ne.x - sw.x),
			(mapControlsRect.height- 2 * padding) / (sw.y - ne.y))
		) + this.getZoom();
	zoom = isNaN(zoom) ? this.getMaxZoom() : Math.min(zoom, this.getMaxZoom());
	zoom = Math.max(zoom, this.getMinZoom());
	if (!isNaN(maxZoom)) zoom = Math.min(zoom, maxZoom); // Limit zoom if maxZoom parameter is passed

	const viewportPadding = {
		top: Math.round(mapControlsRect.top - mapContainerRect.top - mapControlsRectMargin),
		bottom: Math.round(mapContainerRect.bottom - mapControlsRect.bottom - mapControlsRectMargin),
		left: Math.round(mapControlsRect.left - mapContainerRect.left - mapControlsRectMargin),
		right: Math.round(mapContainerRect.right - mapControlsRect.right - mapControlsRectMargin),
	};
	//console.log('fitViewport', mapControlsRect, bbox, maxZoom, zoom, viewportPadding);

	this.flyTo({
		center: [
			(bbox[0] + bbox[2]) / 2,
			(bbox[1] + bbox[3]) / 2
		],
		zoom: zoom,
		padding: viewportPadding,
		duration: 1000,
	});
	
	return this;
};

class CustomDrawingControl {
	constructor(mapInstance, options) {
		this._map = mapInstance;
		this._options = options;

		this._map._draw = new MapboxDraw({
			displayControlsDefault: false,
			controls: {
				polygon: true,
				trash: true,
			},
			styles: [
			    // ACTIVE (being drawn)
			    // line stroke
			    {
			        "id": "gl-draw-line",
			        "type": "line",
			        "filter": ["all", ["==", "$type", "LineString"], ['==', 'active', 'true']],
			        "layout": {
			          "line-cap": "round",
			          "line-join": "round"
			        },
			        "paint": {
			          "line-color": "#D20C0C",
			          "line-dasharray": [0.2, 2],
			          "line-width": 2
			        }
			    },
			    // polygon mid points
			    {
			      'id': 'gl-draw-polygon-midpoint',
			      'type': 'circle',
			      'filter': ['all',
			        ['==', '$type', 'Point'],
			        ['==', 'meta', 'midpoint']],
			      'paint': {
			        'circle-radius': 3,
			        'circle-color': '#fbb03b'
			      }
			    },
			    // polygon outline stroke
			    // This doesn't style the first edge of the polygon, which uses the line stroke styling instead
			    {
			      "id": "gl-draw-polygon-stroke-active",
			      "type": "line",
			      "filter": ["all", ["==", "$type", "Polygon"], ['==', 'active', 'true']],
			      "layout": {
			        "line-cap": "round",
			        "line-join": "round"
			      },
			      "paint": {
			        "line-color": "#D20C0C",
			        "line-dasharray": [0.2, 2],
			        "line-width": 2
			      }
			    },
			    // vertex point halos
			    {
			      "id": "gl-draw-polygon-and-line-vertex-halo-active",
			      "type": "circle",
			      "filter": ["all", ["==", "meta", "vertex"], ["==", "$type", "Point"], ['==', 'active', 'true']],
			      "paint": {
			        "circle-radius": 5,
			        "circle-color": "#FFF"
			      }
			    },
			    // vertex points
			    {
			      "id": "gl-draw-polygon-and-line-vertex-active",
			      "type": "circle",
			      "filter": ["all", ["==", "meta", "vertex"], ["==", "$type", "Point"], ['==', 'active', 'true']],
			      "paint": {
			        "circle-radius": 3,
			        "circle-color": "#D20C0C",
			      }
			    },
			
			    // INACTIVE (static, already drawn)
			    // line stroke
			    {
			        "id": "gl-draw-line-static",
			        "type": "line",
			        "filter": ["all", ["==", "$type", "LineString"], ['!=', 'active', 'true']],
			        "layout": {
			          "line-cap": "round",
			          "line-join": "round"
			        },
			        "paint": {
			          "line-color": "#D20C0C",
			          "line-width": 1,
        			  "line-opacity": 0.5
			        }
			    },
			    // polygon fill
			    {
			      "id": "gl-draw-polygon-fill-static",
			      "type": "fill",
			      "filter": ["all", ["==", "$type", "Polygon"]],
			      "paint": {
			        "fill-color": "#D20C0C",
			        "fill-outline-color": "#D20C0C",
			        "fill-opacity": 0.1
			      }
			    },
			    // polygon outline
			    {
			      "id": "gl-draw-polygon-stroke-static",
			      "type": "line",
			      "filter": ["all", ["==", "$type", "Polygon"], ['!=', 'active', 'true']],
			      "layout": {
			        "line-cap": "round",
			        "line-join": "round"
			      },
			      "paint": {
			        "line-color": "#D20C0C",
			        "line-width": 1,
        			"line-opacity": 0.5
			      }
			    }
			  ]		
		});

		this._map.addControl(this._map._draw, 'top-left');
		this._map.on('draw.modechange', this._modechange.bind(this));
		this._map.on('draw.selectionchange', this._selectionchange.bind(this));
		this._map.on('draw.delete', this._delete.bind(this));
	}
	
	_setTrash() {
		var trashable = (
			(
				(this._map._draw.getMode() == 'simple_select' && this._map._draw.getSelected().features.length > 0) ||
				(this._map._draw.getMode() == 'direct_select' && this._map._draw.getSelectedPoints().features.length > 0)
			) &&
			this._map._draw.getMode() !== 'draw_polygon'
		);
		if (trashable) {
			this._map._drawControl.trashButton.removeAttribute('disabled');
			this._map._drawControl.trashButton.classList.remove('disabled');
		}
		else {
            this._map._drawControl.trashButton.setAttribute('disabled', true);
			this._map._drawControl.trashButton.classList.add('disabled');
		}
	}
	
	_modechange() {
	    this._setTrash();
		switch(this._map._draw.getMode()) {
			case 'draw_polygon':
	            this._map.getCanvas().style.cursor = 'crosshair';
	            break;
			case 'simple_select':
	            this._map.getCanvas().style.cursor = 'grab';
	            break;
			case 'direct_select':
	            this._map.getCanvas().style.cursor = 'pointer';
	            break;
	        default:
	            this._map.getCanvas().style.cursor = 'grab';
		}
	}
	
	_selectionchange() {
	    this._setTrash();
	}
	
	_delete() {
	    this._setTrash();
	}

	onAdd() {		
		this._appendStyles();
		
		this._map._drawControl = this._map.getContainer().querySelector(".mapboxgl-ctrl-group.mapboxgl-ctrl");
		this._map._drawControl.classList.add('maplibregl-ctrl', 'maplibregl-ctrl-group'); // Convert classnames for proper rendering
		
		this._map._drawControl.drawPolygonButton = this._map._drawControl.querySelector('.mapbox-gl-draw_polygon');
        this._map._drawControl.drawPolygonButton.setAttribute('data-bs-title', 'Draw polygon to filter results by area. Double-click to close polygon.');
        
        this._map._drawControl.trashButton = this._map._drawControl.querySelector('.mapbox-gl-draw_trash');
        this._map._drawControl.trashButton.setAttribute('data-bs-title', 'Delete selected polygon (select first by clicking it)');
	    this._setTrash();
		
		if (this._options.hide) this._map._drawControl.style.display = 'none';
		
		return this._map._drawControl;
	}

	onRemove() {
		this._map.removeControl(this._map._draw);
	}
    
    _appendStyles() {
        const style = document.createElement('style');
        style.textContent = `
			#map .mapbox-gl-draw_ctrl-draw-btn.active,
			#map .mapbox-gl-draw_ctrl-draw-btn:hover {
			  background-color: rgb(167 8 8 / 17%);
			}
			#map .mapbox-gl-draw_ctrl-draw-btn.disabled {
			  opacity: .3;
			}
        `;
        document.head.appendChild(style);
    }
}


function handleMapCreationFailure(options, errorMessage, existingMapInstance) {
    const containerId = options.container || 'map';
    const container = typeof containerId === 'string'
        ? document.getElementById(containerId)
        : containerId;

    if (!container) return;

    // If map instance exists (runtime error), show overlay
    if (existingMapInstance) {
        // Don't show multiple overlays
        if (document.getElementById('map-error-overlay')) return existingMapInstance;

        const overlay = document.createElement('div');
        overlay.id = 'map-error-overlay';
        overlay.style.cssText = `
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: rgba(255, 243, 205, 0.95);
            padding: 20px;
            border-radius: 8px;
            border: 2px solid #ffc107;
            z-index: 1000;
            max-width: 400px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        `;
        overlay.innerHTML = `
            <div style="text-align: center;">
                <h4 style="margin: 0 0 10px 0; color: #856404;">⚠️ Map Error</h4>
                <p style="margin: 0 0 15px 0;">The map encountered an error and may not display correctly.</p>
                <button onclick="location.reload()" class="btn btn-sm btn-warning">Refresh Page</button>
                <button onclick="this.parentElement.parentElement.remove()" class="btn btn-sm btn-secondary">Dismiss</button>
            </div>
        `;

        container.appendChild(overlay);
        return existingMapInstance;
    }

    // If no map instance (initial load failure), replace container content
    container.innerHTML = `
        <div style="padding: 20px; background: #fff3cd; border: 1px solid #ffc107; border-radius: 4px; margin: 20px;">
            <h4 style="color: #856404; margin-top: 0;">⚠️ Map Display Issue</h4>
            <p>We're unable to display the map in your browser. This usually happens when:</p>
            <ul>
                <li>WebGL is disabled or unavailable</li>
                <li>Too many browser tabs are using graphics acceleration</li>
                <li>Your graphics drivers need updating</li>
            </ul>
            <p><strong>Try:</strong></p>
            <ul>
                <li>Refreshing the page</li>
                <li>Closing some browser tabs</li>
                <li>Enabling WebGL in browser settings</li>
                <li>Using a different browser</li>
            </ul>
            <details style="margin-top: 10px; font-size: 0.85em; color: #6c757d;">
                <summary style="cursor: pointer;">Technical details</summary>
                <p style="margin: 5px 0 0 0; font-family: monospace;">${errorMessage}</p>
            </details>
        </div>
    `;

    // Return a stub object to prevent further errors
    return {
        on: () => {},
        addControl: () => {},
        remove: () => {},
        getContainer: () => container,
    };
}

const originalMapConstructor = maplibregl.Map;
maplibregl.Map.prototype.baseStyle = {}; // Updated by style control, and used for identifying layers that should be ignored when the map is clicked
maplibregl.Map = function (options = {}) {

    const defaultOptions = {
        container: 'map',
        style: ['WHG'/*, whg-basic-light'*//*, 'whg-basic-dark'*/],
        basemap: [/*'natural-earth-1-landcover', 'natural-earth-2-landcover', 'natural-earth-hypsometric-noshade'*/],
        zoom: 0.2,
        center: [9.2, 33],
        maxBounds: [[-Infinity, -85], [Infinity, 85]],
        minZoom: 0.1,
        maxZoom: 6,
        maxPitch: 85,
        // Controls (layer control is added depending on number of styles and basemaps listed above)
        attributionControl: {position: 'bottom-right', autoClose: true, customAttribution: '&copy; World Historical Gazetteer & contributors'}, // false
        downloadMapControl: false,
        drawingControl: false, // Instantiate hidden control with {hide: true}
        fullscreenControl: false,
        navigationControl: {position: 'top-right', showZoom: true, showCompass: false, visualizePitch: false}, // false
        sequencerControl: false,
	    temporalControl: false,
	    terrainControl: false, // If true, will force display of full navigation controls too
	    scaleControl: false,
        globeControl: false,
		globeMode: false,
    };

    // replace defaultOptions with any passed options
    var chosenOptions = { ...defaultOptions, ...options };

    if (chosenOptions.attributionControl) {
		chosenOptions.customAttributionControl = {...chosenOptions.attributionControl, compact: true};
		chosenOptions.attributionControl = false;
	}

    if (chosenOptions.terrainControl) {
		chosenOptions.navigationControl = {position: 'top-right', showZoom: true, showCompass: true, visualizePitch: true};
	}

	chosenOptions.basemaps = chosenOptions.basemap === "" ? [] : (Array.isArray(chosenOptions.basemap) ? chosenOptions.basemap : [chosenOptions.basemap]);

	chosenOptions.styles = [];
	if (!(typeof chosenOptions.style === 'object' && chosenOptions.style !== null && !Array.isArray(chosenOptions.style))) { // style is not JSON object
		chosenOptions.styles = Array.isArray(chosenOptions.style) ? chosenOptions.style : [chosenOptions.style];
		chosenOptions.style = getStyleURL(chosenOptions.styles[0]);
	}

    // Try to create the map with error handling
    let mapInstance;
    try {
        mapInstance = new originalMapConstructor(chosenOptions);
    } catch (error) {
        console.error('Failed to create map:', error);
        return handleMapCreationFailure(chosenOptions, error.message, null);
    }

    mapInstance.initOptions = chosenOptions;

    mapInstance.on('error', (e) => {
        // Identify if this is a "Fatal" error (WebGL or Critical Resource Failure)
        let isFatal = false;
        let userMessage = 'The map encountered an error.';

        // Check for WebGL/Context errors
        if (e.error && (
            e.error.message?.includes('WebGL') ||
            e.error.message?.includes('context') ||
            e.error.message?.includes('CONTEXT_LOST')
        )) {
            isFatal = true;
            userMessage = 'Graphics context lost. Please refresh.';
        }

        // Check for Style/Network Fetch errors
        else if (e.error && (
             (e.error.status && e.error.status >= 400) || // Catch 404, 500, etc.
             (e.error.message && (
                 e.error.message.includes('Fetch') ||
                 e.error.message.includes('Network') ||
                 e.error.message.includes('Failed to load') ||
                 e.error.message.includes('style') // Catch style parsing/loading errors
             ))
        )) {
            isFatal = true;
            userMessage = 'Unable to load map data or style. Please check your connection.';
            console.warn('Map resource failed to load:', e.error);
        }

        // If fatal, trigger UI overlay
        if (isFatal) {
            handleMapCreationFailure(chosenOptions, userMessage, mapInstance);
        } else {
            // Log non-fatal errors (like missing tiles or minor warnings)
            console.error('MapLibre runtime error:', e);
        }
    });

    mapInstance.on('load', () => { // Add chosen controls

    	const currentStyle = mapInstance.getStyle();
		mapInstance.baseStyle.sources = Object.keys(currentStyle.sources);
		mapInstance.baseStyle.layers = currentStyle.layers.map((layer) => layer.id);

		// Set map container background colour to the value in the style metadata
        const backgroundColor = currentStyle.metadata['whg:backgroundcolour'];
        if (backgroundColor) {
            // mapInstance.getContainer().style.backgroundColor = backgroundColor;
			console.debug('Ignoring map background colour requested by style', backgroundColor);
        }
		mapInstance.getContainer().style.backgroundColor = '#daecf1';

		// Store preferred language on map instance
		mapInstance.preferredLanguage = getPreferredLanguage();
		console.info('Setting map labels to language:', mapInstance.preferredLanguage);

		currentStyle.layers.forEach(layer => {
			if (layer.layout && layer.layout['text-field'] && layer.source === 'openmaptiles') {
				const textField = mapInstance.preferredLanguage === 'local'
					? ['coalesce', ['get', 'name:local'], ['get', 'name'], ['get', 'name:en']]
					: ['coalesce', ['get', `name:${mapInstance.preferredLanguage}`], ['get', 'name'], ['get', 'name:en']];

				mapInstance.setLayoutProperty(layer.id, 'text-field', textField);
			}
		});

		if (chosenOptions.scaleControl) mapInstance.addControl(new maplibregl.ScaleControl({maxWidth: 150, unit: 'metric'}), 'bottom-left');
		
		if (chosenOptions.fullscreenControl) mapInstance.addControl(new maplibregl.FullscreenControl(), 'top-left');
		if (chosenOptions.downloadMapControl) mapInstance.addControl(new downloadMapControl(mapInstance), 'top-left');
		if (chosenOptions.drawingControl) mapInstance.addControl(new CustomDrawingControl(mapInstance, chosenOptions.drawingControl), 'top-left');

		if (chosenOptions.basemaps.length + chosenOptions.styles.length > 1) {
			mapInstance.styleControl = new acmeStyleControl( mapInstance );
			mapInstance.addControl(mapInstance.styleControl, 'top-right');
		}
		if (chosenOptions.globeControl) {
			mapInstance.addControl(new maplibregl.GlobeControl(), 'top-right');
			if (chosenOptions.globeMode) mapInstance.setProjection({ type: 'globe' });
			// MapLibre adds a redundant `title` attribute to the globe control button after first click on it
			const globeButton = mapInstance.getContainer().querySelector('.maplibregl-ctrl-globe');
			if (globeButton) {
				globeButton.removeAttribute('title');
				globeButton.setAttribute('data-bs-title', 'Toggle Globe');
				// Add a click listener to the globe button to remove the title attribute
				globeButton.addEventListener('click', function() {
					this.removeAttribute('title');
				});
			}
		}
		if (chosenOptions.terrainControl) mapInstance.addControl(new CustomTerrainControl({source: 'terrarium-aws'}), 'top-right');
		if (chosenOptions.navigationControl) mapInstance.addControl(new maplibregl.NavigationControl(chosenOptions.navigationControl), chosenOptions.navigationControl.position);
		if (!!chosenOptions.customAttributionControl) {
			mapInstance.addControl(new CustomAttributionControl(chosenOptions.customAttributionControl), chosenOptions.customAttributionControl.position);
		}
		
		$(mapInstance.getContainer().querySelector('.maplibregl-control-container'))
		.tooltip({
	    	selector: 'button:not(.dateline-button), select, summary.maplibregl-ctrl-attrib-button, #dateline.expanded .dateline-button',
	    	// Scope tooltips to this map's own container. Was hard-coded '#map', which is null on any
	    	// page whose map container isn't #map (e.g. the reconciliation review map) → Bootstrap threw.
	    	container: mapInstance.getContainer()
		})
		
    });
    return mapInstance;
};

window.whg_maplibre = maplibregl;
