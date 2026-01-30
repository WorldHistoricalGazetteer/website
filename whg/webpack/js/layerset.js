// layerset.js

class Layerset {

    constructor(mapInstance, dc_id, source_id, paintOption, colour, colour_highlight, number, enlarger, relation_colors, initialTemporalFilter = null) {
        // The following default colours must be expressed in rgba format
        this.colour = (typeof colour !== 'string') ? 'rgba(255,165,0,1)' : colour; // orange
        // this.colour = (typeof colour !== 'string') ? 'rgba(0,158,255,1)' : colour; // blue
        this.colour_outline = 'rgba(0,0,0,1)'; // black
        this.colour_highlight = (typeof colour_highlight !== 'string') ? 'rgba(255,0,0,1)' : colour_highlight; // red

        this.number = (number === undefined) ? false : number;
        this.enlarger = (enlarger === undefined) ? 1 : enlarger;

        this.colour_options = [
            this.colour_highlight,
            relation_colors
                ? ['match', ['get', 'relation'], ...relation_colors, this.colour]
                : this.colour
        ]

        this.temporalFilter = initialTemporalFilter;  // Store initial temporal filter
        this.relationFilter = null;  // Store relation filter

        const paintOptions = {
            'plain': {
				// A `feature-state`-based `highlighter` condition is applied dynamically to each of the expressions given below
				'Polygon': {
					'fill-color': this.colour_options,
			        'fill-opacity': [
						0.5,
						.2
			        ],
					'fill-antialias': false, // Disables what would be a virtually-invisible 1px outline
				},
				'Polygon-line': { // Add extra layer to enable polygon outline styling
					'line-color': [
					    this.colour_highlight,
					    this.colour_outline
					],
					'line-width': [
			            'interpolate', ['exponential', 2], ['zoom'],
			            0, 1, // zoom level, line width
			            10, 5, // zoom level, line width
			        ],
			        'line-opacity': [
						0.5,
						.7
			        ],
			        'line-dasharray': ["literal", [4, 2]]
				},
				'LineString': {
					'line-color': this.colour_options,
					'line-width': [
			            'interpolate', ['exponential', 2], ['zoom'],
			            0, 4, // zoom level, line width
			            10, 10, // zoom level, line width
			        ],
			        'line-opacity': [
						0.5,
						.4
			        ]
				},
				'Point': {
			        'circle-color': this.colour_options,
			        'circle-opacity': [
						0.2,
						.85
			        ],
					'circle-radius': [
			            'interpolate', ['exponential', 2], ['zoom'],
			            0, Math.max(3, .5 * this.enlarger), // zoom level, radius
			            10, Math.max(13, .5 * this.enlarger), // zoom level, radius
			            18, 20 // zoom level, radius
					],
					'circle-stroke-color': [
					    this.colour_highlight,
					    this.colour_outline
					],
					'circle-stroke-opacity': [
						.9,
						['any', ['==', ['get', 'min'], 'null'], ['==', ['get', 'max'], 'null']], .3,
						.7
			        ],
					'circle-stroke-width': [
						7,
						1
					],
				}
            },
            'standard': {
                'Polygon': {
                    'fill-color': this.colour_options,
                    'fill-opacity': [
                        0.6,
                        .4
                    ],
                    'fill-antialias': false, // Disables what would be a virtually-invisible 1px outline
                },
                'Polygon-line': { // Add extra layer to enable polygon outline styling
                    'line-color': [
                        this.colour_highlight,
                        this.colour_outline
                    ],
                    'line-width': [
                        'interpolate', ['exponential', 2], ['zoom'],
                        0, 1, // zoom level, line width
                        10, 5, // zoom level, line width
                    ],
                    'line-opacity': [
                        0.5,
                        .7
                    ],
                },
                'Granular': { // To be replaced with fill-pattern by worker
                    'fill-color': this.colour_options,
                    'fill-opacity': [
                        0.6,
                        .4
                    ],
                    'fill-antialias': false, // Disables what would be a virtually-invisible 1px outline
                },
                'Granular-line': {
                    'line-color': [
                        this.colour_highlight,
                        this.colour // fill-pattern cannot be appropriately coloured
                    ],
                    'line-width': [
                        'interpolate', ['exponential', 2], ['zoom'],
                        0, 1, // zoom level, line width
                        5, 5,
                        21, 1000, // zoom level, line width
                    ],
                    'line-opacity': [
                        0.5,
                        .7
                    ],
                    'line-dasharray': ["literal", [4, 2]],
                    'line-blur': 0.6
                },
                'Point': {
                    'circle-color': this.colour_options,
                    'circle-opacity': [
                        0.4,
                        .65
                    ],
                    'circle-radius': [
                        'interpolate', ['exponential', 2], ['zoom'],
                        0, 4, // Fixed radius at zoom 0 to ensure visibility
                        3, [
                            'max',
                            4, // Minimum visible radius at zoom 3
                            [
                                '*',
                                ['max', 0.6, ['coalesce', ['get', 'granularity'], 1]], // Default radius to 1km if granularity missing or less than 0.6
                                ['*', ['^', 2, 3], 0.01] // granularity * 2^3 (zoom 3) * constant scale (pixels<>km)
                            ]
                        ],
                        21, [
                            '*',
                            ['max', 0.6, ['coalesce', ['get', 'granularity'], 1]], // Default radius to 1km if granularity missing or less than 0.6
                            ['*', ['^', 2, 21], 0.01] // granularity * 2^21 (zoom 21) * constant scale (pixels<>km)
                        ]
                    ],
                    'circle-blur': [
                        'case',
                        ['has', 'granularity'],
                        0.6, // If 'granularity' property exists, set blur to 0.6
                        0    // If 'granularity' property does not exist, set blur to 0
                    ],
                    'circle-stroke-color': [
                        this.colour_highlight,
                        // 'rgba(0,0,0,0)'
                        'rgba(255,255,0,0.8)'
                    ],
                    'circle-stroke-width': [
                        'interpolate', ['exponential', 2], ['zoom'],
                        0, 1, // zoom level, line width
                        4, 2,
                        21, 100
                    ],
                },
                'Point-heatmap': {
                    'heatmap-weight': [
                        'interpolate',
                        ['linear'],
                        ['get', 'granularity'],
                        0, 0,
                        6, 1
                    ],
                    'heatmap-intensity': [
                        'interpolate',
                        ['linear'],
                        ['zoom'],
                        0, 1,
                        9, 3
                    ],
                    'heatmap-color': [
                        'interpolate',
                        ['linear'],
                        ['heatmap-density'],
                        0, 'rgba(0, 0, 255, 0)',
                        0.2, 'royalblue',
                        0.4, 'cyan',
                        0.6, 'lime',
                        0.8, 'yellow',
                        1, 'red'
                    ],
                    'heatmap-radius': [
                        'interpolate',
                        ['linear'],
                        ['zoom'],
                        0, 2,
                        9, 20
                    ],
                    'heatmap-opacity': 0.3
                }
            },
            'nearby-places': {
                'Polygon': {
                    'fill-color': 'rgba(0,255,0,.3)', // green
                    'fill-outline-color': 'rgba(0,255,0,.8)' // green
                },
                'LineString': {
                    'line-color': 'rgba(0,255,0,.8)', // green
                    'line-width': 1
                },
                'Point': {
                    'circle-color': 'rgb(0,255,0)', // green
                    'circle-opacity': .7,
                    'circle-radius': [
                        'interpolate',
                        ['linear'],
                        ['zoom'],
                        0, .5, // zoom, radius
                        16, 20,
                    ],
                    'circle-stroke-color': 'rgb(0,255,0)', // green
                    'circle-stroke-opacity': .7,
                    'circle-stroke-width': 3,
                }
            },
            'countries': {
                'Polygon': {
                    'fill-color': 'rgba(255,0,0,.1)', // red
                    'fill-outline-color': 'rgba(255,0,0,.7)' // red
                }
            },
            'userareas': {
                'Polygon': {
                    'fill-color': 'rgba(255,0,0,.1)', // red
                    'fill-outline-color': 'rgba(255,0,0,.7)' // red
                }
            },
            'hulls': {
                'Polygon': {
                    'fill-color': 'rgba(255,0,0,.1)', // red
                    'fill-outline-color': 'rgba(0,128,0,.8)', // green
                }
            }
        }

        this._style = JSON.parse(JSON.stringify(paintOptions[paintOption || 'standard'])); // Clone `standard` by default

        this._map = mapInstance;
        this._highlighter = ['case', ['boolean', ['feature-state', 'highlight'], false]]
        this._layerIDs = [];
        this._sourceIDs = new Set();
        this._source = source_id || dc_id; // Use `dc_id` if `source_id` is not given

        if (this._source?.metadata?.layers) { // mapdata source
            const {ds_type, id, layers} = this._source.metadata;
            const sourcePrefix = `${ds_type}_${id}`;
            const ignoreAttrs = ['circle-radius', 'circle-blur', 'circle-stroke-width', 'fill-antialias',
                'line-blur', 'line-width', 'line-dasharray',
                'heatmap-weight', 'heatmap-intensity', 'heatmap-radius', 'heatmap-opacity', 'heatmap-color'
            ];
            console.debug(`Ignoring attributes:`, ignoreAttrs);

            Object.entries(this._style).forEach(([geometryType, paintStyle]) => {
                layers.forEach(layerName => {
                    const typeLower = geometryType.toLowerCase();
                    if (!typeLower.startsWith(layerName)) return;
                    if (relation_colors && typeLower === 'point-heatmap') return;

                    const layerID = `${sourcePrefix}_${typeLower}`;

                    // Clone paintStyle because it is modified in place
                    const paint = Object.fromEntries(
                        Object.entries(paintStyle).map(([attr, val]) => [
                            attr,
                            (!paintOption || paintOption === 'standard') && !ignoreAttrs.includes(attr)
                                ? [...this._highlighter, ...val]
                                : val
                        ])
                    );

                    const layer = {
                        id: layerID,
                        type: Object.keys(paint)[0].split('-')[0], // heatmap|fill|line|circle
                        source: `${sourcePrefix}_${layerName}`,
                        paint,
                    };

                    if (typeLower === 'point-heatmap') {
                        layer['minzoom'] = 0;
                        layer['maxzoom'] = 5;
                    }
                    else if (!relation_colors && typeLower === 'point') {
                        layer['minzoom'] = 5;
                    }

                    // Apply initial temporal filter if one exists before adding layer
                    if (this.temporalFilter) {
                        layer['filter'] = this.temporalFilter;
                    }

                    console.debug(`Adding layer "${layerName}" to map...`, layer);

                    mapInstance.addLayer(layer);
                    this._layerIDs.push(layerID);
                    this._sourceIDs.add(`${sourcePrefix}_${layerName}`);

                });
            });
            console.debug(`Layer IDs: ${this._layerIDs}`);
        } else {
            let source = this._map.getSource(this._source);
            this._sourceLayer = (!!source.type && source.type == 'vector') ? 'features' : '';

            Object.keys(this._style).forEach((geometryType) => {
                let paintGeometryStyle = this._style[geometryType];
                const layerID = `${this._source}_${geometryType.toLowerCase()}`;

                Object.keys(paintGeometryStyle).forEach((attribute) => {
                    if ((!paintOption || paintOption == 'plain') && !['circle-radius', 'fill-antialias', 'line-width', 'line-dasharray'].includes(attribute)) {
                        paintGeometryStyle[attribute] = [...this._highlighter, ...paintGeometryStyle[attribute]];
                    }
                });

                const layer = {
                    'id': layerID,
                    'type': Object.keys(paintGeometryStyle)[0].split('-')[0], // fill|line|circle
                    'source': this._source,
                    'source-layer': this._sourceLayer,
                    'paint': paintGeometryStyle,
                    'filter': ['==', '$type', geometryType.split('-')[0]],
                };
                mapInstance.addLayer(layer);
                this._layerIDs.push(layerID);
            });

            if (this.number) {
                const layerID = `${this._source}_numbers`;
                mapInstance.addLayer({
                    'id': layerID,
                    'type': 'symbol',
                    'source': this._source,
                    'layout': {
                        'text-field': ['to-string', ['id']], // Uses index `id` property from feature root, not `properties.id`
                        'text-size': 12,
                        'text-offset': [0, 0],
                        'text-anchor': 'center',
                    },
                    'paint': {
                        'text-color': 'white',
                    },
                });
                this._layerIDs.push(layerID);
            }
        }

        return this;
    }

    addFilter(filterOptions) {
        this._layerIDs.forEach((layerID) => {
            let filter = this._map.getFilter(layerID);
            filter = filter[0] == 'all' ? filter.push(filterOptions) : ['all', filter, filterOptions];
            this._map.setFilter(layerID, filter);
        });
    }

    setTemporalFilter(filter) {
        this.temporalFilter = filter;
        this.updateFilter();
    }

    setRelationFilter(filter) {
        this.relationFilter = filter;
        this.updateFilter();
    }

    setSearchFilter(filter) {
        this.searchFilter = filter;
        this.updateFilter();
    }

    getCombinedFilter() {
        let combinedFilter = [];

        if (this.temporalFilter) {
            combinedFilter.push(this.temporalFilter);
        }

        if (this.relationFilter) {
            combinedFilter.push(this.relationFilter);
        }

        if (this.searchFilter) {
            combinedFilter.push(this.searchFilter);
        }

        return combinedFilter.length > 0 ? ['all', ...combinedFilter] : null;
    }

    updateFilter() {
        const filter = this.getCombinedFilter();
        this._layerIDs.forEach((layerID) => {
            this._map.setFilter(layerID, filter);
        });
    }

    onAdd() {
    }

    onRemove() {
        this._layerIDs.forEach((layerID) => {
            mapInstance.removeLayer(layerID);
        });
    }

    toggleVisibility(show) {
        // If `show` parameter is provided, use it to set visibility, otherwise toggle present state
        const visibility = typeof show === 'boolean' ? (show ? 'visible' : 'none') : null;
        this._layerIDs.forEach((layerID) => {
            const layer = this._map.getLayer(layerID);
            if (layer) {
                const currentVisibility = this._map.getLayoutProperty(layerID, 'visibility');
                const newVisibility = visibility !== null ? visibility : (currentVisibility === 'visible' ? 'none' : 'visible');
                this._map.setLayoutProperty(layerID, 'visibility', newVisibility);
            }
        });
    }

}

export default Layerset;