// /whg/webpack/places.js

import datasetLayers from './mapLayerStyles';
import { attributionString, geomsGeoJSON } from './utilities';
import { bbox } from './6.5.0_turf.min.js';
import { CustomAttributionControl } from './customMapControls';

import '../css/places.css';

let style_code;
if (mapParameters.styleFilter.length == 0) {
	style_code = ['DATAVIZ', 'DEFAULT']
} else {
	style_code = mapParameters.styleFilter[0].split(".");
}

maptilersdk.config.apiKey = mapParameters.mapTilerKey;
let mappy = new maptilersdk.Map({
	container: mapParameters.container,
	center: mapParameters.center,
	zoom: mapParameters.zoom,
	minZoom: mapParameters.minZoom,
	maxZoom: mapParameters.maxZoom,
	style: maptilersdk.MapStyle[style_code[0]][style_code[1]],
	attributionControl: false,
	geolocateControl: false,
	navigationControl: true,
	userProperties: true
});

function waitMapLoad() {
    return new Promise((resolve) => {
        mappy.on('load', () => {
            console.log('Map loaded.');
            
            const pgeom = JSON.parse($('#pgeom script').text());
            const featureCollection = geomsGeoJSON([{geom: pgeom}]);
            console.log(featureCollection);
		    mappy.addSource('places', {
				'type': 'geojson',
				'data': featureCollection,
				'attribution': attributionString(),
			});
			mappy.setFeatureState({ source: 'places', id: 0 }, { highlight: true });
		    datasetLayers.forEach(function(layer) {
				mappy.addLayer(layer);
			});
			mappy.fitBounds(bbox(featureCollection), {
		        padding: 30,
		        duration: 1000,
		    });
			
			mappy.addControl(new CustomAttributionControl({
				compact: true,
		    	autoClose: mapParameters.controls.attribution.open === false,
			}), 'bottom-right');
            
            resolve();
        });
    });
}

function waitDocumentReady() {
    return new Promise((resolve) => {
        $(document).ready(() => resolve());
    });
}


Promise.all([waitMapLoad(), waitDocumentReady()])
    .then(() => {

		//area_type = 'ccodes' // default
		$(".textarea").each(function(index) {
			if (["None", "null"].includes($(this).val())) {
				$(this).val('')
			}
		});
		$("#id_geojson").attr("placeholder", "generated from country codes")
	
		$(".a_more").click(function(e) {
			clicked = $(this)
			clicked.hide()
			clicked.parent().find("#dots").hide()
			clicked.next().removeClass('hidden')
			//clicked.css("display","contents")
			// $(this).hide()
			console.log('clicked', clicked)
			//clicked.removeClass('hidden')
		})
	
		// temporal data from template json_script
		window.pminmax = $('script').filter(function() {
			return this.id == '{{ object.id }}' &&
				this.text &&
				this.text.includes('mm');
		});
		window.pts = $('script').filter(function() {
			return this.id == '{{ object.id }}' &&
				this.text &&
				this.text.includes('ts');
		});
	
		// don't try to make a histogram if no temporal data
		if (pminmax.length > 0 && pts.length > 0) {
			allts = JSON.parse(pts[0].text)['ts']
			minmax = JSON.parse(pminmax[0].text)['mm']
			console.log('allts', allts)
			console.log('minmax', minmax)
	
			// feed to tvis_summary()
			histogram_data(allts, minmax)
		} else {
			$("#place_temporal").hide()
		}

		$(".ds_card").on('click', function(e) {
			// set all layers to default style
			for (i in idToFeature) {
				idToFeature[i].setStyle(styles['Polygon']['default'])
			}
			dsid = $(this).data('id')
			l = idToFeature[dsid]
			// raise z-index
			l.bringToFront().setStyle(styles['Polygon']['focus'])
			// get a centroid
			mappy.fitBounds(l.getBounds().pad(0.5))
		})
		
		$(".ds_card").hover(function() {
				//console.log($(this))
				let id = $(this).data('id')
				feat = idToFeature[id]
				//console.log('feat',feat)
				ogfill = "#ff9999"
				feat.setStyle({
					fillColor: 'yellow',
					color: 'red',
					fillOpacity: 0.3
				})
			},
			function() {
				let id = $(this).data('id')
				feat = idToFeature[id]
				feat.setStyle({
					fillColor: ogfill,
					color: '#333',
					fillOpacity: 0.3
				})
			}
		);
		
		$("#b_download").click(function() {
			var dsids = [];
			$('.modal-body input:checked').each(function() {
				dsids.push($(this).val());
			});
			console.log('download datasets:', dsids)
		})
		
		$(".btn-cancel").click(function() {
			$("#downloadModal").modal('hide')
		})
		
		$(".dl-links a").click(function(e) {
			//e.preventDefault()
			urly = '/datasets/' + $(this).data('id') + '/augmented/' + $(this).attr('ref')
			console.log('urly', urly)
			startDownloadSpinner()
			$.ajax({
				type: 'GET',
				url: urly
			}).done(function() {
				spinner_dl.stop();
			})
		})

		$("[rel='tooltip']").tooltip();

		var clip_geom = new ClipboardJS('#a_clipgeom');
		clip_geom.on('success', function(e) {
			e.clearSelection();
			$("#a_clipgeom").tooltip('hide')
				.attr('data-original-title', 'copied!')
				.attr('data-bs-original-title', 'copied!')
				.tooltip('show');
		});
 
    })
    .catch(error => console.error("An error occurred:", error));

// helpers for histogram_data()
function range(start, stop, step) {
	var a = [start],
		b = start;
	while (b < stop) {
		a.push(b += step || 1);
	}
	return a;
}

function intersects(a, b) {
	min = (a[0] < b[0] ? a : b)
	max = (min == a ? b : a)
	return !(min[1] < max[1])
}

// generate temporal data -> histogram()
function histogram_data(intervals, minmax) {
	step = Number(((minmax[1] - minmax[0]) / 200))
	bins = range(minmax[0], minmax[1], step)
	hist_array = Array.apply(null, Array(bins.length)).map(Number.prototype.valueOf, 0);
	labels = bins.map(function(d) {
		return Math.round(d)
	})
	for (b = 0; b < bins.length; b++) {
		bin = Array(bins[b], bins[b + 1])
		for (i in intervals) {
			if (intersects(bin, intervals[i])) {
				hist_array[b] += 1
			}
		}
	}
	data = hist_array.map(function(d, i) {
		return {
			"bin": labels[i],
			"count": d
		}
	})

	// visualize it
	histogram(data, labels)

}

function histogram(data, labels) {
	// exit if no data 
	if (data[0].bin == "Infinity") {
		$("#histogram").html('<i>None yet</i>');
		return;
	}
	data = data.slice(0, 200)
	curwidth = $("#histogram").width()

	var margin = {
			top: 0,
			right: 10,
			bottom: 0,
			left: 10
		},
		width = 400,
		height = 30,
		padding_h = 20,
		padding_w = 30;

	// set the ranges
	window.xScale = d3.scaleLinear()
		.range([0, width])
	window.yScale = d3.scaleLinear()
		.range([height, 0]);

	xScale.domain(minmax);
	yScale.domain([0, d3.max(data, function(d) {
		return d.count;
	})]);

	// TODO: responsive scaling of svg width
	window.svg_hist = d3.select("#histogram").append("svg")
		.attr("width", '100%')
		.attr("height", height + padding_h)

		.attr('viewBox', '0 0 ' + Math.min(width, height + padding_h) + ' ' + Math.min(width, height + padding_h))
		.attr('preserveAspectRatio', 'xMinYMin')

		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")")

	window.axisB = d3.axisBottom(xScale)
		.tickValues(labels.filter(function(d, i) {
			return !(i % 20)
		}))
		.tickFormat(d3.format("d"));

	var axisL = d3.axisLeft(yScale)

	svg_hist.selectAll(".bar")
		.data(data)
		.enter().append("rect")
		.attr("class", "bar")
		.attr("x", function(d) {
			return xScale(d.bin);
		})
		//.attr("width", function(d) { return xScale(d.x1) - xScale(d.x0) -1 ; })
		.attr("width", 2)
		.attr("y", function(d) {
			return yScale(d.count);
		})
		.attr("height", function(d) {
			return height - yScale(d.count);
		});

	var xAxis = svg_hist.append("g")
		.attr("id", "xaxis")
		.attr("transform", "translate(0," + height + ")")
		.call(axisB)
}

function startDownloadSpinner() {
	window.spinner_dl = new Spin.Spinner().spin();
	$("#ds_cards").append(spinner_dl.el);
}
