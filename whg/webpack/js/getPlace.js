import { minmaxer } from './utilities';
import debounce from 'lodash/debounce';
import './toggle-truncate.js';
import './enlarge.js';

export function popupFeatureHTML(feature, clickable=true) { // TODO: Improve styling with css and content?
	let HTML = '<b>' + feature.properties.title + '</b><br/>' +
    'Temporality: ' + (feature.properties.min ? feature.properties.min : '?') + '/' + (feature.properties.max ? feature.properties.max : '?') +
    (clickable ? '<br/>Click to focus' : '');
    // Community record corrections (plan-record-suggestions): a "Suggest a correction" affordance in the
    // dataset-places detail popup, for beta users (wb-suggest.js delegated handler opens the editor).
    const pid = feature.properties && feature.properties.pid;
    if (pid && window.WHGSuggest && window.WHGSuggest.canSuggest) {
        HTML += '<br/>' + window.WHGSuggest.buttonHTML(pid, 'Suggest a correction');
    }
    return (HTML);
}

export const getPlace = debounce(getPlaceBouncing, 300);
export function getPlaceBouncing(pid, cid, callback) {
    const cidQueryParam = Number.isInteger(cid) ? `?cid=${cid}` : '';
    $.ajax({
        url: `/api/place/${pid}${cidQueryParam}`,
    }).done(data => {
        if (cidQueryParam === '') {
            parsePlace(data);
        } else {
            window.payload = data;
            parseAnno(data);
        }
        $('#detail .toggle-truncate').toggleTruncate();
        $('#detail img').enlarge();
        if (typeof callback === 'function') {
            callback(data);
        }
    });
}

function parsePlace(data) {
	window.featdata = data
	var descrip = '';

	if (!!window.datacollection.metadata.ds_type && window.datacollection.metadata.ds_type === 'collections'/* && !!data.datasets && data.datasets.length > 0*/) {
		const dataset_links = data.datasets.map(ds => `<a href="/datasets/${ds.id}/places" target="_blank" data-bs-toggle="tooltip" data-bs-title="View <i>${ds.title}</i> in a new tab.">${ds.title} <i class="fas fa-external-link-alt linky"></i></a>`).join('; ')
		descrip += `<p class="mb-0"><b>Dataset${data.datasets.length == 1 ? '' : 's'}</b>: ${dataset_links}.</p>`;
	}

	if (!!data.names && data.names.length > 0) {
	    const nameVariants = data.names
		    .filter(name => !!name.toponym && name.toponym.trim() !== '')
	    	.map(name => {
	        let nameHtml = name.toponym;
	        if (name.citations && name.citations.length > 0) {
	            const citation = name.citations[0]; // Assuming only one citation is considered
	            const label = Array.isArray(citation.label || '') ? citation.label[0] || '' : citation.label || ''; // Filter in case of array
	            const id = Array.isArray(citation.id || '') ? citation.id[0] || '' : citation.id || ''; // Filter in case of array
					if (id && (id.startsWith("http://") || id.startsWith("https://")) && (() => { try { new URL(id); return true; } catch { return false; } })() && label !== '') { // Check validity of id as a URL
							// If both id and label exist, wrap the name in a link with tooltip
							nameHtml = `<a href="${id}" target="_blank" data-bs-toggle="tooltip" title="${label}">${nameHtml} <i class="fas fa-external-link-alt linky"></i></a>`;
					} else {
							// If only label exists, show tooltip without link
							nameHtml = `<span data-bs-toggle="tooltip" title="${label}" class="pointer">${nameHtml}</span>`;
					}
	        }
	        return nameHtml;
	    	})
	    	.join('; ');
	
	    if (nameVariants !== '') descrip += `<p><b>Variant${data.names.length == 1 ? '' : 's'}</b>: <span class="toggle-truncate">${nameVariants}</span>.</p>`;
	}

	if (!!data.types && data.types.length > 0) {
			const types = data.types
					.filter(type => !!type.label && type.label.trim() !== '')
					.map(type => {
							let sourceLabels = '';
							if (type.sourceLabel) {
									sourceLabels = type.sourceLabel;
							} else if (type.sourceLabels) {
									sourceLabels = type.sourceLabels.map(sourceLabel => sourceLabel.label).join(' | ');
							}
							return `<${!!type.url ? `a href="${type.url}" target="_blank"` : `span`} data-bs-toggle="tooltip" title="<b>${type.identifier}</b><p>${sourceLabels}</p>">${type.label}${!!type.url ? ` <i class="fas fa-external-link-alt linky"></i>` : ``}</${!!type.url ? `a` : `span`}>`;
					})
					.join('; ');
			if (types !== '') descrip += `<p class="mb-0"><b>Type${data.types.length == 1 ? '' : 's'}</b>: ${types}.</p>`;
	}
	/* sometimes there is neither sourceLabel nor sourceLabels
	if (!!data.types && data.types.length > 0) {
			// may include sourceLabel:"" **OR** sourceLabels[{"label":"","lang":""},...]
			// or neither
	    const types = data.types
		    .filter(type => !!type.label && type.label.trim() !== '')
		    .map(type => {
		        const sourceLabels = type.sourceLabel || type.sourceLabels.map(sourceLabel => sourceLabel.label).join(' | ');
		        return `<${!!type.url ? `a href="${type.url}" target="_blank"` : `span`} data-bs-toggle="tooltip" title="<b>${type.identifier}</b><p>${sourceLabels}</p>">${type.label}${!!type.url ? ` <i class="fas fa-external-link-alt linky"></i>` : ``}</${!!type.url ? `a` : `span`}>`;
		    })
		    .join('; ');
	    if (types !== '') descrip += `<p class="mb-0"><b>Type${data.types.length == 1 ? '' : 's'}</b>: ${types}.</p>`;
	}
	*/

    if (!!data.fclasses && data.fclasses.length > 0) {
        const fclassCodes = data.fclasses.map(fclass => {
            return `<span class="pointer" data-bs-toggle="tooltip" title="${fclass.description}">${fclass.code}</span>`;
        }).join(', ');
        if (fclassCodes !== '') descrip += `<p class="mb-0"><b>Feature Class${data.fclasses.length == 1 ? '' : 'es'}</b>: ${fclassCodes}.</p>`;
    }
	
	function build_links(name, links_array, link_text=false) {
		if (links_array.length == 0) return '';
		const links = links_array
		    .map(link => {
		        const timespan = (!!link.when && !!link.when.timespans) 
		            ? ` (${minmaxer(link.when.timespans)})` 
		            : (!!link.when && !link.when.timespans) 
		            ? ` (${link.when.start.in}-${link.when.end.in})` 
		            : '';
		        return (link_text ? `${link.value}${!!link.url ? ' ' : ''}` : '') +
		            (link_text && !link.url ? '' :
		                (`<${!!link.url ? `a href="${link.url}" target="_blank"` : `span`}${link_text ? ' class="pointer small red-bold"' : ''}>` +
		                `${link_text || link.identifier || link.label}${timespan}${!!link.url ? ` <i class="fas fa-external-link-alt linky"></i>` : ``}` +
		                `</${!!link.url ? `a` : `span`}>`)
		            );
		    })
		    .join('; ');
		return links == '' ? '' : `<p class="mb-0"><b>${name}${links_array.length == 1 || name == 'Related' ? '' : 's'}</b>: <span class="toggle-truncate">${links}</span></p>`;
	}

	if (!!data.links && data.links.length > 0) {
	    descrip += build_links('Link', data.links.filter(link => !!link.identifier && link.identifier.trim() !== ''));	    
	}

	if (!!data.related && data.related.length > 0) {
	    descrip += build_links('Parent', data.related.filter(relative => !!relative.relation_type && relative.relation_type == 'gvp:broaderPartitive'));
	    descrip += build_links('Related', data.related.filter(relative => !!relative.relation_type && relative.relation_type !== 'gvp:broaderPartitive'));
	}

	if (!!data.descriptions && data.descriptions.length > 0) {
	    descrip += build_links('Description', data.descriptions.filter(link => !!link.value && link.value.trim() !== ''), 'Link');	    
	}

	if (!!data.countries && data.countries.length > 0) {
	    descrip += '<p><b>Modern country bounds</b>: ' + data.countries.map(country => `<span class="pointer" data-bs-toggle="tooltip" title="${country.label || ''}">${country.ccode}</span>`).join(', ') + '</p>';
	}

	if (!!data.minmax && data.minmax.length == 2 && (data.minmax[0] || data.minmax[1]) ) {
		descrip += `<p><b>When</b>: earliest: ${data.minmax[0] ? data.minmax[0] : '?'}; latest: ${data.minmax[1] ? data.minmax[1] : '?'}</p>`;
	}

	if (!!data.attestation_years && data.attestation_years.length > 0) {
	    descrip += `<p><b>Source year${data.attestation_years.length == 1 ? '' : 's'}</b>: ` + data.attestation_years.join(', ') + '</p>';
	}
	
	$("#detail").html(`<div><p><b>Title</b>: <a href="/places/portal/${data.id}" target="_blank" data-bs-toggle="tooltip" title="View WHG record(s) for this place."><span id="row_title" class="larger text-danger">${data.title} <i class="fas fa-external-link-alt linky"></i></span></a></p>${descrip}</div>`);
}

// Traces (annotations)
function parseAnno(data) {
    
	var $descrip = $("<div></div>");
	$("#detail").empty().append($descrip);
	
	if (!!data.traces && data.traces.length > 0) {
		data.traces.forEach(trace => {
			const t = trace.fields;
			$descrip.append(`
				<p><b>Title</b>: 
					<a href="${t.include_matches ? `/places/portal/${t.place}` : `/places/${t.place}/detail`}" target="_blank" data-bs-toggle="tooltip" title="View ${t.include_matches ? "WHG record(s)" : "details"} for this place.">
						<span id="row_title" class="larger text-danger">${data.title} <i class="fas fa-external-link-alt linky"></i></span>
					</a>
				</p>`);
			if (t.relation == '[""]' && t.note == null && t.start == null) {
				$descrip.append('<i>none yet</i>');
			} else {
				if (t.relation) {
					$descrip.append(`<p class="mb-0"><b>Relation</b>: ${JSON.parse(t.relation)[0]}</p>`);
				}
				if (t.note) {
					$descrip.append(`<p class="mb-0"><b>Notes</b>: <span class="toggle-truncate">${t.note}</span></p>`);
				}
				if (t.start) {
					$descrip.append(`<p class="mb-0"><b>Begin/End</b>: ${(t.start ?? "")}/${(t.end ?? '')}</p>`);
				}
			}
			let imgpath = t.image_file
			if (imgpath != "") { // display annotation image if any
				$("#detail").prepend(`<img src="/media/${imgpath}" class="thumbnail">`);
			}
		});
	}
	else {
		console.log('no trace for selected place');		
	}
}
