// /whg/webpack/js/mapControls.js

import Dateline from './dateline';
//import generateMapImage from './saveMapImage';
import throttle from 'lodash/throttle';
import { table } from './tableFunctions';
import { scrollToRowByProperty } from './tableFunctions-extended';

class sequencerControl {
	onAdd(map) {
        this._map = map;
        this.minSeq = 0;
        this.maxSeq = window.datacollection.metadata.num_places - 1;
        console.log(`Sequence range (${this.minSeq}-${this.maxSeq}).`);

        // Always create a container
        this._container = document.createElement('div');
        this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group sequencer';

        // If no sequence possible, just hide control and return container
        if (this.minSeq === this.maxSeq) {
            this._container.style.display = 'none';
            return this._container; // ✅ must still return a Node
        }

        this._container.textContent = 'Explore sequence';
        this._container.innerHTML = '';
        this.currentSeq = this.minSeq;
        this.playing = false;
        this.stepdelay = 3;
        this.playInterval = null;
        this.sortedPIDs = [];

        this.buttons = [
			['skip-first','First waypoint','Already at first waypoint','Disabled during play'],
			['skip-previous','Previous waypoint','Already at first waypoint','Disabled during play'],
			['skip-next','Next waypoint','Already at last waypoint','Disabled during play'],
			['skip-last','Last waypoint','Already at last waypoint','Disabled during play'],
			'separator',
			['play','Play from current waypoint: hold to change speed','Cannot play from last waypoint','Stop playing waypoints']
		];

		this.buttons.forEach((button) => {
			this._container.innerHTML += button == 'separator' ? '<span class="separator"/>' : `<button id = "${button[0]}" type="button" style="background-image: url(/static/images/sequencer/${button[0]}-btn.svg)" ${['skip-first', 'skip-previous'].includes(button[0]) ? 'disabled ' : ''}aria-label="${button[1]}" title="${button[1]}" />`
		});

		let longClickTimeout;
		let initialisingSlider = false;
		$('body').on('mousedown', '.sequencer:not(.playing) button#play', () => {
		  createSelect.call(this);
		  longClickTimeout = setTimeout(() => {
		      $('#stepDelayDropbox').show();
		      initialisingSlider = true;
		  }, 1000);
		});

		$('body').on('mouseup','.sequencer button', (e) => {
		    const sequencer = $('.sequencer');
		    const action = $(e.target).attr('id');

		    if (table.search() !== '') { // Clear any table search filter
				table.search('').draw();
			}

		    console.log(`Sequencer action: ${action} from ${this.currentSeq}.`);

			if (window.highlightedFeatureIndex == undefined) {
				if (['skip-previous', 'skip-next'].includes(action)) { // Highlight feature selected in table
					$('#placetable tr.highlight-row').click();
					return;
				}
				else if (action == 'play' && !initialisingSlider) {
					this.currentSeq = Math.max(this.minSeq, this.currentSeq) -1; // Play will commence by re-adding 1
				}
			}

			if (action=='play') {

		  		clearTimeout(longClickTimeout);
				if (initialisingSlider) {
		  			initialisingSlider = false;
		  			return;
				}
				else {
					$('#stepDelayDropbox').hide();
				    if (!this.playing) {
				        sequencer.addClass('playing');
				        this.startPlayback();
				    } else {
						this.stopPlayback();
				        return;
				    }
				}
			}
			else {
				if (action=='skip-first') {
					this.currentSeq = this.minSeq;
				}
				else if (action=='skip-previous') {
					this.currentSeq -= 1;
				}
				else if (action=='skip-next') {
					this.currentSeq += 1;
				}
				else if (action=='skip-last') {
					this.currentSeq = this.maxSeq;
				}

				scrollToRowByProperty(table, 'pid', this.sortedPIDs[this.currentSeq]);
			}

			if (this.playing && this.currentSeq == this.maxSeq) {
				this.stopPlayback();
		        return;
		    }
			this.updateButtons();

		});

		function createSelect() {
		  if ($('#stepDelayDropbox').length === 0) {
		    const $dropboxContainer = $('<div id="stepDelayDropbox" class="sequencer"></div>');
		    $(this._container).append($dropboxContainer);
		    const $select = $('<select title="Set delay between waypoints" aria-label="Set delay between waypoints"></select>');
		    $dropboxContainer.append($select);
		    for (let i = 1; i <= 20; i++) {
		      const $option = $(`<option value="${i}">${i}s</option>`);
		      $select.append($option);
		    }
		    $select.val(this.stepdelay);
		    $select.on('change', (event) => {
		      const newValue = parseInt(event.target.value);
		      this.stepdelay = newValue;
		    });
		  }
		}

		return this._container;
	}

	updateButtons() {
		const sequencer = $('.sequencer');
		
		const highlightedPid = $('#placetable tr.highlight-row').attr('pid');
		this.currentSeq = this.sortedPIDs.indexOf(parseInt(highlightedPid));
		
        if (!this.playing) {
            sequencer.find('button').prop('disabled', false);
            if (this.currentSeq == this.minSeq) {
			    sequencer.find('button#skip-first,button#skip-previous').prop('disabled', true);
			}
			else if (this.currentSeq == this.maxSeq) {
			    sequencer.find('button#skip-last,button#skip-next,button#play').prop('disabled', true);
			}
        } else {
            sequencer.find('button:not(#play)').prop('disabled', true);
        }
        sequencer.find('button').each((i, button) => {
			button.setAttribute('title', this.buttons[i + (i > 3 ? 1 : 0)][button.disabled ? (this.playing ? 3 : 2) : (this.playing && i == 4 ? 3 : 1)]);
			button.setAttribute('aria-label', button.getAttribute('title'));
		});
	}

	clickNext() {
		this.currentSeq += 1;
		this.continuePlay = true;
		console.log(`Sequencer action: play ${this.currentSeq}.`);
		scrollToRowByProperty(table, 'pid', this.sortedPIDs[this.currentSeq]); // Triggers updateButtons()
		if (this.currentSeq == this.maxSeq) {
			this.stopPlayback();
		}
	}

	startPlayback() {
		console.log('Starting sequence play...');
		this.playing = true;
		$('.sequencer').addClass('playing');
		this.clickNext();
        if (this.currentSeq < this.maxSeq) {
			this.playInterval = setInterval(() => {
				this.clickNext();
	        }, this.stepdelay * 1000);
		}
    }

    stopPlayback() {
        clearInterval(this.playInterval);
        this.playInterval = null;
		this.playing = false;
		$('.sequencer').removeClass('playing');
        this.updateButtons();
		console.log('... stopped sequence play.', this);
    }
    
    toggle(show) {
        this.stopPlayback();
        if (show === undefined) {
            this._container.style.display = this._container.style.display === 'none' ? 'flex' : 'none';
        } else {
            if (!show) {
                this._container.style.display = 'none';
            } else {
                this._container.style.display = 'flex';
            }
        }
      	if (this._container.style.display === 'flex') {
			// Update sortedPIDs to match current table sort order
			mapSequencer.sortedPIDs = table.rows({ order: 'current' }).data().map(rowData => rowData.properties.pid);
		}
    }
}

let mapSequencer;
function init_mapControls(whg_map, datelineContainer, toggleFilters, mapParameters, table){

	if (!!mapParameters.controls && !!mapParameters.controls.sequencer && mapParameters.controls.sequencer) {
		mapSequencer = new sequencerControl();
		whg_map.addControl(mapSequencer, 'bottom-left');
	}
			
	const dateRangeChanged = throttle(() => { // Uses imported lodash function
	    toggleFilters(true, whg_map, table);
	}, 300);

	if (window.dateline) {
		window.dateline.destroy();
		window.dateline = null;
	}
	if (datelineContainer) {
		datelineContainer.remove();
		datelineContainer = null;
	}

	if (!!mapParameters.temporalControl && mapParameters.temporalControl) {
		datelineContainer = document.createElement('div');
		datelineContainer.id = 'dateline';
		$('.maplibregl-control-container').first()[0].appendChild(datelineContainer);

		const range = window.datacollection.metadata.max - window.datacollection.metadata.min;
		const buffer = range * 0.1; // 10% buffer

		// Update the temporal settings
		mapParameters.temporalControl.fromValue = window.datacollection.metadata.min;
		mapParameters.temporalControl.toValue = window.datacollection.metadata.max;
		mapParameters.temporalControl.minValue = Math.floor(window.datacollection.metadata.min - buffer);
		mapParameters.temporalControl.maxValue = Math.ceil(window.datacollection.metadata.max + buffer);

		window.dateline = new Dateline({
			...mapParameters.temporalControl,
			onChange: dateRangeChanged
		});
	};

	document.addEventListener('click', function(event) {

        if (event.target && event.target.parentNode) {
			const parentNodeClassList = event.target.parentNode.classList;
			if (parentNodeClassList) {
				if (parentNodeClassList.contains('maplibregl-ctrl-fullscreen')) {
					$('#mapOverlays').addClass('fullscreen');
				}
				else if (parentNodeClassList.contains('maplibregl-ctrl-shrink')) {
					$('#mapOverlays').removeClass('fullscreen');
				}
				else if (parentNodeClassList.contains('dateline-button')) {
		            toggleFilters($('.range_container.expanded').length > 0, whg_map, table);
		        }
			}
		}

	});

	return { datelineContainer, mapParameters, mapSequencer }

}

export { init_mapControls, mapSequencer };
