import VisualisationControl from './visualisationControl.js';
import './enlarge.js';
import {minmaxer, showChooser} from './utilities';
import wireCollectionLicense from './collectionLicenseControl.js';

import '../css/builders-collection-place.css';

$(function () {
    wireCollectionLicense();
    $(".col-place-card").first().click();
    new VisualisationControl(); // Returns `null` if #configurationTable element does not exist

    $("#id_group option:first").text('None')

    $("#id_group").change(function () {
        if ($("#id_group").val() == "") {
            $("#btn_coll_update").attr('value', 'Withdraw')
            $("#submit_prompt").fadeOut()
        } else {
            $("#btn_coll_update").attr('value', 'Submit')
        }
        console.log('changed group setting')
    })

    $("#coll_placelist div:first-child").click()
    var cardList = $('#coll_placelist');
    /*var panelList = $('#draggablePanelList');*/
    cardList.sortable({
        // Only make the .panel-heading child elements support dragging.
        // Omit this to make then entire <li>...</li> draggable.
        /*handle: '.row-handle',*/
        axis: 'y',
        update: function (event, ui) {
            /*console.log('$(this)', $(this))*/
            /*console.log('$(elem)', $(elem))*/
            let new_sequence = {}
            $('.col-place-card', cardList).each(function (index, elem) {
                let id = $(elem).data('id')
                new_sequence[id] = index
                $("#" + id + " .seq-visible").text("id: " + id + "; seq: " + index)
            });
            // Persist the new indices.
            let formData = new FormData()
            formData.append('coll_id', object_id)
            formData.append('seq', JSON.stringify(new_sequence))
            $.ajax({
                type: 'POST',
                url: '/collections/update_sequence/',
                headers: {'X-CSRFToken': csrfToken},
                processData: false,
                contentType: false,
                cache: false,
                data: formData,
                dataType: "json",
                async: false,
                success: function (result) {
                    console.log('result', result)
                }
            });
        }
    })
    // TODO: change text depending on state
    // scroll to place w/last saved annotation
    let offset = sessionStorage.getItem('offset')
    $("#coll_placelist").scrollTop(offset)

    window.remove_these = []

    // FIXED: Use Bootstrap's tab event instead of generic click handler
    // This ensures tabs are properly shown before we manipulate visibility
    $('#coll_tabs button[data-bs-toggle="tab"]').on('shown.bs.tab', function (e) {
        var tab = $(e.target).data('id')
        /*console.log(tab)*/
        if (tab == 'ds') {
            $("#coll_right_anno").addClass('hidden')
            /*$("#coll_right_intro").removeClass('hidden')*/
        } else {
            /*console.log('show anno header if there are place cards')*/
            if ($("#coll_placelist").children(".col-place-card").length > 0) {
                $("#coll_right_anno").removeClass('hidden')
            }
            /*$("#coll_right_intro").addClass('hidden')*/
        }
    })

    // Also handle the main tab list (for switching between form, builder, and collaborators)
    $('#tab-list button[data-bs-toggle="tab"]').on('shown.bs.tab', function (e) {
        var target = $(e.target).attr('data-bs-target')
        console.log('Main tab switched to:', target)
    })

    let dslist = []
    $(".textarea").each(function (index) {
        if (["None", "null"].includes($(this).val())) {
            $(this).val('')
        }
    });

    $("#id_geojson").attr("placeholder", "generated from country codes");

    $('#image_selector .thumbnail').enlarge();

    // ******************* LISTENERS *******************
    $(document).on('click', '#b_anno_submit', function (event) {
        event.preventDefault();
        const button = $(this);
        const formData = new FormData($('#anno_form')[0]);

        // Function to show success/failure tooltip
        function showTooltip(message) {
            // Dispose any existing tooltip
            let tooltip = bootstrap.Tooltip.getInstance(button[0]);
            if (tooltip) tooltip.dispose();

            // Set message and initialize tooltip
            button.attr('data-bs-title', message);
            tooltip = new bootstrap.Tooltip(button[0], {trigger: 'manual'});
            tooltip.show();

            // Automatically hide and dispose after 3s
            setTimeout(() => {
                tooltip.dispose();
                button.removeAttr('data-bs-title');
            }, 3000);
        }

        $.ajax({
            url: $('#anno_form').data('action'),
            type: 'POST',
            data: formData,
            processData: false,
            contentType: false,
            success: function (response) {
                showTooltip('Annotation saved successfully!');

                const placeId = $('#anno_form').find('input[name="place"]').val();
                const placeCard = $(`.col-place-card#${placeId}`);

                if (placeCard.length && !placeCard.find('p i.red-head').length) {
                    placeCard.find('p').append('<i class="red-head ms-3">annotated</i>');
                }
            },
            error: function (xhr) {
                showTooltip('An error occurred: ' + xhr.responseText);
            }
        });
    });

    $(document).on('click', '.js-show-chooser-trigger', function (e) {
        e.preventDefault();
        const type = $(this).data('chooser-target');
        showChooser(type);
    });

    $('#sharing_form').submit(function (event) {
        // Stop form from submitting normally
        event.preventDefault();
        console.log('sharing form submitted via ajax')

        var formData = new FormData(this);

        $.ajax({
            url: $(this).attr('action'),
            type: $(this).attr('method'),
            data: formData,
            processData: false,
            contentType: false,
            csrfmiddlewaretoken: csrfToken,
            success: function (response) {
                console.log('response', response)
                if (response.status == 'ok') {
                    var removalMarkup = '<span class="float-end me-2"><a id="remove_collab" data-uid="' +
                        response.uid + '" href="/collections/collab-remove/' + response.uid +
                        '/' + response.cid + '/"><i class="fas fa-times-circle linky"></i></a></span>';
                    $("#collabs_list").append(
                        '<li>' + response.user + removalMarkup + '</li>')
                    history.replaceState(null, null, '#coll_collaborators');
                    $('.nav-link[data-bs-target="#coll_collaborators"]').tab('show');
                    $('input[name="username"]').val('');
                } else {
                    alert(response.status)
                }
            },
            error: function (response) {
                alert('a problem occurred')
            }
        });
    });

    // remove collaborator from collection
    $(document).on('click', '#remove_collab', function (event) {
        event.preventDefault();
        console.log('remove collab')
        var uid = $(this).data('uid');
        var cid = '{{ object.id }}';

        $.ajax({
            url: '/collections/collab-remove/' + uid + '/' + cid + '/',
            type: 'POST',
            headers: {'X-CSRFToken': csrfToken},
            success: function (response) {
                console.log('response', response)
                $('#remove_collab[data-uid="' + response.uid + '"]').parent().parent().remove();
                // Instead of setting window.location.hash directly, use history.replaceState()
                history.replaceState(null, null, '#coll_collaborators');
                $('.nav-link[data-bs-target="#coll_collaborators"]').tab('show');

            },
            error: function (response) {
                // Handle error
                console.log('error', response)
            }
        });
    });

    $("#check_submitter").click(function (e) {
        $("#submitter").toggle()
    })

    $("#btn_coll_submit").click(function (e) {
        e.preventDefault()
        group_connect('submit')
    })
    $("#btn_coll_unsubmit").click(function (e) {
        e.preventDefault()
        group_connect('unsubmit')
    })

    $("#a_linkpopup").click(function () {
        $("#linkform_popup").fadeIn()
    })

    $("#b_createlink").click(function () {
        create_collection_link()
    })

    // do something with place name (modal popup?)
    $(".coll-place").click(function () {
        /*console.log('place record', $(this).data('pid'));*/
    })

    /*$(".closer").click(function(){*/
    /*		$(".pop").fadeOut()*/
    /* })*/

    $("#b_cancel_link").click(function () {
        $(".pop").fadeOut()
    })

    // remove place(s) from collection
    $("#a_remove").click(function () {
        var coll = object_id
        console.log('remove these; coll', remove_these, coll)
        remove_from_collection(coll, remove_these)
    })

    // mark/unmark place for removal from collection
    $(".mark-place").click(function (e) {
        let card;
        e.stopPropagation()
        var pid = $(this).data('pid')
        var col = $(this).data('col')
        /*console.log('mark ' + pid + ' in ' + col)*/
        if (remove_these.indexOf(pid) > -1) {
            console.log('unmarked', pid)
            // unmark it
            card = $(".col-place-card[data-id=" + pid + "]")
            card.css('opacity', 1.0)
            $(".col-place-card[data-id=" + pid + "] i.fa-plus-circle")
                .addClass('fa-minus-circle')
                .removeClass('fa-plus-circle')
                .css('color', 'lightcoral')
            $(card[0]).find('.restore-place').addClass('remove-place')
                .removeClass('restore-place')
            let idx = remove_these.indexOf(pid)
            remove_these.splice(idx, 1)
            $("#a_remove").html('remove ' + remove_these.length)
            if (remove_these.length == 0) {
                $("#a_remove").html('')
            }
        } else {
            // mark for removal
            console.log('marked', pid, 'added to remove_these')
            card = $(".col-place-card[data-id=" + pid + "]")
            card.css('opacity', 0.5)
            $(".col-place-card[data-id=" + pid + "] i.fa-minus-circle")
                .addClass('fa-plus-circle')
                .removeClass('fa-minus-circle')
                .css('color', 'green')
            $(card[0]).find('.remove-place').addClass('restore-place')
                .removeClass('remove-place')
            remove_these.push(pid)
            $("#a_remove").html('remove ' + remove_these.length)
        }
    })

    // show/hide editor-restricted options
    // $("#editor_options").hide();
    $('.show-hide').click(function (e) {
        $("#editor_options").slideToggle("fast");
        var val = $(this).html() == "↑" ? "↓" : "↑";
        /*var val = $(this).html() == "-" ? "+" : "-";*/
        $(this).hide().html(val).fadeIn("fast");
        /*$(this).hide().html(val).fadeIn("fast");*/
        e.preventDefault();
    });

    $(".col-place-card").click(function () {
        // clear highlights
        $(".col-place-card").removeClass('card-highlight')
        // current index
        /*console.log('idx', $(this).data('idx'))*/
        // get pid
        let pid = $(this).data('id')
        /*console.log('clicked $(this), pid', $(this), pid)*/
        let card = $(this).addClass('card-highlight')
        // position of clicked card
        let cardtop = card.offset().top
        sessionStorage.setItem('cardtop', cardtop)
        // position of list scroller
        let scrolltop = $("#coll_placelist").scrollTop()
        sessionStorage.setItem('scrolltop', scrolltop)
        // permanent position of list on page
        let listtop = $("#coll_placelist").offset().top
        let offset = cardtop + scrolltop - listtop
        sessionStorage.setItem('offset', offset)
        /*console.log('offset',offset)*/
        /*console.log('cardtop, scrolltop, listtop',*/
        /*	  cardtop, scrolltop, listtop )*/
        getAnno(pid)
    })

    // add dataset to collection
    $("#select_ds").change(function () {
        /*console.log('selected from list', $(this).val())*/
        $("further_explanation").hide()

        if ($("a.active").data('id') == 'pl' && place_list) {
            $("#col_right_anno").fadeIn()
        }
        addDataset('create')
    })

    $('.show-chooser').click(function (event) {
        event.preventDefault();
        $(this).parent().find('.chooser').show();
    });

    // ******************* FUNCTIONS *******************

    // flag collection as submitted, shows up on leader's list
    function group_connect(action) {
        console.log('submitting collection {{ object.id }} to group ' + $("#id_group").val())
        var formData = new FormData()
        formData.append('action', action)
        formData.append('coll', '{{ object.id }}')
        formData.append('group', $("#id_group").val())
        $.ajax({
            type: 'POST',
            enctype: 'multipart/form-data',
            url: '{% url "collection:group-connect" %}',
            headers: {'X-CSRFToken': csrfToken},
            processData: false,
            contentType: false,
            cache: false,
            data: formData,
            success: function (response) {
                if (response.status == 'ok') {
                    console.log(response)
                } else {
                    console.log(response)
                }
            }
        })
    }

    /*function goBrowse() {*/
    /*  window.location = '/collections/{{ object.id }}/browse_pl'*/

    /* }*/

    function saveExit() {
        $("#collection_form").submit()
    }

    // add link to collection
    function create_collection_link() {
        // console.log('create_collection_link', create_link_url)
        var formData = new FormData()
        formData.append('model', 'Collection')
        formData.append('objectid', object_id)
        formData.append('uri', $("#l_uri").val())
        formData.append('label', $("#l_label").val())
        formData.append('link_type', $("#select_linktype").val())
        formData.append('license', '')
        $.ajax({
            type: 'POST',
            enctype: 'multipart/form-data',
            // url: '{% url "create-link" %}',
            url: create_link_url,
            headers: {'X-CSRFToken': csrfToken},
            processData: false,
            contentType: false,
            cache: false,
            data: formData,
            success: function (response) {
                if (response.result == 'dupe') {
                    alert('That url is already linked to this collection!')
                } else if (response.result == 'bad uri') {
                    alert('That url is not formed correctly!')
                } else {
                    let linky = response.result
                    console.log(linky)
                    $("#linklist").append(linky.link_icon + ' <a href="' + linky.uri +
                        '" target="_blank">' + linky.label + '</a>')
                    $("#added_flash").fadeIn().delay(2000).fadeOut()
                    if ($("#linklist svg").length >= 2) {
                        $(".link-counter").html("(that's 3!)")
                    }
                    /*console.log('added! gimmee feedback', response.result)*/
                }
            }
        })
        /*$("#addtocoll").hide()*/
        $("#linkform_popup input").val('')
        $("#linkform_popup").hide()
    }

    // builds link for external place record
    function url_extplace(identifier) {
        // abbreviate links not in aliases.base_urls
        var link;
        if (identifier.startsWith('http')) {
            let tag = identifier.replace(/.+\/\/|www.|\..+/g, '')
            link = '<a href="' + identifier + '" target="_blank">' + tag + '<i class="fas fa-external-link-alt linky"></i>,  </a>'
        } else {
            link = '<a href="" class="ext" data-target="#ext_site">' + identifier + '<i class="fas fa-external-link-alt linky"></i></a>, '
        }
        return link
    }

    // builds link for external placetype record
    function url_exttype(type) {
        /*console.dir(type)*/
        var link = ' <a href="#" class="exttab" data-id=' + type.identifier +
            '>(' + type.label.label + ' <i class="fas fa-external-link-alt linky"></i>)</a>'
        return link
    }

    // return html for display
    function parsePlace(data) {
        var descrip;
        window.featdata = data
        let trace = JSON.parse(data.traces).find(function (t) {
            return t.fields.collection === object_id
        })
        if (trace) {
            /*console.log('has trace for this collection', trace.fields)*/
            $("#anno_detail").html(JSON.stringify(trace.fields))
        } else {
            /*console.log('no trace for this collection')*/
        }

        function onlyUnique(array) {
            const map = new Map();
            const result = [];
            for (const item of array) {
                if (!map.has(item.identifier)) {
                    map.set(item.identifier, true);
                    result.push({
                        identifier: item.identifier,
                        type: item.type,
                        aug: item.aug
                    });
                }
            }
            return (result)
        }

        // TITLE
        descrip = '<p><b>Title</b>: <span id="row_title" class="larger text-danger">' + data.title + '</span>'
        // NAME VARIANTS
        descrip += '<p class="scroll65"><b>Variants</b>: '
        for (let n in data.names) {
            let name = data.names[n]
            descrip += '<p>' + name.toponym != '' ? name.toponym + '; ' : ''
        }
        // TYPES
        // may include sourceLabel:"" **OR** sourceLabels[{"label":"","lang":""},...]
        // console.log('data.types',data.types)
        //{"label":"","identifier":"aat:___","sourceLabels":[{"label":" ","lang":"en"}]}
        descrip += '</p><p><b>Types</b>: '
        var typeids;
        for (let t in data.types) {
            let str = ''
            let type = data.types[t]
            /*console.log('type',type)*/
            if ('sourceLabels' in type) {
                let srclabels = type.sourceLabels
                for (let l in srclabels) {
                    label = srclabels[l]['label']
                    str = label != '' ? label + '; ' : ''
                }
            } else if ('sourceLabel' in type) {
                str = type.sourceLabel != '' ? type.sourceLabel + '; ' : ''
            }
            if (type.label != '') {
                str += url_exttype(type) + ' '
            }
            typeids += str
        }
        descrip += typeids.replace(/(; $)/g, "") + '</p>'

        // LINKS
        //
        descrip += '<p class="mb-0"><b>Links</b>: <i>original: </i>'
        let close_count = 0;
        let added_count = 0;
        let html_close = '';
        let html_added = '';

        if (data.links.length > 0) {
            let links = data.links
            let links_arr = onlyUnique(data.links)
            /*console.log('distinct data.links',links_arr)*/
            for (let l in links_arr) {
                //console.log('link',links_arr[l])
                if (links_arr[l].aug == true) {
                    added_count += 1
                    html_added += url_extplace(links_arr[l].identifier)
                } else {
                    close_count += 1
                    html_close += url_extplace(links_arr[l].identifier)
                }
            }
            descrip += close_count > 0 ? html_close : 'none; '
            descrip += added_count > 0 ? '<i>added</i>: ' + html_added : '<i>added</i>: none'
        } else {
            descrip += "<i class='small'>no links established yet</i>"
        }
        descrip += '</p>'

        // RELATED
        //right=''
        var related;
        var parent;
        if (data.related.length > 0) {
            parent = '<p class="mb-0"><b>Parent(s)</b>: ';
            related = '<p class="mb-0"><b>Related</b>: ';
            for (let r in data.related) {
                let rel = data.related[r]
                //console.log('rel',rel)
                if (rel.relation_type == 'gvp:broaderPartitive') {
                    parent += '<span class="h1em">' + rel.label
                    parent += 'when' in rel && !('timespans' in rel.when) ?
                        ', ' + rel.when.start.in + '-' + rel.when.end.in :
                        'when' in rel && ('timespans' in rel.when) ? ', ' +
                            minmaxer(rel.when.timespans) : ''
                    //rel.when.timespans : ''
                    parent += '</span>; '
                } else {
                    related += '<p class="small h1em">' + rel.label + ', ' + rel.when.start.in + '-' + rel.when.end.in + '</p>'
                }
            }
            descrip += parent.length > 39 ? parent : ''
            descrip += related.length > 37 ? related : ''
        }

        // DESCRIPTIONS
        // TODO: link description to identifier URI if present
        if (data.descriptions.length > 0) {
            let val = data.descriptions[0]['value'].substring(0, 300)
            descrip += '<p><b>Description</b>: ' + (val.startsWith('http') ? '<a href="' + val + '" target="_blank">Link</a>' : val) +
                ' ... </p>'
        }

        // CCODES
        //
        if (!!data.countries) {
            //console.log('data.countries',data.countries)
            descrip += '<p><b>Modern country bounds</b>: ' + data.countries.join(', ') + '</p>'
        }

        // MINMAX
        //
        var mm = data.minmax
        if (data.minmax && !(mm[0] == null && mm[1] == null)) {
            descrip += '<p><b>When</b>: earliest: ' + data.minmax[0] + '; latest: ' + data.minmax[1]
        }

        // TRACES
        trace = JSON.parse(data.traces).find(function (t) {
            return t.fields.collection = object_id
        })

        // if geom(s) and 'certainty', add it
        if (data.geoms.length > 0) {
            let cert = data.geoms[0].certainty
            if (cert != undefined) {
                descrip += '<p><b>Location certainty</b>: ' + cert + '</p>'
            }
        }
        descrip += '</div>'
        return descrip
    }

    function getAnno(pid) {
        //let coll_id = "{{ object.id }}"
        // traces.views.get_form()
        // returns TraceAnnotationForm; w/instance if trace_annotation record exists & not archived, else empty
        const url = "/collections/annoform/?c=" + object_id + "&p=" + pid
        fetch(url)
            .then(function (response) {
                return response.json();
            })
            .then(function (data) {
                const formContainer = document.querySelector("#annotator");
                formContainer.innerHTML = data.form;
                $("#keyword_color").html('#keyword_color')
            })
    }

    // render dataset to html
    function listDataset(d) {
        dslist.push(d.id)
        $("#id_datasets [value=" + d.id + "]").attr("checked", "checked");

        let card = '<div class="ds_card" id="card_' + d.id + '">' +
            '<p class="mb-0"><a href="#"><span class="ds_title">' + d.title + '</span></a> (' + d.label + '/' + d.id + ')</p>' +
            '<div class="ds_fields">' +
            '<p class="my-1"><b>Description</b>: ' + d.description + '</p>' +
            '<p class="my-1"><b>Create date</b>: ' + d.create_date.slice(0, 10) + '; <b># rows</b>: ' + d.numrows + '</p>' +
            '<p class="my-1"><a id="removeDataset_' + d.id + '" href="#"><i class="fas fa-minus-square" style="color:#336699"></i> remove</a>' +
            '<span class="float-end"><a id="addPlaces_' + d.id + '" href="#">add all ' +
            d.numrows + ' places</a></span' +
            '</p></div></div>';

        $("#coll_dscards_create").append(card)

        // Add event listeners
        document.getElementById('addPlaces_' + d.id).addEventListener('click', function (event) {
            event.preventDefault();
            addPlaces(d.id);
        });

        document.getElementById('removeDataset_' + d.id).addEventListener('click', function (event) {
            event.preventDefault();
            removeDataset(d.id);
        });
    }

    // remove places from collection (archiving annotations)
    function remove_from_collection(coll, pids) {
        var formData = new FormData()
        formData.append('collection', coll)
        formData.append('place_list', pids)
        $.ajax({
            type: 'POST',
            enctype: 'multipart/form-data',
            url: '/collections/archive_traces/',
            headers: {'X-CSRFToken': csrfToken},
            processData: false,
            contentType: false,
            cache: false,
            data: formData,
            success: function (response) {
                /*console.log('result:', response)*/
            }
        })
        // remove card from dom & hide link
        $(".col-place-card").each(function (index) {
            if (remove_these.includes($(this).data('id'))) {
                $(this).remove()
            }
        })
        // update count in tab
        var newcount = $(".col-place-card").length
        $("#place_count").text(newcount)
        remove_these = [];
        $("#a_remove").text('')
    }

    // adds all dataset places to placelist
    function addPlaces(dsid) {
        ``
        let url = "/collections/add_dsplaces/" + object_id + "/" + dsid
        // collection.views.add_dataset
        /*console.log('adding dataset w/o save',dsid, {{ object.id }}, url)*/
        document.location.href = url
        /*url = "{% url 'collection:add-ds' ds_id="+dsid+" coll_id=456 %}"*/
        /*document.location.href = url.replace('123', dsid).replace('456',{{ object.id }})*/
        /*)*/
        /*$("#collection_form").submit('remain')*/
    }

    // lists dataset on dropdown select
    function addDataset(action) {
        /*console.log('selected', $("#select_ds").val())*/

        $.get("/collections/list_ds", {
                ds_id: $("#select_ds").val()
            },
            function (data) {
                // render to html
                listDataset(data)
                /*console.log('ds to list:',data)*/
                // append ds.id to form
            });
        // reset select
        $("#select_ds").val(0)
        $("#msg").html('').hide()
    }

    // clears dataset card (cancels add)
    function clearDataset(dsid) {
        /*console.log('clear card for ds', dsid)*/
        $("#card_" + dsid).remove()
    }

    // removes dataset from collection
    function removeDataset(dsid) {
        /*console.log('removing ' + dsid + 'from collection '+ "{{object.id}}")*/
        $("#id_datasets [value=" + dsid + "]").prop("checked", false);
        let idx = dslist.indexOf(dsid)
        dslist.splice(idx, dslist.length);
        let card = "#card_" + dsid
        $(card).remove()
        if (dslist.length == 0) {
            $("#msg").html('None yet...').show()
        }
        /*console.log('removed '+dsid+' from dslist[] and dom')*/
        // reset select
        $("#select_ds").val(0)
    }

    $(".col-place-card").first().click();
}) // end doc ready