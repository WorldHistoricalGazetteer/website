/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ "./whg/webpack/css/wb-suggest.css":
/*!****************************************!*\
  !*** ./whg/webpack/css/wb-suggest.css ***!
  \****************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n// extracted by mini-css-extract-plugin\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiLi93aGcvd2VicGFjay9jc3Mvd2Itc3VnZ2VzdC5jc3MiLCJtYXBwaW5ncyI6IjtBQUFBIiwic291cmNlcyI6WyJ3ZWJwYWNrOi8vd2hnLXdlYnBhY2svLi93aGcvd2VicGFjay9jc3Mvd2Itc3VnZ2VzdC5jc3M/ZmFiYyJdLCJzb3VyY2VzQ29udGVudCI6WyIvLyBleHRyYWN0ZWQgYnkgbWluaS1jc3MtZXh0cmFjdC1wbHVnaW5cbmV4cG9ydCB7fTsiXSwibmFtZXMiOltdLCJzb3VyY2VSb290IjoiIn0=\n//# sourceURL=webpack-internal:///./whg/webpack/css/wb-suggest.css\n");

/***/ }),

/***/ "./whg/webpack/js/wb-suggest.js":
/*!**************************************!*\
  !*** ./whg/webpack/js/wb-suggest.js ***!
  \**************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n/* harmony import */ var _css_wb_suggest_css__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../css/wb-suggest.css */ \"./whg/webpack/css/wb-suggest.css\");\n// wb-suggest.js — shared, framework-free client for community record corrections\n// (plan-record-suggestions §5). Exposes window.WHGSuggest so it can be driven from any surface\n// (place detail, portal source-boxes, dataset-places detail pane) without importing it into those\n// bundles. Two jobs:\n//   1. a delegated click handler that turns any [data-wb-suggest][data-place-id] element into\n//      \"open this record in the Workbench to correct/suggest\" (checkout → redirect to the editor);\n//   2. mountInsets(root): fill [data-wb-suggest-inset][data-place-id] containers with the pending\n//      suggestions for that record (count for everyone; details for staff/owner/proposer).\n\n\n\n// eslint-disable-next-line camelcase, no-undef\n__webpack_require__.p = '/static/webpack/';\n\nfunction csrf() {\n  var input = document.querySelector('input[name=csrfmiddlewaretoken]');\n  if (input && input.value) return input.value;\n  var m = document.cookie.match(/(?:^|;\\s*)csrftoken=([^;]+)/);\n  return m ? decodeURIComponent(m[1]) : '';\n}\n\nfunction esc(s) {\n  return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;')\n    .replace(/>/g, '&gt;').replace(/\"/g, '&quot;').replace(/'/g, '&#39;');\n}\n\n// Check out a record → editor. Any beta user may; the editor shows Publish (owners/staff) or Submit\n// suggestion (everyone else) based on the server's can_apply.\nfunction openInWorkbench(placeId, el) {\n  if (el) { el.dataset.busy = '1'; el.style.pointerEvents = 'none'; el.style.opacity = '.6'; }\n  return fetch('/reconciliation/checkout/place/' + placeId + '/', {\n    method: 'POST', credentials: 'same-origin',\n    headers: { 'X-CSRFToken': csrf(), 'Content-Type': 'application/json' }, body: '{}'\n  }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); })\n    .then(function (res) {\n      if (res.ok && res.d && res.d.id) { window.location.href = '/workbench/record/?project=' + res.d.id; }\n      else {\n        alert((res.d && res.d.error) || 'Could not open this record in the Workbench.');\n        if (el) { el.dataset.busy = ''; el.style.pointerEvents = ''; el.style.opacity = ''; }\n      }\n    }).catch(function () {\n      alert('Could not reach the Workbench — please try again.');\n      if (el) { el.dataset.busy = ''; el.style.pointerEvents = ''; el.style.opacity = ''; }\n    });\n}\n\nfunction renderInset(box, data) {\n  var n = data.count || 0;\n  if (!n) { box.innerHTML = ''; box.classList.remove('wb-inset--has'); return; }\n  box.classList.add('wb-inset--has');\n  var items = data.items || [];\n  var head = '<span class=\"wb-inset-badge\">' + n + ' correction' + (n !== 1 ? 's' : '') + ' proposed</span>';\n  if (!items.length) {\n    // public: count only, no content (plan §1e)\n    box.innerHTML = '<div class=\"wb-inset\">' + head + ' <span class=\"wb-inset-muted\">— awaiting review</span></div>';\n    return;\n  }\n  var rows = items.map(function (it) {\n    var fields = (it.changed_fields || []).map(function (f) { return '<span class=\"wb-inset-chip\">' + esc(f) + '</span>'; }).join(' ');\n    var why = it.rationale ? ' <span class=\"wb-inset-muted\">— “' + esc(it.rationale) + '”</span>' : '';\n    var rev = data.can_review ? ' <a href=\"/workbench/suggestions/\" class=\"wb-inset-link\">review →</a>' : '';\n    return '<li>' + fields + ' <span class=\"wb-inset-muted\">by ' + esc(it.proposer) + '</span>' + why + rev + '</li>';\n  }).join('');\n  box.innerHTML = '<div class=\"wb-inset\">' + head +\n    '<ul class=\"wb-inset-list\">' + rows + '</ul></div>';\n}\n\nfunction mountInsets(root) {\n  var scope = root || document;\n  scope.querySelectorAll('[data-wb-suggest-inset][data-place-id]').forEach(function (box) {\n    if (box.dataset.wbLoaded) return;\n    box.dataset.wbLoaded = '1';\n    fetch('/reconciliation/suggestions/for-place/' + box.dataset.placeId + '/', { credentials: 'same-origin' })\n      .then(function (r) { return r.ok ? r.json() : null; })\n      .then(function (d) { if (d) renderInset(box, d); })\n      .catch(function () { /* silent */ });\n  });\n}\n\n// Delegated: any click on a [data-wb-suggest][data-place-id] opens the record in the Workbench.\ndocument.addEventListener('click', function (e) {\n  var t = e.target.closest && e.target.closest('[data-wb-suggest][data-place-id]');\n  if (!t || t.dataset.busy) return;\n  e.preventDefault();\n  openInWorkbench(t.dataset.placeId, t);\n});\n\nwindow.WHGSuggest = {\n  // Surfaces that inject the affordance into dynamic DOM (portal, ds_places) read this to decide\n  // whether to render the button. Set from the page via window.WHG_CAN_SUGGEST (a beta flag).\n  canSuggest: !!window.WHG_CAN_SUGGEST,\n  open: openInWorkbench,\n  mountInsets: mountInsets,\n  // Injected button markup surfaces can drop into dynamic DOM (e.g. portal source-boxes).\n  buttonHTML: function (placeId, label) {\n    return '<a href=\"#\" class=\"wb-suggest-btn\" data-wb-suggest data-place-id=\"' + placeId + '\">' +\n      '<i class=\"fas fa-pen-to-square\"></i> ' + esc(label || 'Suggest a correction') + '</a>';\n  },\n  insetHTML: function (placeId) {\n    return '<span class=\"wb-inset-wrap\" data-wb-suggest-inset data-place-id=\"' + placeId + '\"></span>';\n  }\n};\n\nif (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { mountInsets(document); });\nelse mountInsets(document);\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiLi93aGcvd2VicGFjay9qcy93Yi1zdWdnZXN0LmpzIiwibWFwcGluZ3MiOiI7O0FBQUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSx5REFBeUQ7O0FBRTFCOztBQUUvQjtBQUNBLHFCQUF1Qjs7QUFFdkI7QUFDQTtBQUNBO0FBQ0EsdUNBQXVDLGtCQUFrQjtBQUN6RDtBQUNBOztBQUVBO0FBQ0Esd0RBQXdELHNCQUFzQjtBQUM5RSx3QkFBd0Isd0JBQXdCLHVCQUF1QjtBQUN2RTs7QUFFQSxtREFBbUQ7QUFDbkQ7QUFDQTtBQUNBLFlBQVksdUJBQXVCLGlDQUFpQztBQUNwRTtBQUNBO0FBQ0EsZUFBZSwyREFBMkQsV0FBVztBQUNyRixHQUFHLHNCQUFzQixvQ0FBb0MsU0FBUyxtQkFBbUIsSUFBSTtBQUM3RjtBQUNBLHlDQUF5QztBQUN6QztBQUNBO0FBQ0Esa0JBQWtCLHNCQUFzQiw2QkFBNkI7QUFDckU7QUFDQSxLQUFLO0FBQ0w7QUFDQSxnQkFBZ0Isc0JBQXNCLDZCQUE2QjtBQUNuRSxLQUFLO0FBQ0w7O0FBRUE7QUFDQTtBQUNBLFlBQVksb0JBQW9CLHVDQUF1QztBQUN2RTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSw4REFBOEQsNkRBQTZEO0FBQzNIO0FBQ0E7QUFDQTtBQUNBLEdBQUc7QUFDSDtBQUNBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLGtGQUFrRiw0QkFBNEI7QUFDOUcsMkJBQTJCLGdDQUFnQztBQUMzRCwyQkFBMkIsNkJBQTZCO0FBQ3hELDJCQUEyQixjQUFjO0FBQ3pDLEdBQUc7QUFDSDs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxDQUFDOztBQUVEO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsR0FBRztBQUNIO0FBQ0E7QUFDQTtBQUNBOztBQUVBLG1HQUFtRyx3QkFBd0I7QUFDM0giLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly93aGctd2VicGFjay8uL3doZy93ZWJwYWNrL2pzL3diLXN1Z2dlc3QuanM/Y2VkZiJdLCJzb3VyY2VzQ29udGVudCI6WyIvLyB3Yi1zdWdnZXN0LmpzIOKAlCBzaGFyZWQsIGZyYW1ld29yay1mcmVlIGNsaWVudCBmb3IgY29tbXVuaXR5IHJlY29yZCBjb3JyZWN0aW9uc1xuLy8gKHBsYW4tcmVjb3JkLXN1Z2dlc3Rpb25zIMKnNSkuIEV4cG9zZXMgd2luZG93LldIR1N1Z2dlc3Qgc28gaXQgY2FuIGJlIGRyaXZlbiBmcm9tIGFueSBzdXJmYWNlXG4vLyAocGxhY2UgZGV0YWlsLCBwb3J0YWwgc291cmNlLWJveGVzLCBkYXRhc2V0LXBsYWNlcyBkZXRhaWwgcGFuZSkgd2l0aG91dCBpbXBvcnRpbmcgaXQgaW50byB0aG9zZVxuLy8gYnVuZGxlcy4gVHdvIGpvYnM6XG4vLyAgIDEuIGEgZGVsZWdhdGVkIGNsaWNrIGhhbmRsZXIgdGhhdCB0dXJucyBhbnkgW2RhdGEtd2Itc3VnZ2VzdF1bZGF0YS1wbGFjZS1pZF0gZWxlbWVudCBpbnRvXG4vLyAgICAgIFwib3BlbiB0aGlzIHJlY29yZCBpbiB0aGUgV29ya2JlbmNoIHRvIGNvcnJlY3Qvc3VnZ2VzdFwiIChjaGVja291dCDihpIgcmVkaXJlY3QgdG8gdGhlIGVkaXRvcik7XG4vLyAgIDIuIG1vdW50SW5zZXRzKHJvb3QpOiBmaWxsIFtkYXRhLXdiLXN1Z2dlc3QtaW5zZXRdW2RhdGEtcGxhY2UtaWRdIGNvbnRhaW5lcnMgd2l0aCB0aGUgcGVuZGluZ1xuLy8gICAgICBzdWdnZXN0aW9ucyBmb3IgdGhhdCByZWNvcmQgKGNvdW50IGZvciBldmVyeW9uZTsgZGV0YWlscyBmb3Igc3RhZmYvb3duZXIvcHJvcG9zZXIpLlxuXG5pbXBvcnQgJy4uL2Nzcy93Yi1zdWdnZXN0LmNzcyc7XG5cbi8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBjYW1lbGNhc2UsIG5vLXVuZGVmXG5fX3dlYnBhY2tfcHVibGljX3BhdGhfXyA9ICcvc3RhdGljL3dlYnBhY2svJztcblxuZnVuY3Rpb24gY3NyZigpIHtcbiAgdmFyIGlucHV0ID0gZG9jdW1lbnQucXVlcnlTZWxlY3RvcignaW5wdXRbbmFtZT1jc3JmbWlkZGxld2FyZXRva2VuXScpO1xuICBpZiAoaW5wdXQgJiYgaW5wdXQudmFsdWUpIHJldHVybiBpbnB1dC52YWx1ZTtcbiAgdmFyIG0gPSBkb2N1bWVudC5jb29raWUubWF0Y2goLyg/Ol58O1xccyopY3NyZnRva2VuPShbXjtdKykvKTtcbiAgcmV0dXJuIG0gPyBkZWNvZGVVUklDb21wb25lbnQobVsxXSkgOiAnJztcbn1cblxuZnVuY3Rpb24gZXNjKHMpIHtcbiAgcmV0dXJuIFN0cmluZyhzID09IG51bGwgPyAnJyA6IHMpLnJlcGxhY2UoLyYvZywgJyZhbXA7JykucmVwbGFjZSgvPC9nLCAnJmx0OycpXG4gICAgLnJlcGxhY2UoLz4vZywgJyZndDsnKS5yZXBsYWNlKC9cIi9nLCAnJnF1b3Q7JykucmVwbGFjZSgvJy9nLCAnJiMzOTsnKTtcbn1cblxuLy8gQ2hlY2sgb3V0IGEgcmVjb3JkIOKGkiBlZGl0b3IuIEFueSBiZXRhIHVzZXIgbWF5OyB0aGUgZWRpdG9yIHNob3dzIFB1Ymxpc2ggKG93bmVycy9zdGFmZikgb3IgU3VibWl0XG4vLyBzdWdnZXN0aW9uIChldmVyeW9uZSBlbHNlKSBiYXNlZCBvbiB0aGUgc2VydmVyJ3MgY2FuX2FwcGx5LlxuZnVuY3Rpb24gb3BlbkluV29ya2JlbmNoKHBsYWNlSWQsIGVsKSB7XG4gIGlmIChlbCkgeyBlbC5kYXRhc2V0LmJ1c3kgPSAnMSc7IGVsLnN0eWxlLnBvaW50ZXJFdmVudHMgPSAnbm9uZSc7IGVsLnN0eWxlLm9wYWNpdHkgPSAnLjYnOyB9XG4gIHJldHVybiBmZXRjaCgnL3JlY29uY2lsaWF0aW9uL2NoZWNrb3V0L3BsYWNlLycgKyBwbGFjZUlkICsgJy8nLCB7XG4gICAgbWV0aG9kOiAnUE9TVCcsIGNyZWRlbnRpYWxzOiAnc2FtZS1vcmlnaW4nLFxuICAgIGhlYWRlcnM6IHsgJ1gtQ1NSRlRva2VuJzogY3NyZigpLCAnQ29udGVudC1UeXBlJzogJ2FwcGxpY2F0aW9uL2pzb24nIH0sIGJvZHk6ICd7fSdcbiAgfSkudGhlbihmdW5jdGlvbiAocikgeyByZXR1cm4gci5qc29uKCkudGhlbihmdW5jdGlvbiAoZCkgeyByZXR1cm4geyBvazogci5vaywgZDogZCB9OyB9KTsgfSlcbiAgICAudGhlbihmdW5jdGlvbiAocmVzKSB7XG4gICAgICBpZiAocmVzLm9rICYmIHJlcy5kICYmIHJlcy5kLmlkKSB7IHdpbmRvdy5sb2NhdGlvbi5ocmVmID0gJy93b3JrYmVuY2gvcmVjb3JkLz9wcm9qZWN0PScgKyByZXMuZC5pZDsgfVxuICAgICAgZWxzZSB7XG4gICAgICAgIGFsZXJ0KChyZXMuZCAmJiByZXMuZC5lcnJvcikgfHwgJ0NvdWxkIG5vdCBvcGVuIHRoaXMgcmVjb3JkIGluIHRoZSBXb3JrYmVuY2guJyk7XG4gICAgICAgIGlmIChlbCkgeyBlbC5kYXRhc2V0LmJ1c3kgPSAnJzsgZWwuc3R5bGUucG9pbnRlckV2ZW50cyA9ICcnOyBlbC5zdHlsZS5vcGFjaXR5ID0gJyc7IH1cbiAgICAgIH1cbiAgICB9KS5jYXRjaChmdW5jdGlvbiAoKSB7XG4gICAgICBhbGVydCgnQ291bGQgbm90IHJlYWNoIHRoZSBXb3JrYmVuY2gg4oCUIHBsZWFzZSB0cnkgYWdhaW4uJyk7XG4gICAgICBpZiAoZWwpIHsgZWwuZGF0YXNldC5idXN5ID0gJyc7IGVsLnN0eWxlLnBvaW50ZXJFdmVudHMgPSAnJzsgZWwuc3R5bGUub3BhY2l0eSA9ICcnOyB9XG4gICAgfSk7XG59XG5cbmZ1bmN0aW9uIHJlbmRlckluc2V0KGJveCwgZGF0YSkge1xuICB2YXIgbiA9IGRhdGEuY291bnQgfHwgMDtcbiAgaWYgKCFuKSB7IGJveC5pbm5lckhUTUwgPSAnJzsgYm94LmNsYXNzTGlzdC5yZW1vdmUoJ3diLWluc2V0LS1oYXMnKTsgcmV0dXJuOyB9XG4gIGJveC5jbGFzc0xpc3QuYWRkKCd3Yi1pbnNldC0taGFzJyk7XG4gIHZhciBpdGVtcyA9IGRhdGEuaXRlbXMgfHwgW107XG4gIHZhciBoZWFkID0gJzxzcGFuIGNsYXNzPVwid2ItaW5zZXQtYmFkZ2VcIj4nICsgbiArICcgY29ycmVjdGlvbicgKyAobiAhPT0gMSA/ICdzJyA6ICcnKSArICcgcHJvcG9zZWQ8L3NwYW4+JztcbiAgaWYgKCFpdGVtcy5sZW5ndGgpIHtcbiAgICAvLyBwdWJsaWM6IGNvdW50IG9ubHksIG5vIGNvbnRlbnQgKHBsYW4gwqcxZSlcbiAgICBib3guaW5uZXJIVE1MID0gJzxkaXYgY2xhc3M9XCJ3Yi1pbnNldFwiPicgKyBoZWFkICsgJyA8c3BhbiBjbGFzcz1cIndiLWluc2V0LW11dGVkXCI+4oCUIGF3YWl0aW5nIHJldmlldzwvc3Bhbj48L2Rpdj4nO1xuICAgIHJldHVybjtcbiAgfVxuICB2YXIgcm93cyA9IGl0ZW1zLm1hcChmdW5jdGlvbiAoaXQpIHtcbiAgICB2YXIgZmllbGRzID0gKGl0LmNoYW5nZWRfZmllbGRzIHx8IFtdKS5tYXAoZnVuY3Rpb24gKGYpIHsgcmV0dXJuICc8c3BhbiBjbGFzcz1cIndiLWluc2V0LWNoaXBcIj4nICsgZXNjKGYpICsgJzwvc3Bhbj4nOyB9KS5qb2luKCcgJyk7XG4gICAgdmFyIHdoeSA9IGl0LnJhdGlvbmFsZSA/ICcgPHNwYW4gY2xhc3M9XCJ3Yi1pbnNldC1tdXRlZFwiPuKAlCDigJwnICsgZXNjKGl0LnJhdGlvbmFsZSkgKyAn4oCdPC9zcGFuPicgOiAnJztcbiAgICB2YXIgcmV2ID0gZGF0YS5jYW5fcmV2aWV3ID8gJyA8YSBocmVmPVwiL3dvcmtiZW5jaC9zdWdnZXN0aW9ucy9cIiBjbGFzcz1cIndiLWluc2V0LWxpbmtcIj5yZXZpZXcg4oaSPC9hPicgOiAnJztcbiAgICByZXR1cm4gJzxsaT4nICsgZmllbGRzICsgJyA8c3BhbiBjbGFzcz1cIndiLWluc2V0LW11dGVkXCI+YnkgJyArIGVzYyhpdC5wcm9wb3NlcikgKyAnPC9zcGFuPicgKyB3aHkgKyByZXYgKyAnPC9saT4nO1xuICB9KS5qb2luKCcnKTtcbiAgYm94LmlubmVySFRNTCA9ICc8ZGl2IGNsYXNzPVwid2ItaW5zZXRcIj4nICsgaGVhZCArXG4gICAgJzx1bCBjbGFzcz1cIndiLWluc2V0LWxpc3RcIj4nICsgcm93cyArICc8L3VsPjwvZGl2Pic7XG59XG5cbmZ1bmN0aW9uIG1vdW50SW5zZXRzKHJvb3QpIHtcbiAgdmFyIHNjb3BlID0gcm9vdCB8fCBkb2N1bWVudDtcbiAgc2NvcGUucXVlcnlTZWxlY3RvckFsbCgnW2RhdGEtd2Itc3VnZ2VzdC1pbnNldF1bZGF0YS1wbGFjZS1pZF0nKS5mb3JFYWNoKGZ1bmN0aW9uIChib3gpIHtcbiAgICBpZiAoYm94LmRhdGFzZXQud2JMb2FkZWQpIHJldHVybjtcbiAgICBib3guZGF0YXNldC53YkxvYWRlZCA9ICcxJztcbiAgICBmZXRjaCgnL3JlY29uY2lsaWF0aW9uL3N1Z2dlc3Rpb25zL2Zvci1wbGFjZS8nICsgYm94LmRhdGFzZXQucGxhY2VJZCArICcvJywgeyBjcmVkZW50aWFsczogJ3NhbWUtb3JpZ2luJyB9KVxuICAgICAgLnRoZW4oZnVuY3Rpb24gKHIpIHsgcmV0dXJuIHIub2sgPyByLmpzb24oKSA6IG51bGw7IH0pXG4gICAgICAudGhlbihmdW5jdGlvbiAoZCkgeyBpZiAoZCkgcmVuZGVySW5zZXQoYm94LCBkKTsgfSlcbiAgICAgIC5jYXRjaChmdW5jdGlvbiAoKSB7IC8qIHNpbGVudCAqLyB9KTtcbiAgfSk7XG59XG5cbi8vIERlbGVnYXRlZDogYW55IGNsaWNrIG9uIGEgW2RhdGEtd2Itc3VnZ2VzdF1bZGF0YS1wbGFjZS1pZF0gb3BlbnMgdGhlIHJlY29yZCBpbiB0aGUgV29ya2JlbmNoLlxuZG9jdW1lbnQuYWRkRXZlbnRMaXN0ZW5lcignY2xpY2snLCBmdW5jdGlvbiAoZSkge1xuICB2YXIgdCA9IGUudGFyZ2V0LmNsb3Nlc3QgJiYgZS50YXJnZXQuY2xvc2VzdCgnW2RhdGEtd2Itc3VnZ2VzdF1bZGF0YS1wbGFjZS1pZF0nKTtcbiAgaWYgKCF0IHx8IHQuZGF0YXNldC5idXN5KSByZXR1cm47XG4gIGUucHJldmVudERlZmF1bHQoKTtcbiAgb3BlbkluV29ya2JlbmNoKHQuZGF0YXNldC5wbGFjZUlkLCB0KTtcbn0pO1xuXG53aW5kb3cuV0hHU3VnZ2VzdCA9IHtcbiAgLy8gU3VyZmFjZXMgdGhhdCBpbmplY3QgdGhlIGFmZm9yZGFuY2UgaW50byBkeW5hbWljIERPTSAocG9ydGFsLCBkc19wbGFjZXMpIHJlYWQgdGhpcyB0byBkZWNpZGVcbiAgLy8gd2hldGhlciB0byByZW5kZXIgdGhlIGJ1dHRvbi4gU2V0IGZyb20gdGhlIHBhZ2UgdmlhIHdpbmRvdy5XSEdfQ0FOX1NVR0dFU1QgKGEgYmV0YSBmbGFnKS5cbiAgY2FuU3VnZ2VzdDogISF3aW5kb3cuV0hHX0NBTl9TVUdHRVNULFxuICBvcGVuOiBvcGVuSW5Xb3JrYmVuY2gsXG4gIG1vdW50SW5zZXRzOiBtb3VudEluc2V0cyxcbiAgLy8gSW5qZWN0ZWQgYnV0dG9uIG1hcmt1cCBzdXJmYWNlcyBjYW4gZHJvcCBpbnRvIGR5bmFtaWMgRE9NIChlLmcuIHBvcnRhbCBzb3VyY2UtYm94ZXMpLlxuICBidXR0b25IVE1MOiBmdW5jdGlvbiAocGxhY2VJZCwgbGFiZWwpIHtcbiAgICByZXR1cm4gJzxhIGhyZWY9XCIjXCIgY2xhc3M9XCJ3Yi1zdWdnZXN0LWJ0blwiIGRhdGEtd2Itc3VnZ2VzdCBkYXRhLXBsYWNlLWlkPVwiJyArIHBsYWNlSWQgKyAnXCI+JyArXG4gICAgICAnPGkgY2xhc3M9XCJmYXMgZmEtcGVuLXRvLXNxdWFyZVwiPjwvaT4gJyArIGVzYyhsYWJlbCB8fCAnU3VnZ2VzdCBhIGNvcnJlY3Rpb24nKSArICc8L2E+JztcbiAgfSxcbiAgaW5zZXRIVE1MOiBmdW5jdGlvbiAocGxhY2VJZCkge1xuICAgIHJldHVybiAnPHNwYW4gY2xhc3M9XCJ3Yi1pbnNldC13cmFwXCIgZGF0YS13Yi1zdWdnZXN0LWluc2V0IGRhdGEtcGxhY2UtaWQ9XCInICsgcGxhY2VJZCArICdcIj48L3NwYW4+JztcbiAgfVxufTtcblxuaWYgKGRvY3VtZW50LnJlYWR5U3RhdGUgPT09ICdsb2FkaW5nJykgZG9jdW1lbnQuYWRkRXZlbnRMaXN0ZW5lcignRE9NQ29udGVudExvYWRlZCcsIGZ1bmN0aW9uICgpIHsgbW91bnRJbnNldHMoZG9jdW1lbnQpOyB9KTtcbmVsc2UgbW91bnRJbnNldHMoZG9jdW1lbnQpO1xuIl0sIm5hbWVzIjpbXSwic291cmNlUm9vdCI6IiJ9\n//# sourceURL=webpack-internal:///./whg/webpack/js/wb-suggest.js\n");

/***/ })

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		var cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		__webpack_modules__[moduleId](module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
/******/ 	/* webpack/runtime/global */
/******/ 	(() => {
/******/ 		__webpack_require__.g = (function() {
/******/ 			if (typeof globalThis === 'object') return globalThis;
/******/ 			try {
/******/ 				return this || new Function('return this')();
/******/ 			} catch (e) {
/******/ 				if (typeof window === 'object') return window;
/******/ 			}
/******/ 		})();
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/make namespace object */
/******/ 	(() => {
/******/ 		// define __esModule on exports
/******/ 		__webpack_require__.r = (exports) => {
/******/ 			if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 				Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 			}
/******/ 			Object.defineProperty(exports, '__esModule', { value: true });
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/publicPath */
/******/ 	(() => {
/******/ 		var scriptUrl;
/******/ 		if (__webpack_require__.g.importScripts) scriptUrl = __webpack_require__.g.location + "";
/******/ 		var document = __webpack_require__.g.document;
/******/ 		if (!scriptUrl && document) {
/******/ 			if (document.currentScript && document.currentScript.tagName.toUpperCase() === 'SCRIPT')
/******/ 				scriptUrl = document.currentScript.src;
/******/ 			if (!scriptUrl) {
/******/ 				var scripts = document.getElementsByTagName("script");
/******/ 				if(scripts.length) {
/******/ 					var i = scripts.length - 1;
/******/ 					while (i > -1 && (!scriptUrl || !/^http(s?):/.test(scriptUrl))) scriptUrl = scripts[i--].src;
/******/ 				}
/******/ 			}
/******/ 		}
/******/ 		// When supporting browsers where an automatic publicPath is not supported you must specify an output.publicPath manually via configuration
/******/ 		// or pass an empty string ("") and set the __webpack_public_path__ variable from your code to use your own logic.
/******/ 		if (!scriptUrl) throw new Error("Automatic publicPath is not supported in this browser");
/******/ 		scriptUrl = scriptUrl.replace(/^blob:/, "").replace(/#.*$/, "").replace(/\?.*$/, "").replace(/\/[^\/]+$/, "/");
/******/ 		__webpack_require__.p = scriptUrl;
/******/ 	})();
/******/ 	
/************************************************************************/
/******/ 	
/******/ 	// startup
/******/ 	// Load entry module and return exports
/******/ 	// This entry module can't be inlined because the eval-source-map devtool is used.
/******/ 	var __webpack_exports__ = __webpack_require__("./whg/webpack/js/wb-suggest.js");
/******/ 	
/******/ })()
;