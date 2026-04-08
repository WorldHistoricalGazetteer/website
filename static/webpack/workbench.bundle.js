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

/***/ "./whg/webpack/css/workbench.css":
/*!***************************************!*\
  !*** ./whg/webpack/css/workbench.css ***!
  \***************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n// extracted by mini-css-extract-plugin\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiLi93aGcvd2VicGFjay9jc3Mvd29ya2JlbmNoLmNzcyIsIm1hcHBpbmdzIjoiO0FBQUEiLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly93aGctd2VicGFjay8uL3doZy93ZWJwYWNrL2Nzcy93b3JrYmVuY2guY3NzPzdmNzYiXSwic291cmNlc0NvbnRlbnQiOlsiLy8gZXh0cmFjdGVkIGJ5IG1pbmktY3NzLWV4dHJhY3QtcGx1Z2luXG5leHBvcnQge307Il0sIm5hbWVzIjpbXSwic291cmNlUm9vdCI6IiJ9\n//# sourceURL=webpack-internal:///./whg/webpack/css/workbench.css\n");

/***/ }),

/***/ "./whg/webpack/js/workbench.js":
/*!*************************************!*\
  !*** ./whg/webpack/js/workbench.js ***!
  \*************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n/* harmony import */ var _css_workbench_css__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../css/workbench.css */ \"./whg/webpack/css/workbench.css\");\n// Purpose: workbench-specific js\n\n\n\n$(document).ready(function () {\n    $(\".help-matches\").click(function () {\n        window.open(`https://docs.whgazetteer.org/content/${$(this).data('url')}`, '_blank').focus();\n    });\n})//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiLi93aGcvd2VicGFjay9qcy93b3JrYmVuY2guanMiLCJtYXBwaW5ncyI6Ijs7QUFBQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EiLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly93aGctd2VicGFjay8uL3doZy93ZWJwYWNrL2pzL3dvcmtiZW5jaC5qcz82ZTRkIl0sInNvdXJjZXNDb250ZW50IjpbIi8vIFB1cnBvc2U6IHdvcmtiZW5jaC1zcGVjaWZpYyBqc1xuXG5pbXBvcnQgJy4uL2Nzcy93b3JrYmVuY2guY3NzJztcblxuJChkb2N1bWVudCkucmVhZHkoZnVuY3Rpb24gKCkge1xuICAgICQoXCIuaGVscC1tYXRjaGVzXCIpLmNsaWNrKGZ1bmN0aW9uICgpIHtcbiAgICAgICAgd2luZG93Lm9wZW4oYGh0dHBzOi8vZG9jcy53aGdhemV0dGVlci5vcmcvY29udGVudC8keyQodGhpcykuZGF0YSgndXJsJyl9YCwgJ19ibGFuaycpLmZvY3VzKCk7XG4gICAgfSk7XG59KSJdLCJuYW1lcyI6W10sInNvdXJjZVJvb3QiOiIifQ==\n//# sourceURL=webpack-internal:///./whg/webpack/js/workbench.js\n");

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
/************************************************************************/
/******/ 	
/******/ 	// startup
/******/ 	// Load entry module and return exports
/******/ 	// This entry module can't be inlined because the eval-source-map devtool is used.
/******/ 	var __webpack_exports__ = __webpack_require__("./whg/webpack/js/workbench.js");
/******/ 	
/******/ })()
;