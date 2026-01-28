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

/***/ "./whg/webpack/js/workers/granularity.js":
/*!***********************************************!*\
  !*** ./whg/webpack/js/workers/granularity.js ***!
  \***********************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n/* harmony import */ var _turf_turf__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @turf/turf */ \"./node_modules/@turf/turf/dist/es/index.js\");\n\n\n/**\n * Generates an array of ImageBitmaps representing mottled fill patterns for different colours.\n * The pattern is created on an OffscreenCanvas and consists of randomly positioned,\n * semi-transparent circles with a radial gradient.\n *\n * @async\n * @param {string[]} colours - An array of CSS colour strings to generate patterns for.\n * @param {number} finalSize - The desired size (width and height) of the individual pattern image (before tiling).\n * @returns {Promise<ImageBitmap[]>} A promise that resolves to an array of ImageBitmaps, one for each colour.\n */\nasync function generateFillPatterns(colours, finalSize) {\n    const drawCanvas = new OffscreenCanvas(finalSize * 2, finalSize * 2);\n    const drawCtx = drawCanvas.getContext('2d');\n\n    const maxRadius = finalSize / 3;\n    const circleCount = Math.round(3 * Math.pow(finalSize, 2) / (Math.PI * Math.pow(maxRadius, 2)));\n    const circleData = [];\n\n    for (let i = 0; i < circleCount; i++) {\n        const x = Math.random() * finalSize;\n        const y = Math.random() * finalSize;\n        const radius = maxRadius * (Math.random() * 0.5 + 0.5);\n        circleData.push({ x, y, radius });\n    }\n\n    function drawCircle(x, y, radius, colour, ctx) {\n        const gradient = ctx.createRadialGradient(x, y, 0, x, y, radius);\n        gradient.addColorStop(0, colour);\n        gradient.addColorStop(1, colour.replace(/[^,]+(?=\\))/, '0')); // fade to transparent\n        ctx.beginPath();\n        ctx.arc(x, y, radius, 0, Math.PI * 2);\n        ctx.fillStyle = gradient;\n        ctx.fill();\n    }\n\n    function drawPattern(colour) {\n        drawCtx.clearRect(0, 0, finalSize * 2, finalSize * 2);\n        circleData.forEach(({ x, y, radius }) => {\n            drawCircle(x, y, radius, colour, drawCtx);\n            drawCircle(x + finalSize, y, radius, colour, drawCtx);\n            drawCircle(x, y + finalSize, radius, colour, drawCtx);\n            drawCircle(x + finalSize, y + finalSize, radius, colour, drawCtx);\n        });\n    }\n\n    const bitmaps = [];\n\n    for (const colour of colours) {\n        drawPattern(colour);\n\n        // Create a canvas to extract the central tile-sized square\n        const cropCanvas = new OffscreenCanvas(finalSize, finalSize);\n        const cropCtx = cropCanvas.getContext('2d');\n\n        cropCtx.drawImage(\n            drawCanvas,\n            finalSize / 4, finalSize / 4, // start cropping from the centre\n            finalSize, finalSize,         // width and height of the crop\n            0, 0,                         // destination top-left corner\n            finalSize, finalSize          // destination width and height\n        );\n\n        const bitmap = await createImageBitmap(await cropCanvas.convertToBlob());\n        bitmaps.push(bitmap);\n    }\n\n    return bitmaps;\n}\n\n/**\n * Buffers features in a GeoJSON FeatureCollection based on their geometry type and properties.\n * Features with a 'granularity' property will be buffered by that amount (in kilometers).\n * Other LineString and MultiLineString geometries will be buffered by a fixed amount (0.25 kilometers).\n * GeometryCollections will have their individual geometries processed recursively.\n *\n * @async\n * @param {turf.FeatureCollection} featureCollection - The GeoJSON FeatureCollection to process.\n * @param {string[]} colours - An array of CSS colour strings to generate fill patterns with (if buffering occurs).\n * @param {number} patternSize - The desired size of the generated fill pattern images.\n * @returns {Promise<{bufferedFeatureCollection: turf.FeatureCollection | null, patternImageBitmaps: string[] | null}>}\n * A promise that resolves to an object containing the buffered FeatureCollection and an array of pattern image URLs (if any buffering occurred).\n */\nasync function bufferFeatureCollection(featureCollection, colours, patternSize) {\n    const result = {\n        bufferedFeatureCollection: null,\n        patternImageBitmaps: null\n    };\n\n    let anyGranular = false;\n\n    async function bufferIfGranular(feature) {\n        const { geometry } = feature;\n\n        if (feature?.properties?.granularity) { // False if granularity is zero (or undefined or null)\n            anyGranular = true;\n            const buffered = _turf_turf__WEBPACK_IMPORTED_MODULE_0__.buffer(geometry, feature.properties.granularity, { units: 'kilometers' });\n            return buffered.geometry;\n        }\n\n        // Convert any remaining LineString and MultiLineString geometries to polygons with breadth of 0.5 km\n        if (geometry.type === 'LineString' || geometry.type === 'MultiLineString') {\n            const buffered = _turf_turf__WEBPACK_IMPORTED_MODULE_0__.buffer(geometry, 0.25, { units: 'kilometers' });\n            return buffered.geometry;\n        }\n\n        return geometry;\n    }\n\n    /**\n     * Processes a single GeoJSON Feature. If it's a GeometryCollection, it processes its geometries recursively.\n     * Otherwise, it buffers the geometry using the bufferIfGranular function.\n     *\n     * @async\n     * @param {turf.Feature<turf.Geometry>} feature - The GeoJSON Feature to process.\n     * @returns {Promise<turf.Feature<turf.Geometry>>} The processed GeoJSON Feature with the (potentially) buffered geometry.\n     */\n    async function processFeature(feature) {\n        const geometry = feature.geometry;\n\n        if (!geometry) return feature;\n\n        try {\n            if (geometry.type === 'GeometryCollection') {\n                const processedGeometries = [];\n\n                for (let geom of geometry.geometries) {\n                    const wrapped = { geometry: geom, properties: feature.properties };\n                    const processed = await bufferIfGranular(wrapped);\n                    processedGeometries.push(processed);\n                }\n\n                const allPolygons = processedGeometries.every(\n                    g => g.type === 'Polygon' || g.type === 'MultiPolygon'\n                );\n\n                const outputGeometry = allPolygons\n                    ? _turf_turf__WEBPACK_IMPORTED_MODULE_0__.multiPolygon(processedGeometries.map(g => g.coordinates))\n                    : _turf_turf__WEBPACK_IMPORTED_MODULE_0__.geometryCollection(processedGeometries);\n\n                return _turf_turf__WEBPACK_IMPORTED_MODULE_0__.feature(outputGeometry, feature.properties, { id: feature.id });\n            } else {\n                const bufferedGeometry = await bufferIfGranular(feature);\n                return _turf_turf__WEBPACK_IMPORTED_MODULE_0__.feature(bufferedGeometry, feature.properties, { id: feature.id });\n            }\n        }\n        catch (error) {\n            console.error('Error processing feature:', error);\n            return feature; // Return the original feature if an error occurs\n        }\n    }\n\n    const processedFeatures = [];\n    try {\n        for (let feature of featureCollection.features) {\n            processedFeatures.push(await processFeature(feature));\n        }\n    }\n    catch (error) {\n        console.error('Error processing feature collection:', error);\n        return result; // Return early if an error occurs\n    }\n\n    result.bufferedFeatureCollection = _turf_turf__WEBPACK_IMPORTED_MODULE_0__.featureCollection(processedFeatures);\n\n    // Only generate patterns if any buffering occurred\n    if (anyGranular) {\n        result.patternImageBitmaps = await generateFillPatterns(colours, patternSize);\n    }\n\n    return result;\n}\n\n/**\n * Handles messages received by the Web Worker. It expects an event with data containing\n * a 'featureCollection' (GeoJSON FeatureCollection) and an array of 'colours'.\n * It calls the bufferFeatureCollection function to buffer the features and generate\n * fill patterns if necessary. The result (buffered GeoJSON and pattern image URLs)\n * is then posted back to the main thread.\n *\n * @param {MessageEvent} event - The message event received by the worker.\n * @param {object} event.data - The data sent with the message.\n * @param {turf.FeatureCollection} event.data.featureCollection - The GeoJSON FeatureCollection to process.\n * @param {string[]} event.data.colours - An array of CSS colour strings for pattern generation.\n */\nself.onmessage = async function(event) {\n    const { colours, featureCollection } = event.data;\n    const patternSize = 64;\n\n    try {\n        const { bufferedFeatureCollection, patternImageBitmaps } =\n            await bufferFeatureCollection(featureCollection, colours, patternSize);\n\n        self.postMessage({\n            patterns: patternImageBitmaps,\n            bufferedGeoJSON: bufferedFeatureCollection\n        });\n    } catch (error) {\n        console.error('Error processing data in worker:', error);\n        self.postMessage({ error: 'Error processing data' });\n    }\n};\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiLi93aGcvd2VicGFjay9qcy93b3JrZXJzL2dyYW51bGFyaXR5LmpzIiwibWFwcGluZ3MiOiI7O0FBQW1DOztBQUVuQztBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxXQUFXLFVBQVU7QUFDckIsV0FBVyxRQUFRO0FBQ25CLGFBQWEsd0JBQXdCO0FBQ3JDO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0E7QUFDQTs7QUFFQSxvQkFBb0IsaUJBQWlCO0FBQ3JDO0FBQ0E7QUFDQTtBQUNBLDBCQUEwQixjQUFjO0FBQ3hDOztBQUVBO0FBQ0E7QUFDQTtBQUNBLHNFQUFzRTtBQUN0RTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0E7QUFDQSw4QkFBOEIsY0FBYztBQUM1QztBQUNBO0FBQ0E7QUFDQTtBQUNBLFNBQVM7QUFDVDs7QUFFQTs7QUFFQTtBQUNBOztBQUVBO0FBQ0E7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7O0FBRUE7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLFdBQVcsd0JBQXdCO0FBQ25DLFdBQVcsVUFBVTtBQUNyQixXQUFXLFFBQVE7QUFDbkIsYUFBYSxTQUFTLCtGQUErRjtBQUNySDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUFFQTs7QUFFQTtBQUNBLGdCQUFnQixXQUFXOztBQUUzQixnREFBZ0Q7QUFDaEQ7QUFDQSw2QkFBNkIsOENBQVcsNkNBQTZDLHFCQUFxQjtBQUMxRztBQUNBOztBQUVBO0FBQ0E7QUFDQSw2QkFBNkIsOENBQVcsbUJBQW1CLHFCQUFxQjtBQUNoRjtBQUNBOztBQUVBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLGVBQWUsNkJBQTZCO0FBQzVDLGlCQUFpQixzQ0FBc0M7QUFDdkQ7QUFDQTtBQUNBOztBQUVBOztBQUVBO0FBQ0E7QUFDQTs7QUFFQTtBQUNBLHNDQUFzQztBQUN0QztBQUNBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBOztBQUVBO0FBQ0Esc0JBQXNCLG9EQUFpQjtBQUN2QyxzQkFBc0IsMERBQXVCOztBQUU3Qyx1QkFBdUIsK0NBQVksdUNBQXVDLGdCQUFnQjtBQUMxRixjQUFjO0FBQ2Q7QUFDQSx1QkFBdUIsK0NBQVkseUNBQXlDLGdCQUFnQjtBQUM1RjtBQUNBO0FBQ0E7QUFDQTtBQUNBLDRCQUE0QjtBQUM1QjtBQUNBOztBQUVBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSx1QkFBdUI7QUFDdkI7O0FBRUEsdUNBQXVDLHlEQUFzQjs7QUFFN0Q7QUFDQTtBQUNBO0FBQ0E7O0FBRUE7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLFdBQVcsY0FBYztBQUN6QixXQUFXLFFBQVE7QUFDbkIsV0FBVyx3QkFBd0I7QUFDbkMsV0FBVyxVQUFVO0FBQ3JCO0FBQ0E7QUFDQSxZQUFZLDZCQUE2QjtBQUN6Qzs7QUFFQTtBQUNBLGdCQUFnQixpREFBaUQ7QUFDakU7O0FBRUE7QUFDQTtBQUNBO0FBQ0EsU0FBUztBQUNULE1BQU07QUFDTjtBQUNBLDJCQUEyQixnQ0FBZ0M7QUFDM0Q7QUFDQSIsInNvdXJjZXMiOlsid2VicGFjazovL3doZy13ZWJwYWNrLy4vd2hnL3dlYnBhY2svanMvd29ya2Vycy9ncmFudWxhcml0eS5qcz8wN2MwIl0sInNvdXJjZXNDb250ZW50IjpbImltcG9ydCAqIGFzIHR1cmYgZnJvbSAnQHR1cmYvdHVyZic7XG5cbi8qKlxuICogR2VuZXJhdGVzIGFuIGFycmF5IG9mIEltYWdlQml0bWFwcyByZXByZXNlbnRpbmcgbW90dGxlZCBmaWxsIHBhdHRlcm5zIGZvciBkaWZmZXJlbnQgY29sb3Vycy5cbiAqIFRoZSBwYXR0ZXJuIGlzIGNyZWF0ZWQgb24gYW4gT2Zmc2NyZWVuQ2FudmFzIGFuZCBjb25zaXN0cyBvZiByYW5kb21seSBwb3NpdGlvbmVkLFxuICogc2VtaS10cmFuc3BhcmVudCBjaXJjbGVzIHdpdGggYSByYWRpYWwgZ3JhZGllbnQuXG4gKlxuICogQGFzeW5jXG4gKiBAcGFyYW0ge3N0cmluZ1tdfSBjb2xvdXJzIC0gQW4gYXJyYXkgb2YgQ1NTIGNvbG91ciBzdHJpbmdzIHRvIGdlbmVyYXRlIHBhdHRlcm5zIGZvci5cbiAqIEBwYXJhbSB7bnVtYmVyfSBmaW5hbFNpemUgLSBUaGUgZGVzaXJlZCBzaXplICh3aWR0aCBhbmQgaGVpZ2h0KSBvZiB0aGUgaW5kaXZpZHVhbCBwYXR0ZXJuIGltYWdlIChiZWZvcmUgdGlsaW5nKS5cbiAqIEByZXR1cm5zIHtQcm9taXNlPEltYWdlQml0bWFwW10+fSBBIHByb21pc2UgdGhhdCByZXNvbHZlcyB0byBhbiBhcnJheSBvZiBJbWFnZUJpdG1hcHMsIG9uZSBmb3IgZWFjaCBjb2xvdXIuXG4gKi9cbmFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRmlsbFBhdHRlcm5zKGNvbG91cnMsIGZpbmFsU2l6ZSkge1xuICAgIGNvbnN0IGRyYXdDYW52YXMgPSBuZXcgT2Zmc2NyZWVuQ2FudmFzKGZpbmFsU2l6ZSAqIDIsIGZpbmFsU2l6ZSAqIDIpO1xuICAgIGNvbnN0IGRyYXdDdHggPSBkcmF3Q2FudmFzLmdldENvbnRleHQoJzJkJyk7XG5cbiAgICBjb25zdCBtYXhSYWRpdXMgPSBmaW5hbFNpemUgLyAzO1xuICAgIGNvbnN0IGNpcmNsZUNvdW50ID0gTWF0aC5yb3VuZCgzICogTWF0aC5wb3coZmluYWxTaXplLCAyKSAvIChNYXRoLlBJICogTWF0aC5wb3cobWF4UmFkaXVzLCAyKSkpO1xuICAgIGNvbnN0IGNpcmNsZURhdGEgPSBbXTtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2lyY2xlQ291bnQ7IGkrKykge1xuICAgICAgICBjb25zdCB4ID0gTWF0aC5yYW5kb20oKSAqIGZpbmFsU2l6ZTtcbiAgICAgICAgY29uc3QgeSA9IE1hdGgucmFuZG9tKCkgKiBmaW5hbFNpemU7XG4gICAgICAgIGNvbnN0IHJhZGl1cyA9IG1heFJhZGl1cyAqIChNYXRoLnJhbmRvbSgpICogMC41ICsgMC41KTtcbiAgICAgICAgY2lyY2xlRGF0YS5wdXNoKHsgeCwgeSwgcmFkaXVzIH0pO1xuICAgIH1cblxuICAgIGZ1bmN0aW9uIGRyYXdDaXJjbGUoeCwgeSwgcmFkaXVzLCBjb2xvdXIsIGN0eCkge1xuICAgICAgICBjb25zdCBncmFkaWVudCA9IGN0eC5jcmVhdGVSYWRpYWxHcmFkaWVudCh4LCB5LCAwLCB4LCB5LCByYWRpdXMpO1xuICAgICAgICBncmFkaWVudC5hZGRDb2xvclN0b3AoMCwgY29sb3VyKTtcbiAgICAgICAgZ3JhZGllbnQuYWRkQ29sb3JTdG9wKDEsIGNvbG91ci5yZXBsYWNlKC9bXixdKyg/PVxcKSkvLCAnMCcpKTsgLy8gZmFkZSB0byB0cmFuc3BhcmVudFxuICAgICAgICBjdHguYmVnaW5QYXRoKCk7XG4gICAgICAgIGN0eC5hcmMoeCwgeSwgcmFkaXVzLCAwLCBNYXRoLlBJICogMik7XG4gICAgICAgIGN0eC5maWxsU3R5bGUgPSBncmFkaWVudDtcbiAgICAgICAgY3R4LmZpbGwoKTtcbiAgICB9XG5cbiAgICBmdW5jdGlvbiBkcmF3UGF0dGVybihjb2xvdXIpIHtcbiAgICAgICAgZHJhd0N0eC5jbGVhclJlY3QoMCwgMCwgZmluYWxTaXplICogMiwgZmluYWxTaXplICogMik7XG4gICAgICAgIGNpcmNsZURhdGEuZm9yRWFjaCgoeyB4LCB5LCByYWRpdXMgfSkgPT4ge1xuICAgICAgICAgICAgZHJhd0NpcmNsZSh4LCB5LCByYWRpdXMsIGNvbG91ciwgZHJhd0N0eCk7XG4gICAgICAgICAgICBkcmF3Q2lyY2xlKHggKyBmaW5hbFNpemUsIHksIHJhZGl1cywgY29sb3VyLCBkcmF3Q3R4KTtcbiAgICAgICAgICAgIGRyYXdDaXJjbGUoeCwgeSArIGZpbmFsU2l6ZSwgcmFkaXVzLCBjb2xvdXIsIGRyYXdDdHgpO1xuICAgICAgICAgICAgZHJhd0NpcmNsZSh4ICsgZmluYWxTaXplLCB5ICsgZmluYWxTaXplLCByYWRpdXMsIGNvbG91ciwgZHJhd0N0eCk7XG4gICAgICAgIH0pO1xuICAgIH1cblxuICAgIGNvbnN0IGJpdG1hcHMgPSBbXTtcblxuICAgIGZvciAoY29uc3QgY29sb3VyIG9mIGNvbG91cnMpIHtcbiAgICAgICAgZHJhd1BhdHRlcm4oY29sb3VyKTtcblxuICAgICAgICAvLyBDcmVhdGUgYSBjYW52YXMgdG8gZXh0cmFjdCB0aGUgY2VudHJhbCB0aWxlLXNpemVkIHNxdWFyZVxuICAgICAgICBjb25zdCBjcm9wQ2FudmFzID0gbmV3IE9mZnNjcmVlbkNhbnZhcyhmaW5hbFNpemUsIGZpbmFsU2l6ZSk7XG4gICAgICAgIGNvbnN0IGNyb3BDdHggPSBjcm9wQ2FudmFzLmdldENvbnRleHQoJzJkJyk7XG5cbiAgICAgICAgY3JvcEN0eC5kcmF3SW1hZ2UoXG4gICAgICAgICAgICBkcmF3Q2FudmFzLFxuICAgICAgICAgICAgZmluYWxTaXplIC8gNCwgZmluYWxTaXplIC8gNCwgLy8gc3RhcnQgY3JvcHBpbmcgZnJvbSB0aGUgY2VudHJlXG4gICAgICAgICAgICBmaW5hbFNpemUsIGZpbmFsU2l6ZSwgICAgICAgICAvLyB3aWR0aCBhbmQgaGVpZ2h0IG9mIHRoZSBjcm9wXG4gICAgICAgICAgICAwLCAwLCAgICAgICAgICAgICAgICAgICAgICAgICAvLyBkZXN0aW5hdGlvbiB0b3AtbGVmdCBjb3JuZXJcbiAgICAgICAgICAgIGZpbmFsU2l6ZSwgZmluYWxTaXplICAgICAgICAgIC8vIGRlc3RpbmF0aW9uIHdpZHRoIGFuZCBoZWlnaHRcbiAgICAgICAgKTtcblxuICAgICAgICBjb25zdCBiaXRtYXAgPSBhd2FpdCBjcmVhdGVJbWFnZUJpdG1hcChhd2FpdCBjcm9wQ2FudmFzLmNvbnZlcnRUb0Jsb2IoKSk7XG4gICAgICAgIGJpdG1hcHMucHVzaChiaXRtYXApO1xuICAgIH1cblxuICAgIHJldHVybiBiaXRtYXBzO1xufVxuXG4vKipcbiAqIEJ1ZmZlcnMgZmVhdHVyZXMgaW4gYSBHZW9KU09OIEZlYXR1cmVDb2xsZWN0aW9uIGJhc2VkIG9uIHRoZWlyIGdlb21ldHJ5IHR5cGUgYW5kIHByb3BlcnRpZXMuXG4gKiBGZWF0dXJlcyB3aXRoIGEgJ2dyYW51bGFyaXR5JyBwcm9wZXJ0eSB3aWxsIGJlIGJ1ZmZlcmVkIGJ5IHRoYXQgYW1vdW50IChpbiBraWxvbWV0ZXJzKS5cbiAqIE90aGVyIExpbmVTdHJpbmcgYW5kIE11bHRpTGluZVN0cmluZyBnZW9tZXRyaWVzIHdpbGwgYmUgYnVmZmVyZWQgYnkgYSBmaXhlZCBhbW91bnQgKDAuMjUga2lsb21ldGVycykuXG4gKiBHZW9tZXRyeUNvbGxlY3Rpb25zIHdpbGwgaGF2ZSB0aGVpciBpbmRpdmlkdWFsIGdlb21ldHJpZXMgcHJvY2Vzc2VkIHJlY3Vyc2l2ZWx5LlxuICpcbiAqIEBhc3luY1xuICogQHBhcmFtIHt0dXJmLkZlYXR1cmVDb2xsZWN0aW9ufSBmZWF0dXJlQ29sbGVjdGlvbiAtIFRoZSBHZW9KU09OIEZlYXR1cmVDb2xsZWN0aW9uIHRvIHByb2Nlc3MuXG4gKiBAcGFyYW0ge3N0cmluZ1tdfSBjb2xvdXJzIC0gQW4gYXJyYXkgb2YgQ1NTIGNvbG91ciBzdHJpbmdzIHRvIGdlbmVyYXRlIGZpbGwgcGF0dGVybnMgd2l0aCAoaWYgYnVmZmVyaW5nIG9jY3VycykuXG4gKiBAcGFyYW0ge251bWJlcn0gcGF0dGVyblNpemUgLSBUaGUgZGVzaXJlZCBzaXplIG9mIHRoZSBnZW5lcmF0ZWQgZmlsbCBwYXR0ZXJuIGltYWdlcy5cbiAqIEByZXR1cm5zIHtQcm9taXNlPHtidWZmZXJlZEZlYXR1cmVDb2xsZWN0aW9uOiB0dXJmLkZlYXR1cmVDb2xsZWN0aW9uIHwgbnVsbCwgcGF0dGVybkltYWdlQml0bWFwczogc3RyaW5nW10gfCBudWxsfT59XG4gKiBBIHByb21pc2UgdGhhdCByZXNvbHZlcyB0byBhbiBvYmplY3QgY29udGFpbmluZyB0aGUgYnVmZmVyZWQgRmVhdHVyZUNvbGxlY3Rpb24gYW5kIGFuIGFycmF5IG9mIHBhdHRlcm4gaW1hZ2UgVVJMcyAoaWYgYW55IGJ1ZmZlcmluZyBvY2N1cnJlZCkuXG4gKi9cbmFzeW5jIGZ1bmN0aW9uIGJ1ZmZlckZlYXR1cmVDb2xsZWN0aW9uKGZlYXR1cmVDb2xsZWN0aW9uLCBjb2xvdXJzLCBwYXR0ZXJuU2l6ZSkge1xuICAgIGNvbnN0IHJlc3VsdCA9IHtcbiAgICAgICAgYnVmZmVyZWRGZWF0dXJlQ29sbGVjdGlvbjogbnVsbCxcbiAgICAgICAgcGF0dGVybkltYWdlQml0bWFwczogbnVsbFxuICAgIH07XG5cbiAgICBsZXQgYW55R3JhbnVsYXIgPSBmYWxzZTtcblxuICAgIGFzeW5jIGZ1bmN0aW9uIGJ1ZmZlcklmR3JhbnVsYXIoZmVhdHVyZSkge1xuICAgICAgICBjb25zdCB7IGdlb21ldHJ5IH0gPSBmZWF0dXJlO1xuXG4gICAgICAgIGlmIChmZWF0dXJlPy5wcm9wZXJ0aWVzPy5ncmFudWxhcml0eSkgeyAvLyBGYWxzZSBpZiBncmFudWxhcml0eSBpcyB6ZXJvIChvciB1bmRlZmluZWQgb3IgbnVsbClcbiAgICAgICAgICAgIGFueUdyYW51bGFyID0gdHJ1ZTtcbiAgICAgICAgICAgIGNvbnN0IGJ1ZmZlcmVkID0gdHVyZi5idWZmZXIoZ2VvbWV0cnksIGZlYXR1cmUucHJvcGVydGllcy5ncmFudWxhcml0eSwgeyB1bml0czogJ2tpbG9tZXRlcnMnIH0pO1xuICAgICAgICAgICAgcmV0dXJuIGJ1ZmZlcmVkLmdlb21ldHJ5O1xuICAgICAgICB9XG5cbiAgICAgICAgLy8gQ29udmVydCBhbnkgcmVtYWluaW5nIExpbmVTdHJpbmcgYW5kIE11bHRpTGluZVN0cmluZyBnZW9tZXRyaWVzIHRvIHBvbHlnb25zIHdpdGggYnJlYWR0aCBvZiAwLjUga21cbiAgICAgICAgaWYgKGdlb21ldHJ5LnR5cGUgPT09ICdMaW5lU3RyaW5nJyB8fCBnZW9tZXRyeS50eXBlID09PSAnTXVsdGlMaW5lU3RyaW5nJykge1xuICAgICAgICAgICAgY29uc3QgYnVmZmVyZWQgPSB0dXJmLmJ1ZmZlcihnZW9tZXRyeSwgMC4yNSwgeyB1bml0czogJ2tpbG9tZXRlcnMnIH0pO1xuICAgICAgICAgICAgcmV0dXJuIGJ1ZmZlcmVkLmdlb21ldHJ5O1xuICAgICAgICB9XG5cbiAgICAgICAgcmV0dXJuIGdlb21ldHJ5O1xuICAgIH1cblxuICAgIC8qKlxuICAgICAqIFByb2Nlc3NlcyBhIHNpbmdsZSBHZW9KU09OIEZlYXR1cmUuIElmIGl0J3MgYSBHZW9tZXRyeUNvbGxlY3Rpb24sIGl0IHByb2Nlc3NlcyBpdHMgZ2VvbWV0cmllcyByZWN1cnNpdmVseS5cbiAgICAgKiBPdGhlcndpc2UsIGl0IGJ1ZmZlcnMgdGhlIGdlb21ldHJ5IHVzaW5nIHRoZSBidWZmZXJJZkdyYW51bGFyIGZ1bmN0aW9uLlxuICAgICAqXG4gICAgICogQGFzeW5jXG4gICAgICogQHBhcmFtIHt0dXJmLkZlYXR1cmU8dHVyZi5HZW9tZXRyeT59IGZlYXR1cmUgLSBUaGUgR2VvSlNPTiBGZWF0dXJlIHRvIHByb2Nlc3MuXG4gICAgICogQHJldHVybnMge1Byb21pc2U8dHVyZi5GZWF0dXJlPHR1cmYuR2VvbWV0cnk+Pn0gVGhlIHByb2Nlc3NlZCBHZW9KU09OIEZlYXR1cmUgd2l0aCB0aGUgKHBvdGVudGlhbGx5KSBidWZmZXJlZCBnZW9tZXRyeS5cbiAgICAgKi9cbiAgICBhc3luYyBmdW5jdGlvbiBwcm9jZXNzRmVhdHVyZShmZWF0dXJlKSB7XG4gICAgICAgIGNvbnN0IGdlb21ldHJ5ID0gZmVhdHVyZS5nZW9tZXRyeTtcblxuICAgICAgICBpZiAoIWdlb21ldHJ5KSByZXR1cm4gZmVhdHVyZTtcblxuICAgICAgICB0cnkge1xuICAgICAgICAgICAgaWYgKGdlb21ldHJ5LnR5cGUgPT09ICdHZW9tZXRyeUNvbGxlY3Rpb24nKSB7XG4gICAgICAgICAgICAgICAgY29uc3QgcHJvY2Vzc2VkR2VvbWV0cmllcyA9IFtdO1xuXG4gICAgICAgICAgICAgICAgZm9yIChsZXQgZ2VvbSBvZiBnZW9tZXRyeS5nZW9tZXRyaWVzKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IHdyYXBwZWQgPSB7IGdlb21ldHJ5OiBnZW9tLCBwcm9wZXJ0aWVzOiBmZWF0dXJlLnByb3BlcnRpZXMgfTtcbiAgICAgICAgICAgICAgICAgICAgY29uc3QgcHJvY2Vzc2VkID0gYXdhaXQgYnVmZmVySWZHcmFudWxhcih3cmFwcGVkKTtcbiAgICAgICAgICAgICAgICAgICAgcHJvY2Vzc2VkR2VvbWV0cmllcy5wdXNoKHByb2Nlc3NlZCk7XG4gICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgY29uc3QgYWxsUG9seWdvbnMgPSBwcm9jZXNzZWRHZW9tZXRyaWVzLmV2ZXJ5KFxuICAgICAgICAgICAgICAgICAgICBnID0+IGcudHlwZSA9PT0gJ1BvbHlnb24nIHx8IGcudHlwZSA9PT0gJ011bHRpUG9seWdvbidcbiAgICAgICAgICAgICAgICApO1xuXG4gICAgICAgICAgICAgICAgY29uc3Qgb3V0cHV0R2VvbWV0cnkgPSBhbGxQb2x5Z29uc1xuICAgICAgICAgICAgICAgICAgICA/IHR1cmYubXVsdGlQb2x5Z29uKHByb2Nlc3NlZEdlb21ldHJpZXMubWFwKGcgPT4gZy5jb29yZGluYXRlcykpXG4gICAgICAgICAgICAgICAgICAgIDogdHVyZi5nZW9tZXRyeUNvbGxlY3Rpb24ocHJvY2Vzc2VkR2VvbWV0cmllcyk7XG5cbiAgICAgICAgICAgICAgICByZXR1cm4gdHVyZi5mZWF0dXJlKG91dHB1dEdlb21ldHJ5LCBmZWF0dXJlLnByb3BlcnRpZXMsIHsgaWQ6IGZlYXR1cmUuaWQgfSk7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgIGNvbnN0IGJ1ZmZlcmVkR2VvbWV0cnkgPSBhd2FpdCBidWZmZXJJZkdyYW51bGFyKGZlYXR1cmUpO1xuICAgICAgICAgICAgICAgIHJldHVybiB0dXJmLmZlYXR1cmUoYnVmZmVyZWRHZW9tZXRyeSwgZmVhdHVyZS5wcm9wZXJ0aWVzLCB7IGlkOiBmZWF0dXJlLmlkIH0pO1xuICAgICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgY29uc29sZS5lcnJvcignRXJyb3IgcHJvY2Vzc2luZyBmZWF0dXJlOicsIGVycm9yKTtcbiAgICAgICAgICAgIHJldHVybiBmZWF0dXJlOyAvLyBSZXR1cm4gdGhlIG9yaWdpbmFsIGZlYXR1cmUgaWYgYW4gZXJyb3Igb2NjdXJzXG4gICAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBwcm9jZXNzZWRGZWF0dXJlcyA9IFtdO1xuICAgIHRyeSB7XG4gICAgICAgIGZvciAobGV0IGZlYXR1cmUgb2YgZmVhdHVyZUNvbGxlY3Rpb24uZmVhdHVyZXMpIHtcbiAgICAgICAgICAgIHByb2Nlc3NlZEZlYXR1cmVzLnB1c2goYXdhaXQgcHJvY2Vzc0ZlYXR1cmUoZmVhdHVyZSkpO1xuICAgICAgICB9XG4gICAgfVxuICAgIGNhdGNoIChlcnJvcikge1xuICAgICAgICBjb25zb2xlLmVycm9yKCdFcnJvciBwcm9jZXNzaW5nIGZlYXR1cmUgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgICAgIHJldHVybiByZXN1bHQ7IC8vIFJldHVybiBlYXJseSBpZiBhbiBlcnJvciBvY2N1cnNcbiAgICB9XG5cbiAgICByZXN1bHQuYnVmZmVyZWRGZWF0dXJlQ29sbGVjdGlvbiA9IHR1cmYuZmVhdHVyZUNvbGxlY3Rpb24ocHJvY2Vzc2VkRmVhdHVyZXMpO1xuXG4gICAgLy8gT25seSBnZW5lcmF0ZSBwYXR0ZXJucyBpZiBhbnkgYnVmZmVyaW5nIG9jY3VycmVkXG4gICAgaWYgKGFueUdyYW51bGFyKSB7XG4gICAgICAgIHJlc3VsdC5wYXR0ZXJuSW1hZ2VCaXRtYXBzID0gYXdhaXQgZ2VuZXJhdGVGaWxsUGF0dGVybnMoY29sb3VycywgcGF0dGVyblNpemUpO1xuICAgIH1cblxuICAgIHJldHVybiByZXN1bHQ7XG59XG5cbi8qKlxuICogSGFuZGxlcyBtZXNzYWdlcyByZWNlaXZlZCBieSB0aGUgV2ViIFdvcmtlci4gSXQgZXhwZWN0cyBhbiBldmVudCB3aXRoIGRhdGEgY29udGFpbmluZ1xuICogYSAnZmVhdHVyZUNvbGxlY3Rpb24nIChHZW9KU09OIEZlYXR1cmVDb2xsZWN0aW9uKSBhbmQgYW4gYXJyYXkgb2YgJ2NvbG91cnMnLlxuICogSXQgY2FsbHMgdGhlIGJ1ZmZlckZlYXR1cmVDb2xsZWN0aW9uIGZ1bmN0aW9uIHRvIGJ1ZmZlciB0aGUgZmVhdHVyZXMgYW5kIGdlbmVyYXRlXG4gKiBmaWxsIHBhdHRlcm5zIGlmIG5lY2Vzc2FyeS4gVGhlIHJlc3VsdCAoYnVmZmVyZWQgR2VvSlNPTiBhbmQgcGF0dGVybiBpbWFnZSBVUkxzKVxuICogaXMgdGhlbiBwb3N0ZWQgYmFjayB0byB0aGUgbWFpbiB0aHJlYWQuXG4gKlxuICogQHBhcmFtIHtNZXNzYWdlRXZlbnR9IGV2ZW50IC0gVGhlIG1lc3NhZ2UgZXZlbnQgcmVjZWl2ZWQgYnkgdGhlIHdvcmtlci5cbiAqIEBwYXJhbSB7b2JqZWN0fSBldmVudC5kYXRhIC0gVGhlIGRhdGEgc2VudCB3aXRoIHRoZSBtZXNzYWdlLlxuICogQHBhcmFtIHt0dXJmLkZlYXR1cmVDb2xsZWN0aW9ufSBldmVudC5kYXRhLmZlYXR1cmVDb2xsZWN0aW9uIC0gVGhlIEdlb0pTT04gRmVhdHVyZUNvbGxlY3Rpb24gdG8gcHJvY2Vzcy5cbiAqIEBwYXJhbSB7c3RyaW5nW119IGV2ZW50LmRhdGEuY29sb3VycyAtIEFuIGFycmF5IG9mIENTUyBjb2xvdXIgc3RyaW5ncyBmb3IgcGF0dGVybiBnZW5lcmF0aW9uLlxuICovXG5zZWxmLm9ubWVzc2FnZSA9IGFzeW5jIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc3QgeyBjb2xvdXJzLCBmZWF0dXJlQ29sbGVjdGlvbiB9ID0gZXZlbnQuZGF0YTtcbiAgICBjb25zdCBwYXR0ZXJuU2l6ZSA9IDY0O1xuXG4gICAgdHJ5IHtcbiAgICAgICAgY29uc3QgeyBidWZmZXJlZEZlYXR1cmVDb2xsZWN0aW9uLCBwYXR0ZXJuSW1hZ2VCaXRtYXBzIH0gPVxuICAgICAgICAgICAgYXdhaXQgYnVmZmVyRmVhdHVyZUNvbGxlY3Rpb24oZmVhdHVyZUNvbGxlY3Rpb24sIGNvbG91cnMsIHBhdHRlcm5TaXplKTtcblxuICAgICAgICBzZWxmLnBvc3RNZXNzYWdlKHtcbiAgICAgICAgICAgIHBhdHRlcm5zOiBwYXR0ZXJuSW1hZ2VCaXRtYXBzLFxuICAgICAgICAgICAgYnVmZmVyZWRHZW9KU09OOiBidWZmZXJlZEZlYXR1cmVDb2xsZWN0aW9uXG4gICAgICAgIH0pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoJ0Vycm9yIHByb2Nlc3NpbmcgZGF0YSBpbiB3b3JrZXI6JywgZXJyb3IpO1xuICAgICAgICBzZWxmLnBvc3RNZXNzYWdlKHsgZXJyb3I6ICdFcnJvciBwcm9jZXNzaW5nIGRhdGEnIH0pO1xuICAgIH1cbn07XG4iXSwibmFtZXMiOltdLCJzb3VyY2VSb290IjoiIn0=\n//# sourceURL=webpack-internal:///./whg/webpack/js/workers/granularity.js\n");

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
/******/ 		__webpack_modules__[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = __webpack_modules__;
/******/ 	
/******/ 	// the startup function
/******/ 	__webpack_require__.x = () => {
/******/ 		// Load entry module and return exports
/******/ 		// This entry module depends on other loaded chunks and execution need to be delayed
/******/ 		var __webpack_exports__ = __webpack_require__.O(undefined, ["vendors-node_modules_turf_turf_dist_es_index_js"], () => (__webpack_require__("./whg/webpack/js/workers/granularity.js")))
/******/ 		__webpack_exports__ = __webpack_require__.O(__webpack_exports__);
/******/ 		return __webpack_exports__;
/******/ 	};
/******/ 	
/************************************************************************/
/******/ 	/* webpack/runtime/chunk loaded */
/******/ 	(() => {
/******/ 		var deferred = [];
/******/ 		__webpack_require__.O = (result, chunkIds, fn, priority) => {
/******/ 			if(chunkIds) {
/******/ 				priority = priority || 0;
/******/ 				for(var i = deferred.length; i > 0 && deferred[i - 1][2] > priority; i--) deferred[i] = deferred[i - 1];
/******/ 				deferred[i] = [chunkIds, fn, priority];
/******/ 				return;
/******/ 			}
/******/ 			var notFulfilled = Infinity;
/******/ 			for (var i = 0; i < deferred.length; i++) {
/******/ 				var [chunkIds, fn, priority] = deferred[i];
/******/ 				var fulfilled = true;
/******/ 				for (var j = 0; j < chunkIds.length; j++) {
/******/ 					if ((priority & 1 === 0 || notFulfilled >= priority) && Object.keys(__webpack_require__.O).every((key) => (__webpack_require__.O[key](chunkIds[j])))) {
/******/ 						chunkIds.splice(j--, 1);
/******/ 					} else {
/******/ 						fulfilled = false;
/******/ 						if(priority < notFulfilled) notFulfilled = priority;
/******/ 					}
/******/ 				}
/******/ 				if(fulfilled) {
/******/ 					deferred.splice(i--, 1)
/******/ 					var r = fn();
/******/ 					if (r !== undefined) result = r;
/******/ 				}
/******/ 			}
/******/ 			return result;
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/define property getters */
/******/ 	(() => {
/******/ 		// define getter functions for harmony exports
/******/ 		__webpack_require__.d = (exports, definition) => {
/******/ 			for(var key in definition) {
/******/ 				if(__webpack_require__.o(definition, key) && !__webpack_require__.o(exports, key)) {
/******/ 					Object.defineProperty(exports, key, { enumerable: true, get: definition[key] });
/******/ 				}
/******/ 			}
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/ensure chunk */
/******/ 	(() => {
/******/ 		__webpack_require__.f = {};
/******/ 		// This file contains only the entry chunk.
/******/ 		// The chunk loading function for additional chunks
/******/ 		__webpack_require__.e = (chunkId) => {
/******/ 			return Promise.all(Object.keys(__webpack_require__.f).reduce((promises, key) => {
/******/ 				__webpack_require__.f[key](chunkId, promises);
/******/ 				return promises;
/******/ 			}, []));
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/get javascript chunk filename */
/******/ 	(() => {
/******/ 		// This function allow to reference async chunks and sibling chunks for the entrypoint
/******/ 		__webpack_require__.u = (chunkId) => {
/******/ 			// return url for filenames based on template
/******/ 			return "" + chunkId + ".bundle.js";
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/get mini-css chunk filename */
/******/ 	(() => {
/******/ 		// This function allow to reference async chunks and sibling chunks for the entrypoint
/******/ 		__webpack_require__.miniCssF = (chunkId) => {
/******/ 			// return url for filenames based on template
/******/ 			return undefined;
/******/ 		};
/******/ 	})();
/******/ 	
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
/******/ 	/* webpack/runtime/hasOwnProperty shorthand */
/******/ 	(() => {
/******/ 		__webpack_require__.o = (obj, prop) => (Object.prototype.hasOwnProperty.call(obj, prop))
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
/******/ 	/* webpack/runtime/importScripts chunk loading */
/******/ 	(() => {
/******/ 		// no baseURI
/******/ 		
/******/ 		// object to store loaded chunks
/******/ 		// "1" means "already loaded"
/******/ 		var installedChunks = {
/******/ 			"whg_webpack_js_workers_granularity_js": 1
/******/ 		};
/******/ 		
/******/ 		// importScripts chunk loading
/******/ 		var installChunk = (data) => {
/******/ 			var [chunkIds, moreModules, runtime] = data;
/******/ 			for(var moduleId in moreModules) {
/******/ 				if(__webpack_require__.o(moreModules, moduleId)) {
/******/ 					__webpack_require__.m[moduleId] = moreModules[moduleId];
/******/ 				}
/******/ 			}
/******/ 			if(runtime) runtime(__webpack_require__);
/******/ 			while(chunkIds.length)
/******/ 				installedChunks[chunkIds.pop()] = 1;
/******/ 			parentChunkLoadingFunction(data);
/******/ 		};
/******/ 		__webpack_require__.f.i = (chunkId, promises) => {
/******/ 			// "1" is the signal for "already loaded"
/******/ 			if(!installedChunks[chunkId]) {
/******/ 				if(true) { // all chunks have JS
/******/ 					importScripts(__webpack_require__.p + __webpack_require__.u(chunkId));
/******/ 				}
/******/ 			}
/******/ 		};
/******/ 		
/******/ 		var chunkLoadingGlobal = self["webpackChunkwhg_webpack"] = self["webpackChunkwhg_webpack"] || [];
/******/ 		var parentChunkLoadingFunction = chunkLoadingGlobal.push.bind(chunkLoadingGlobal);
/******/ 		chunkLoadingGlobal.push = installChunk;
/******/ 		
/******/ 		// no HMR
/******/ 		
/******/ 		// no HMR manifest
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/startup chunk dependencies */
/******/ 	(() => {
/******/ 		var next = __webpack_require__.x;
/******/ 		__webpack_require__.x = () => {
/******/ 			return __webpack_require__.e("vendors-node_modules_turf_turf_dist_es_index_js").then(next);
/******/ 		};
/******/ 	})();
/******/ 	
/************************************************************************/
/******/ 	
/******/ 	// run startup
/******/ 	var __webpack_exports__ = __webpack_require__.x();
/******/ 	
/******/ })()
;