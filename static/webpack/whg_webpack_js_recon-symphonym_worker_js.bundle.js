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

/***/ "./whg/webpack/js/recon-symphonym-preprocess.js":
/*!******************************************************!*\
  !*** ./whg/webpack/js/recon-symphonym-preprocess.js ***!
  \******************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export */ __webpack_require__.d(__webpack_exports__, {\n/* harmony export */   detectScript: () => (/* binding */ detectScript),\n/* harmony export */   tokenise: () => (/* binding */ tokenise)\n/* harmony export */ });\n// recon-symphonym-preprocess.js\n// Exact JS port of the Symphonym tokeniser/preprocessing from indexing/hf/inference.py — the input\n// side of the in-browser phonetic embedding (Phase 7). Parity with the reference is essential: the\n// ONNX encoder was trained on these ids, so any drift silently degrades embeddings. Verified against\n// inference.py._tokenise for a multi-script sample set (see the node parity check in developer notes).\n//\n// Produces the four ONNX inputs for a (text, lang) pair:\n//   char_ids : Int32Array  (per-codepoint ids from char_vocab; <UNK> fallback)\n//   script_id: dominant Unicode script id (script_vocab; OTHER→…; default 0)\n//   lang_id  : language id (lang_vocab; <UNK>/0 fallback)\n//   length   : codepoint count\n\n// Unicode script ranges used during training — order matters (first matching range wins per char).\nconst SCRIPT_RANGES = [\n  ['LATIN', [[0x0041, 0x007A], [0x00C0, 0x024F], [0x1E00, 0x1EFF]]],\n  ['CYRILLIC', [[0x0400, 0x04FF], [0x0500, 0x052F]]],\n  ['ARABIC', [[0x0600, 0x06FF], [0x0750, 0x077F], [0xFB50, 0xFDFF], [0xFE70, 0xFEFF]]],\n  ['CJK', [[0x4E00, 0x9FFF], [0x3400, 0x4DBF], [0x20000, 0x2A6DF], [0xF900, 0xFAFF]]],\n  ['HANGUL', [[0xAC00, 0xD7AF], [0x1100, 0x11FF], [0x3130, 0x318F]]],\n  ['HIRAGANA', [[0x3041, 0x3096]]],\n  ['KATAKANA', [[0x30A1, 0x30FA], [0x31F0, 0x31FF]]],\n  ['DEVANAGARI', [[0x0900, 0x097F]]],\n  ['BENGALI', [[0x0980, 0x09FF]]],\n  ['GUJARATI', [[0x0A80, 0x0AFF]]],\n  ['GURMUKHI', [[0x0A00, 0x0A7F]]],\n  ['TAMIL', [[0x0B80, 0x0BFF]]],\n  ['TELUGU', [[0x0C00, 0x0C7F]]],\n  ['KANNADA', [[0x0C80, 0x0CFF]]],\n  ['MALAYALAM', [[0x0D00, 0x0D7F]]],\n  ['THAI', [[0x0E00, 0x0E7F]]],\n  ['GEORGIAN', [[0x10A0, 0x10FF]]],\n  ['ARMENIAN', [[0x0530, 0x058F]]],\n  ['HEBREW', [[0x0590, 0x05FF], [0xFB1D, 0xFB4F]]],\n  ['GREEK', [[0x0370, 0x03FF], [0x1F00, 0x1FFF]]],\n];\n\n// Dominant script by codepoint count; first matching range wins; unmatched → OTHER; empty → OTHER.\nfunction detectScript(text) {\n  const counts = Object.create(null);\n  for (const ch of text) {\n    const cp = ch.codePointAt(0);\n    let matched = false;\n    for (const [name, ranges] of SCRIPT_RANGES) {\n      for (const [lo, hi] of ranges) {\n        if (cp >= lo && cp <= hi) { counts[name] = (counts[name] || 0) + 1; matched = true; break; }\n      }\n      if (matched) break;\n    }\n    if (!matched) counts.OTHER = (counts.OTHER || 0) + 1;\n  }\n  let best = null, bestN = -1;\n  for (const k in counts) if (counts[k] > bestN) { best = k; bestN = counts[k]; }\n  return best == null ? 'OTHER' : best;\n}\n\n// vocabs: { charToId, scriptToId, langToId } — the *_to_id maps from the JSON vocab files.\nfunction tokenise(text, lang, vocabs) {\n  const charToId = vocabs.charToId, scriptToId = vocabs.scriptToId, langToId = vocabs.langToId;\n  const unkChar = charToId['<UNK>'] != null ? charToId['<UNK>'] : 1;\n  const unkLang = langToId['<UNK>'] != null ? langToId['<UNK>'] : 0;\n  const chars = Array.from(text); // iterate by codepoint, matching Python str iteration\n  const charIds = new Int32Array(chars.length);\n  for (let i = 0; i < chars.length; i++) {\n    const id = charToId[chars[i]];\n    charIds[i] = id != null ? id : unkChar;\n  }\n  const scriptId = scriptToId[detectScript(text)];\n  const langId = langToId[lang];\n  return {\n    charIds,\n    scriptId: scriptId != null ? scriptId : 0,\n    langId: langId != null ? langId : unkLang,\n    length: chars.length,\n  };\n}\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiLi93aGcvd2VicGFjay9qcy9yZWNvbi1zeW1waG9ueW0tcHJlcHJvY2Vzcy5qcyIsIm1hcHBpbmdzIjoiOzs7OztBQUFBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsZ0VBQWdFO0FBQ2hFLDBEQUEwRCxTQUFTO0FBQ25FLHlDQUF5QztBQUN6Qzs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQUVBLHVDQUF1QywyQkFBMkIsbUJBQW1CO0FBQzlFO0FBQ1A7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0Esb0NBQW9DLHdDQUF3QyxnQkFBZ0I7QUFDNUY7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsbURBQW1ELFVBQVU7QUFDN0Q7QUFDQTs7QUFFQSxhQUFhLGlDQUFpQztBQUN2QztBQUNQO0FBQ0E7QUFDQTtBQUNBLGtDQUFrQztBQUNsQztBQUNBLGtCQUFrQixrQkFBa0I7QUFDcEM7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBIiwic291cmNlcyI6WyJ3ZWJwYWNrOi8vd2hnLXdlYnBhY2svLi93aGcvd2VicGFjay9qcy9yZWNvbi1zeW1waG9ueW0tcHJlcHJvY2Vzcy5qcz81NzJiIl0sInNvdXJjZXNDb250ZW50IjpbIi8vIHJlY29uLXN5bXBob255bS1wcmVwcm9jZXNzLmpzXG4vLyBFeGFjdCBKUyBwb3J0IG9mIHRoZSBTeW1waG9ueW0gdG9rZW5pc2VyL3ByZXByb2Nlc3NpbmcgZnJvbSBpbmRleGluZy9oZi9pbmZlcmVuY2UucHkg4oCUIHRoZSBpbnB1dFxuLy8gc2lkZSBvZiB0aGUgaW4tYnJvd3NlciBwaG9uZXRpYyBlbWJlZGRpbmcgKFBoYXNlIDcpLiBQYXJpdHkgd2l0aCB0aGUgcmVmZXJlbmNlIGlzIGVzc2VudGlhbDogdGhlXG4vLyBPTk5YIGVuY29kZXIgd2FzIHRyYWluZWQgb24gdGhlc2UgaWRzLCBzbyBhbnkgZHJpZnQgc2lsZW50bHkgZGVncmFkZXMgZW1iZWRkaW5ncy4gVmVyaWZpZWQgYWdhaW5zdFxuLy8gaW5mZXJlbmNlLnB5Ll90b2tlbmlzZSBmb3IgYSBtdWx0aS1zY3JpcHQgc2FtcGxlIHNldCAoc2VlIHRoZSBub2RlIHBhcml0eSBjaGVjayBpbiBkZXZlbG9wZXIgbm90ZXMpLlxuLy9cbi8vIFByb2R1Y2VzIHRoZSBmb3VyIE9OTlggaW5wdXRzIGZvciBhICh0ZXh0LCBsYW5nKSBwYWlyOlxuLy8gICBjaGFyX2lkcyA6IEludDMyQXJyYXkgIChwZXItY29kZXBvaW50IGlkcyBmcm9tIGNoYXJfdm9jYWI7IDxVTks+IGZhbGxiYWNrKVxuLy8gICBzY3JpcHRfaWQ6IGRvbWluYW50IFVuaWNvZGUgc2NyaXB0IGlkIChzY3JpcHRfdm9jYWI7IE9USEVS4oaS4oCmOyBkZWZhdWx0IDApXG4vLyAgIGxhbmdfaWQgIDogbGFuZ3VhZ2UgaWQgKGxhbmdfdm9jYWI7IDxVTks+LzAgZmFsbGJhY2spXG4vLyAgIGxlbmd0aCAgIDogY29kZXBvaW50IGNvdW50XG5cbi8vIFVuaWNvZGUgc2NyaXB0IHJhbmdlcyB1c2VkIGR1cmluZyB0cmFpbmluZyDigJQgb3JkZXIgbWF0dGVycyAoZmlyc3QgbWF0Y2hpbmcgcmFuZ2Ugd2lucyBwZXIgY2hhcikuXG5jb25zdCBTQ1JJUFRfUkFOR0VTID0gW1xuICBbJ0xBVElOJywgW1sweDAwNDEsIDB4MDA3QV0sIFsweDAwQzAsIDB4MDI0Rl0sIFsweDFFMDAsIDB4MUVGRl1dXSxcbiAgWydDWVJJTExJQycsIFtbMHgwNDAwLCAweDA0RkZdLCBbMHgwNTAwLCAweDA1MkZdXV0sXG4gIFsnQVJBQklDJywgW1sweDA2MDAsIDB4MDZGRl0sIFsweDA3NTAsIDB4MDc3Rl0sIFsweEZCNTAsIDB4RkRGRl0sIFsweEZFNzAsIDB4RkVGRl1dXSxcbiAgWydDSksnLCBbWzB4NEUwMCwgMHg5RkZGXSwgWzB4MzQwMCwgMHg0REJGXSwgWzB4MjAwMDAsIDB4MkE2REZdLCBbMHhGOTAwLCAweEZBRkZdXV0sXG4gIFsnSEFOR1VMJywgW1sweEFDMDAsIDB4RDdBRl0sIFsweDExMDAsIDB4MTFGRl0sIFsweDMxMzAsIDB4MzE4Rl1dXSxcbiAgWydISVJBR0FOQScsIFtbMHgzMDQxLCAweDMwOTZdXV0sXG4gIFsnS0FUQUtBTkEnLCBbWzB4MzBBMSwgMHgzMEZBXSwgWzB4MzFGMCwgMHgzMUZGXV1dLFxuICBbJ0RFVkFOQUdBUkknLCBbWzB4MDkwMCwgMHgwOTdGXV1dLFxuICBbJ0JFTkdBTEknLCBbWzB4MDk4MCwgMHgwOUZGXV1dLFxuICBbJ0dVSkFSQVRJJywgW1sweDBBODAsIDB4MEFGRl1dXSxcbiAgWydHVVJNVUtISScsIFtbMHgwQTAwLCAweDBBN0ZdXV0sXG4gIFsnVEFNSUwnLCBbWzB4MEI4MCwgMHgwQkZGXV1dLFxuICBbJ1RFTFVHVScsIFtbMHgwQzAwLCAweDBDN0ZdXV0sXG4gIFsnS0FOTkFEQScsIFtbMHgwQzgwLCAweDBDRkZdXV0sXG4gIFsnTUFMQVlBTEFNJywgW1sweDBEMDAsIDB4MEQ3Rl1dXSxcbiAgWydUSEFJJywgW1sweDBFMDAsIDB4MEU3Rl1dXSxcbiAgWydHRU9SR0lBTicsIFtbMHgxMEEwLCAweDEwRkZdXV0sXG4gIFsnQVJNRU5JQU4nLCBbWzB4MDUzMCwgMHgwNThGXV1dLFxuICBbJ0hFQlJFVycsIFtbMHgwNTkwLCAweDA1RkZdLCBbMHhGQjFELCAweEZCNEZdXV0sXG4gIFsnR1JFRUsnLCBbWzB4MDM3MCwgMHgwM0ZGXSwgWzB4MUYwMCwgMHgxRkZGXV1dLFxuXTtcblxuLy8gRG9taW5hbnQgc2NyaXB0IGJ5IGNvZGVwb2ludCBjb3VudDsgZmlyc3QgbWF0Y2hpbmcgcmFuZ2Ugd2luczsgdW5tYXRjaGVkIOKGkiBPVEhFUjsgZW1wdHkg4oaSIE9USEVSLlxuZXhwb3J0IGZ1bmN0aW9uIGRldGVjdFNjcmlwdCh0ZXh0KSB7XG4gIGNvbnN0IGNvdW50cyA9IE9iamVjdC5jcmVhdGUobnVsbCk7XG4gIGZvciAoY29uc3QgY2ggb2YgdGV4dCkge1xuICAgIGNvbnN0IGNwID0gY2guY29kZVBvaW50QXQoMCk7XG4gICAgbGV0IG1hdGNoZWQgPSBmYWxzZTtcbiAgICBmb3IgKGNvbnN0IFtuYW1lLCByYW5nZXNdIG9mIFNDUklQVF9SQU5HRVMpIHtcbiAgICAgIGZvciAoY29uc3QgW2xvLCBoaV0gb2YgcmFuZ2VzKSB7XG4gICAgICAgIGlmIChjcCA+PSBsbyAmJiBjcCA8PSBoaSkgeyBjb3VudHNbbmFtZV0gPSAoY291bnRzW25hbWVdIHx8IDApICsgMTsgbWF0Y2hlZCA9IHRydWU7IGJyZWFrOyB9XG4gICAgICB9XG4gICAgICBpZiAobWF0Y2hlZCkgYnJlYWs7XG4gICAgfVxuICAgIGlmICghbWF0Y2hlZCkgY291bnRzLk9USEVSID0gKGNvdW50cy5PVEhFUiB8fCAwKSArIDE7XG4gIH1cbiAgbGV0IGJlc3QgPSBudWxsLCBiZXN0TiA9IC0xO1xuICBmb3IgKGNvbnN0IGsgaW4gY291bnRzKSBpZiAoY291bnRzW2tdID4gYmVzdE4pIHsgYmVzdCA9IGs7IGJlc3ROID0gY291bnRzW2tdOyB9XG4gIHJldHVybiBiZXN0ID09IG51bGwgPyAnT1RIRVInIDogYmVzdDtcbn1cblxuLy8gdm9jYWJzOiB7IGNoYXJUb0lkLCBzY3JpcHRUb0lkLCBsYW5nVG9JZCB9IOKAlCB0aGUgKl90b19pZCBtYXBzIGZyb20gdGhlIEpTT04gdm9jYWIgZmlsZXMuXG5leHBvcnQgZnVuY3Rpb24gdG9rZW5pc2UodGV4dCwgbGFuZywgdm9jYWJzKSB7XG4gIGNvbnN0IGNoYXJUb0lkID0gdm9jYWJzLmNoYXJUb0lkLCBzY3JpcHRUb0lkID0gdm9jYWJzLnNjcmlwdFRvSWQsIGxhbmdUb0lkID0gdm9jYWJzLmxhbmdUb0lkO1xuICBjb25zdCB1bmtDaGFyID0gY2hhclRvSWRbJzxVTks+J10gIT0gbnVsbCA/IGNoYXJUb0lkWyc8VU5LPiddIDogMTtcbiAgY29uc3QgdW5rTGFuZyA9IGxhbmdUb0lkWyc8VU5LPiddICE9IG51bGwgPyBsYW5nVG9JZFsnPFVOSz4nXSA6IDA7XG4gIGNvbnN0IGNoYXJzID0gQXJyYXkuZnJvbSh0ZXh0KTsgLy8gaXRlcmF0ZSBieSBjb2RlcG9pbnQsIG1hdGNoaW5nIFB5dGhvbiBzdHIgaXRlcmF0aW9uXG4gIGNvbnN0IGNoYXJJZHMgPSBuZXcgSW50MzJBcnJheShjaGFycy5sZW5ndGgpO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNoYXJzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgaWQgPSBjaGFyVG9JZFtjaGFyc1tpXV07XG4gICAgY2hhcklkc1tpXSA9IGlkICE9IG51bGwgPyBpZCA6IHVua0NoYXI7XG4gIH1cbiAgY29uc3Qgc2NyaXB0SWQgPSBzY3JpcHRUb0lkW2RldGVjdFNjcmlwdCh0ZXh0KV07XG4gIGNvbnN0IGxhbmdJZCA9IGxhbmdUb0lkW2xhbmddO1xuICByZXR1cm4ge1xuICAgIGNoYXJJZHMsXG4gICAgc2NyaXB0SWQ6IHNjcmlwdElkICE9IG51bGwgPyBzY3JpcHRJZCA6IDAsXG4gICAgbGFuZ0lkOiBsYW5nSWQgIT0gbnVsbCA/IGxhbmdJZCA6IHVua0xhbmcsXG4gICAgbGVuZ3RoOiBjaGFycy5sZW5ndGgsXG4gIH07XG59XG4iXSwibmFtZXMiOltdLCJzb3VyY2VSb290IjoiIn0=\n//# sourceURL=webpack-internal:///./whg/webpack/js/recon-symphonym-preprocess.js\n");

/***/ }),

/***/ "./whg/webpack/js/recon-symphonym.worker.js":
/*!**************************************************!*\
  !*** ./whg/webpack/js/recon-symphonym.worker.js ***!
  \**************************************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n/* harmony import */ var onnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! onnxruntime-web/wasm */ \"./node_modules/onnxruntime-web/dist/ort.wasm.bundle.min.mjs?361e\");\n/* harmony import */ var _recon_symphonym_preprocess_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./recon-symphonym-preprocess.js */ \"./whg/webpack/js/recon-symphonym-preprocess.js\");\n// recon-symphonym.worker.js\n// In-browser Symphonym phonetic encoder (Phase 7). Runs the int8 ONNX Student encoder via\n// onnxruntime-web (WASM backend, single-threaded — no SharedArrayBuffer / cross-origin isolation\n// needed) to embed toponyms, then clusters near-duplicate / phonetically-similar names by cosine\n// similarity. All local; nothing leaves the browser. Assets are self-hosted under /static/webpack/symphonym/.\n\n\n\n\nconst BASE = '/static/webpack/symphonym/';\nonnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__.env.wasm.wasmPaths = BASE;      // self-hosted ort-wasm-simd-threaded.{wasm,mjs}\nonnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__.env.wasm.numThreads = 1;        // single-threaded: no SAB, works without COOP/COEP headers\n\nlet session = null;\nlet vocabs = null;\nlet ready = null;\n\nasync function init() {\n  if (ready) return ready;\n  ready = (async () => {\n    const [cv, sv, lv] = await Promise.all([\n      fetch(BASE + 'char_vocab.json').then((r) => r.json()),\n      fetch(BASE + 'script_vocab.json').then((r) => r.json()),\n      fetch(BASE + 'lang_vocab.json').then((r) => r.json()),\n    ]);\n    vocabs = {\n      charToId: cv.char_to_id || cv,\n      scriptToId: sv.script_to_id || sv,\n      langToId: lv.lang_to_id || lv,\n    };\n    session = await onnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__.InferenceSession.create(BASE + 'symphonym.onnx', { executionProviders: ['wasm'] });\n  })();\n  return ready;\n}\n\nconst unkChar = () => (vocabs.charToId['<UNK>'] != null ? vocabs.charToId['<UNK>'] : 1);\n\n// Embed one (text, lang) → Float32Array(128), L2-normalised (batch=1; the LSTM export is batch-1).\nasync function embedOne(text, lang) {\n  const t = (0,_recon_symphonym_preprocess_js__WEBPACK_IMPORTED_MODULE_1__.tokenise)(String(text || ''), lang || 'und', vocabs);\n  const ids = t.length ? t.charIds : new Int32Array([unkChar()]); // never feed a zero-length sequence\n  const len = ids.length;\n  const charIds = BigInt64Array.from(ids, (v) => BigInt(v));\n  const feeds = {\n    char_ids: new onnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__.Tensor('int64', charIds, [1, len]),\n    script_id: new onnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__.Tensor('int64', BigInt64Array.from([BigInt(t.scriptId)]), [1]),\n    lang_id: new onnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__.Tensor('int64', BigInt64Array.from([BigInt(t.langId)]), [1]),\n    length: new onnxruntime_web_wasm__WEBPACK_IMPORTED_MODULE_0__.Tensor('int64', BigInt64Array.from([BigInt(len)]), [1]),\n  };\n  const out = await session.run(feeds);\n  return out.embedding.data; // Float32Array(128)\n}\n\n// Embed a list of names, reporting progress. Returns Float32Array(N*128).\nasync function embedAll(names, lang) {\n  const N = names.length;\n  const embs = new Float32Array(N * 128);\n  for (let i = 0; i < N; i++) {\n    const e = await embedOne(names[i], lang);\n    embs.set(e, i * 128);\n    if ((i & 31) === 0 || i === N - 1) self.postMessage({ type: 'progress', done: i + 1, total: N });\n  }\n  return embs;\n}\n\nfunction cosine(a, ai, b, bi) {\n  let dot = 0;\n  for (let k = 0; k < 128; k++) dot += a[ai * 128 + k] * b[bi * 128 + k]; // both L2-normalised → dot = cosine\n  return dot;\n}\n\n// Single-link clustering by cosine ≥ threshold (union-find). Returns groups of size ≥ 2.\nfunction cluster(embs, N, threshold) {\n  const parent = new Int32Array(N).map((_, i) => i);\n  const find = (x) => { while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; };\n  const union = (a, b) => { const ra = find(a), rb = find(b); if (ra !== rb) parent[ra] = rb; };\n  for (let i = 0; i < N; i++) {\n    for (let j = i + 1; j < N; j++) {\n      if (cosine(embs, i, embs, j) >= threshold) union(i, j);\n    }\n  }\n  const groups = new Map();\n  for (let i = 0; i < N; i++) { const r = find(i); if (!groups.has(r)) groups.set(r, []); groups.get(r).push(i); }\n  return [...groups.values()].filter((g) => g.length > 1);\n}\n\nself.onmessage = async (e) => {\n  const msg = e.data || {};\n  try {\n    await init();\n    if (msg.type === 'cluster') {\n      const names = msg.names || [];\n      const embs = await embedAll(names, msg.lang);\n      const groups = cluster(embs, names.length, msg.threshold != null ? msg.threshold : 0.9);\n      self.postMessage({ type: 'clusters', id: msg.id, groups });\n    } else if (msg.type === 'embed') {\n      const embs = await embedAll(msg.names || [], msg.lang);\n      self.postMessage({ type: 'embeddings', id: msg.id, embs }, [embs.buffer]);\n    }\n  } catch (err) {\n    self.postMessage({ type: 'error', id: msg.id, error: String((err && err.message) || err) });\n  }\n};\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiLi93aGcvd2VicGFjay9qcy9yZWNvbi1zeW1waG9ueW0ud29ya2VyLmpzIiwibWFwcGluZ3MiOiI7OztBQUFBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsMEJBQTBCOztBQUVrQjtBQUNlOztBQUUzRDtBQUNBLHFEQUFPLDZCQUE2Qix1Q0FBdUM7QUFDM0UscURBQU8sNkJBQTZCOztBQUVwQztBQUNBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxvQkFBb0Isa0VBQW9CLG1DQUFtQyw4QkFBOEI7QUFDekcsR0FBRztBQUNIO0FBQ0E7O0FBRUE7O0FBRUEsdUVBQXVFO0FBQ3ZFO0FBQ0EsWUFBWSx3RUFBUTtBQUNwQixrRUFBa0U7QUFDbEU7QUFDQTtBQUNBO0FBQ0Esa0JBQWtCLHdEQUFVO0FBQzVCLG1CQUFtQix3REFBVTtBQUM3QixpQkFBaUIsd0RBQVU7QUFDM0IsZ0JBQWdCLHdEQUFVO0FBQzFCO0FBQ0E7QUFDQSw2QkFBNkI7QUFDN0I7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQSxrQkFBa0IsT0FBTztBQUN6QjtBQUNBO0FBQ0EsMERBQTBELHlDQUF5QztBQUNuRztBQUNBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBLGtCQUFrQixTQUFTLCtDQUErQztBQUMxRTtBQUNBOztBQUVBO0FBQ0E7QUFDQTtBQUNBLHdCQUF3QiwwQkFBMEIsK0JBQStCLGlCQUFpQjtBQUNsRyw0QkFBNEIsa0NBQWtDO0FBQzlELGtCQUFrQixPQUFPO0FBQ3pCLHdCQUF3QixPQUFPO0FBQy9CO0FBQ0E7QUFDQTtBQUNBO0FBQ0Esa0JBQWtCLE9BQU8sT0FBTyxtQkFBbUIsdUNBQXVDO0FBQzFGO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLHlCQUF5QixzQ0FBc0M7QUFDL0QsTUFBTTtBQUNOO0FBQ0EseUJBQXlCLHNDQUFzQztBQUMvRDtBQUNBLElBQUk7QUFDSix1QkFBdUIsdUVBQXVFO0FBQzlGO0FBQ0EiLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly93aGctd2VicGFjay8uL3doZy93ZWJwYWNrL2pzL3JlY29uLXN5bXBob255bS53b3JrZXIuanM/YzQwNCJdLCJzb3VyY2VzQ29udGVudCI6WyIvLyByZWNvbi1zeW1waG9ueW0ud29ya2VyLmpzXG4vLyBJbi1icm93c2VyIFN5bXBob255bSBwaG9uZXRpYyBlbmNvZGVyIChQaGFzZSA3KS4gUnVucyB0aGUgaW50OCBPTk5YIFN0dWRlbnQgZW5jb2RlciB2aWFcbi8vIG9ubnhydW50aW1lLXdlYiAoV0FTTSBiYWNrZW5kLCBzaW5nbGUtdGhyZWFkZWQg4oCUIG5vIFNoYXJlZEFycmF5QnVmZmVyIC8gY3Jvc3Mtb3JpZ2luIGlzb2xhdGlvblxuLy8gbmVlZGVkKSB0byBlbWJlZCB0b3BvbnltcywgdGhlbiBjbHVzdGVycyBuZWFyLWR1cGxpY2F0ZSAvIHBob25ldGljYWxseS1zaW1pbGFyIG5hbWVzIGJ5IGNvc2luZVxuLy8gc2ltaWxhcml0eS4gQWxsIGxvY2FsOyBub3RoaW5nIGxlYXZlcyB0aGUgYnJvd3Nlci4gQXNzZXRzIGFyZSBzZWxmLWhvc3RlZCB1bmRlciAvc3RhdGljL3dlYnBhY2svc3ltcGhvbnltLy5cblxuaW1wb3J0ICogYXMgb3J0IGZyb20gJ29ubnhydW50aW1lLXdlYi93YXNtJztcbmltcG9ydCB7IHRva2VuaXNlIH0gZnJvbSAnLi9yZWNvbi1zeW1waG9ueW0tcHJlcHJvY2Vzcy5qcyc7XG5cbmNvbnN0IEJBU0UgPSAnL3N0YXRpYy93ZWJwYWNrL3N5bXBob255bS8nO1xub3J0LmVudi53YXNtLndhc21QYXRocyA9IEJBU0U7ICAgICAgLy8gc2VsZi1ob3N0ZWQgb3J0LXdhc20tc2ltZC10aHJlYWRlZC57d2FzbSxtanN9XG5vcnQuZW52Lndhc20ubnVtVGhyZWFkcyA9IDE7ICAgICAgICAvLyBzaW5nbGUtdGhyZWFkZWQ6IG5vIFNBQiwgd29ya3Mgd2l0aG91dCBDT09QL0NPRVAgaGVhZGVyc1xuXG5sZXQgc2Vzc2lvbiA9IG51bGw7XG5sZXQgdm9jYWJzID0gbnVsbDtcbmxldCByZWFkeSA9IG51bGw7XG5cbmFzeW5jIGZ1bmN0aW9uIGluaXQoKSB7XG4gIGlmIChyZWFkeSkgcmV0dXJuIHJlYWR5O1xuICByZWFkeSA9IChhc3luYyAoKSA9PiB7XG4gICAgY29uc3QgW2N2LCBzdiwgbHZdID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZmV0Y2goQkFTRSArICdjaGFyX3ZvY2FiLmpzb24nKS50aGVuKChyKSA9PiByLmpzb24oKSksXG4gICAgICBmZXRjaChCQVNFICsgJ3NjcmlwdF92b2NhYi5qc29uJykudGhlbigocikgPT4gci5qc29uKCkpLFxuICAgICAgZmV0Y2goQkFTRSArICdsYW5nX3ZvY2FiLmpzb24nKS50aGVuKChyKSA9PiByLmpzb24oKSksXG4gICAgXSk7XG4gICAgdm9jYWJzID0ge1xuICAgICAgY2hhclRvSWQ6IGN2LmNoYXJfdG9faWQgfHwgY3YsXG4gICAgICBzY3JpcHRUb0lkOiBzdi5zY3JpcHRfdG9faWQgfHwgc3YsXG4gICAgICBsYW5nVG9JZDogbHYubGFuZ190b19pZCB8fCBsdixcbiAgICB9O1xuICAgIHNlc3Npb24gPSBhd2FpdCBvcnQuSW5mZXJlbmNlU2Vzc2lvbi5jcmVhdGUoQkFTRSArICdzeW1waG9ueW0ub25ueCcsIHsgZXhlY3V0aW9uUHJvdmlkZXJzOiBbJ3dhc20nXSB9KTtcbiAgfSkoKTtcbiAgcmV0dXJuIHJlYWR5O1xufVxuXG5jb25zdCB1bmtDaGFyID0gKCkgPT4gKHZvY2Ficy5jaGFyVG9JZFsnPFVOSz4nXSAhPSBudWxsID8gdm9jYWJzLmNoYXJUb0lkWyc8VU5LPiddIDogMSk7XG5cbi8vIEVtYmVkIG9uZSAodGV4dCwgbGFuZykg4oaSIEZsb2F0MzJBcnJheSgxMjgpLCBMMi1ub3JtYWxpc2VkIChiYXRjaD0xOyB0aGUgTFNUTSBleHBvcnQgaXMgYmF0Y2gtMSkuXG5hc3luYyBmdW5jdGlvbiBlbWJlZE9uZSh0ZXh0LCBsYW5nKSB7XG4gIGNvbnN0IHQgPSB0b2tlbmlzZShTdHJpbmcodGV4dCB8fCAnJyksIGxhbmcgfHwgJ3VuZCcsIHZvY2Ficyk7XG4gIGNvbnN0IGlkcyA9IHQubGVuZ3RoID8gdC5jaGFySWRzIDogbmV3IEludDMyQXJyYXkoW3Vua0NoYXIoKV0pOyAvLyBuZXZlciBmZWVkIGEgemVyby1sZW5ndGggc2VxdWVuY2VcbiAgY29uc3QgbGVuID0gaWRzLmxlbmd0aDtcbiAgY29uc3QgY2hhcklkcyA9IEJpZ0ludDY0QXJyYXkuZnJvbShpZHMsICh2KSA9PiBCaWdJbnQodikpO1xuICBjb25zdCBmZWVkcyA9IHtcbiAgICBjaGFyX2lkczogbmV3IG9ydC5UZW5zb3IoJ2ludDY0JywgY2hhcklkcywgWzEsIGxlbl0pLFxuICAgIHNjcmlwdF9pZDogbmV3IG9ydC5UZW5zb3IoJ2ludDY0JywgQmlnSW50NjRBcnJheS5mcm9tKFtCaWdJbnQodC5zY3JpcHRJZCldKSwgWzFdKSxcbiAgICBsYW5nX2lkOiBuZXcgb3J0LlRlbnNvcignaW50NjQnLCBCaWdJbnQ2NEFycmF5LmZyb20oW0JpZ0ludCh0LmxhbmdJZCldKSwgWzFdKSxcbiAgICBsZW5ndGg6IG5ldyBvcnQuVGVuc29yKCdpbnQ2NCcsIEJpZ0ludDY0QXJyYXkuZnJvbShbQmlnSW50KGxlbildKSwgWzFdKSxcbiAgfTtcbiAgY29uc3Qgb3V0ID0gYXdhaXQgc2Vzc2lvbi5ydW4oZmVlZHMpO1xuICByZXR1cm4gb3V0LmVtYmVkZGluZy5kYXRhOyAvLyBGbG9hdDMyQXJyYXkoMTI4KVxufVxuXG4vLyBFbWJlZCBhIGxpc3Qgb2YgbmFtZXMsIHJlcG9ydGluZyBwcm9ncmVzcy4gUmV0dXJucyBGbG9hdDMyQXJyYXkoTioxMjgpLlxuYXN5bmMgZnVuY3Rpb24gZW1iZWRBbGwobmFtZXMsIGxhbmcpIHtcbiAgY29uc3QgTiA9IG5hbWVzLmxlbmd0aDtcbiAgY29uc3QgZW1icyA9IG5ldyBGbG9hdDMyQXJyYXkoTiAqIDEyOCk7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgTjsgaSsrKSB7XG4gICAgY29uc3QgZSA9IGF3YWl0IGVtYmVkT25lKG5hbWVzW2ldLCBsYW5nKTtcbiAgICBlbWJzLnNldChlLCBpICogMTI4KTtcbiAgICBpZiAoKGkgJiAzMSkgPT09IDAgfHwgaSA9PT0gTiAtIDEpIHNlbGYucG9zdE1lc3NhZ2UoeyB0eXBlOiAncHJvZ3Jlc3MnLCBkb25lOiBpICsgMSwgdG90YWw6IE4gfSk7XG4gIH1cbiAgcmV0dXJuIGVtYnM7XG59XG5cbmZ1bmN0aW9uIGNvc2luZShhLCBhaSwgYiwgYmkpIHtcbiAgbGV0IGRvdCA9IDA7XG4gIGZvciAobGV0IGsgPSAwOyBrIDwgMTI4OyBrKyspIGRvdCArPSBhW2FpICogMTI4ICsga10gKiBiW2JpICogMTI4ICsga107IC8vIGJvdGggTDItbm9ybWFsaXNlZCDihpIgZG90ID0gY29zaW5lXG4gIHJldHVybiBkb3Q7XG59XG5cbi8vIFNpbmdsZS1saW5rIGNsdXN0ZXJpbmcgYnkgY29zaW5lIOKJpSB0aHJlc2hvbGQgKHVuaW9uLWZpbmQpLiBSZXR1cm5zIGdyb3VwcyBvZiBzaXplIOKJpSAyLlxuZnVuY3Rpb24gY2x1c3RlcihlbWJzLCBOLCB0aHJlc2hvbGQpIHtcbiAgY29uc3QgcGFyZW50ID0gbmV3IEludDMyQXJyYXkoTikubWFwKChfLCBpKSA9PiBpKTtcbiAgY29uc3QgZmluZCA9ICh4KSA9PiB7IHdoaWxlIChwYXJlbnRbeF0gIT09IHgpIHsgcGFyZW50W3hdID0gcGFyZW50W3BhcmVudFt4XV07IHggPSBwYXJlbnRbeF07IH0gcmV0dXJuIHg7IH07XG4gIGNvbnN0IHVuaW9uID0gKGEsIGIpID0+IHsgY29uc3QgcmEgPSBmaW5kKGEpLCByYiA9IGZpbmQoYik7IGlmIChyYSAhPT0gcmIpIHBhcmVudFtyYV0gPSByYjsgfTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBOOyBpKyspIHtcbiAgICBmb3IgKGxldCBqID0gaSArIDE7IGogPCBOOyBqKyspIHtcbiAgICAgIGlmIChjb3NpbmUoZW1icywgaSwgZW1icywgaikgPj0gdGhyZXNob2xkKSB1bmlvbihpLCBqKTtcbiAgICB9XG4gIH1cbiAgY29uc3QgZ3JvdXBzID0gbmV3IE1hcCgpO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IE47IGkrKykgeyBjb25zdCByID0gZmluZChpKTsgaWYgKCFncm91cHMuaGFzKHIpKSBncm91cHMuc2V0KHIsIFtdKTsgZ3JvdXBzLmdldChyKS5wdXNoKGkpOyB9XG4gIHJldHVybiBbLi4uZ3JvdXBzLnZhbHVlcygpXS5maWx0ZXIoKGcpID0+IGcubGVuZ3RoID4gMSk7XG59XG5cbnNlbGYub25tZXNzYWdlID0gYXN5bmMgKGUpID0+IHtcbiAgY29uc3QgbXNnID0gZS5kYXRhIHx8IHt9O1xuICB0cnkge1xuICAgIGF3YWl0IGluaXQoKTtcbiAgICBpZiAobXNnLnR5cGUgPT09ICdjbHVzdGVyJykge1xuICAgICAgY29uc3QgbmFtZXMgPSBtc2cubmFtZXMgfHwgW107XG4gICAgICBjb25zdCBlbWJzID0gYXdhaXQgZW1iZWRBbGwobmFtZXMsIG1zZy5sYW5nKTtcbiAgICAgIGNvbnN0IGdyb3VwcyA9IGNsdXN0ZXIoZW1icywgbmFtZXMubGVuZ3RoLCBtc2cudGhyZXNob2xkICE9IG51bGwgPyBtc2cudGhyZXNob2xkIDogMC45KTtcbiAgICAgIHNlbGYucG9zdE1lc3NhZ2UoeyB0eXBlOiAnY2x1c3RlcnMnLCBpZDogbXNnLmlkLCBncm91cHMgfSk7XG4gICAgfSBlbHNlIGlmIChtc2cudHlwZSA9PT0gJ2VtYmVkJykge1xuICAgICAgY29uc3QgZW1icyA9IGF3YWl0IGVtYmVkQWxsKG1zZy5uYW1lcyB8fCBbXSwgbXNnLmxhbmcpO1xuICAgICAgc2VsZi5wb3N0TWVzc2FnZSh7IHR5cGU6ICdlbWJlZGRpbmdzJywgaWQ6IG1zZy5pZCwgZW1icyB9LCBbZW1icy5idWZmZXJdKTtcbiAgICB9XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIHNlbGYucG9zdE1lc3NhZ2UoeyB0eXBlOiAnZXJyb3InLCBpZDogbXNnLmlkLCBlcnJvcjogU3RyaW5nKChlcnIgJiYgZXJyLm1lc3NhZ2UpIHx8IGVycikgfSk7XG4gIH1cbn07XG4iXSwibmFtZXMiOltdLCJzb3VyY2VSb290IjoiIn0=\n//# sourceURL=webpack-internal:///./whg/webpack/js/recon-symphonym.worker.js\n");

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
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = __webpack_modules__;
/******/ 	
/******/ 	// the startup function
/******/ 	__webpack_require__.x = () => {
/******/ 		// Load entry module and return exports
/******/ 		// This entry module depends on other loaded chunks and execution need to be delayed
/******/ 		var __webpack_exports__ = __webpack_require__.O(undefined, ["vendors-node_modules_onnxruntime-web_dist_ort_wasm_bundle_min_mjs"], () => (__webpack_require__("./whg/webpack/js/recon-symphonym.worker.js")))
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
/******/ 		__webpack_require__.b = self.location + "";
/******/ 		
/******/ 		// object to store loaded chunks
/******/ 		// "1" means "already loaded"
/******/ 		var installedChunks = {
/******/ 			"whg_webpack_js_recon-symphonym_worker_js": 1
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
/******/ 			return __webpack_require__.e("vendors-node_modules_onnxruntime-web_dist_ort_wasm_bundle_min_mjs").then(next);
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