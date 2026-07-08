// recon-textextract.js — lazy DOCX / PDF → plain text for the Map-your-Data place-name extractor.
// Split into its own webpack chunks so mammoth (.docx) and pdf.js (.pdf) cost nothing until a user
// actually picks such a file. Both run entirely in the browser; only the extracted text is later
// sent to WHG's NER service (the UI says so).

export async function extractDocx(arrayBuffer) {
  const mod = await import(/* webpackChunkName: "recon-mammoth" */ 'mammoth/mammoth.browser');
  const mammoth = mod.default || mod;
  const res = await mammoth.extractRawText({ arrayBuffer });
  return (res && res.value) || '';
}

export async function extractPdf(arrayBuffer) {
  const pdfjs = await import(/* webpackChunkName: "recon-pdfjs" */ 'pdfjs-dist');
  // Let webpack bundle the pdf.js worker as its own chunk (served as .js, so nginx MIME is a non-issue)
  // and run it as a module worker — more robust than pointing workerSrc at the raw .mjs asset.
  pdfjs.GlobalWorkerOptions.workerPort = new Worker(
    new URL('pdfjs-dist/build/pdf.worker.min.mjs', import.meta.url), { type: 'module' },
  );
  const doc = await pdfjs.getDocument({ data: arrayBuffer }).promise;
  const parts = [];
  for (let n = 1; n <= doc.numPages; n += 1) {
    const page = await doc.getPage(n);
    const content = await page.getTextContent();
    parts.push(content.items.map((it) => it.str || '').join(' '));
  }
  try { await doc.destroy(); } catch (_) { /* ignore */ }
  return parts.join('\n\n');
}
