// recon-xlsx.js — Excel (.xlsx/.xls) parsing for the Map-your-Data importer.
//
// Lazy-loaded (its own webpack chunk) so SheetJS costs nothing until a user actually imports a
// spreadsheet — the initial workbench bundle stays small. Reads the first non-empty sheet into the
// same {columns, rows, total} shape the CSV/JSON importers produce. `raw:false` emits each cell's
// *displayed* text (formatted dates, numbers, …), so what lands in the table matches what the user
// sees in Excel.

import * as XLSX from 'xlsx';

export function parseWorkbook(arrayBuffer) {
  const wb = XLSX.read(arrayBuffer, { type: 'array' });
  if (!wb.SheetNames || !wb.SheetNames.length) throw new Error('The spreadsheet has no sheets.');

  // Use the first sheet that actually contains rows.
  let aoa = [];
  let usedSheet = wb.SheetNames[0];
  for (const name of wb.SheetNames) {
    const rows = XLSX.utils.sheet_to_json(wb.Sheets[name], {
      header: 1, raw: false, defval: '', blankrows: false,
    });
    if (rows.length) { aoa = rows; usedSheet = name; break; }
  }
  if (!aoa.length) throw new Error('The spreadsheet is empty.');

  const header = aoa[0] || [];
  const columns = header.map((h, i) => (h != null && String(h).trim()) || `column_${i + 1}`);
  const rows = aoa.slice(1).map((r) => columns.map((_, i) => (r[i] == null ? '' : String(r[i]))));
  return { columns, rows, total: rows.length, sheet: usedSheet };
}

// Round-trip export: rewrite ONE sheet of the original workbook with the current table, leaving every
// other sheet untouched, and return the .xlsx bytes. If `sourceBytes` is null (dataset didn't arrive
// as Excel) a fresh single-sheet workbook is produced instead.
export function writeWorkbook(sourceBytes, sheetName, columns, rows) {
  const wb = sourceBytes
    ? XLSX.read(sourceBytes, { type: 'array' })
    : XLSX.utils.book_new();
  const aoa = [columns].concat(rows);
  const ws = XLSX.utils.aoa_to_sheet(aoa);
  const name = sheetName && wb.SheetNames.includes(sheetName)
    ? sheetName
    : (sheetName || (wb.SheetNames[0]) || 'Sheet1');
  if (!wb.SheetNames.includes(name)) XLSX.utils.book_append_sheet(wb, ws, name);
  else wb.Sheets[name] = ws; // replace just this sheet's values; all others stay intact
  return XLSX.write(wb, { type: 'array', bookType: 'xlsx' }); // ArrayBuffer
}
