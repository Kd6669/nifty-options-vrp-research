import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { SpreadsheetFile } from "@oai/artifact-tool";

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, "../../..");
const input = path.join(repo, "submission/NIFTY_VRP_Research_Tearsheet.xlsx");
const bytes = await fs.readFile(input);
const arrayBuffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
const wb = await SpreadsheetFile.importXlsx(arrayBuffer);
const tear = await wb.inspect({ kind: "table", range: "Tearsheet!A4:B28", include: "values,formulas", tableMaxRows: 30, tableMaxCols: 3 });
const errors = await wb.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A", options: { useRegex: true, maxResults: 200 }, summary: "final workbook formula error scan" });
if ((errors.ndjson ?? "").includes('"kind":"match"')) throw new Error(errors.ndjson);
const text = tear.ndjson ?? "";
for (const expected of ['"Candidate signals",132', '"Executed trades",86', '"Skipped signals",46']) {
  if (!text.includes(expected)) throw new Error(`Missing expected workbook result: ${expected}`);
}
const preview = await wb.render({ sheetName: "Tearsheet", autoCrop: "all", scale: 0.9, format: "png" });
const previewPath = path.join(repo, ".tmp/module5_workbook_renders/Tearsheet_final.png");
await fs.writeFile(previewPath, new Uint8Array(await preview.arrayBuffer()));
console.log(JSON.stringify({ status: "verified", input, bytes: bytes.length, formulaErrors: 0 }, null, 2));
process.exitCode = 0;
