import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, "../../..");
const payloadPath = path.join(repo, "research/module5_final_submission/results/workbook_payload.json");
const outputPath = path.join(repo, "submission/NIFTY_VRP_Research_Tearsheet.xlsx");
const renderDir = path.join(repo, ".tmp/module5_workbook_renders");
const payload = JSON.parse(await fs.readFile(payloadPath, "utf8"));

const wb = Workbook.create();
const sheetNames = [
  "Cover", "Tearsheet", "Trades", "Equity", "Monthly", "Annual", "Drawdowns",
  "Capacity", "Exec Decay", "Nov24 Break", "Events", "Live Monitor", "Sources", "Checks",
];
const sheets = Object.fromEntries(sheetNames.map((name) => [name, wb.worksheets.add(name)]));
const navy = "#0F172A";
const blue = "#1D4ED8";
const pale = "#EFF6FF";
const green = "#166534";
const red = "#B91C1C";
const headerFormat = { fill: navy, font: { bold: true, color: "#FFFFFF" }, verticalAlignment: "center" };
const sectionFormat = { fill: blue, font: { bold: true, color: "#FFFFFF" } };

function colLetter(n) {
  let out = "";
  while (n > 0) { n -= 1; out = String.fromCharCode(65 + (n % 26)) + out; n = Math.floor(n / 26); }
  return out;
}

function clean(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && !Number.isFinite(value)) return null;
  return value;
}

function addTable(sheet, records, preferred = null) {
  if (!records.length) return { headers: [], rows: 0 };
  const headers = preferred ?? Object.keys(records[0]);
  const matrix = [headers, ...records.map((row) => headers.map((key) => clean(row[key])))];
  const end = colLetter(headers.length);
  sheet.getRange(`A1:${end}${matrix.length}`).values = matrix;
  sheet.getRange(`A1:${end}1`).format = headerFormat;
  sheet.freezePanes.freezeRows(1);
  const used = sheet.getUsedRange();
  used.format.autofitColumns();
  used.format.autofitRows();
  for (let i = 0; i < headers.length; i += 1) {
    const range = sheet.getRange(`${colLetter(i + 1)}:${colLetter(i + 1)}`);
    range.format.columnWidth = Math.min(Math.max(11, headers[i].length + 2), 24);
  }
  return { headers, rows: records.length };
}

// Cover
const cover = sheets.Cover;
cover.mergeCells("A1:H2");
cover.getRange("A1").values = [["NIFTY Intraday VRP — Final Research Submission"]];
cover.getRange("A1:H2").format = { fill: navy, font: { bold: true, color: "#FFFFFF", size: 22 }, verticalAlignment: "center", horizontalAlignment: "center" };
cover.getRange("A4:B13").values = [
  ["Decision", "SHADOW ONLY — ZERO LIVE CAPITAL"],
  ["Base hypothesis", "Rejected net of costs at 60–180 minutes"],
  ["Post-hoc candidate", "Upper-85 short iron fly with frozen gates/sizing"],
  ["Starting capital", 1000000],
  ["Signals / executions", "132 / 86"],
  ["Dataset scope", "Nearest weekly NIFTY, rolling ATM±10, 2021–2026"],
  ["Entry / exit boundary", "ATM±3 at entry; maximum validated horizon 180 minutes"],
  ["Costs", "Date-aware Groww charges + quantity-aware slippage"],
  ["Margin", "Timestamp-aware SPAN; 35% policy ceiling"],
  ["Review order", "Tearsheet → Trades → robustness → Live Monitor"],
];
cover.getRange("A4:A13").format = { fill: pale, font: { bold: true, color: navy } };
cover.getRange("B4:B13").format.wrapText = true;
cover.getRange("A4:B13").format.autofitRows();
cover.getRange("A:A").format.columnWidth = 24;
cover.getRange("B:B").format.columnWidth = 68;
cover.getRange("B7").format.numberFormat = "₹#,##0";

// Data sheets first, so formulas can safely reference them.
const tradeCols = [
  "trade_id", "trade_date", "split", "event_week", "event_type", "structural_regime", "executed", "skip_reason",
  "structure", "entry_ts", "exit_ts", "entry_dte", "spot", "atm_iv", "trailing_rv_act365", "signal_vrp_var_act365",
  "vrp_tod_percentile", "confidence_score", "lots", "lot_size", "margin_rupees", "margin_utilization",
  "cash_risk_utilization", "gross_pnl_rupees", "charges_rupees", "slippage_rupees", "total_cost_rupees",
  "net_pnl_rupees", "turnover_rupees", "one_lot_net_pnl", "gate_cushion", "iv_change_5m", "iv_change_15m",
  "rv_change_5m", "put_skew", "call_skew", "risk_reversal", "smile_curvature", "drawdown_after_pct",
];
const tradeMeta = addTable(sheets.Trades, payload.trades, tradeCols);
const equityMeta = addTable(sheets.Equity, payload.equity);
addTable(sheets.Monthly, payload.monthly);
addTable(sheets.Annual, payload.annual);
addTable(sheets.Drawdowns, payload.drawdowns);
addTable(sheets.Capacity, payload.capacity);
addTable(sheets["Exec Decay"], payload.execution_decay);
addTable(sheets["Nov24 Break"], payload.structural_break);
addTable(sheets.Events, payload.event_conditioning);

// Formats on data sheets.
for (const name of ["Trades", "Equity", "Monthly", "Annual", "Drawdowns", "Capacity", "Exec Decay", "Nov24 Break", "Events"]) {
  sheets[name].getUsedRange().format.font = { name: "Aptos", size: 9 };
  sheets[name].getRange("A1:AZ1").format = headerFormat;
}
sheets.Trades.getRange(`N2:Q${tradeMeta.rows + 1}`).format.numberFormat = "0.0000";
sheets.Trades.getRange(`U2:AC${tradeMeta.rows + 1}`).format.numberFormat = "₹#,##0.00";
sheets.Trades.getRange(`V2:W${tradeMeta.rows + 1}`).format.numberFormat = "0.00%";
sheets.Equity.getRange(`D2:L${equityMeta.rows + 1}`).format.numberFormat = "₹#,##0.00";
sheets.Equity.getRange(`L2:M${equityMeta.rows + 1}`).format.numberFormat = "0.000%";

// Tearsheet: derived values are formulas, never pasted headline values.
const t = sheets.Tearsheet;
t.mergeCells("A1:D2");
t.getRange("A1").values = [["Formula-backed tear sheet"]];
t.getRange("A1:D2").format = { fill: navy, font: { bold: true, color: "#FFFFFF", size: 20 }, horizontalAlignment: "center", verticalAlignment: "center" };
t.getRange("A4:B4").values = [["Metric", "Formula result"]];
t.getRange("A4:B4").format = headerFormat;
const metricLabels = [
  "Candidate signals", "Executed trades", "Skipped signals", "Gross P&L", "Charges", "Modeled slippage", "Total costs", "Net P&L",
  "Starting capital", "Ending equity", "Total return", "CAGR", "Annualized volatility", "Sharpe", "Sortino", "Maximum drawdown",
  "Hit rate", "Average win", "Average loss", "Profit factor", "Turnover", "Cost drag / gross", "Average entry margin", "Return / average entry margin",
];
t.getRange(`A5:A${4 + metricLabels.length}`).values = metricLabels.map((x) => [x]);
t.getRange("B5:B28").formulas = [
  [`=COUNTA(Trades!A2:A${tradeMeta.rows + 1})`],
  [`=COUNTIF(Trades!S2:S${tradeMeta.rows + 1},">0")`],
  ["=B5-B6"],
  [`=SUM(Trades!X2:X${tradeMeta.rows + 1})`],
  [`=SUM(Trades!Y2:Y${tradeMeta.rows + 1})`],
  [`=SUM(Trades!Z2:Z${tradeMeta.rows + 1})`],
  [`=SUM(Trades!AA2:AA${tradeMeta.rows + 1})`],
  [`=SUM(Trades!AB2:AB${tradeMeta.rows + 1})`],
  ["=1000000"],
  ["=B13+B12"],
  ["=B12/B13"],
  [`=POWER(B14/B13,365.25/(DATEVALUE(Equity!A${equityMeta.rows + 1})-DATEVALUE(Equity!A2)))-1`],
  [`=STDEV.S(Equity!M2:M${equityMeta.rows + 1})*SQRT(252)`],
  [`=AVERAGE(Equity!M2:M${equityMeta.rows + 1})/STDEV.S(Equity!M2:M${equityMeta.rows + 1})*SQRT(252)`],
  [`=AVERAGE(Equity!M2:M${equityMeta.rows + 1})/SQRT(SUMPRODUCT((Equity!M2:M${equityMeta.rows + 1}<0)*(Equity!M2:M${equityMeta.rows + 1}^2))/COUNT(Equity!M2:M${equityMeta.rows + 1}))*SQRT(252)`],
  [`=MIN(Equity!L2:L${equityMeta.rows + 1})`],
  [`=COUNTIFS(Trades!S2:S${tradeMeta.rows + 1},">0",Trades!AB2:AB${tradeMeta.rows + 1},">0")/B6`],
  [`=AVERAGEIFS(Trades!AB2:AB${tradeMeta.rows + 1},Trades!S2:S${tradeMeta.rows + 1},">0",Trades!AB2:AB${tradeMeta.rows + 1},">0")`],
  [`=AVERAGEIFS(Trades!AB2:AB${tradeMeta.rows + 1},Trades!S2:S${tradeMeta.rows + 1},">0",Trades!AB2:AB${tradeMeta.rows + 1},"<=0")`],
  [`=SUMIFS(Trades!AB2:AB${tradeMeta.rows + 1},Trades!AB2:AB${tradeMeta.rows + 1},">0")/-SUMIFS(Trades!AB2:AB${tradeMeta.rows + 1},Trades!AB2:AB${tradeMeta.rows + 1},"<=0")`],
  [`=SUM(Trades!AC2:AC${tradeMeta.rows + 1})`],
  ["=B11/B8"],
  [`=AVERAGEIF(Trades!S2:S${tradeMeta.rows + 1},">0",Trades!U2:U${tradeMeta.rows + 1})`],
  ["=B12/B27"],
];
t.getRange("A5:A28").format = { fill: pale, font: { bold: true, color: navy } };
t.getRange("B8:B14").format.numberFormat = "₹#,##0.00";
t.getRange("B15:B17").format.numberFormat = "0.00%";
t.getRange("B18:B19").format.numberFormat = "0.00";
t.getRange("B20:B21").format.numberFormat = "0.00%";
t.getRange("B22:B23").format.numberFormat = "₹#,##0.00";
t.getRange("B24").format.numberFormat = "0.00";
t.getRange("B25").format.numberFormat = "₹#,##0.00";
t.getRange("B26").format.numberFormat = "0.00%";
t.getRange("B27").format.numberFormat = "₹#,##0.00";
t.getRange("B28").format.numberFormat = "0.00%";
t.getRange("A:A").format.columnWidth = 34;
t.getRange("B:B").format.columnWidth = 22;
t.getRange("D4:H4").merge();
t.getRange("D4").values = [["Decision and interpretation"]];
t.getRange("D4:H4").format = sectionFormat;
t.getRange("D5:H12").merge();
t.getRange("D5").values = [["Base VRP hypothesis rejected net of costs. The positive upper-85 short-iron-fly curve is post-hoc and remains a shadow-only candidate. Zero live capital until 100 new non-overlapping trades over 12 months pass the frozen promotion gates."]];
t.getRange("D5:H12").format = { fill: "#FEF2F2", font: { bold: true, color: red }, wrapText: true, verticalAlignment: "center" };
const monthlyHelper = [["month", "ending_equity_rupees"], ...payload.monthly.map((row) => [row.month, row.ending_equity_rupees])];
t.getRange(`J1:K${monthlyHelper.length}`).values = monthlyHelper;
const tearChart = t.charts.add("line", t.getRange(`J1:K${monthlyHelper.length}`));
tearChart.title = "Monthly ending equity"; tearChart.hasLegend = false; tearChart.setPosition("D14", "I28");

// Charts.
const equityHelper = [["date", "equity_rupees"], ...payload.equity.map((row) => [row.date, row.equity_rupees])];
sheets.Equity.getRange(`O1:P${equityMeta.rows + 1}`).values = equityHelper;
const eqChart = sheets.Equity.charts.add("line", sheets.Equity.getRange(`O1:P${equityMeta.rows + 1}`));
eqChart.title = "Daily equity curve"; eqChart.hasLegend = false; eqChart.setPosition("R2", "Z20");
const decayRows = payload.execution_decay.length + 1;
const decayHelper = [["slippage_multiplier", "net_pnl_rupees"], ...payload.execution_decay.map((row) => [row.slippage_multiplier, row.net_pnl_rupees])];
sheets["Exec Decay"].getRange(`J1:K${decayRows}`).values = decayHelper;
const decayChart = sheets["Exec Decay"].charts.add("line", sheets["Exec Decay"].getRange(`J1:K${decayRows}`));
decayChart.title = "Net P&L versus slippage multiplier"; decayChart.hasLegend = false; decayChart.setPosition("M2", "U18");

// Live monitor specification.
const live = sheets["Live Monitor"];
live.getRange("A1:D1").values = [["Control", "Threshold", "State", "Action"]];
live.getRange("A1:D1").format = headerFormat;
live.getRange("A2:D13").values = [
  ["Live allocation", "₹0", "SHADOW", "One-lot telemetry only"],
  ["Quote age", "≤ 2 minutes", "INPUT", "No entry if breached"],
  ["Margin utilization", "≤ 35%", "INPUT", "No entry if breached"],
  ["Cost-reserved cash risk", "≤ 4%", "INPUT", "No entry if breached"],
  ["Quality score", "> 40%", "INPUT", "No entry if breached"],
  ["Daily loss", "< 0.75%", "INPUT", "Stop new entries"],
  ["Peak-to-trough drawdown", "< 1.50%", "INPUT", "Flatten and review"],
  ["20-fill slippage / model", "≤ 1.50×", "INPUT", "Stop and recalibrate"],
  ["Single fill slippage / model", "≤ 3.00×", "INPUT", "Incident review"],
  ["Forward sample", "≥100 trades / 12 months", "PENDING", "Required for promotion"],
  ["Forward halves", "Both net positive", "PENDING", "Required for promotion"],
  ["Score rank CI", "95% lower bound > 0", "PENDING", "Required for promotion"],
];
live.getRange("A2:A13").format = { fill: pale, font: { bold: true } };
live.getRange("A1:D13").format.autofitColumns();
live.getRange("A:A").format.columnWidth = 29; live.getRange("D:D").format.columnWidth = 29;

// Sources and formula checks.
addTable(sheets.Sources, [
  { source: "NSE contract specifications", url: "https://www.nseindia.com/static/products-services/equity-derivatives-contract-specifications", use: "Tuesday expiry and current weekly scope" },
  { source: "NSE FAOP70616", url: "https://nsearchives.nseindia.com/content/circulars/FAOP70616.pdf", use: "NIFTY lot 75 to 65" },
  { source: "SEBI derivatives measures", url: "https://www.sebi.gov.in/sebi_data/attachdocs/jul-2025/1751900271726.pdf", use: "November-2024 structural date" },
  { source: "RBI MPC archive", url: "https://www.rbi.org.in/scripts/Annualpolicy.aspx.html", use: "MPC event dates" },
  { source: "India Budget", url: "https://www.indiabudget.gov.in/", use: "Budget event dates" },
  { source: "Election Commission 2024", url: "https://elections24.eci.gov.in/", use: "4-Jun-2024 result date" },
]);
const checks = sheets.Checks;
checks.getRange("A1:C1").values = [["Check", "Formula", "Expected"]]; checks.getRange("A1:C1").format = headerFormat;
checks.getRange("A2:A6").values = [["Signals"], ["Executions"], ["Skips"], ["Gross-cost=net"], ["Decision"]];
checks.getRange("B2:B5").formulas = [["=Tearsheet!B5"], ["=Tearsheet!B6"], ["=Tearsheet!B7"], ["=ROUND(Tearsheet!B8-Tearsheet!B11-Tearsheet!B12,2)"]];
checks.getRange("B6").values = [["SHADOW_ONLY"]];
checks.getRange("C2:C6").values = [[132], [86], [46], [0], ["SHADOW_ONLY"]];
checks.getRange("A1:C6").format.autofitColumns();

await fs.mkdir(path.dirname(outputPath), { recursive: true });
await fs.mkdir(renderDir, { recursive: true });
const inspect = await wb.inspect({ kind: "table", range: "Tearsheet!A1:H28", include: "values,formulas", tableMaxRows: 35, tableMaxCols: 10 });
await fs.writeFile(path.join(renderDir, "inspection.json"), JSON.stringify(inspect, null, 2));
const errors = await wb.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A", options: { useRegex: true, maxResults: 200 }, summary: "formula error scan" });
await fs.writeFile(path.join(renderDir, "formula_errors.json"), JSON.stringify(errors, null, 2));
if ((errors.ndjson ?? "").includes('"kind":"match"')) throw new Error(`Formula error scan failed: ${errors.ndjson}`);
if (process.env.SKIP_RENDERS !== "1") {
  for (const name of sheetNames) {
    const largeRows = name === "Trades" ? tradeMeta.rows + 1 : name === "Equity" ? equityMeta.rows + 1 : 0;
    if (largeRows > 30) sheets[name].getRange(`A31:AZ${largeRows}`).format.rowHeight = 0;
    const preview = await wb.render({ sheetName: name, autoCrop: "all", scale: name === "Trades" || name === "Equity" ? 0.55 : 0.9, format: "png" });
    await fs.writeFile(path.join(renderDir, `${name.replaceAll(" ", "_")}.png`), new Uint8Array(await preview.arrayBuffer()));
    if (largeRows > 30) sheets[name].getRange(`A31:AZ${largeRows}`).format.rowHeight = 15;
  }
}
const xlsx = await SpreadsheetFile.exportXlsx(wb);
await xlsx.save(outputPath);
await fs.rm(`${outputPath}.inspect.ndjson`, { force: true });
console.log(JSON.stringify({ outputPath, renderDir, sheets: sheetNames, tradeRows: tradeMeta.rows }, null, 2));
