import { NextRequest, NextResponse } from "next/server";

export const runtime = "edge";
export const maxDuration = 60;

// ── Supabase config ───────────────────────────────────────────────────────────
const SB_URL = "https://cvbtnmqchpzgsjcjcorj.supabase.co";
const SB_KEY = process.env.SUPABASE_KEY!;

// ── Supabase REST query builder ───────────────────────────────────────────────
interface QueryOptions {
  select?: string;
  filters?: string[];   // PostgREST filter strings, e.g. "status=eq.aktiv"
  order?: string;
  limit?: number;
}

const SB_HEADERS = {
  Authorization: `Bearer ${SB_KEY}`,
  apikey: SB_KEY,
  Accept: "application/json",
} as Record<string, string>;

async function sbQuery(table: string, opts: QueryOptions): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (opts.select) params.set("select", opts.select);
  if (opts.order)  params.set("order", opts.order);
  if (opts.limit)  params.set("limit", String(opts.limit));

  const url = `${SB_URL}/rest/v1/${table}?${params}`;
  const filterUrl = opts.filters?.length ? `${url}&${opts.filters.join("&")}` : url;

  const res = await fetch(filterUrl, { headers: SB_HEADERS });
  if (!res.ok) throw new Error(`Supabase ${res.status}: ${await res.text()}`);
  return res.json();
}

async function sbRpc(fn: string, body: Record<string, unknown>): Promise<unknown> {
  const res = await fetch(`${SB_URL}/rest/v1/rpc/${fn}`, {
    method: "POST",
    headers: { ...SB_HEADERS, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Supabase RPC ${fn} ${res.status}: ${await res.text()}`);
  return res.json();
}

// ── Core fields to select for list views ─────────────────────────────────────
const LEAD_SELECT = "cvr,navn,branchekode,branchetekst,vejnavn,husnummer,postnummer,postdistrikt,kommunenavn,telefon,email,antal_ansatte";
const COMPANY_SELECT = "cvr,navn,status,stiftelsesdato,branchekode,branchetekst,virksomhedsform,vejnavn,husnummer,postnummer,postdistrikt,kommunenavn,kommunekode,telefon,email,antal_ansatte";

// ── Tool definitions ──────────────────────────────────────────────────────────
const TOOLS = [
  {
    name: "find_leads",
    description: "Find potentielle leads med kontaktinfo (telefon/email). Virksomheder der leverer og installerer — VVS, el, tømrer, maler, gulv, tag osv. Returnerer navn, adresse, kontakt, ansatte.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord, f.eks. 'VVS', 'tømrer', 'maler', 'elektro', 'gulv', 'tag', 'kloak'" },
        kommunenavn:      { type: "string", description: "Kommune/by, f.eks. 'Aarhus', 'Odense', 'Aalborg', 'København'" },
        postnummer:       { type: "string", description: "Eksakt postnummer, f.eks. '8000'" },
        min_ansatte:      { type: "number", description: "Minimum antal ansatte" },
        max_ansatte:      { type: "number", description: "Maksimum antal ansatte" },
        kun_med_kontakt:  { type: "boolean", description: "Kun firmaer med telefonnummer (default: true)", default: true },
        har_hjemmeside:   { type: "boolean", description: "Kun firmaer der sandsynligvis har egen hjemmeside (email på eget domæne, ikke gmail/hotmail osv.)" },
        limit:            { type: "number", description: "Max resultater (default 50, max 200)", default: 50 },
      },
      required: ["branche_contains"],
    },
  },
  {
    name: "find_companies",
    description: "Søg virksomheder med fleksible filtre — branche, kommune, postnummer, antal ansatte.",
    inputSchema: {
      type: "object",
      properties: {
        branchekode:      { type: "string", description: "Eksakt DB07 branchekode, f.eks. '432200'" },
        branche_contains: { type: "string", description: "Søg i branchetekst, f.eks. 'tømrer'" },
        kommunenavn:      { type: "string", description: "Kommunenavn" },
        postnummer:       { type: "string", description: "Postnummer" },
        min_ansatte:      { type: "number", description: "Min ansatte" },
        max_ansatte:      { type: "number", description: "Max ansatte" },
        har_hjemmeside:   { type: "boolean", description: "Kun firmaer der sandsynligvis har egen hjemmeside" },
        limit:            { type: "number", description: "Max resultater (default 50, max 200)", default: 50 },
      },
    },
  },
  {
    name: "count_companies",
    description: "Tæl aktive virksomheder med givne filtre. Hurtig markedsstørrelse-check.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Søg i branchetekst" },
        branchekode:      { type: "string", description: "Eksakt branchekode" },
        kommunenavn:      { type: "string", description: "Kommunenavn" },
        postnummer:       { type: "string", description: "Postnummer" },
        min_ansatte:      { type: "number", description: "Min ansatte" },
        max_ansatte:      { type: "number", description: "Max ansatte" },
      },
    },
  },
  {
    name: "get_company",
    description: "Hent fuld profil på én virksomhed via CVR nummer.",
    inputSchema: {
      type: "object",
      properties: { cvr_nummer: { type: "string", description: "CVR nummer (8 cifre)" } },
      required: ["cvr_nummer"],
    },
  },
  {
    name: "search_by_name",
    description: "Søg virksomheder efter firmanavn (delvist match).",
    inputSchema: {
      type: "object",
      properties: {
        navn:  { type: "string", description: "Firmanavn eller del deraf, f.eks. 'Hansen VVS'" },
        limit: { type: "number", description: "Max resultater (default 20)", default: 20 },
      },
      required: ["navn"],
    },
  },
  {
    name: "list_branches",
    description: "Find relevante branchekoder og -beskrivelser der matcher et søgeord.",
    inputSchema: {
      type: "object",
      properties: {
        contains: { type: "string", description: "Søgeord, f.eks. 'bygge', 'VVS', 'transport'" },
        limit:    { type: "number", description: "Max resultater (default 30)", default: 30 },
      },
      required: ["contains"],
    },
  },
  {
    name: "market_by_municipality",
    description: "Se antal virksomheder per kommune for en branche.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord" },
        branchekode:      { type: "string", description: "Eksakt branchekode" },
        min_ansatte:      { type: "number", description: "Kun firmaer med mindst X ansatte" },
        limit:            { type: "number", description: "Antal kommuner (default 20)", default: 20 },
      },
    },
  },
  {
    name: "employee_distribution",
    description: "Vis størrelsesfordeling (ansatte-histogram) for en branche.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord" },
        branchekode:      { type: "string", description: "Eksakt branchekode" },
        kommunenavn:      { type: "string", description: "Begræns til kommune" },
      },
    },
  },
  {
    name: "market_overview",
    description: "Samlet markedsoverblik for en branche — total antal, top-kommuner og underkategorier.",
    inputSchema: {
      type: "object",
      properties: { branche_contains: { type: "string", description: "Branche søgeord, f.eks. 'maler', 'VVS', 'tømrer'" } },
      required: ["branche_contains"],
    },
  },
  {
    name: "database_stats",
    description: "Hent komplet databaseoverblik i ét kald — total virksomhedsantal, top 25 brancher med antal, top 25 kommuner, selskabsformer og stiftelsesår fordeling. Brug dette til statistik og visualiseringer over hele CVR.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "top_branches",
    description: "Hent alle brancher med antal aktive virksomheder — komplet liste til grafer og sammenligninger. Ét kald returnerer op til 100 brancher.",
    inputSchema: {
      type: "object",
      properties: {
        min_count:   { type: "number", description: "Mindste antal virksomheder per branche (default 50)", default: 50 },
        max_results: { type: "number", description: "Max antal brancher (default 100)", default: 100 },
      },
    },
  },
  {
    name: "new_companies",
    description: "Find nyest oprettede virksomheder i en periode — sorteret efter stiftelsesdato, nyeste først. Kan filtreres på branche, kommune og dato.",
    inputSchema: {
      type: "object",
      properties: {
        from_date:   { type: "string", description: "Fra dato, f.eks. '2026-01-01'" },
        to_date:     { type: "string", description: "Til dato, f.eks. '2026-04-24'" },
        branche:     { type: "string", description: "Branche søgeord, f.eks. 'elektro', 'VVS'" },
        kode:        { type: "string", description: "Eksakt branchekode" },
        kommune:     { type: "string", description: "Kommunenavn" },
        max_results: { type: "number", description: "Max resultater (default 50)", default: 50 },
      },
    },
  },
  {
    name: "sql_query",
    description: `Kør en custom SQL SELECT direkte på companies-tabellen. Brug til kompleks analytik som andre tools ikke dækker — window functions, CTEs, avanceret GROUP BY, korrelationsanalyser osv.

Tabel: companies
Kolonner:
  cvr TEXT, navn TEXT, status TEXT ('aktiv'/'ophørt'),
  stiftelsesdato TEXT (YYYY-MM-DD format),
  vejnavn TEXT, husnummer TEXT, postnummer TEXT, postdistrikt TEXT,
  kommunenavn TEXT, kommunekode TEXT,
  branchekode TEXT, branchetekst TEXT, virksomhedsform TEXT,
  telefon TEXT, email TEXT,
  antal_ansatte INTEGER (null indtil Phase 2 import),
  has_website BOOLEAN

Eksempler:
  SELECT branchetekst, COUNT(*) FROM companies WHERE status='aktiv' GROUP BY branchetekst ORDER BY count DESC LIMIT 20
  WITH ranked AS (SELECT kommunenavn, branchetekst, COUNT(*) as n, RANK() OVER (PARTITION BY branchetekst ORDER BY COUNT(*) DESC) as r FROM companies WHERE status='aktiv' GROUP BY kommunenavn, branchetekst) SELECT * FROM ranked WHERE r <= 3
  SELECT EXTRACT(YEAR FROM stiftelsesdato::date) as aar, COUNT(*) FROM companies WHERE status='aktiv' AND stiftelsesdato IS NOT NULL GROUP BY aar ORDER BY aar`,
    inputSchema: {
      type: "object",
      properties: {
        query:     { type: "string", description: "SQL SELECT eller WITH (CTE) query" },
        row_limit: { type: "number", description: "Max antal rækker (default 500, max 1000)", default: 500 },
      },
      required: ["query"],
    },
  },
  {
    name: "find_by_postcode_range",
    description: "Find virksomheder i et geografisk postnummerinterval.",
    inputSchema: {
      type: "object",
      properties: {
        postnummer_fra:   { type: "string", description: "Start postnummer, f.eks. '8000'" },
        postnummer_til:   { type: "string", description: "Slut postnummer, f.eks. '8999'" },
        branche_contains: { type: "string", description: "Branche søgeord (valgfri)" },
        min_ansatte:      { type: "number", description: "Min ansatte" },
        max_ansatte:      { type: "number", description: "Max ansatte" },
        limit:            { type: "number", description: "Max resultater (default 50)", default: 50 },
      },
      required: ["postnummer_fra", "postnummer_til"],
    },
  },
];

// ── Shared query builder ──────────────────────────────────────────────────────
type Args = Record<string, unknown>;

function buildFilters(args: Args & { branche_contains?: string; branchekode?: string; kommunenavn?: string; postnummer?: string; min_ansatte?: number; max_ansatte?: number; kun_med_kontakt?: boolean; har_hjemmeside?: boolean }): string[] {
  const f: string[] = ["status=eq.aktiv"];

  if (args.branche_contains)
    f.push(`branchetekst=ilike.*${args.branche_contains}*`);
  if (args.branchekode)
    f.push(`branchekode=eq.${args.branchekode}`);
  if (args.kommunenavn)
    f.push(`kommunenavn=ilike.*${args.kommunenavn}*`);
  if (args.postnummer)
    f.push(`postnummer=eq.${args.postnummer}`);
  if (args.min_ansatte !== undefined)
    f.push(`antal_ansatte=gte.${args.min_ansatte}`);
  if (args.max_ansatte !== undefined)
    f.push(`antal_ansatte=lte.${args.max_ansatte}`);
  if (args.kun_med_kontakt !== false)
    f.push("telefon=not.is.null");
  if (args.har_hjemmeside === true)
    f.push("has_website=eq.true");

  return f;
}

// ── Formatting ────────────────────────────────────────────────────────────────
type Row = Record<string, string | number | null>;

function formatLead(r: Row, i: number): string {
  const adr = [r.vejnavn && `${r.vejnavn} ${r.husnummer ?? ""}`.trim(), r.postnummer, r.postdistrikt]
    .filter(Boolean).join(" ");
  return `${i + 1}. **${r.navn ?? "Ukendt"}** | CVR: ${r.cvr} | ${r.antal_ansatte ?? "?"} ans.\n   📍 ${adr || "–"}, ${r.kommunenavn ?? "–"}\n   📞 ${r.telefon ?? "–"} | ✉️ ${r.email ?? "–"}\n   🏭 ${r.branchetekst ?? "–"}`;
}

function formatCompany(r: Row): string {
  const adr = [r.vejnavn && `${r.vejnavn} ${r.husnummer ?? ""}`.trim(), r.postnummer, r.postdistrikt]
    .filter(Boolean).join(" ");
  return `**${r.navn ?? "Ukendt"}** (CVR: ${r.cvr})\n  Branche: ${r.branchetekst ?? "–"}${r.branchekode ? ` [${r.branchekode}]` : ""}\n  Adresse: ${adr || "–"}, ${r.kommunenavn ?? "–"}\n  Kontakt: ${[r.telefon, r.email].filter(Boolean).join(" | ") || "–"}\n  Ansatte: ${r.antal_ansatte ?? "ukendt"}`;
}

// ── Tool implementations ──────────────────────────────────────────────────────

async function find_leads(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const filters = buildFilters(args as Parameters<typeof buildFilters>[0]);

  const rows = await sbQuery("companies", {
    select: LEAD_SELECT,
    filters,
    order: args.min_ansatte !== undefined || args.max_ansatte !== undefined
      ? "antal_ansatte.desc.nullslast"
      : "cvr.asc",
    limit,
  }) as Row[];

  if (!rows.length)
    return `Ingen leads fundet for "${args.branche_contains}"${args.kommunenavn ? ` i ${args.kommunenavn}` : ""}.`;
  return `**${rows.length} leads** (${args.branche_contains}${args.kommunenavn ? `, ${args.kommunenavn}` : ""}):\n\n` +
    rows.map(formatLead).join("\n\n");
}

async function find_companies(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const filters = buildFilters(args as Parameters<typeof buildFilters>[0]);
  // find_companies doesn't require contact info
  const noContact = filters.filter(f => f !== "telefon=not.is.null");

  const rows = await sbQuery("companies", {
    select: COMPANY_SELECT,
    filters: noContact,
    limit,
  }) as Row[];

  if (!rows.length) return "Ingen virksomheder fundet med disse kriterier.";
  return `Fandt ${rows.length} virksomheder:\n\n` + rows.map(formatCompany).join("\n\n");
}

async function count_companies(args: Args): Promise<string> {
  const filters = buildFilters({ ...(args as Parameters<typeof buildFilters>[0]), kun_med_kontakt: false });
  const filterQs = filters.join("&");
  const url = `${SB_URL}/rest/v1/companies?select=cvr&${filterQs}&limit=1`;

  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${SB_KEY}`,
      apikey: SB_KEY,
      Accept: "application/json",
      Prefer: "count=exact",
    },
  });
  if (!res.ok) throw new Error(`Supabase ${res.status}`);

  const count = res.headers.get("content-range")?.split("/")[1] ?? "?";
  const label = [
    args.branche_contains ? `"${args.branche_contains}"` : null,
    args.branchekode ? `kode ${args.branchekode}` : null,
    args.kommunenavn ? String(args.kommunenavn) : null,
    args.postnummer ? `postnr ${args.postnummer}` : null,
    args.min_ansatte !== undefined ? `≥${args.min_ansatte} ans.` : null,
    args.max_ansatte !== undefined ? `≤${args.max_ansatte} ans.` : null,
  ].filter(Boolean).join(", ");

  return `**${count}** aktive virksomheder${label ? ` — ${label}` : ""}`;
}

async function get_company(args: Args): Promise<string> {
  const rows = await sbQuery("companies", {
    select: COMPANY_SELECT,
    filters: [`cvr=eq.${args.cvr_nummer}`],
    limit: 1,
  }) as Row[];

  if (!rows.length) return `Ingen virksomhed fundet med CVR ${args.cvr_nummer}`;
  const r = rows[0];
  const adr = [r.vejnavn && `${r.vejnavn} ${r.husnummer ?? ""}`.trim(), r.postnummer, r.postdistrikt]
    .filter(Boolean).join(" ");
  return [
    `**${r.navn ?? "Ukendt"}**`,
    `CVR: ${r.cvr}  |  Form: ${r.virksomhedsform ?? "–"}  |  Status: ${r.status ?? "–"}`,
    `Startet: ${r.stiftelsesdato ?? "–"}`,
    ``,
    `Branche: ${r.branchetekst ?? "–"} [${r.branchekode ?? "–"}]`,
    `Adresse: ${adr || "–"}, ${r.kommunenavn ?? "–"} (${r.kommunekode ?? "–"})`,
    ``,
    `📞 ${r.telefon ?? "–"}  |  ✉️ ${r.email ?? "–"}`,
    ``,
    `Ansatte: ${r.antal_ansatte ?? "ukendt (Phase 2 data)"}`,
  ].join("\n");
}

async function search_by_name(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 20), 100);
  const rows = await sbQuery("companies", {
    select: LEAD_SELECT,
    filters: [`navn=ilike.*${args.navn}*`, "status=eq.aktiv"],
    limit,
  }) as Row[];

  if (!rows.length) return `Ingen virksomheder fundet med navn der indeholder "${args.navn}"`;
  return `Fandt ${rows.length} virksomheder:\n\n` +
    rows.map(r => {
      return `**${r.navn}** (CVR: ${r.cvr})\n  ${r.branchetekst ?? "–"} | ${r.postnummer ?? ""} ${r.postdistrikt ?? r.kommunenavn ?? "–"}\n  📞 ${r.telefon ?? "–"} | ${r.antal_ansatte ?? "?"} ans.`;
    }).join("\n\n");
}

async function list_branches(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 30), 100);
  const rows = await sbRpc("list_branches_agg", {
    search_term: String(args.contains),
    max_results: limit,
  }) as Row[];

  if (!rows.length) return `Ingen brancher fundet med "${args.contains}"`;
  return `Brancher der matcher "${args.contains}":\n\n` +
    rows.map(r => `**${r.branchetekst}** [${r.branchekode}] — ${r.antal} virksomheder`).join("\n");
}

async function market_by_municipality(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 20), 50);
  const rows = await sbRpc("market_by_municipality_agg", {
    search_term: args.branche_contains ?? null,
    kode:        args.branchekode ?? null,
    min_emp:     args.min_ansatte ?? null,
    max_results: limit,
  }) as Row[];

  if (!rows.length) return "Ingen virksomheder fundet med disse filtre.";
  const total = rows.reduce((s, r) => s + Number(r.antal ?? 0), 0);
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  return `**"${label}" per kommune** (top ${rows.length}, total vist: ${total}):\n\n` +
    rows.map(r => `${r.kommunenavn}: **${r.antal}**`).join("\n");
}

async function employee_distribution(args: Args): Promise<string> {
  const rows = await sbRpc("employee_distribution_agg", {
    search_term: args.branche_contains ?? null,
    kode:        args.branchekode ?? null,
    kommune:     args.kommunenavn ?? null,
  }) as Row[];

  if (!rows.length) return "Ingen ansatte-data endnu — kør Phase 2 import for at hente beskæftigelsesdata.";

  const total = rows.reduce((s, r) => s + Number(r.antal ?? 0), 0);
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  return `**Størrelsesfordeling: "${label}"** (${args.kommunenavn ?? "hele Danmark"}, ${total} virksomheder med data):\n\n` +
    rows.map(r => {
      const n = Number(r.antal);
      const pct = Math.round(n / total * 20);
      return `${String(r.stoerrelse).padEnd(7)}  ${"█".repeat(pct)}${"░".repeat(20 - pct)}  ${n} (${Math.round(n / total * 100)}%)`;
    }).join("\n");
}

async function market_overview(args: Args): Promise<string> {
  const data = await sbRpc("market_overview_agg", {
    search_term: String(args.branche_contains),
  }) as { total: number; top_kommuner: Row[]; top_brancher: Row[] };

  const topKommuner = data.top_kommuner ?? [];
  const topBrancher = data.top_brancher ?? [];

  return [
    `## Markedsoverblik: "${args.branche_contains}"`,
    ``,
    `**Aktive virksomheder:** ${data.total}`,
    ``,
    `**Top kommuner:**`,
    topKommuner.length ? topKommuner.map(r => `  ${r.kommunenavn}: ${r.antal}`).join("\n") : "  (ingen data)",
    ``,
    `**Underkategorier:**`,
    topBrancher.length ? topBrancher.map(r => `  ${r.branchetekst}: ${r.antal}`).join("\n") : "  (ingen data)",
  ].join("\n");
}

async function find_by_postcode_range(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const filters = [
    "status=eq.aktiv",
    `postnummer=gte.${args.postnummer_fra}`,
    `postnummer=lte.${args.postnummer_til}`,
  ];
  if (args.branche_contains) filters.push(`branchetekst=ilike.*${args.branche_contains}*`);
  if (args.min_ansatte !== undefined) filters.push(`antal_ansatte=gte.${args.min_ansatte}`);
  if (args.max_ansatte !== undefined) filters.push(`antal_ansatte=lte.${args.max_ansatte}`);

  const rows = await sbQuery("companies", {
    select: LEAD_SELECT,
    filters,
    order: "postnummer.asc",
    limit,
  }) as Row[];

  if (!rows.length) return `Ingen virksomheder fundet i postnr ${args.postnummer_fra}–${args.postnummer_til}.`;
  return `**${rows.length} virksomheder** (postnr ${args.postnummer_fra}–${args.postnummer_til}${args.branche_contains ? `, ${args.branche_contains}` : ""}):\n\n` +
    rows.map(formatLead).join("\n\n");
}

async function sql_query(args: Args): Promise<string> {
  if (!args.query) return "Mangler query parameter.";
  const limit = Math.min(Number(args.row_limit ?? 500), 1000);

  const rows = await sbRpc("run_query", {
    query:     String(args.query),
    row_limit: limit,
  }) as Row[] | null;

  if (!rows || !rows.length) return "Ingen resultater.";

  // Format as a readable table
  const cols = Object.keys(rows[0]);
  const colWidths = cols.map(c => Math.min(Math.max(c.length, ...rows.map(r => String(r[c] ?? "").length)), 40));

  const header = cols.map((c, i) => c.padEnd(colWidths[i])).join("  ");
  const divider = colWidths.map(w => "─".repeat(w)).join("  ");
  const rowLines = rows.map(r =>
    cols.map((c, i) => String(r[c] ?? "").slice(0, 40).padEnd(colWidths[i])).join("  ")
  );

  return `**${rows.length} rækker** (limit ${limit}):\n\n\`\`\`\n${header}\n${divider}\n${rowLines.join("\n")}\n\`\`\``;
}

async function database_stats(_args: Args): Promise<string> {
  const data = await sbRpc("database_stats", {}) as {
    total_active: number; total_all: number;
    with_phone: number; with_email: number; with_website: number;
    top_branches: Row[]; top_municipalities: Row[];
    by_company_form: Row[]; founded_by_year: Row[];
  };

  const pct = (n: number) => `${Math.round(n / data.total_active * 100)}%`;

  return [
    `## CVR Database — komplet overblik`,
    ``,
    `**Virksomheder:** ${data.total_active.toLocaleString("da")} aktive (${data.total_all.toLocaleString("da")} total)`,
    `**Kontaktdata:** ${data.with_phone.toLocaleString("da")} med tlf (${pct(data.with_phone)}) · ${data.with_email.toLocaleString("da")} med email (${pct(data.with_email)}) · ${data.with_website.toLocaleString("da")} med hjemmeside (${pct(data.with_website)})`,
    ``,
    `**Top 25 brancher:**`,
    data.top_branches.map(r => `  ${r.branchetekst} [${r.branchekode}]: ${Number(r.antal).toLocaleString("da")}`).join("\n"),
    ``,
    `**Top 25 kommuner:**`,
    data.top_municipalities.map(r => `  ${r.kommunenavn}: ${Number(r.antal).toLocaleString("da")}`).join("\n"),
    ``,
    `**Selskabsformer:**`,
    data.by_company_form.map(r => `  ${r.virksomhedsform}: ${Number(r.antal).toLocaleString("da")}`).join("\n"),
    ``,
    `**Stiftede per år (siden 1980):**`,
    data.founded_by_year.map(r => `  ${r.aar}: ${Number(r.antal).toLocaleString("da")}`).join("\n"),
  ].join("\n");
}

async function top_branches(args: Args): Promise<string> {
  const rows = await sbRpc("top_branches", {
    min_count:   args.min_count   ?? 50,
    max_results: args.max_results ?? 100,
  }) as Row[];

  if (!rows.length) return "Ingen brancher fundet.";
  const total = rows.reduce((s, r) => s + Number(r.antal), 0);
  return `**${rows.length} brancher** (min ${args.min_count ?? 50} virksomheder, total vist: ${total.toLocaleString("da")}):\n\n` +
    rows.map((r, i) => `${i + 1}. **${r.branchetekst}** [${r.branchekode}] — ${Number(r.antal).toLocaleString("da")}`).join("\n");
}

async function new_companies(args: Args): Promise<string> {
  const rows = await sbRpc("new_companies", {
    from_date:   args.from_date   ?? null,
    to_date:     args.to_date     ?? null,
    branche:     args.branche     ?? null,
    kode:        args.kode        ?? null,
    kommune:     args.kommune     ?? null,
    max_results: args.max_results ?? 50,
  }) as Row[];

  if (!rows.length) return "Ingen virksomheder fundet i den periode.";
  return `**${rows.length} nyest oprettede virksomheder**${args.branche ? ` (${args.branche})` : ""}${args.kommune ? ` i ${args.kommune}` : ""}:\n\n` +
    rows.map((r, i) => {
      const adr = [r.vejnavn && `${r.vejnavn} ${r.husnummer ?? ""}`.trim(), r.postnummer, r.postdistrikt].filter(Boolean).join(" ");
      return `${i + 1}. **${r.navn}** (CVR: ${r.cvr})\n   📅 Stiftet: ${r.stiftelsesdato} | 🏭 ${r.branchetekst ?? "–"}\n   📍 ${adr || r.kommunenavn || "–"} | 📞 ${r.telefon ?? "–"} | ✉️ ${r.email ?? "–"}`;
    }).join("\n\n");
}

// ── JSON-RPC router ───────────────────────────────────────────────────────────
function jsonrpc(id: unknown, result: unknown) {
  return NextResponse.json({ jsonrpc: "2.0", id, result });
}
function jsonrpc_error(id: unknown, code: number, message: string) {
  return NextResponse.json({ jsonrpc: "2.0", id, error: { code, message } });
}

export async function POST(req: NextRequest) {
  let body: { jsonrpc: string; id: unknown; method: string; params?: unknown };
  try { body = await req.json(); }
  catch { return jsonrpc_error(null, -32700, "Parse error"); }

  const { id, method, params } = body;

  if (method === "initialize") {
    return jsonrpc(id, {
      protocolVersion: "2024-11-05",
      capabilities: { tools: {} },
      serverInfo: { name: "CVR Danmark", version: "4.0.0" },
    });
  }
  if (method === "notifications/initialized") return new NextResponse(null, { status: 204 });
  if (method === "tools/list") return jsonrpc(id, { tools: TOOLS });

  if (method === "tools/call") {
    const { name, arguments: args = {} } = params as { name: string; arguments?: Args };
    try {
      const handlers: Record<string, (a: Args) => Promise<string>> = {
        find_leads, find_companies, count_companies, get_company,
        search_by_name, list_branches, market_by_municipality,
        employee_distribution, market_overview, find_by_postcode_range,
        database_stats, top_branches, new_companies, sql_query,
      };
      const fn = handlers[name];
      if (!fn) return jsonrpc_error(id, -32601, `Unknown tool: ${name}`);
      const text = await fn(args);
      return jsonrpc(id, { content: [{ type: "text", text }] });
    } catch (e) {
      return jsonrpc_error(id, -32603, String(e));
    }
  }

  return jsonrpc_error(id, -32601, `Method not found: ${method}`);
}

export async function GET() {
  return NextResponse.json({ name: "CVR Danmark MCP v4", tools: TOOLS.map(t => t.name) });
}
