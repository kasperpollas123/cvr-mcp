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

async function sbQuery(table: string, opts: QueryOptions): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (opts.select) params.set("select", opts.select);
  if (opts.order)  params.set("order", opts.order);
  if (opts.limit)  params.set("limit", String(opts.limit));

  const url = `${SB_URL}/rest/v1/${table}?${params}`;

  // Filters are added as individual headers/params — append to URL
  const filterUrl = opts.filters?.length
    ? `${url}&${opts.filters.join("&")}`
    : url;

  const res = await fetch(filterUrl, {
    headers: {
      Authorization: `Bearer ${SB_KEY}`,
      apikey: SB_KEY,
      Accept: "application/json",
    },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Supabase ${res.status}: ${body}`);
  }
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

function buildFilters(args: Args & { branche_contains?: string; branchekode?: string; kommunenavn?: string; postnummer?: string; min_ansatte?: number; max_ansatte?: number; kun_med_kontakt?: boolean }): string[] {
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

  // Get distinct branch codes matching the search term
  const rows = await sbQuery("companies", {
    select: "branchekode,branchetekst",
    filters: [`branchetekst=ilike.*${args.contains}*`, "status=eq.aktiv", "branchekode=not.is.null"],
    limit: limit * 20, // over-fetch to deduplicate
  }) as Row[];

  // Deduplicate + count by branchekode
  const codeMap = new Map<string, { tekst: string; count: number }>();
  for (const r of rows) {
    const kode = r.branchekode as string;
    if (!kode) continue;
    const entry = codeMap.get(kode);
    if (entry) entry.count++;
    else codeMap.set(kode, { tekst: (r.branchetekst as string) ?? kode, count: 1 });
  }

  if (!codeMap.size) return `Ingen brancher fundet med "${args.contains}"`;

  // Sort by count desc
  const sorted = [...codeMap.entries()]
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, limit);

  return `Brancher der matcher "${args.contains}":\n\n` +
    sorted.map(([kode, { tekst, count }]) => `**${tekst}** [${kode}] — ~${count} virksomheder`).join("\n");
}

async function market_by_municipality(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 20), 50);
  const filters = buildFilters({ ...(args as Parameters<typeof buildFilters>[0]), kun_med_kontakt: false });
  const noNull = [...filters, "kommunenavn=not.is.null"];

  const rows = await sbQuery("companies", {
    select: "kommunenavn",
    filters: noNull,
    limit: 10000, // get enough to group by municipality
  }) as Row[];

  if (!rows.length) return "Ingen virksomheder fundet med disse filtre.";

  // Count by municipality
  const counts = new Map<string, number>();
  for (const r of rows) {
    const k = r.kommunenavn as string;
    if (k) counts.set(k, (counts.get(k) ?? 0) + 1);
  }

  const sorted = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit);

  const total = rows.length;
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  return `**"${label}" per kommune** (top ${sorted.length}, total vist: ${total}):\n\n` +
    sorted.map(([k, n]) => `${k}: **${n}**`).join("\n");
}

async function employee_distribution(args: Args): Promise<string> {
  const filters = buildFilters({ ...(args as Parameters<typeof buildFilters>[0]), kun_med_kontakt: false });
  const withEmp = [...filters, "antal_ansatte=not.is.null"];

  const rows = await sbQuery("companies", {
    select: "antal_ansatte",
    filters: withEmp,
    limit: 50000,
  }) as Row[];

  if (!rows.length) return "Ingen ansatte-data endnu — kør Phase 2 import for at hente beskæftigelsesdata.";

  // Bucket into size ranges
  const buckets: Record<string, number> = {
    "0": 0, "1–4": 0, "5–9": 0, "10–19": 0, "20–49": 0, "50–99": 0, "100–249": 0, "250+": 0,
  };
  const order = ["0", "1–4", "5–9", "10–19", "20–49", "50–99", "100–249", "250+"];

  for (const r of rows) {
    const n = Number(r.antal_ansatte);
    if (n === 0)       buckets["0"]++;
    else if (n <= 4)   buckets["1–4"]++;
    else if (n <= 9)   buckets["5–9"]++;
    else if (n <= 19)  buckets["10–19"]++;
    else if (n <= 49)  buckets["20–49"]++;
    else if (n <= 99)  buckets["50–99"]++;
    else if (n <= 249) buckets["100–249"]++;
    else               buckets["250+"]++;
  }

  const total = rows.length;
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  return `**Størrelsesfordeling: "${label}"** (${args.kommunenavn ?? "hele Danmark"}, ${total} virksomheder med data):\n\n` +
    order.map(b => {
      const n = buckets[b];
      const pct = Math.round(n / total * 20);
      return `${b.padEnd(7)}  ${"█".repeat(pct)}${"░".repeat(20 - pct)}  ${n} (${Math.round(n / total * 100)}%)`;
    }).join("\n");
}

async function market_overview(args: Args): Promise<string> {
  const contains = String(args.branche_contains);

  // Run count + sample in parallel
  const [countRes, sampleRows] = await Promise.all([
    fetch(`${SB_URL}/rest/v1/companies?select=cvr&status=eq.aktiv&branchetekst=ilike.*${contains}*&limit=1`, {
      headers: { Authorization: `Bearer ${SB_KEY}`, apikey: SB_KEY, Prefer: "count=exact" },
    }),
    sbQuery("companies", {
      select: "kommunenavn,branchetekst,branchekode",
      filters: ["status=eq.aktiv", `branchetekst=ilike.*${contains}*`, "kommunenavn=not.is.null"],
      limit: 5000,
    }) as Promise<Row[]>,
  ]);

  const total = countRes.headers.get("content-range")?.split("/")[1] ?? "?";

  // Top municipalities
  const komuneCounts = new Map<string, number>();
  const brancheCounts = new Map<string, number>();
  for (const r of sampleRows) {
    const k = r.kommunenavn as string;
    if (k) komuneCounts.set(k, (komuneCounts.get(k) ?? 0) + 1);
    const b = r.branchetekst as string;
    if (b) brancheCounts.set(b, (brancheCounts.get(b) ?? 0) + 1);
  }

  const topKommuner = [...komuneCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
  const topBrancher = [...brancheCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);

  return [
    `## Markedsoverblik: "${contains}"`,
    ``,
    `**Aktive virksomheder:** ${total}`,
    ``,
    `**Top kommuner:**`,
    topKommuner.length ? topKommuner.map(([k, n]) => `  ${k}: ${n}`).join("\n") : "  (ingen data)",
    ``,
    `**Underkategorier:**`,
    topBrancher.length ? topBrancher.map(([b, n]) => `  ${b}: ${n}`).join("\n") : "  (ingen data)",
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
