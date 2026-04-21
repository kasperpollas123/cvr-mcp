import { NextRequest, NextResponse } from "next/server";
import { tursoQuery } from "@/lib/turso";

export const maxDuration = 60;

// ── Tool definitions (JSON Schema) ───────────────────────────────────────
const TOOLS = [
  {
    name: "count_companies",
    description: "Tæl antallet af virksomheder i Danmark med givne kriterier",
    inputSchema: {
      type: "object",
      properties: {
        branchekode: { type: "string", description: "Branchekode (f.eks. '412000' for byggeentreprenører)" },
        branche_contains: { type: "string", description: "Søg i branchetekst (f.eks. 'tømrer', 'maler', 'VVS')" },
        kommunekode: { type: "string", description: "Kommunekode (f.eks. '101' for København)" },
        kommunenavn: { type: "string", description: "Kommunenavn (f.eks. 'Aarhus', 'Odense')" },
        min_ansatte: { type: "number", description: "Minimum antal ansatte" },
        max_ansatte: { type: "number", description: "Maksimum antal ansatte" },
      },
    },
  },
  {
    name: "find_companies",
    description: "Find virksomheder i Danmark med navn, branche, adresse og antal ansatte",
    inputSchema: {
      type: "object",
      properties: {
        branchekode: { type: "string", description: "Branchekode (f.eks. '412000')" },
        branche_contains: { type: "string", description: "Søg i branchetekst (f.eks. 'tømrer', 'maler', 'installatør')" },
        kommunekode: { type: "string", description: "Kommunekode" },
        kommunenavn: { type: "string", description: "Kommunenavn (f.eks. 'Aarhus', 'København')" },
        postnummer: { type: "string", description: "Postnummer" },
        min_ansatte: { type: "number", description: "Minimum antal ansatte" },
        max_ansatte: { type: "number", description: "Maksimum antal ansatte" },
        limit: { type: "number", description: "Max antal resultater (default 50, max 200)", default: 50 },
      },
    },
  },
  {
    name: "get_company",
    description: "Hent fuld information om en virksomhed via CVR nummer",
    inputSchema: {
      type: "object",
      properties: {
        cvr_nummer: { type: "string", description: "CVR nummer (8 cifre)" },
      },
      required: ["cvr_nummer"],
    },
  },
  {
    name: "find_leads",
    description: "Find potentielle leads — virksomheder der leverer og installerer (f.eks. VVS, el, tømrer, maler) med kontaktinfo",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord (f.eks. 'VVS', 'elektro', 'tømrer', 'maler', 'gulv')" },
        kommunenavn: { type: "string", description: "By/kommune (f.eks. 'Aarhus', 'Odense')" },
        min_ansatte: { type: "number", description: "Minimum antal ansatte" },
        max_ansatte: { type: "number", description: "Maksimum antal ansatte" },
        kun_med_kontakt: { type: "boolean", description: "Kun virksomheder med telefon eller email (default true)", default: true },
        limit: { type: "number", description: "Max resultater (default 50)", default: 50 },
      },
      required: ["branche_contains"],
    },
  },
];

// ── Tool implementations ──────────────────────────────────────────────────
type Args = Record<string, unknown>;

async function count_companies(args: Args): Promise<string> {
  const conditions: string[] = ["v.status = 'aktiv'"];
  const params: string[] = [];

  if (args.branchekode) { conditions.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.branche_contains) { conditions.push(`b.vaerdiTekst LIKE ?`); params.push(`%${args.branche_contains}%`); }
  if (args.kommunekode) { conditions.push(`a.CVRAdresse_kommunekode = ?`); params.push(String(args.kommunekode)); }
  if (args.kommunenavn) { conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  let beskJoin = "";
  if (hasEmp) {
    beskJoin = `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`;
    if (args.min_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
    if (args.max_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }
  }

  const sql = `
    SELECT COUNT(DISTINCT v.CVRNummer) as antal
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    ${beskJoin}
    WHERE ${conditions.join(" AND ")}
  `;
  const rows = await tursoQuery(sql, params);
  return `Antal virksomheder: ${rows[0]?.antal ?? "0"}`;
}

async function find_companies(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const conditions: string[] = ["v.status = 'aktiv'"];
  const params: string[] = [];

  if (args.branchekode) { conditions.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.branche_contains) { conditions.push(`b.vaerdiTekst LIKE ?`); params.push(`%${args.branche_contains}%`); }
  if (args.kommunekode) { conditions.push(`a.CVRAdresse_kommunekode = ?`); params.push(String(args.kommunekode)); }
  if (args.kommunenavn) { conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.postnummer) { conditions.push(`a.CVRAdresse_postnummer = ?`); params.push(String(args.postnummer)); }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  const beskJoin = hasEmp
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`
    : `LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`;

  if (hasEmp) {
    if (args.min_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
    if (args.max_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }
  }
  params.push(String(limit));

  const rows = await tursoQuery(`
    SELECT v.CVRNummer, n.vaerdi AS navn, b.vaerdiTekst AS branche, b.vaerdi AS branchekode,
      a.CVRAdresse_vejnavn AS vej, a.CVRAdresse_husnummerFra AS husnr,
      a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
      a.CVRAdresse_kommunenavn AS kommune, t.vaerdi AS telefon, e.vaerdi AS email,
      besk.antal AS ansatte
    FROM Virksomhed v
    LEFT JOIN Navn n ON n.CVREnhedsId = v.id AND n.virkningTil IS NULL
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    LEFT JOIN Telefonnummer t ON t.CVREnhedsId = v.id 
    LEFT JOIN e_mailadresse e ON e.CVREnhedsId = v.id 
    ${beskJoin}
    WHERE ${conditions.join(" AND ")}
    LIMIT ?
  `, params);

  if (!rows.length) return "Ingen virksomheder fundet.";

  return `Fandt ${rows.length} virksomheder:\n\n` + rows.map(r => {
    const adr = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
    return `**${r.navn ?? "Ukendt"}** (CVR: ${r.CVRNummer})\n  Branche: ${r.branche ?? "–"}\n  Adresse: ${adr || "–"}, ${r.kommune ?? "–"}\n  Kontakt: ${[r.telefon, r.email].filter(Boolean).join(" | ") || "–"}\n  Ansatte: ${r.ansatte ?? "ukendt"}`;
  }).join("\n\n");
}

async function get_company(args: Args): Promise<string> {
  const rows = await tursoQuery(`
    SELECT v.CVRNummer, v.virksomhedStartdato, v.virksomhedOphoersdato, v.status,
      n.vaerdi AS navn, b.vaerdiTekst AS branche, b.vaerdi AS branchekode,
      a.CVRAdresse_vejnavn AS vej, a.CVRAdresse_husnummerFra AS husnr,
      a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
      a.CVRAdresse_kommunenavn AS kommune, t.vaerdi AS telefon, e.vaerdi AS email,
      vf.vaerdiTekst AS virksomhedsform,
      besk.antal AS ansatte, besk.datoFra AS ansatte_dato, besk2.antal AS aarsvaerk
    FROM Virksomhed v
    LEFT JOIN Navn n ON n.CVREnhedsId = v.id AND n.virkningTil IS NULL
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    LEFT JOIN Telefonnummer t ON t.CVREnhedsId = v.id 
    LEFT JOIN e_mailadresse e ON e.CVREnhedsId = v.id 
    LEFT JOIN Virksomhedsform vf ON vf.CVREnhedsId = v.id 
    LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'
    LEFT JOIN Beskaeftigelse_latest besk2 ON besk2.CVREnhedsId = v.id AND besk2.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAarsvaerk'
    WHERE v.CVRNummer = ? LIMIT 1
  `, [String(args.cvr_nummer)]);

  if (!rows.length) return `Ingen virksomhed fundet med CVR ${args.cvr_nummer}`;
  const r = rows[0];
  const adr = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
  return [
    `**${r.navn ?? "Ukendt"}**`, `CVR: ${r.CVRNummer}`, `Form: ${r.virksomhedsform ?? "–"}`,
    `Status: ${r.status ?? "–"}`, `Startet: ${r.virksomhedStartdato ?? "–"}`,
    ...(r.virksomhedOphoersdato ? [`Ophørt: ${r.virksomhedOphoersdato}`] : []),
    ``, `Branche: ${r.branche ?? "–"} (${r.branchekode ?? "–"})`,
    `Adresse: ${adr || "–"}`, `Kommune: ${r.kommune ?? "–"}`,
    ``, `Telefon: ${r.telefon ?? "–"}`, `Email: ${r.email ?? "–"}`,
    ``, `Ansatte: ${r.ansatte ?? "ukendt"} (per ${r.ansatte_dato ?? "–"})`,
    `Årsværk: ${r.aarsvaerk ?? "ukendt"}`,
  ].join("\n");
}

async function find_leads(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const conditions: string[] = ["v.status = 'aktiv'", `b.vaerdiTekst LIKE ?`];
  const params: string[] = [`%${args.branche_contains}%`];

  if (args.kommunenavn) { conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.kun_med_kontakt !== false) { conditions.push(`(t.vaerdi IS NOT NULL OR e.vaerdi IS NOT NULL)`); }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  const beskJoin = hasEmp
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`
    : `LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`;

  if (hasEmp) {
    if (args.min_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
    if (args.max_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }
  }
  params.push(String(limit));

  const rows = await tursoQuery(`
    SELECT v.CVRNummer, n.vaerdi AS navn, b.vaerdiTekst AS branche,
      a.CVRAdresse_vejnavn AS vej, a.CVRAdresse_husnummerFra AS husnr,
      a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
      a.CVRAdresse_kommunenavn AS kommune, t.vaerdi AS telefon, e.vaerdi AS email,
      besk.antal AS ansatte
    FROM Virksomhed v
    LEFT JOIN Navn n ON n.CVREnhedsId = v.id AND n.virkningTil IS NULL
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    LEFT JOIN Telefonnummer t ON t.CVREnhedsId = v.id 
    LEFT JOIN e_mailadresse e ON e.CVREnhedsId = v.id 
    ${beskJoin}
    WHERE ${conditions.join(" AND ")}
    ORDER BY CAST(besk.antal AS INTEGER) DESC
    LIMIT ?
  `, params);

  if (!rows.length) return "Ingen leads fundet.";
  return `**${rows.length} leads** (${args.branche_contains}${args.kommunenavn ? `, ${args.kommunenavn}` : ""}):\n\n` +
    rows.map((r, i) => {
      const adr = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
      return `${i+1}. **${r.navn ?? "Ukendt"}** | CVR: ${r.CVRNummer} | ${r.ansatte ?? "?"} ans.\n   📍 ${adr}, ${r.kommune ?? "–"}\n   📞 ${r.telefon ?? "–"} | ✉️ ${r.email ?? "–"}\n   🏭 ${r.branche ?? "–"}`;
    }).join("\n\n");
}

// ── JSON-RPC router ────────────────────────────────────────────────────────
function jsonrpc(id: unknown, result: unknown) {
  return NextResponse.json({ jsonrpc: "2.0", id, result }, {
    headers: { "Content-Type": "application/json" },
  });
}
function jsonrpc_error(id: unknown, code: number, message: string) {
  return NextResponse.json({ jsonrpc: "2.0", id, error: { code, message } }, {
    headers: { "Content-Type": "application/json" },
  });
}

export async function POST(req: NextRequest) {
  let body: { jsonrpc: string; id: unknown; method: string; params?: unknown };
  try {
    body = await req.json();
  } catch {
    return jsonrpc_error(null, -32700, "Parse error");
  }

  const { id, method, params } = body;

  if (method === "initialize") {
    return jsonrpc(id, {
      protocolVersion: "2024-11-05",
      capabilities: { tools: {} },
      serverInfo: { name: "CVR Danmark", version: "1.0.0" },
    });
  }

  if (method === "notifications/initialized") {
    return new NextResponse(null, { status: 204 });
  }

  if (method === "tools/list") {
    return jsonrpc(id, { tools: TOOLS });
  }

  if (method === "tools/call") {
    const { name, arguments: args = {} } = params as { name: string; arguments?: Args };
    try {
      let text: string;
      if (name === "count_companies") text = await count_companies(args);
      else if (name === "find_companies") text = await find_companies(args);
      else if (name === "get_company") text = await get_company(args);
      else if (name === "find_leads") text = await find_leads(args);
      else return jsonrpc_error(id, -32601, `Unknown tool: ${name}`);
      return jsonrpc(id, { content: [{ type: "text", text }] });
    } catch (e) {
      return jsonrpc_error(id, -32603, String(e));
    }
  }

  return jsonrpc_error(id, -32601, `Method not found: ${method}`);
}

// Claude.ai also sends GET for discovery
export async function GET() {
  return NextResponse.json({ name: "CVR Danmark MCP", tools: TOOLS.map(t => t.name) });
}
