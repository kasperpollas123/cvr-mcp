import { NextRequest, NextResponse } from "next/server";
import { tursoQuery } from "@/lib/turso";

export const runtime = "edge";
export const maxDuration = 60;

// ── Fast query builder helpers ────────────────────────────────────────────
// All queries start from Branche (small, indexed, filtered early)
// then JOIN outward. Correlated subqueries only in SELECT (fast with indexes).

const N   = `(SELECT vaerdi FROM Navn WHERE CVREnhedsId=v.id LIMIT 1)`;
const TLF = `(SELECT vaerdi FROM Telefonnummer WHERE CVREnhedsId=v.id LIMIT 1)`;
const EML = `(SELECT vaerdi FROM e_mailadresse WHERE CVREnhedsId=v.id LIMIT 1)`;
const VF  = `(SELECT vaerdiTekst FROM Virksomhedsform WHERE CVREnhedsId=v.id LIMIT 1)`;
// Address as single subquery (optional - only when filtering by location)
const KOMMUNE = `(SELECT CVRAdresse_kommunenavn FROM Adressering WHERE CVREnhedsId=v.id AND AdresseringAnvendelse='POSTADRESSE' LIMIT 1)`;
const POSTNR  = `(SELECT CVRAdresse_postnummer FROM Adressering WHERE CVREnhedsId=v.id AND AdresseringAnvendelse='POSTADRESSE' LIMIT 1)`;
const BY      = `(SELECT CVRAdresse_postdistrikt FROM Adressering WHERE CVREnhedsId=v.id AND AdresseringAnvendelse='POSTADRESSE' LIMIT 1)`;
const VEJ     = `(SELECT CVRAdresse_vejnavn||' '||COALESCE(CVRAdresse_husnummerFra,'') FROM Adressering WHERE CVREnhedsId=v.id AND AdresseringAnvendelse='POSTADRESSE' LIMIT 1)`;
const HAS_TLF = `(SELECT 1 FROM Telefonnummer WHERE CVREnhedsId=v.id LIMIT 1) IS NOT NULL`;

// ── Tool definitions ──────────────────────────────────────────────────────
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
        min_ansatte:      { type: "number", description: "Minimum antal ansatte (kræver Beskaeftigelse data)" },
        max_ansatte:      { type: "number", description: "Maksimum antal ansatte" },
        kun_med_kontakt:  { type: "boolean", description: "Kun firmaer med telefonnummer (default: true)", default: true },
        limit:            { type: "number", description: "Max resultater (default 50, max 200)", default: 50 },
      },
      required: ["branche_contains"],
    },
  },
  {
    name: "find_companies",
    description: "Søg virksomheder med fleksible filtre — branche, kommune, postnummer, antal ansatte. Bredere end find_leads (inkl. firmaer uden kontaktinfo).",
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
    description: "Find relevante branchekoder og -beskrivelser der matcher et søgeord. Brug dette til at finde den præcise branchekode inden du søger.",
    inputSchema: {
      type: "object",
      properties: {
        contains: { type: "string", description: "Søgeord, f.eks. 'bygge', 'VVS', 'transport', 'rengøring'" },
        limit:    { type: "number", description: "Max resultater (default 30)", default: 30 },
      },
      required: ["contains"],
    },
  },
  {
    name: "market_by_municipality",
    description: "Se antal virksomheder per kommune for en branche. Prioritér geografiske markeder.",
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
    description: "Vis størrelsesfordeling (ansatte-histogram) for en branche. Se hvor mange mikro/små/mellemstore firmaer der findes.",
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
    description: "Find virksomheder i et geografisk postnummerinterval, f.eks. Østjylland (8000–8999), Nordjylland (9000–9999), Sjælland (4000–4999).",
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

// ── Shared query core ─────────────────────────────────────────────────────
type Args = Record<string, unknown>;

interface LeadRow {
  CVRNummer: string | null;
  navn: string | null;
  branche: string | null;
  branchekode?: string | null;
  vej?: string | null;
  postnr: string | null;
  by: string | null;
  kommune: string | null;
  telefon: string | null;
  email: string | null;
  ansatte?: string | null;
}

function formatLead(r: LeadRow, i: number): string {
  const adr = [r.vej, r.postnr, r.by].filter(Boolean).join(" ");
  return `${i+1}. **${r.navn ?? "Ukendt"}** | CVR: ${r.CVRNummer} | ${r.ansatte ?? "?"} ans.\n   📍 ${adr || "–"}, ${r.kommune ?? "–"}\n   📞 ${r.telefon ?? "–"} | ✉️ ${r.email ?? "–"}\n   🏭 ${r.branche ?? "–"}`;
}

function formatCompany(r: LeadRow): string {
  const adr = [r.vej, r.postnr, r.by].filter(Boolean).join(" ");
  return `**${r.navn ?? "Ukendt"}** (CVR: ${r.CVRNummer})\n  Branche: ${r.branche ?? "–"}${r.branchekode ? ` [${r.branchekode}]` : ""}\n  Adresse: ${adr || "–"}, ${r.kommune ?? "–"}\n  Kontakt: ${[r.telefon, r.email].filter(Boolean).join(" | ") || "–"}\n  Ansatte: ${r.ansatte ?? "ukendt"}`;
}

// ── Branch code helpers ───────────────────────────────────────────────────

// Fast code lookup: queries the 1702-row Branche_koder table instead of 2.4M Branche rows
async function getMatchingCodes(contains: string): Promise<string[]> {
  const rows = await tursoQuery(
    `SELECT vaerdi FROM Branche_koder WHERE vaerdiTekst LIKE ? LIMIT 100`,
    [`%${contains}%`]
  ) as Record<string, string>[];
  return rows.map(r => r.vaerdi).filter(Boolean);
}

// Builds `b.vaerdi IN (?,?,...)` filter — fast with idx_b_sek_vaerdi
function addCodeFilter(codes: string[], where: string[], params: string[]): void {
  const ph = codes.map(() => "?").join(",");
  where.push(`b.vaerdi IN (${ph})`);
  params.push(...codes);
}

// Two-step: get candidate CVREnhedsIds from Branche (indexed, fast, no DISTINCT needed)
async function getCandidateIds(codes: string[], idLimit: number): Promise<string[]> {
  const ph = codes.map(() => "?").join(",");
  const rows = await tursoQuery(
    `SELECT CVREnhedsId FROM Branche WHERE sekvens='0' AND vaerdi IN (${ph}) LIMIT ${idLimit}`,
    codes
  ) as Record<string, string>[];
  // deduplicate in JS (faster than DISTINCT in SQL for this case)
  return [...new Set(rows.map(r => r.CVREnhedsId).filter(Boolean))];
}

// Correlated subqueries for branche when starting from Virksomhed
const B_TEKST = `(SELECT vaerdiTekst FROM Branche WHERE CVREnhedsId=v.id AND sekvens='0' LIMIT 1)`;
const B_KODE  = `(SELECT vaerdi FROM Branche WHERE CVREnhedsId=v.id AND sekvens='0' LIMIT 1)`;

// ── Tool implementations ──────────────────────────────────────────────────

async function find_leads(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);

  // Step 1: fast code lookup in Branche_koder (1702 rows)
  const codes = await getMatchingCodes(String(args.branche_contains));
  if (!codes.length) return `Ingen brancher fundet med "${args.branche_contains}" — prøv et andet søgeord.`;

  // Step 2: get candidate IDs from Branche index (no full-table scan)
  const ids = await getCandidateIds(codes, limit * 8);
  if (!ids.length) return `Ingen virksomheder fundet i branche "${args.branche_contains}".`;
  const idPh = ids.map(() => "?").join(",");

  // Step 3: fetch details from Virksomhed WHERE id IN (...) — no GROUP BY needed
  const where: string[] = [`v.status='aktiv'`, `v.id IN (${idPh})`];
  const params: string[] = [...ids];

  if (args.kun_med_kontakt !== false) where.push(HAS_TLF);
  if (args.kommunenavn) { where.push(`${KOMMUNE} LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.postnummer)  { where.push(`${POSTNR} = ?`); params.push(String(args.postnummer)); }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  const beskJoin = hasEmp
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'`
    : `LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'`;
  if (args.min_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
  if (args.max_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }
  params.push(String(limit));

  const rows = await tursoQuery(`
    SELECT v.CVRNummer, ${N} AS navn, ${B_TEKST} AS branche,
      ${VEJ} AS vej, ${POSTNR} AS postnr, ${BY} AS by, ${KOMMUNE} AS kommune,
      ${TLF} AS telefon, ${EML} AS email, besk.antal AS ansatte
    FROM Virksomhed v
    ${beskJoin}
    WHERE ${where.join(" AND ")}
    ORDER BY CAST(besk.antal AS INTEGER) DESC
    LIMIT ?
  `, params) as unknown as LeadRow[];

  if (!rows.length) return `Ingen leads fundet for "${args.branche_contains}"${args.kommunenavn ? ` i ${args.kommunenavn}` : ""}.\n\nNB: Adresse- og ansatte-data importeres stadig — prøv uden kommune/ansatte-filter, eller prøv et andet søgeord.`;
  return `**${rows.length} leads** (${args.branche_contains}${args.kommunenavn ? `, ${args.kommunenavn}` : ""}):\n\n` + rows.map(formatLead).join("\n\n");
}

async function find_companies(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);

  // Branch filtering: two-step (codes → IDs → details)
  let ids: string[] | null = null;
  if (args.branchekode || args.branche_contains) {
    let codes: string[];
    if (args.branchekode) {
      codes = [String(args.branchekode)];
    } else {
      codes = await getMatchingCodes(String(args.branche_contains));
      if (!codes.length) return `Ingen brancher fundet med "${args.branche_contains}" — prøv et andet søgeord.`;
    }
    ids = await getCandidateIds(codes, limit * 8);
    if (!ids.length) return "Ingen virksomheder fundet med disse kriterier.";
  }

  // Build main Virksomhed query (no GROUP BY — IDs already unique)
  const where: string[] = [`v.status='aktiv'`];
  const params: string[] = [];

  if (ids) {
    const idPh = ids.map(() => "?").join(",");
    where.push(`v.id IN (${idPh})`);
    params.push(...ids);
  }
  if (args.kommunenavn) { where.push(`${KOMMUNE} LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.postnummer)  { where.push(`${POSTNR} = ?`); params.push(String(args.postnummer)); }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  const beskJoin = hasEmp
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'`
    : `LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'`;
  if (args.min_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
  if (args.max_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }
  params.push(String(limit));

  const rows = await tursoQuery(`
    SELECT v.CVRNummer, ${N} AS navn, ${B_TEKST} AS branche, ${B_KODE} AS branchekode,
      ${VEJ} AS vej, ${POSTNR} AS postnr, ${BY} AS by, ${KOMMUNE} AS kommune,
      ${TLF} AS telefon, ${EML} AS email, besk.antal AS ansatte
    FROM Virksomhed v
    ${beskJoin}
    WHERE ${where.join(" AND ")}
    LIMIT ?
  `, params) as unknown as LeadRow[];

  if (!rows.length) return "Ingen virksomheder fundet med disse kriterier.";
  return `Fandt ${rows.length} virksomheder:\n\n` + rows.map(formatCompany).join("\n\n");
}

async function count_companies(args: Args): Promise<string> {
  const where: string[] = [`b.sekvens='0'`, `v.status='aktiv'`];
  const params: string[] = [];

  if (args.branchekode) { where.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.branche_contains) {
    const codes = await getMatchingCodes(String(args.branche_contains));
    if (!codes.length) return `Ingen brancher fundet med "${args.branche_contains}".`;
    addCodeFilter(codes, where, params);
  }

  let adrJoin = ``;
  if (args.kommunenavn) { adrJoin = `JOIN Adressering a ON a.CVREnhedsId=v.id AND a.AdresseringAnvendelse='POSTADRESSE'`; where.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.postnummer)  { adrJoin = adrJoin || `JOIN Adressering a ON a.CVREnhedsId=v.id AND a.AdresseringAnvendelse='POSTADRESSE'`; where.push(`a.CVRAdresse_postnummer = ?`); params.push(String(args.postnummer)); }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  const beskJoin = hasEmp ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'` : ``;
  if (args.min_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
  if (args.max_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }

  const rows = await tursoQuery(`
    SELECT COUNT(DISTINCT v.id) AS antal
    FROM Branche b
    JOIN Virksomhed v ON v.id=b.CVREnhedsId AND v.status='aktiv'
    ${adrJoin} ${beskJoin}
    WHERE ${where.join(" AND ")}
  `, params);

  const n = rows[0]?.antal ?? "0";
  const filters = [
    args.branche_contains ? `"${args.branche_contains}"` : null,
    args.branchekode ? `kode ${args.branchekode}` : null,
    args.kommunenavn ? `${args.kommunenavn}` : null,
    args.postnummer ? `postnr ${args.postnummer}` : null,
    args.min_ansatte !== undefined ? `≥${args.min_ansatte} ans.` : null,
    args.max_ansatte !== undefined ? `≤${args.max_ansatte} ans.` : null,
  ].filter(Boolean).join(", ");

  return `**${n}** aktive virksomheder${filters ? ` — ${filters}` : ""}`;
}

async function get_company(args: Args): Promise<string> {
  const rows = await tursoQuery(`
    SELECT v.CVRNummer, v.virksomhedStartdato, v.virksomhedOphoersdato, v.status,
      ${N} AS navn, ${VF} AS virksomhedsform,
      b.vaerdiTekst AS branche, b.vaerdi AS branchekode,
      ${VEJ} AS vej, ${POSTNR} AS postnr, ${BY} AS by, ${KOMMUNE} AS kommune,
      ${TLF} AS telefon, ${EML} AS email,
      besk.antal AS ansatte, besk.datoFra AS ansatte_dato,
      besk2.antal AS aarsvaerk
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId=v.id AND b.sekvens='0'
    LEFT JOIN Beskaeftigelse_latest besk  ON besk.CVREnhedsId=v.id  AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'
    LEFT JOIN Beskaeftigelse_latest besk2 ON besk2.CVREnhedsId=v.id AND besk2.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAarsvaerk'
    WHERE v.CVRNummer = ? LIMIT 1
  `, [String(args.cvr_nummer)]) as unknown as LeadRow[];

  if (!rows.length) return `Ingen virksomhed fundet med CVR ${args.cvr_nummer}`;
  const r = rows as unknown as Record<string, string | null>[];
  const row = r[0];
  const adr = [row.vej, row.postnr, row.by].filter(Boolean).join(" ");
  return [
    `**${row.navn ?? "Ukendt"}**`,
    `CVR: ${row.CVRNummer}  |  Form: ${row.virksomhedsform ?? "–"}  |  Status: ${row.status ?? "–"}`,
    `Startet: ${row.virksomhedStartdato ?? "–"}`,
    ...(row.virksomhedOphoersdato ? [`Ophørt: ${row.virksomhedOphoersdato}`] : []),
    ``,
    `Branche: ${row.branche ?? "–"} [${row.branchekode ?? "–"}]`,
    `Adresse: ${adr || "–"}, ${row.kommune ?? "–"}`,
    ``,
    `📞 ${row.telefon ?? "–"}  |  ✉️ ${row.email ?? "–"}`,
    ``,
    `Ansatte: ${row.ansatte ?? "ukendt"} (data fra ${row.ansatte_dato ?? "–"})`,
    `Årsværk: ${row.aarsvaerk ?? "ukendt"}`,
  ].join("\n");
}

async function search_by_name(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 20), 100);
  const rows = await tursoQuery(`
    SELECT v.CVRNummer, n.vaerdi AS navn,
      ${KOMMUNE} AS kommune, ${POSTNR} AS postnr, ${BY} AS by,
      b.vaerdiTekst AS branche, ${TLF} AS telefon,
      besk.antal AS ansatte
    FROM Navn n
    JOIN Virksomhed v ON v.id=n.CVREnhedsId AND v.status='aktiv'
    LEFT JOIN Branche b ON b.CVREnhedsId=v.id AND b.sekvens='0'
    LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'
    WHERE n.vaerdi LIKE ?
    GROUP BY v.CVRNummer
    LIMIT ?
  `, [`%${args.navn}%`, String(limit)]) as Record<string, string | null>[];

  if (!rows.length) return `Ingen virksomheder fundet med navn der indeholder "${args.navn}"`;
  return `Fandt ${rows.length} virksomheder:\n\n` +
    rows.map(r => `**${r.navn}** (CVR: ${r.CVRNummer})\n  ${r.branche ?? "–"} | ${r.postnr ?? ""} ${r.by ?? r.kommune ?? "–"}\n  📞 ${r.telefon ?? "–"} | ${r.ansatte ?? "?"} ans.`).join("\n\n");
}

async function list_branches(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 30), 100);

  // Step 1: fast search in 1702-row Branche_koder (never times out)
  const codeRows = await tursoQuery(
    `SELECT vaerdi AS kode, vaerdiTekst AS tekst FROM Branche_koder WHERE vaerdiTekst LIKE ? ORDER BY vaerdiTekst LIMIT ?`,
    [`%${args.contains}%`, String(limit)]
  ) as Record<string, string | null>[];

  if (!codeRows.length) return `Ingen brancher fundet med "${args.contains}"`;

  // Step 2: count active companies per matched code (indexed IN lookup — fast)
  const codes = codeRows.map(r => r.kode!).filter(Boolean);
  const ph = codes.map(() => "?").join(",");
  const countRows = await tursoQuery(
    `SELECT b.vaerdi, COUNT(DISTINCT b.CVREnhedsId) AS antal
     FROM Branche b
     JOIN Virksomhed v ON v.id=b.CVREnhedsId AND v.status='aktiv'
     WHERE b.sekvens='0' AND b.vaerdi IN (${ph})
     GROUP BY b.vaerdi`,
    codes
  ) as Record<string, string | null>[];

  const countMap = new Map(countRows.map(r => [r.vaerdi, r.antal]));

  return `Brancher der matcher "${args.contains}":\n\n` +
    codeRows.map(r => `**${r.tekst}** [${r.kode}] — ${countMap.get(r.kode!) ?? "0"} virksomheder`).join("\n");
}

async function market_by_municipality(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 20), 50);
  const where: string[] = [`b.sekvens='0'`, `v.status='aktiv'`, `a.CVRAdresse_kommunenavn IS NOT NULL`];
  const params: string[] = [];

  if (args.branchekode) { where.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.branche_contains) {
    const codes = await getMatchingCodes(String(args.branche_contains));
    if (!codes.length) return `Ingen brancher fundet med "${args.branche_contains}".`;
    addCodeFilter(codes, where, params);
  }

  const beskJoin = args.min_ansatte !== undefined
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'` : ``;
  if (args.min_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
  params.push(String(limit));

  const rows = await tursoQuery(`
    SELECT a.CVRAdresse_kommunenavn AS kommune, COUNT(DISTINCT v.id) AS antal
    FROM Branche b
    JOIN Virksomhed v ON v.id=b.CVREnhedsId AND v.status='aktiv'
    JOIN Adressering a ON a.CVREnhedsId=v.id AND a.AdresseringAnvendelse='POSTADRESSE'
    ${beskJoin}
    WHERE ${where.join(" AND ")}
    GROUP BY kommune ORDER BY antal DESC LIMIT ?
  `, params) as Record<string, string | null>[];

  if (!rows.length) return "Ingen data — adressedata importeres stadig.";
  const total = rows.reduce((s, r) => s + Number(r.antal ?? 0), 0);
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  return `**"${label}" per kommune** (top ${rows.length}, total vist: ${total}):\n\n` +
    rows.map(r => `${r.kommune}: **${r.antal}**`).join("\n");
}

async function employee_distribution(args: Args): Promise<string> {
  const where: string[] = [`b.sekvens='0'`, `v.status='aktiv'`];
  const params: string[] = [];

  if (args.branchekode) { where.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.branche_contains) {
    const codes = await getMatchingCodes(String(args.branche_contains));
    if (!codes.length) return `Ingen brancher fundet med "${args.branche_contains}".`;
    addCodeFilter(codes, where, params);
  }
  if (args.kommunenavn) {
    where.push(`(SELECT CVRAdresse_kommunenavn FROM Adressering WHERE CVREnhedsId=v.id AND AdresseringAnvendelse='POSTADRESSE' LIMIT 1) LIKE ?`);
    params.push(`%${args.kommunenavn}%`);
  }

  const rows = await tursoQuery(`
    SELECT
      CASE
        WHEN CAST(besk.antal AS INTEGER) = 0    THEN '0'
        WHEN CAST(besk.antal AS INTEGER) <= 4   THEN '1–4'
        WHEN CAST(besk.antal AS INTEGER) <= 9   THEN '5–9'
        WHEN CAST(besk.antal AS INTEGER) <= 19  THEN '10–19'
        WHEN CAST(besk.antal AS INTEGER) <= 49  THEN '20–49'
        WHEN CAST(besk.antal AS INTEGER) <= 99  THEN '50–99'
        WHEN CAST(besk.antal AS INTEGER) <= 249 THEN '100–249'
        ELSE '250+'
      END AS stoerrelse,
      COUNT(DISTINCT v.id) AS antal,
      MIN(CAST(besk.antal AS INTEGER)) AS sort_key
    FROM Branche b
    JOIN Virksomhed v ON v.id=b.CVREnhedsId AND v.status='aktiv'
    JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'
    WHERE ${where.join(" AND ")}
    GROUP BY stoerrelse ORDER BY sort_key
  `, params) as Record<string, string | null>[];

  if (!rows.length) return "Ingen ansatte-data endnu — Beskaeftigelse_latest importeres i baggrunden (kan tage 2–3 dage).";
  const total = rows.reduce((s, r) => s + Number(r.antal ?? 0), 0);
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  return `**Størrelsesfordeling: "${label}"** (${args.kommunenavn ?? "hele Danmark"}, ${total} virksomheder med data):\n\n` +
    rows.map(r => {
      const pct = Math.round(Number(r.antal) / total * 20);
      return `${String(r.stoerrelse).padEnd(7)}  ${"█".repeat(pct)}${"░".repeat(20-pct)}  ${r.antal} (${Math.round(Number(r.antal)/total*100)}%)`;
    }).join("\n");
}

async function market_overview(args: Args): Promise<string> {
  // Fast code lookup first
  const codes = await getMatchingCodes(String(args.branche_contains));
  if (!codes.length) return `Ingen brancher fundet med "${args.branche_contains}".`;

  const ph = codes.map(() => "?").join(",");
  const base = `FROM Branche b JOIN Virksomhed v ON v.id=b.CVREnhedsId AND v.status='aktiv' WHERE b.sekvens='0' AND b.vaerdi IN (${ph})`;

  const [countRow, topKommuner, topBrancher] = await Promise.all([
    tursoQuery(`SELECT COUNT(DISTINCT v.id) AS total ${base}`, codes),
    tursoQuery(`
      SELECT a.CVRAdresse_kommunenavn AS kommune, COUNT(DISTINCT v.id) AS antal
      ${base}
      JOIN Adressering a ON a.CVREnhedsId=v.id AND a.AdresseringAnvendelse='POSTADRESSE'
      AND a.CVRAdresse_kommunenavn IS NOT NULL
      GROUP BY kommune ORDER BY antal DESC LIMIT 8
    `, codes),
    tursoQuery(`
      SELECT b.vaerdiTekst AS branche, COUNT(DISTINCT v.id) AS antal
      ${base} GROUP BY branche ORDER BY antal DESC LIMIT 8
    `, codes),
  ]) as Record<string, string | null>[][];

  const total = countRow[0]?.total ?? "?";
  return [
    `## Markedsoverblik: "${args.branche_contains}"`,
    ``,
    `**Aktive virksomheder:** ${total}`,
    ``,
    `**Top kommuner** (adressedata ${Math.round(920/2800*100)}% importeret):`,
    topKommuner.length ? topKommuner.map(r => `  ${r.kommune}: ${r.antal}`).join("\n") : "  (mangler adressedata endnu)",
    ``,
    `**Underkategorier:**`,
    topBrancher.length ? topBrancher.map(r => `  ${r.branche}: ${r.antal}`).join("\n") : "  (ingen data)",
  ].join("\n");
}

async function find_by_postcode_range(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const where: string[] = [
    `b.sekvens='0'`, `v.status='aktiv'`,
    `a.CVRAdresse_postnummer >= ?`, `a.CVRAdresse_postnummer <= ?`,
  ];
  const params: string[] = [String(args.postnummer_fra), String(args.postnummer_til)];

  // Branch filter: two-step
  let branchIds: string[] | null = null;
  if (args.branche_contains) {
    const codes = await getMatchingCodes(String(args.branche_contains));
    if (!codes.length) return `Ingen brancher fundet med "${args.branche_contains}".`;
    branchIds = await getCandidateIds(codes, limit * 8);
    if (!branchIds.length) return "Ingen virksomheder fundet med disse kriterier.";
    const idPh = branchIds.map(() => "?").join(",");
    where.push(`v.id IN (${idPh})`);
    params.push(...branchIds);
  }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  const beskJoin = hasEmp
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'`
    : `LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId=v.id AND besk.beskaeftigelsestalstype='AarsbeskaeftigelseAntalAnsatte'`;
  if (args.min_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
  if (args.max_ansatte !== undefined) { where.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }
  params.push(String(limit));

  const rows = await tursoQuery(`
    SELECT v.CVRNummer, ${N} AS navn, ${B_TEKST} AS branche,
      ${VEJ} AS vej, ${POSTNR} AS postnr, ${BY} AS by, ${KOMMUNE} AS kommune,
      ${TLF} AS telefon, ${EML} AS email, besk.antal AS ansatte
    FROM Virksomhed v
    JOIN Adressering a ON a.CVREnhedsId=v.id AND a.AdresseringAnvendelse='POSTADRESSE'
    ${beskJoin}
    WHERE ${where.join(" AND ")}
    ORDER BY a.CVRAdresse_postnummer
    LIMIT ?
  `, params) as unknown as LeadRow[];

  if (!rows.length) return `Ingen virksomheder fundet i postnr ${args.postnummer_fra}–${args.postnummer_til}.`;
  return `**${rows.length} virksomheder** (postnr ${args.postnummer_fra}–${args.postnummer_til}${args.branche_contains ? `, ${args.branche_contains}` : ""}):\n\n` +
    rows.map(formatLead).join("\n\n");
}

// ── JSON-RPC router ───────────────────────────────────────────────────────
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
      serverInfo: { name: "CVR Danmark", version: "3.0.0" },
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
  return NextResponse.json({ name: "CVR Danmark MCP v3", tools: TOOLS.map(t => t.name) });
}
