import { NextRequest, NextResponse } from "next/server";
import { tursoQuery } from "@/lib/turso";

export const maxDuration = 60;

// ── Shared SQL helpers ────────────────────────────────────────────────────
const NAVN_SUB    = `(SELECT vaerdi FROM Navn WHERE CVREnhedsId = v.id LIMIT 1)`;
const TELEFON_SUB = `(SELECT vaerdi FROM Telefonnummer WHERE CVREnhedsId = v.id LIMIT 1)`;
const EMAIL_SUB   = `(SELECT vaerdi FROM e_mailadresse WHERE CVREnhedsId = v.id LIMIT 1)`;
const VFORM_SUB   = `(SELECT vaerdiTekst FROM Virksomhedsform WHERE CVREnhedsId = v.id LIMIT 1)`;

// ── Tool definitions ──────────────────────────────────────────────────────
const TOOLS = [
  {
    name: "find_leads",
    description: "Find potentielle leads — virksomheder der leverer og installerer (VVS, el, tømrer, maler osv.) med kontaktinfo. Returnerer navn, adresse, telefon, email og antal ansatte.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord, f.eks. 'VVS', 'tømrer', 'maler', 'elektro', 'gulv', 'tag'" },
        kommunenavn: { type: "string", description: "Kommune/by, f.eks. 'Aarhus', 'Odense', 'Aalborg'" },
        postnummer: { type: "string", description: "Postnummer, f.eks. '8000'" },
        min_ansatte: { type: "number", description: "Minimum antal ansatte" },
        max_ansatte: { type: "number", description: "Maksimum antal ansatte" },
        kun_med_kontakt: { type: "boolean", description: "Kun firmaer med telefon/email (default: true)", default: true },
        limit: { type: "number", description: "Max resultater (default 50, max 200)", default: 50 },
      },
      required: ["branche_contains"],
    },
  },
  {
    name: "find_companies",
    description: "Find virksomheder med fleksible filtre — branche, kommune, postnummer, antal ansatte. Bredere søgning end find_leads.",
    inputSchema: {
      type: "object",
      properties: {
        branchekode: { type: "string", description: "Eksakt branchekode (DB07), f.eks. '412000'" },
        branche_contains: { type: "string", description: "Søg i branchetekst" },
        kommunenavn: { type: "string", description: "Kommunenavn" },
        postnummer: { type: "string", description: "Postnummer" },
        min_ansatte: { type: "number", description: "Min ansatte" },
        max_ansatte: { type: "number", description: "Max ansatte" },
        limit: { type: "number", description: "Max resultater (default 50, max 200)", default: 50 },
      },
    },
  },
  {
    name: "count_companies",
    description: "Tæl virksomheder med givne filtre — nyttigt til at forstå markedsstørrelse.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Søg i branchetekst" },
        branchekode: { type: "string", description: "Eksakt branchekode" },
        kommunenavn: { type: "string", description: "Kommunenavn" },
        postnummer: { type: "string", description: "Postnummer" },
        min_ansatte: { type: "number", description: "Min ansatte" },
        max_ansatte: { type: "number", description: "Max ansatte" },
      },
    },
  },
  {
    name: "get_company",
    description: "Hent fuld profil på én virksomhed via CVR nummer — navn, branche, adresse, kontakt, ansatte, virksomhedsform.",
    inputSchema: {
      type: "object",
      properties: {
        cvr_nummer: { type: "string", description: "CVR nummer (8 cifre)" },
      },
      required: ["cvr_nummer"],
    },
  },
  {
    name: "search_by_name",
    description: "Søg virksomheder efter navn (delvist match). Nyttigt når du kender firma-navnet men ikke CVR.",
    inputSchema: {
      type: "object",
      properties: {
        navn: { type: "string", description: "Virksomhedsnavn eller del deraf, f.eks. 'Hansen VVS'" },
        limit: { type: "number", description: "Max resultater (default 20)", default: 20 },
      },
      required: ["navn"],
    },
  },
  {
    name: "list_branches",
    description: "List de mest brugte brancher der matcher et søgeord — få branchekoder og beskrivelser.",
    inputSchema: {
      type: "object",
      properties: {
        contains: { type: "string", description: "Søgeord i branchetekst, f.eks. 'bygge', 'VVS', 'transport'" },
        limit: { type: "number", description: "Max resultater (default 30)", default: 30 },
      },
      required: ["contains"],
    },
  },
  {
    name: "market_by_municipality",
    description: "Se fordeling af virksomheder på tværs af kommuner for en given branche. Hjælper med at prioritere geografiske markeder.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord" },
        branchekode: { type: "string", description: "Eksakt branchekode" },
        min_ansatte: { type: "number", description: "Min ansatte filter" },
        limit: { type: "number", description: "Antal kommuner at vise (default 20)", default: 20 },
      },
    },
  },
  {
    name: "employee_distribution",
    description: "Vis fordelingen af virksomheder efter antal ansatte i en branche — hjælper med at forstå markedet og vælge målgruppe.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord" },
        branchekode: { type: "string", description: "Eksakt branchekode" },
        kommunenavn: { type: "string", description: "Begræns til kommune" },
      },
    },
  },
  {
    name: "market_overview",
    description: "Giv et overblik over et marked — antal virksomheder, størrelsesfordeling, top-kommuner og top-brancher.",
    inputSchema: {
      type: "object",
      properties: {
        branche_contains: { type: "string", description: "Branche søgeord, f.eks. 'maler', 'VVS', 'tømrer'" },
      },
      required: ["branche_contains"],
    },
  },
  {
    name: "find_companies_by_postcode_range",
    description: "Find virksomheder i et postnummerområde, f.eks. alle i Østjylland (8000-8999).",
    inputSchema: {
      type: "object",
      properties: {
        postnummer_fra: { type: "string", description: "Start postnummer, f.eks. '8000'" },
        postnummer_til: { type: "string", description: "Slut postnummer, f.eks. '8999'" },
        branche_contains: { type: "string", description: "Branche søgeord" },
        min_ansatte: { type: "number", description: "Min ansatte" },
        max_ansatte: { type: "number", description: "Max ansatte" },
        limit: { type: "number", description: "Max resultater (default 50)", default: 50 },
      },
      required: ["postnummer_fra", "postnummer_til"],
    },
  },
];

// ── Tool implementations ──────────────────────────────────────────────────
type Args = Record<string, unknown>;

async function find_leads(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const conditions: string[] = ["v.status = 'aktiv'", `b.vaerdiTekst LIKE ?`];
  const params: string[] = [`%${args.branche_contains}%`];

  if (args.kommunenavn) { conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.postnummer)  { conditions.push(`a.CVRAdresse_postnummer = ?`); params.push(String(args.postnummer)); }
  if (args.kun_med_kontakt !== false) {
    conditions.push(`(EXISTS(SELECT 1 FROM Telefonnummer WHERE CVREnhedsId = v.id) OR EXISTS(SELECT 1 FROM e_mailadresse WHERE CVREnhedsId = v.id))`);
  }

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
    SELECT v.CVRNummer, ${NAVN_SUB} AS navn, b.vaerdiTekst AS branche,
      a.CVRAdresse_vejnavn AS vej, a.CVRAdresse_husnummerFra AS husnr,
      a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
      a.CVRAdresse_kommunenavn AS kommune,
      ${TELEFON_SUB} AS telefon, ${EMAIL_SUB} AS email,
      besk.antal AS ansatte
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    ${beskJoin}
    WHERE ${conditions.join(" AND ")}
    GROUP BY v.CVRNummer
    ORDER BY CAST(besk.antal AS INTEGER) DESC
    LIMIT ?
  `, params);

  if (!rows.length) return "Ingen leads fundet med disse kriterier.";
  return `**${rows.length} leads** (${args.branche_contains}${args.kommunenavn ? `, ${args.kommunenavn}` : ""}):\n\n` +
    rows.map((r, i) => {
      const adr = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
      return `${i+1}. **${r.navn ?? "Ukendt"}** | CVR: ${r.CVRNummer} | ${r.ansatte ?? "?"} ans.\n   📍 ${adr || "–"}, ${r.kommune ?? "–"}\n   📞 ${r.telefon ?? "–"} | ✉️ ${r.email ?? "–"}\n   🏭 ${r.branche ?? "–"}`;
    }).join("\n\n");
}

async function find_companies(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const conditions: string[] = ["v.status = 'aktiv'"];
  const params: string[] = [];

  if (args.branchekode)      { conditions.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.branche_contains) { conditions.push(`b.vaerdiTekst LIKE ?`); params.push(`%${args.branche_contains}%`); }
  if (args.kommunenavn)      { conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.postnummer)       { conditions.push(`a.CVRAdresse_postnummer = ?`); params.push(String(args.postnummer)); }

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
    SELECT v.CVRNummer, ${NAVN_SUB} AS navn, b.vaerdiTekst AS branche, b.vaerdi AS branchekode,
      a.CVRAdresse_vejnavn AS vej, a.CVRAdresse_husnummerFra AS husnr,
      a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
      a.CVRAdresse_kommunenavn AS kommune,
      ${TELEFON_SUB} AS telefon, ${EMAIL_SUB} AS email,
      besk.antal AS ansatte
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    ${beskJoin}
    WHERE ${conditions.join(" AND ")}
    GROUP BY v.CVRNummer
    LIMIT ?
  `, params);

  if (!rows.length) return "Ingen virksomheder fundet.";
  return `Fandt ${rows.length} virksomheder:\n\n` +
    rows.map(r => {
      const adr = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
      return `**${r.navn ?? "Ukendt"}** (CVR: ${r.CVRNummer})\n  Branche: ${r.branche ?? "–"} [${r.branchekode ?? "–"}]\n  Adresse: ${adr || "–"}, ${r.kommune ?? "–"}\n  Kontakt: ${[r.telefon, r.email].filter(Boolean).join(" | ") || "–"}\n  Ansatte: ${r.ansatte ?? "ukendt"}`;
    }).join("\n\n");
}

async function count_companies(args: Args): Promise<string> {
  const conditions: string[] = ["v.status = 'aktiv'"];
  const params: string[] = [];

  if (args.branchekode)      { conditions.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.branche_contains) { conditions.push(`b.vaerdiTekst LIKE ?`); params.push(`%${args.branche_contains}%`); }
  if (args.kommunenavn)      { conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }
  if (args.postnummer)       { conditions.push(`a.CVRAdresse_postnummer = ?`); params.push(String(args.postnummer)); }

  const hasEmp = args.min_ansatte !== undefined || args.max_ansatte !== undefined;
  const beskJoin = hasEmp
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`
    : ``;
  if (hasEmp) {
    if (args.min_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) >= ?`); params.push(String(args.min_ansatte)); }
    if (args.max_ansatte !== undefined) { conditions.push(`CAST(besk.antal AS INTEGER) <= ?`); params.push(String(args.max_ansatte)); }
  }

  const sql = `
    SELECT COUNT(DISTINCT v.id) AS antal
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    ${beskJoin}
    WHERE ${conditions.join(" AND ")}
  `;
  const rows = await tursoQuery(sql, params);
  const n = rows[0]?.antal ?? "0";

  const filters = [
    args.branche_contains ? `branche="${args.branche_contains}"` : null,
    args.branchekode ? `kode=${args.branchekode}` : null,
    args.kommunenavn ? `kommune="${args.kommunenavn}"` : null,
    args.postnummer ? `postnr=${args.postnummer}` : null,
    args.min_ansatte !== undefined ? `min ${args.min_ansatte} ans.` : null,
    args.max_ansatte !== undefined ? `max ${args.max_ansatte} ans.` : null,
  ].filter(Boolean).join(", ");

  return `**${n}** aktive virksomheder${filters ? ` (${filters})` : ""}`;
}

async function get_company(args: Args): Promise<string> {
  const rows = await tursoQuery(`
    SELECT v.CVRNummer, v.virksomhedStartdato, v.virksomhedOphoersdato, v.status,
      ${NAVN_SUB} AS navn, b.vaerdiTekst AS branche, b.vaerdi AS branchekode,
      a.CVRAdresse_vejnavn AS vej, a.CVRAdresse_husnummerFra AS husnr,
      a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
      a.CVRAdresse_kommunenavn AS kommune,
      ${TELEFON_SUB} AS telefon, ${EMAIL_SUB} AS email,
      ${VFORM_SUB} AS virksomhedsform,
      besk.antal AS ansatte, besk.datoFra AS ansatte_dato,
      besk2.antal AS aarsvaerk
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    LEFT JOIN Beskaeftigelse_latest besk  ON besk.CVREnhedsId  = v.id AND besk.beskaeftigelsestalstype  = 'AarsbeskaeftigelseAntalAnsatte'
    LEFT JOIN Beskaeftigelse_latest besk2 ON besk2.CVREnhedsId = v.id AND besk2.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAarsvaerk'
    WHERE v.CVRNummer = ? LIMIT 1
  `, [String(args.cvr_nummer)]);

  if (!rows.length) return `Ingen virksomhed fundet med CVR ${args.cvr_nummer}`;
  const r = rows[0];
  const adr = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
  return [
    `**${r.navn ?? "Ukendt"}**`,
    `CVR: ${r.CVRNummer}`,
    `Form: ${r.virksomhedsform ?? "–"}`,
    `Status: ${r.status ?? "–"}`,
    `Startet: ${r.virksomhedStartdato ?? "–"}`,
    ...(r.virksomhedOphoersdato ? [`Ophørt: ${r.virksomhedOphoersdato}`] : []),
    ``,
    `Branche: ${r.branche ?? "–"} [${r.branchekode ?? "–"}]`,
    `Adresse: ${adr || "–"}`,
    `Kommune: ${r.kommune ?? "–"}`,
    ``,
    `Telefon: ${r.telefon ?? "–"}`,
    `Email: ${r.email ?? "–"}`,
    ``,
    `Ansatte: ${r.ansatte ?? "ukendt"} (data fra ${r.ansatte_dato ?? "–"})`,
    `Årsværk: ${r.aarsvaerk ?? "ukendt"}`,
  ].join("\n");
}

async function search_by_name(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 20), 100);
  const rows = await tursoQuery(`
    SELECT v.CVRNummer, n.vaerdi AS navn, b.vaerdiTekst AS branche,
      a.CVRAdresse_postdistrikt AS by, a.CVRAdresse_kommunenavn AS kommune,
      ${TELEFON_SUB} AS telefon, ${EMAIL_SUB} AS email,
      besk.antal AS ansatte
    FROM Navn n
    JOIN Virksomhed v ON v.id = n.CVREnhedsId AND v.status = 'aktiv'
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'
    WHERE n.vaerdi LIKE ?
    GROUP BY v.CVRNummer
    LIMIT ?
  `, [`%${args.navn}%`, String(limit)]);

  if (!rows.length) return `Ingen virksomheder fundet med navn der indeholder "${args.navn}"`;
  return `Fandt ${rows.length} virksomheder:\n\n` +
    rows.map(r =>
      `**${r.navn}** (CVR: ${r.CVRNummer})\n  ${r.branche ?? "–"} | ${r.by ?? r.kommune ?? "–"}\n  📞 ${r.telefon ?? "–"} | ${r.ansatte ?? "?"} ans.`
    ).join("\n\n");
}

async function list_branches(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 30), 100);
  const rows = await tursoQuery(`
    SELECT b.vaerdi AS kode, b.vaerdiTekst AS tekst, COUNT(DISTINCT b.CVREnhedsId) AS antal
    FROM Branche b
    JOIN Virksomhed v ON v.id = b.CVREnhedsId AND v.status = 'aktiv'
    WHERE b.vaerdiTekst LIKE ? AND b.sekvens = '0'
    GROUP BY b.vaerdi, b.vaerdiTekst
    ORDER BY antal DESC
    LIMIT ?
  `, [`%${args.contains}%`, String(limit)]);

  if (!rows.length) return `Ingen brancher fundet med "${args.contains}"`;
  return `Brancher der matcher "${args.contains}":\n\n` +
    rows.map(r => `**${r.tekst}** [${r.kode}] — ${r.antal} virksomheder`).join("\n");
}

async function market_by_municipality(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 20), 50);
  const conditions: string[] = ["v.status = 'aktiv'"];
  const params: string[] = [];

  if (args.branche_contains) { conditions.push(`b.vaerdiTekst LIKE ?`); params.push(`%${args.branche_contains}%`); }
  if (args.branchekode)      { conditions.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }

  const hasEmp = args.min_ansatte !== undefined;
  const beskJoin = hasEmp
    ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`
    : ``;
  if (hasEmp) {
    conditions.push(`CAST(besk.antal AS INTEGER) >= ?`);
    params.push(String(args.min_ansatte));
  }
  params.push(String(limit));

  const rows = await tursoQuery(`
    SELECT a.CVRAdresse_kommunenavn AS kommune, COUNT(DISTINCT v.id) AS antal
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    ${beskJoin}
    WHERE ${conditions.join(" AND ")} AND a.CVRAdresse_kommunenavn IS NOT NULL
    GROUP BY a.CVRAdresse_kommunenavn
    ORDER BY antal DESC
    LIMIT ?
  `, params);

  if (!rows.length) return "Ingen data fundet.";
  const total = rows.reduce((s, r) => s + Number(r.antal ?? 0), 0);
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  return `**Fordeling af "${label}" virksomheder per kommune** (top ${rows.length}):\n\n` +
    rows.map(r => `${r.kommune ?? "Ukendt"}: **${r.antal}**`).join("\n") +
    `\n\nTotal (vist): ${total}`;
}

async function employee_distribution(args: Args): Promise<string> {
  const conditions: string[] = ["v.status = 'aktiv'"];
  const params: string[] = [];

  if (args.branche_contains) { conditions.push(`b.vaerdiTekst LIKE ?`); params.push(`%${args.branche_contains}%`); }
  if (args.branchekode)      { conditions.push(`b.vaerdi = ?`); params.push(String(args.branchekode)); }
  if (args.kommunenavn)      { conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`); params.push(`%${args.kommunenavn}%`); }

  const rows = await tursoQuery(`
    SELECT
      CASE
        WHEN CAST(besk.antal AS INTEGER) = 0   THEN '0'
        WHEN CAST(besk.antal AS INTEGER) <= 4   THEN '1-4'
        WHEN CAST(besk.antal AS INTEGER) <= 9   THEN '5-9'
        WHEN CAST(besk.antal AS INTEGER) <= 19  THEN '10-19'
        WHEN CAST(besk.antal AS INTEGER) <= 49  THEN '20-49'
        WHEN CAST(besk.antal AS INTEGER) <= 99  THEN '50-99'
        WHEN CAST(besk.antal AS INTEGER) <= 249 THEN '100-249'
        ELSE '250+'
      END AS stoerrelse,
      COUNT(DISTINCT v.id) AS antal
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.id AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'
    WHERE ${conditions.join(" AND ")}
    GROUP BY stoerrelse
    ORDER BY MIN(CAST(besk.antal AS INTEGER))
  `, params);

  if (!rows.length) return "Ingen data — Beskaeftigelse_latest er endnu ikke importeret (kører i baggrunden).";
  const label = args.branche_contains ?? args.branchekode ?? "alle";
  const total = rows.reduce((s, r) => s + Number(r.antal ?? 0), 0);
  return `**Størrelsesfordeling for "${label}"** (${args.kommunenavn ?? "hele Danmark"}):\n\n` +
    rows.map(r => {
      const bar = "█".repeat(Math.round(Number(r.antal) / total * 20));
      return `${String(r.stoerrelse).padEnd(7)} ${bar} ${r.antal} virksomheder`;
    }).join("\n") +
    `\n\nTotal: ${total} virksomheder med kendte ansatte`;
}

async function market_overview(args: Args): Promise<string> {
  const kw = `%${args.branche_contains}%`;

  const [countRow, topKommuner, topBrancher] = await Promise.all([
    tursoQuery(`
      SELECT COUNT(DISTINCT v.id) AS total
      FROM Virksomhed v
      JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0' AND b.vaerdiTekst LIKE ?
      WHERE v.status = 'aktiv'
    `, [kw]),
    tursoQuery(`
      SELECT a.CVRAdresse_kommunenavn AS kommune, COUNT(DISTINCT v.id) AS antal
      FROM Virksomhed v
      JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0' AND b.vaerdiTekst LIKE ?
      LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
      WHERE v.status = 'aktiv' AND a.CVRAdresse_kommunenavn IS NOT NULL
      GROUP BY kommune ORDER BY antal DESC LIMIT 8
    `, [kw]),
    tursoQuery(`
      SELECT b.vaerdiTekst AS branche, COUNT(DISTINCT v.id) AS antal
      FROM Virksomhed v
      JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0' AND b.vaerdiTekst LIKE ?
      WHERE v.status = 'aktiv'
      GROUP BY branche ORDER BY antal DESC LIMIT 8
    `, [kw]),
  ]);

  const total = countRow[0]?.total ?? "?";
  const kommuneLines = topKommuner.map(r => `  ${r.kommune}: ${r.antal}`).join("\n");
  const brancheLines = topBrancher.map(r => `  ${r.branche}: ${r.antal}`).join("\n");

  return [
    `## Markedsoverblik: "${args.branche_contains}"`,
    ``,
    `**Aktive virksomheder i alt:** ${total}`,
    ``,
    `**Top kommuner:**`,
    kommuneLines || "  (ingen data endnu)",
    ``,
    `**Underkategorier:**`,
    brancheLines || "  (ingen data endnu)",
  ].join("\n");
}

async function find_companies_by_postcode_range(args: Args): Promise<string> {
  const limit = Math.min(Number(args.limit ?? 50), 200);
  const conditions: string[] = [
    "v.status = 'aktiv'",
    `a.CVRAdresse_postnummer >= ?`,
    `a.CVRAdresse_postnummer <= ?`,
  ];
  const params: string[] = [String(args.postnummer_fra), String(args.postnummer_til)];

  if (args.branche_contains) { conditions.push(`b.vaerdiTekst LIKE ?`); params.push(`%${args.branche_contains}%`); }

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
    SELECT v.CVRNummer, ${NAVN_SUB} AS navn, b.vaerdiTekst AS branche,
      a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
      a.CVRAdresse_kommunenavn AS kommune,
      ${TELEFON_SUB} AS telefon, ${EMAIL_SUB} AS email,
      besk.antal AS ansatte
    FROM Virksomhed v
    LEFT JOIN Branche b ON b.CVREnhedsId = v.id AND b.sekvens = '0'
    LEFT JOIN Adressering a ON a.CVREnhedsId = v.id AND a.AdresseringAnvendelse = 'POSTADRESSE'
    ${beskJoin}
    WHERE ${conditions.join(" AND ")}
    GROUP BY v.CVRNummer
    ORDER BY a.CVRAdresse_postnummer
    LIMIT ?
  `, params);

  if (!rows.length) return "Ingen virksomheder fundet i dette postnummerinterval.";
  return `**${rows.length} virksomheder** (postnr ${args.postnummer_fra}–${args.postnummer_til}${args.branche_contains ? `, ${args.branche_contains}` : ""}):\n\n` +
    rows.map((r, i) =>
      `${i+1}. **${r.navn ?? "Ukendt"}** | CVR: ${r.CVRNummer} | ${r.ansatte ?? "?"} ans.\n   📍 ${r.postnr} ${r.by ?? ""}, ${r.kommune ?? "–"}\n   📞 ${r.telefon ?? "–"} | ✉️ ${r.email ?? "–"}\n   🏭 ${r.branche ?? "–"}`
    ).join("\n\n");
}

// ── JSON-RPC router ───────────────────────────────────────────────────────
function jsonrpc(id: unknown, result: unknown) {
  return NextResponse.json({ jsonrpc: "2.0", id, result }, {
    headers: { "Content-Type": "application/json" },
  });
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
      serverInfo: { name: "CVR Danmark", version: "2.0.0" },
    });
  }
  if (method === "notifications/initialized") return new NextResponse(null, { status: 204 });
  if (method === "tools/list") return jsonrpc(id, { tools: TOOLS });

  if (method === "tools/call") {
    const { name, arguments: args = {} } = params as { name: string; arguments?: Args };
    try {
      const handlers: Record<string, (a: Args) => Promise<string>> = {
        find_leads,
        find_companies,
        count_companies,
        get_company,
        search_by_name,
        list_branches,
        market_by_municipality,
        employee_distribution,
        market_overview,
        find_companies_by_postcode_range,
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
  return NextResponse.json({ name: "CVR Danmark MCP v2", tools: TOOLS.map(t => t.name) });
}
