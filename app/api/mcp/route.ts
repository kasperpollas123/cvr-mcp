import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { z } from "zod";
import { tursoQuery } from "@/lib/turso";
import { NextRequest } from "next/server";

export const maxDuration = 60;

function buildServer() {
  const server = new McpServer({
    name: "CVR Danmark",
    version: "1.0.0",
  });

  // ── Tool: count_companies ────────────────────────────────────────────────
  server.tool(
    "count_companies",
    "Tæl antallet af virksomheder i Danmark med givne kriterier",
    {
      branchekode: z.string().optional().describe("Branchekode (f.eks. '412000' for byggeentreprenører)"),
      branche_contains: z.string().optional().describe("Søg i branchetekst (f.eks. 'tømrer', 'maler', 'VVS')"),
      kommunekode: z.string().optional().describe("Kommunekode (f.eks. '101' for København)"),
      min_ansatte: z.number().optional().describe("Minimum antal ansatte"),
      max_ansatte: z.number().optional().describe("Maksimum antal ansatte"),
    },
    async ({ branchekode, branche_contains, kommunekode, min_ansatte, max_ansatte }) => {
      const conditions: string[] = ["v.status = 'NORMAL'"];
      const args: string[] = [];

      if (branchekode) {
        conditions.push(`b.vaerdi = ?`);
        args.push(branchekode);
      }
      if (branche_contains) {
        conditions.push(`b.vaerdiTekst LIKE ?`);
        args.push(`%${branche_contains}%`);
      }
      if (kommunekode) {
        conditions.push(`a.CVRAdresse_kommunekode = ?`);
        args.push(kommunekode);
      }

      const hasEmpFilter = min_ansatte !== undefined || max_ansatte !== undefined;
      if (hasEmpFilter) {
        if (min_ansatte !== undefined) {
          conditions.push(`CAST(besk.antal AS INTEGER) >= ?`);
          args.push(String(min_ansatte));
        }
        if (max_ansatte !== undefined) {
          conditions.push(`CAST(besk.antal AS INTEGER) <= ?`);
          args.push(String(max_ansatte));
        }
      }

      const beskJoin = hasEmpFilter
        ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.CVRNummer AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`
        : ``;

      const sql = `
        SELECT COUNT(DISTINCT v.CVRNummer) as antal
        FROM Virksomhed v
        LEFT JOIN Branche b ON b.CVREnhedsId = v.CVRNummer AND b.sekvens = '1' AND b.virkningTil IS NULL
        LEFT JOIN Adressering a ON a.CVREnhedsId = v.CVRNummer AND a.AdresseringAnvendelse = 'POSTADRESSE' AND a.virkningTil IS NULL
        ${beskJoin}
        WHERE ${conditions.join(" AND ")}
      `;

      const rows = await tursoQuery(sql, args);
      const antal = rows[0]?.antal ?? "0";
      return {
        content: [{ type: "text", text: `Antal virksomheder: ${antal}` }],
      };
    }
  );

  // ── Tool: find_companies ─────────────────────────────────────────────────
  server.tool(
    "find_companies",
    "Find virksomheder i Danmark med navn, branche, adresse og antal ansatte",
    {
      branchekode: z.string().optional().describe("Branchekode (f.eks. '412000')"),
      branche_contains: z.string().optional().describe("Søg i branchetekst (f.eks. 'tømrer', 'maler', 'installatør')"),
      kommunekode: z.string().optional().describe("Kommunekode"),
      kommunenavn: z.string().optional().describe("Kommunenavn (f.eks. 'Aarhus', 'København')"),
      postnummer: z.string().optional().describe("Postnummer"),
      min_ansatte: z.number().optional().describe("Minimum antal ansatte"),
      max_ansatte: z.number().optional().describe("Maksimum antal ansatte"),
      limit: z.number().optional().default(50).describe("Max antal resultater (default 50, max 200)"),
    },
    async ({ branchekode, branche_contains, kommunekode, kommunenavn, postnummer, min_ansatte, max_ansatte, limit }) => {
      const maxLimit = Math.min(limit ?? 50, 200);
      const conditions: string[] = ["v.status = 'NORMAL'"];
      const args: string[] = [];

      if (branchekode) {
        conditions.push(`b.vaerdi = ?`);
        args.push(branchekode);
      }
      if (branche_contains) {
        conditions.push(`b.vaerdiTekst LIKE ?`);
        args.push(`%${branche_contains}%`);
      }
      if (kommunekode) {
        conditions.push(`a.CVRAdresse_kommunekode = ?`);
        args.push(kommunekode);
      }
      if (kommunenavn) {
        conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`);
        args.push(`%${kommunenavn}%`);
      }
      if (postnummer) {
        conditions.push(`a.CVRAdresse_postnummer = ?`);
        args.push(postnummer);
      }

      const hasEmpFilter = min_ansatte !== undefined || max_ansatte !== undefined;
      if (hasEmpFilter) {
        if (min_ansatte !== undefined) {
          conditions.push(`CAST(besk.antal AS INTEGER) >= ?`);
          args.push(String(min_ansatte));
        }
        if (max_ansatte !== undefined) {
          conditions.push(`CAST(besk.antal AS INTEGER) <= ?`);
          args.push(String(max_ansatte));
        }
      }

      const beskJoin = hasEmpFilter
        ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.CVRNummer AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`
        : `LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.CVRNummer AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`;

      const sql = `
        SELECT
          v.CVRNummer,
          n.vaerdi AS navn,
          b.vaerdiTekst AS branche,
          b.vaerdi AS branchekode,
          a.CVRAdresse_vejnavn AS vej,
          a.CVRAdresse_husnummerFra AS husnr,
          a.CVRAdresse_postnummer AS postnr,
          a.CVRAdresse_postdistrikt AS by,
          a.CVRAdresse_kommunenavn AS kommune,
          t.vaerdi AS telefon,
          e.vaerdi AS email,
          besk.antal AS ansatte,
          besk.datoFra AS ansatte_dato
        FROM Virksomhed v
        LEFT JOIN Navn n ON n.CVREnhedsId = v.CVRNummer AND n.virkningTil IS NULL
        LEFT JOIN Branche b ON b.CVREnhedsId = v.CVRNummer AND b.sekvens = '1' AND b.virkningTil IS NULL
        LEFT JOIN Adressering a ON a.CVREnhedsId = v.CVRNummer AND a.AdresseringAnvendelse = 'POSTADRESSE' AND a.virkningTil IS NULL
        LEFT JOIN Telefonnummer t ON t.CVREnhedsId = v.CVRNummer AND t.virkningTil IS NULL
        LEFT JOIN e_mailadresse e ON e.CVREnhedsId = v.CVRNummer AND e.virkningTil IS NULL
        ${beskJoin}
        WHERE ${conditions.join(" AND ")}
        LIMIT ?
      `;
      args.push(String(maxLimit));

      const rows = await tursoQuery(sql, args);

      if (rows.length === 0) {
        return { content: [{ type: "text", text: "Ingen virksomheder fundet med disse kriterier." }] };
      }

      const lines = rows.map((r) => {
        const adresse = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
        const ansatte = r.ansatte ? `${r.ansatte} ansatte` : "ansatte ukendt";
        const kontakt = [r.telefon, r.email].filter(Boolean).join(" | ");
        return [
          `**${r.navn ?? "Ukendt"}** (CVR: ${r.CVRNummer})`,
          `  Branche: ${r.branche ?? "–"} (${r.branchekode ?? "–"})`,
          `  Adresse: ${adresse || "–"}, ${r.kommune ?? "–"}`,
          `  Kontakt: ${kontakt || "–"}`,
          `  Ansatte: ${ansatte}`,
        ].join("\n");
      });

      return {
        content: [{
          type: "text",
          text: `Fandt ${rows.length} virksomheder:\n\n${lines.join("\n\n")}`,
        }],
      };
    }
  );

  // ── Tool: get_company ────────────────────────────────────────────────────
  server.tool(
    "get_company",
    "Hent fuld information om en virksomhed via CVR nummer",
    {
      cvr_nummer: z.string().describe("CVR nummer (8 cifre)"),
    },
    async ({ cvr_nummer }) => {
      const sql = `
        SELECT
          v.CVRNummer, v.virksomhedStartdato, v.virksomhedOphoersdato, v.status,
          n.vaerdi AS navn,
          b.vaerdiTekst AS branche, b.vaerdi AS branchekode,
          a.CVRAdresse_vejnavn AS vej, a.CVRAdresse_husnummerFra AS husnr,
          a.CVRAdresse_postnummer AS postnr, a.CVRAdresse_postdistrikt AS by,
          a.CVRAdresse_kommunenavn AS kommune,
          t.vaerdi AS telefon,
          e.vaerdi AS email,
          vf.vaerdiTekst AS virksomhedsform,
          besk.antal AS ansatte, besk.datoFra AS ansatte_dato,
          besk2.antal AS aarsvaerk
        FROM Virksomhed v
        LEFT JOIN Navn n ON n.CVREnhedsId = v.CVRNummer AND n.virkningTil IS NULL
        LEFT JOIN Branche b ON b.CVREnhedsId = v.CVRNummer AND b.sekvens = '1' AND b.virkningTil IS NULL
        LEFT JOIN Adressering a ON a.CVREnhedsId = v.CVRNummer AND a.AdresseringAnvendelse = 'POSTADRESSE' AND a.virkningTil IS NULL
        LEFT JOIN Telefonnummer t ON t.CVREnhedsId = v.CVRNummer AND t.virkningTil IS NULL
        LEFT JOIN e_mailadresse e ON e.CVREnhedsId = v.CVRNummer AND e.virkningTil IS NULL
        LEFT JOIN Virksomhedsform vf ON vf.CVREnhedsId = v.CVRNummer AND vf.virkningTil IS NULL
        LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.CVRNummer AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'
        LEFT JOIN Beskaeftigelse_latest besk2 ON besk2.CVREnhedsId = v.CVRNummer AND besk2.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAarsvaerk'
        WHERE v.CVRNummer = ?
        LIMIT 1
      `;

      const rows = await tursoQuery(sql, [cvr_nummer]);
      if (rows.length === 0) {
        return { content: [{ type: "text", text: `Ingen virksomhed fundet med CVR ${cvr_nummer}` }] };
      }

      const r = rows[0];
      const adresse = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
      const text = [
        `**${r.navn ?? "Ukendt navn"}**`,
        `CVR: ${r.CVRNummer}`,
        `Form: ${r.virksomhedsform ?? "–"}`,
        `Status: ${r.status ?? "–"}`,
        `Startet: ${r.virksomhedStartdato ?? "–"}`,
        ...(r.virksomhedOphoersdato ? [`Ophørt: ${r.virksomhedOphoersdato}`] : []),
        ``,
        `Branche: ${r.branche ?? "–"} (${r.branchekode ?? "–"})`,
        `Adresse: ${adresse || "–"}`,
        `Kommune: ${r.kommune ?? "–"}`,
        ``,
        `Telefon: ${r.telefon ?? "–"}`,
        `Email: ${r.email ?? "–"}`,
        ``,
        `Ansatte: ${r.ansatte ?? "ukendt"} (per ${r.ansatte_dato ?? "–"})`,
        `Årsværk: ${r.aarsvaerk ?? "ukendt"}`,
      ].join("\n");

      return { content: [{ type: "text", text }] };
    }
  );

  // ── Tool: find_leads ─────────────────────────────────────────────────────
  server.tool(
    "find_leads",
    "Find potentielle leads — virksomheder der leverer og installerer (f.eks. VVS, el, tømrer, maler) med kontaktinfo",
    {
      branche_contains: z.string().describe("Branche søgeord (f.eks. 'VVS', 'elektro', 'tømrer', 'maler', 'gulv')"),
      kommunenavn: z.string().optional().describe("By/kommune (f.eks. 'Aarhus', 'Odense')"),
      min_ansatte: z.number().optional().describe("Minimum antal ansatte"),
      max_ansatte: z.number().optional().describe("Maksimum antal ansatte"),
      kun_med_kontakt: z.boolean().optional().default(true).describe("Kun virksomheder med telefon eller email"),
      limit: z.number().optional().default(50).describe("Max resultater"),
    },
    async ({ branche_contains, kommunenavn, min_ansatte, max_ansatte, kun_med_kontakt, limit }) => {
      const maxLimit = Math.min(limit ?? 50, 200);
      const conditions: string[] = ["v.status = 'NORMAL'", `b.vaerdiTekst LIKE ?`];
      const args: string[] = [`%${branche_contains}%`];

      if (kommunenavn) {
        conditions.push(`a.CVRAdresse_kommunenavn LIKE ?`);
        args.push(`%${kommunenavn}%`);
      }
      if (kun_med_kontakt !== false) {
        conditions.push(`(t.vaerdi IS NOT NULL OR e.vaerdi IS NOT NULL)`);
      }

      const hasEmpFilter = min_ansatte !== undefined || max_ansatte !== undefined;
      if (hasEmpFilter) {
        if (min_ansatte !== undefined) {
          conditions.push(`CAST(besk.antal AS INTEGER) >= ?`);
          args.push(String(min_ansatte));
        }
        if (max_ansatte !== undefined) {
          conditions.push(`CAST(besk.antal AS INTEGER) <= ?`);
          args.push(String(max_ansatte));
        }
      }

      const beskJoin = hasEmpFilter
        ? `JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.CVRNummer AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`
        : `LEFT JOIN Beskaeftigelse_latest besk ON besk.CVREnhedsId = v.CVRNummer AND besk.beskaeftigelsestalstype = 'AarsbeskaeftigelseAntalAnsatte'`;

      const sql = `
        SELECT
          v.CVRNummer,
          n.vaerdi AS navn,
          b.vaerdiTekst AS branche,
          a.CVRAdresse_vejnavn AS vej,
          a.CVRAdresse_husnummerFra AS husnr,
          a.CVRAdresse_postnummer AS postnr,
          a.CVRAdresse_postdistrikt AS by,
          a.CVRAdresse_kommunenavn AS kommune,
          t.vaerdi AS telefon,
          e.vaerdi AS email,
          besk.antal AS ansatte
        FROM Virksomhed v
        LEFT JOIN Navn n ON n.CVREnhedsId = v.CVRNummer AND n.virkningTil IS NULL
        LEFT JOIN Branche b ON b.CVREnhedsId = v.CVRNummer AND b.sekvens = '1' AND b.virkningTil IS NULL
        LEFT JOIN Adressering a ON a.CVREnhedsId = v.CVRNummer AND a.AdresseringAnvendelse = 'POSTADRESSE' AND a.virkningTil IS NULL
        LEFT JOIN Telefonnummer t ON t.CVREnhedsId = v.CVRNummer AND t.virkningTil IS NULL
        LEFT JOIN e_mailadresse e ON e.CVREnhedsId = v.CVRNummer AND e.virkningTil IS NULL
        ${beskJoin}
        WHERE ${conditions.join(" AND ")}
        ORDER BY CAST(besk.antal AS INTEGER) DESC
        LIMIT ?
      `;
      args.push(String(maxLimit));

      const rows = await tursoQuery(sql, args);

      if (rows.length === 0) {
        return { content: [{ type: "text", text: "Ingen leads fundet med disse kriterier." }] };
      }

      const lines = rows.map((r, idx) => {
        const adresse = [r.vej, r.husnr, r.postnr, r.by].filter(Boolean).join(" ");
        const ansatte = r.ansatte ?? "?";
        return `${idx + 1}. **${r.navn ?? "Ukendt"}** | CVR: ${r.CVRNummer} | ${ansatte} ans.\n   📍 ${adresse}, ${r.kommune ?? "–"}\n   📞 ${r.telefon ?? "–"} | ✉️ ${r.email ?? "–"}\n   🏭 ${r.branche ?? "–"}`;
      });

      return {
        content: [{
          type: "text",
          text: `**${rows.length} leads fundet** (${branche_contains}${kommunenavn ? `, ${kommunenavn}` : ""}):\n\n${lines.join("\n\n")}`,
        }],
      };
    }
  );

  return server;
}

// ── HTTP handler ──────────────────────────────────────────────────────────
async function handleMcpRequest(req: NextRequest): Promise<Response> {
  const server = buildServer();
  const transport = new WebStandardStreamableHTTPServerTransport({
    sessionIdGenerator: undefined, // stateless
  });

  await server.connect(transport);

  const response = await transport.handleRequest(req);

  await server.close();
  return response;
}

export async function POST(req: NextRequest) {
  return handleMcpRequest(req);
}

export async function GET(req: NextRequest) {
  return handleMcpRequest(req);
}

export async function DELETE(req: NextRequest) {
  return handleMcpRequest(req);
}
