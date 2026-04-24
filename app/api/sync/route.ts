import { NextRequest, NextResponse } from "next/server";

// ── Config ─────────────────────────────────────────────────────────────────
const SB_URL   = "https://cvbtnmqchpzgsjcjcorj.supabase.co";
const SB_KEY   = process.env.SUPABASE_KEY!;
const DF_KEY   = process.env.DATAFORDELER_KEY!;
const SECRET   = process.env.SYNC_SECRET!;          // protects the endpoint
const GQL      = `https://graphql.datafordeler.dk/CVR/v1?apikey=${DF_KEY}`;
const BATCH    = 50;                                 // IDs per GraphQL in-query
const MAX_PAGES = 20;                               // max pagination pages per run (~1000 companies)

// ── Helpers ────────────────────────────────────────────────────────────────

/** Convert ALL CAPS kommune names to title case, e.g. "KØBENHAVN" → "København" */
function toTitleCase(s: string | null | undefined): string | null {
  if (!s) return s ?? null;
  return s.toLowerCase().replace(/(?:^|\s|-)\S/g, c => c.toUpperCase());
}
const sbHeaders = {
  Authorization: `Bearer ${SB_KEY}`,
  apikey: SB_KEY,
  "Content-Type": "application/json",
  Prefer: "resolution=merge-duplicates,return=minimal",
};

async function gql(query: string): Promise<unknown> {
  const res = await fetch(GQL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  return res.json();
}

async function sbGet(path: string): Promise<unknown> {
  const res = await fetch(`${SB_URL}${path}`, { headers: sbHeaders });
  return res.json();
}

async function sbUpsert(rows: object[]): Promise<void> {
  if (!rows.length) return;
  await fetch(`${SB_URL}/rest/v1/companies`, {
    method: "POST",
    headers: sbHeaders,
    body: JSON.stringify(rows),
  });
}

async function sbUpdateSyncState(ts: string): Promise<void> {
  await fetch(`${SB_URL}/rest/v1/sync_state?key=eq.last_synced_at`, {
    method: "PATCH",
    headers: sbHeaders,
    body: JSON.stringify({ value: ts, updated_at: new Date().toISOString() }),
  });
}

// ── GraphQL queries ─────────────────────────────────────────────────────────

/** Get all Virksomhed IDs changed since a timestamp, with pagination */
async function getChangedVirksomheder(since: string): Promise<{ id: string; cvr: string; status: string; startdato: string | null }[]> {
  const results: { id: string; cvr: string; status: string; startdato: string | null }[] = [];
  let cursor: string | null = null;
  let page = 0;

  while (page < MAX_PAGES) {
    const afterClause = cursor ? `, after: "${cursor}"` : "";
    const data = await gql(`{
      CVR_Virksomhed(first: ${BATCH}, where: { registreringFra: { gt: "${since}" } }${afterClause}) {
        nodes { id CVRNummer status virksomhedStartdato virksomhedOphoersdato }
        pageInfo { hasNextPage endCursor }
      }
    }`) as { data?: { CVR_Virksomhed?: { nodes: { id: string; CVRNummer: number; status: string; virksomhedStartdato: string | null; virksomhedOphoersdato: string | null }[]; pageInfo: { hasNextPage: boolean; endCursor: string } } } };

    const conn = data?.data?.CVR_Virksomhed;
    if (!conn?.nodes?.length) break;

    for (const n of conn.nodes) {
      results.push({ id: n.id, cvr: String(n.CVRNummer), status: n.status, startdato: n.virksomhedStartdato });
    }

    if (!conn.pageInfo.hasNextPage) break;
    cursor = conn.pageInfo.endCursor;
    page++;
  }
  return results;
}

/** Batch-fetch current Navn for a list of CVREnhedsIds */
async function batchNavn(ids: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const idList = ids.map(id => `"${id}"`).join(",");
  const data = await gql(`{
    CVR_Navn(first: ${ids.length * 5}, where: { CVREnhedsId: { in: [${idList}] }, virkningTil: { eq: null } }) {
      nodes { CVREnhedsId vaerdi sekvens registreringTil }
    }
  }`) as { data?: { CVR_Navn?: { nodes: { CVREnhedsId: string; vaerdi: string; sekvens: number; registreringTil: string | null }[] } } };

  for (const n of data?.data?.CVR_Navn?.nodes ?? []) {
    if (n.registreringTil !== null) continue;
    if (!map.has(n.CVREnhedsId) || n.sekvens < 999) {
      map.set(n.CVREnhedsId, n.vaerdi);
    }
  }
  return map;
}

/** Batch-fetch current Adressering */
async function batchAdressering(ids: string[]): Promise<Map<string, { vejnavn: string | null; husnummer: string | null; postnummer: string | null; postdistrikt: string | null; kommunenavn: string | null; kommunekode: string | null }>> {
  const map = new Map<string, { vejnavn: string | null; husnummer: string | null; postnummer: string | null; postdistrikt: string | null; kommunenavn: string | null; kommunekode: string | null }>();
  const idList = ids.map(id => `"${id}"`).join(",");
  const data = await gql(`{
    CVR_Adressering(first: ${ids.length * 3}, where: { CVREnhedsId: { in: [${idList}] }, registreringTil: { eq: null } }) {
      nodes { CVREnhedsId CVRAdresse_vejnavn CVRAdresse_husnummerFra CVRAdresse_postnummer CVRAdresse_postdistrikt CVRAdresse_kommunenavn CVRAdresse_kommunekode AdresseringAnvendelse }
    }
  }`) as { data?: { CVR_Adressering?: { nodes: { CVREnhedsId: string; CVRAdresse_vejnavn: string | null; CVRAdresse_husnummerFra: string | null; CVRAdresse_postnummer: string | null; CVRAdresse_postdistrikt: string | null; CVRAdresse_kommunenavn: string | null; CVRAdresse_kommunekode: string | null; AdresseringAnvendelse: string }[] } } };

  for (const n of data?.data?.CVR_Adressering?.nodes ?? []) {
    const pri = n.AdresseringAnvendelse === "beliggenhedsadresse" ? 0 : 1;
    if (!map.has(n.CVREnhedsId) || pri === 0) {
      map.set(n.CVREnhedsId, {
        vejnavn: n.CVRAdresse_vejnavn,
        husnummer: n.CVRAdresse_husnummerFra,
        postnummer: n.CVRAdresse_postnummer,
        postdistrikt: n.CVRAdresse_postdistrikt,
        kommunenavn: n.CVRAdresse_kommunenavn,
        kommunekode: n.CVRAdresse_kommunekode,
      });
    }
  }
  return map;
}

/** Batch-fetch current Branche (sekvens=0 = primary) */
async function batchBranche(ids: string[]): Promise<Map<string, { branchekode: string; branchetekst: string }>> {
  const map = new Map<string, { branchekode: string; branchetekst: string }>();
  const idList = ids.map(id => `"${id}"`).join(",");
  const data = await gql(`{
    CVR_Branche(first: ${ids.length * 3}, where: { CVREnhedsId: { in: [${idList}] }, sekvens: { eq: 0 }, virkningTil: { eq: null } }) {
      nodes { CVREnhedsId vaerdi vaerdiTekst }
    }
  }`) as { data?: { CVR_Branche?: { nodes: { CVREnhedsId: string; vaerdi: string; vaerdiTekst: string }[] } } };

  for (const n of data?.data?.CVR_Branche?.nodes ?? []) {
    if (!map.has(n.CVREnhedsId)) map.set(n.CVREnhedsId, { branchekode: n.vaerdi, branchetekst: n.vaerdiTekst });
  }
  return map;
}

/** Batch-fetch current Telefon */
async function batchTelefon(ids: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const idList = ids.map(id => `"${id}"`).join(",");
  const data = await gql(`{
    CVR_Telefonnummer(first: ${ids.length * 2}, where: { CVREnhedsId: { in: [${idList}] }, virkningTil: { eq: null } }) {
      nodes { CVREnhedsId vaerdi }
    }
  }`) as { data?: { CVR_Telefonnummer?: { nodes: { CVREnhedsId: string; vaerdi: string }[] } } };

  for (const n of data?.data?.CVR_Telefonnummer?.nodes ?? []) {
    if (!map.has(n.CVREnhedsId)) map.set(n.CVREnhedsId, n.vaerdi);
  }
  return map;
}

/** Batch-fetch current Email */
async function batchEmail(ids: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const idList = ids.map(id => `"${id}"`).join(",");
  const data = await gql(`{
    CVR_Emailadresse(first: ${ids.length * 2}, where: { CVREnhedsId: { in: [${idList}] }, virkningTil: { eq: null } }) {
      nodes { CVREnhedsId vaerdi }
    }
  }`) as { data?: { CVR_Emailadresse?: { nodes: { CVREnhedsId: string; vaerdi: string }[] } } };

  for (const n of data?.data?.CVR_Emailadresse?.nodes ?? []) {
    if (!map.has(n.CVREnhedsId)) map.set(n.CVREnhedsId, n.vaerdi);
  }
  return map;
}

/** Batch-fetch current Virksomhedsform */
async function batchVirksomhedsform(ids: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const idList = ids.map(id => `"${id}"`).join(",");
  const data = await gql(`{
    CVR_Virksomhedsform(first: ${ids.length * 2}, where: { CVREnhedsId: { in: [${idList}] }, virkningTil: { eq: null } }) {
      nodes { CVREnhedsId vaerdiTekst }
    }
  }`) as { data?: { CVR_Virksomhedsform?: { nodes: { CVREnhedsId: string; vaerdiTekst: string }[] } } };

  for (const n of data?.data?.CVR_Virksomhedsform?.nodes ?? []) {
    if (!map.has(n.CVREnhedsId)) map.set(n.CVREnhedsId, n.vaerdiTekst);
  }
  return map;
}

// ── Main sync logic ─────────────────────────────────────────────────────────

async function runSync(): Promise<{ synced: number; since: string; newSyncTime: string }> {
  // 1. Get last sync time
  const stateRes = await sbGet("/rest/v1/sync_state?key=eq.last_synced_at&select=value") as { value: string }[];
  const since = stateRes?.[0]?.value ?? new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();

  // 2. Get all changed companies since last sync
  const changed = await getChangedVirksomheder(since);
  if (!changed.length) {
    const newTs = new Date().toISOString();
    await sbUpdateSyncState(newTs);
    return { synced: 0, since, newSyncTime: newTs };
  }

  // 3. Process in batches of BATCH size
  let totalSynced = 0;
  for (let i = 0; i < changed.length; i += BATCH) {
    const batch = changed.slice(i, i + BATCH);
    const ids = batch.map(c => c.id);

    // Parallel fetch all entity types
    const [navne, adresser, brancher, telefoner, emails, former] = await Promise.all([
      batchNavn(ids),
      batchAdressering(ids),
      batchBranche(ids),
      batchTelefon(ids),
      batchEmail(ids),
      batchVirksomhedsform(ids),
    ]);

    // Build company records
    const rows = batch.map(c => {
      const adr = adresser.get(c.id) ?? {};
      const br  = brancher.get(c.id) ?? {};
      return {
        cvr:            c.cvr,
        navn:           navne.get(c.id) ?? null,
        status:         c.status,
        stiftelsesdato: c.startdato,
        vejnavn:        (adr as { vejnavn?: string | null }).vejnavn ?? null,
        husnummer:      (adr as { husnummer?: string | null }).husnummer ?? null,
        postnummer:     (adr as { postnummer?: string | null }).postnummer ?? null,
        postdistrikt:   (adr as { postdistrikt?: string | null }).postdistrikt ?? null,
        kommunenavn:    toTitleCase((adr as { kommunenavn?: string | null }).kommunenavn),
        kommunekode:    (adr as { kommunekode?: string | null }).kommunekode ?? null,
        branchekode:    (br as { branchekode?: string }).branchekode ?? null,
        branchetekst:   (br as { branchetekst?: string }).branchetekst ?? null,
        virksomhedsform: former.get(c.id) ?? null,
        telefon:        telefoner.get(c.id) ?? null,
        email:          emails.get(c.id) ?? null,
      };
    });

    await sbUpsert(rows);
    totalSynced += rows.length;
  }

  // 4. Update sync state
  const newTs = new Date().toISOString();
  await sbUpdateSyncState(newTs);

  return { synced: totalSynced, since, newSyncTime: newTs };
}

// ── Route handler ────────────────────────────────────────────────────────────

export async function GET(req: NextRequest) {
  // Protect with secret token
  const token = req.nextUrl.searchParams.get("secret") ?? req.headers.get("x-sync-secret");
  if (token !== SECRET) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const result = await runSync();
    return NextResponse.json({ ok: true, ...result });
  } catch (err) {
    console.error("Sync error:", err);
    return NextResponse.json({ ok: false, error: String(err) }, { status: 500 });
  }
}

// Allow Vercel cron (POST)
export async function POST(req: NextRequest) {
  return GET(req);
}
