const TURSO_URL = process.env.TURSO_URL!;
const TURSO_TOK = process.env.TURSO_TOKEN!;

interface TursoRow {
  type: string;
  value: string;
}

export async function tursoQuery(sql: string, args: string[] = []): Promise<Record<string, string | null>[]> {
  const payload = JSON.stringify({
    requests: [
      {
        type: "execute",
        stmt: {
          sql,
          args: args.map((v) => ({ type: "text", value: v })),
        },
      },
      { type: "close" },
    ],
  });

  const res = await fetch(`${TURSO_URL}/v2/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TURSO_TOK}`,
      "Content-Type": "application/json",
    },
    body: payload,
  });

  const data = await res.json();
  const result = data.results[0];
  if (result.type === "error") throw new Error(result.error.message);

  const { cols, rows } = result.response.result;
  return rows.map((row: TursoRow[]) => {
    const obj: Record<string, string | null> = {};
    cols.forEach((col: { name: string }, i: number) => {
      obj[col.name] = row[i]?.value ?? null;
    });
    return obj;
  });
}
