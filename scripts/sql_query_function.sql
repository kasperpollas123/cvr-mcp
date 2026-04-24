-- ============================================================
-- run_query: safe read-only SQL execution via RPC
-- Only SELECT and WITH (CTE) queries allowed.
-- Enforces row limit + statement timeout.
-- ============================================================

CREATE OR REPLACE FUNCTION run_query(query TEXT, row_limit INT DEFAULT 500)
RETURNS JSON
LANGUAGE plpgsql STABLE SECURITY DEFINER AS $$
DECLARE
  result     JSON;
  first_word TEXT;
  safe_sql   TEXT;
BEGIN
  -- Reject anything that isn't a SELECT or CTE
  first_word := upper(regexp_replace(trim(query), '\s+', ' ', 'g'));
  first_word := split_part(first_word, ' ', 1);

  IF first_word NOT IN ('SELECT', 'WITH') THEN
    RAISE EXCEPTION 'Only SELECT and WITH (CTE) queries are allowed. Received: %', first_word;
  END IF;

  -- Hard cap: never return more than 1000 rows
  row_limit := LEAST(COALESCE(row_limit, 500), 1000);

  -- Wrap in JSON aggregation with limit
  safe_sql := format(
    'SELECT json_agg(t) FROM (SELECT * FROM (%s) _q LIMIT %s) t',
    query, row_limit
  );

  -- Statement timeout: 8 seconds (Vercel limit is 60s, but queries should be fast)
  SET LOCAL statement_timeout = '8s';

  EXECUTE safe_sql INTO result;

  RETURN COALESCE(result, '[]'::json);
END;
$$;

GRANT EXECUTE ON FUNCTION run_query(TEXT, INT) TO anon, authenticated, service_role;

-- Helpful comment: the companies table schema for Claude to reference
COMMENT ON FUNCTION run_query IS
'Read-only SQL tool. Table: companies
Columns: cvr TEXT, navn TEXT, status TEXT (aktiv/ophørt),
  stiftelsesdato TEXT (YYYY-MM-DD), vejnavn TEXT, husnummer TEXT,
  postnummer TEXT, postdistrikt TEXT, kommunenavn TEXT, kommunekode TEXT,
  branchekode TEXT, branchetekst TEXT, virksomhedsform TEXT,
  telefon TEXT, email TEXT, antal_ansatte INTEGER, has_website BOOLEAN.
Only aktiv companies have meaningful data. Filter with: WHERE status = ''aktiv''';
