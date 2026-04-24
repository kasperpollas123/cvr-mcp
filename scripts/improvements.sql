-- ============================================================
-- Improvements: proper aggregation RPC functions + has_website
-- ============================================================

-- 1. has_website computed column
-- True when email exists and domain is NOT a free provider
ALTER TABLE companies ADD COLUMN IF NOT EXISTS has_website BOOLEAN
  GENERATED ALWAYS AS (
    email IS NOT NULL
    AND email NOT ILIKE '%@gmail.%'
    AND email NOT ILIKE '%@hotmail.%'
    AND email NOT ILIKE '%@yahoo.%'
    AND email NOT ILIKE '%@outlook.%'
    AND email NOT ILIKE '%@live.%'
    AND email NOT ILIKE '%@icloud.%'
    AND email NOT ILIKE '%@me.com'
    AND email NOT ILIKE '%@msn.com'
    AND email NOT ILIKE '%@mail.dk'
    AND email NOT ILIKE '%@webspeed.dk'
    AND email NOT ILIKE '%@jubii.dk'
    AND email NOT ILIKE '%@post.tele.dk'
  ) STORED;

CREATE INDEX IF NOT EXISTS idx_companies_has_website ON companies(has_website);

-- ============================================================
-- 2. RPC: list_branches — proper GROUP BY (replaces client-side dedup)
-- Usage: POST /rest/v1/rpc/list_branches_agg
-- Body:  {"search_term": "VVS", "max_results": 30}
-- ============================================================
CREATE OR REPLACE FUNCTION list_branches_agg(
  search_term TEXT,
  max_results  INT DEFAULT 30
)
RETURNS TABLE(branchekode TEXT, branchetekst TEXT, antal BIGINT)
LANGUAGE SQL STABLE SECURITY DEFINER AS $$
  SELECT branchekode, branchetekst, COUNT(*) AS antal
  FROM   companies
  WHERE  status = 'aktiv'
    AND  branchetekst ILIKE '%' || search_term || '%'
    AND  branchekode IS NOT NULL
  GROUP  BY branchekode, branchetekst
  ORDER  BY antal DESC
  LIMIT  max_results;
$$;

-- ============================================================
-- 3. RPC: market_by_municipality — proper GROUP BY
-- Usage: POST /rest/v1/rpc/market_by_municipality_agg
-- Body:  {"search_term": "tømrer", "max_results": 20}
-- ============================================================
CREATE OR REPLACE FUNCTION market_by_municipality_agg(
  search_term TEXT    DEFAULT NULL,
  kode        TEXT    DEFAULT NULL,
  min_emp     INT     DEFAULT NULL,
  max_results INT     DEFAULT 20
)
RETURNS TABLE(kommunenavn TEXT, antal BIGINT)
LANGUAGE SQL STABLE SECURITY DEFINER AS $$
  SELECT kommunenavn, COUNT(*) AS antal
  FROM   companies
  WHERE  status = 'aktiv'
    AND  kommunenavn IS NOT NULL
    AND  (search_term IS NULL OR branchetekst ILIKE '%' || search_term || '%')
    AND  (kode        IS NULL OR branchekode = kode)
    AND  (min_emp     IS NULL OR antal_ansatte >= min_emp)
  GROUP  BY kommunenavn
  ORDER  BY antal DESC
  LIMIT  max_results;
$$;

-- ============================================================
-- 4. RPC: employee_distribution — proper histogram
-- Usage: POST /rest/v1/rpc/employee_distribution_agg
-- Body:  {"search_term": "maler", "kommune": "Aarhus"}
-- ============================================================
CREATE OR REPLACE FUNCTION employee_distribution_agg(
  search_term TEXT DEFAULT NULL,
  kode        TEXT DEFAULT NULL,
  kommune     TEXT DEFAULT NULL
)
RETURNS TABLE(stoerrelse TEXT, antal BIGINT, sort_key INT)
LANGUAGE SQL STABLE SECURITY DEFINER AS $$
  SELECT
    CASE
      WHEN antal_ansatte = 0    THEN '0'
      WHEN antal_ansatte <= 4   THEN '1-4'
      WHEN antal_ansatte <= 9   THEN '5-9'
      WHEN antal_ansatte <= 19  THEN '10-19'
      WHEN antal_ansatte <= 49  THEN '20-49'
      WHEN antal_ansatte <= 99  THEN '50-99'
      WHEN antal_ansatte <= 249 THEN '100-249'
      ELSE '250+'
    END                        AS stoerrelse,
    COUNT(*)                   AS antal,
    MIN(antal_ansatte)         AS sort_key
  FROM   companies
  WHERE  status = 'aktiv'
    AND  antal_ansatte IS NOT NULL
    AND  (search_term IS NULL OR branchetekst ILIKE '%' || search_term || '%')
    AND  (kode        IS NULL OR branchekode = kode)
    AND  (kommune     IS NULL OR kommunenavn ILIKE '%' || kommune || '%')
  GROUP  BY stoerrelse
  ORDER  BY sort_key;
$$;

-- ============================================================
-- 5. RPC: market_overview — single query for full overview
-- Usage: POST /rest/v1/rpc/market_overview_agg
-- Body:  {"search_term": "VVS"}
-- ============================================================
CREATE OR REPLACE FUNCTION market_overview_agg(search_term TEXT)
RETURNS JSON
LANGUAGE SQL STABLE SECURITY DEFINER AS $$
  SELECT json_build_object(
    'total', (
      SELECT COUNT(*) FROM companies
      WHERE status = 'aktiv' AND branchetekst ILIKE '%' || search_term || '%'
    ),
    'top_kommuner', (
      SELECT json_agg(r) FROM (
        SELECT kommunenavn, COUNT(*) AS antal
        FROM   companies
        WHERE  status = 'aktiv'
          AND  branchetekst ILIKE '%' || search_term || '%'
          AND  kommunenavn IS NOT NULL
        GROUP  BY kommunenavn
        ORDER  BY antal DESC
        LIMIT  10
      ) r
    ),
    'top_brancher', (
      SELECT json_agg(r) FROM (
        SELECT branchetekst, branchekode, COUNT(*) AS antal
        FROM   companies
        WHERE  status = 'aktiv'
          AND  branchetekst ILIKE '%' || search_term || '%'
        GROUP  BY branchetekst, branchekode
        ORDER  BY antal DESC
        LIMIT  10
      ) r
    )
  );
$$;

-- Allow public (anon + service_role) to call these functions
GRANT EXECUTE ON FUNCTION list_branches_agg        TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION market_by_municipality_agg TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION employee_distribution_agg  TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION market_overview_agg        TO anon, authenticated, service_role;
