-- ============================================================
-- Bulk statistics RPC functions — single-call data retrieval
-- ============================================================

-- 1. Full database overview in one call
--    Returns totals, top branches, top municipalities, company forms, founding years
--    Usage: POST /rest/v1/rpc/database_stats  (no body needed)
-- ============================================================
CREATE OR REPLACE FUNCTION database_stats()
RETURNS JSON
LANGUAGE SQL STABLE SECURITY DEFINER AS $$
  SELECT json_build_object(
    'total_active',   (SELECT COUNT(*) FROM companies WHERE status = 'aktiv'),
    'total_all',      (SELECT COUNT(*) FROM companies),
    'with_phone',     (SELECT COUNT(*) FROM companies WHERE status = 'aktiv' AND telefon IS NOT NULL),
    'with_email',     (SELECT COUNT(*) FROM companies WHERE status = 'aktiv' AND email IS NOT NULL),
    'with_website',   (SELECT COUNT(*) FROM companies WHERE status = 'aktiv' AND has_website = true),
    'top_branches', (
      SELECT json_agg(r) FROM (
        SELECT branchekode, branchetekst, COUNT(*) AS antal
        FROM   companies
        WHERE  status = 'aktiv' AND branchekode IS NOT NULL
        GROUP  BY branchekode, branchetekst
        ORDER  BY antal DESC
        LIMIT  25
      ) r
    ),
    'top_municipalities', (
      SELECT json_agg(r) FROM (
        SELECT kommunenavn, COUNT(*) AS antal
        FROM   companies
        WHERE  status = 'aktiv' AND kommunenavn IS NOT NULL
        GROUP  BY kommunenavn
        ORDER  BY antal DESC
        LIMIT  25
      ) r
    ),
    'by_company_form', (
      SELECT json_agg(r) FROM (
        SELECT virksomhedsform, COUNT(*) AS antal
        FROM   companies
        WHERE  status = 'aktiv' AND virksomhedsform IS NOT NULL
        GROUP  BY virksomhedsform
        ORDER  BY antal DESC
        LIMIT  15
      ) r
    ),
    'founded_by_year', (
      SELECT json_agg(r) FROM (
        SELECT EXTRACT(YEAR FROM stiftelsesdato::date)::INT AS aar, COUNT(*) AS antal
        FROM   companies
        WHERE  status = 'aktiv'
          AND  stiftelsesdato IS NOT NULL
          AND  stiftelsesdato >= '1980-01-01'
          AND  stiftelsesdato::date <= CURRENT_DATE
        GROUP  BY aar
        ORDER  BY aar
      ) r
    )
  );
$$;

-- 2. Top branches — returns all branches above a minimum count
--    Great for building complete charts without multiple calls
--    Usage: POST /rest/v1/rpc/top_branches
--    Body:  {"min_count": 100, "max_results": 100}
-- ============================================================
CREATE OR REPLACE FUNCTION top_branches(
  min_count   INT DEFAULT 50,
  max_results INT DEFAULT 100
)
RETURNS TABLE(branchekode TEXT, branchetekst TEXT, antal BIGINT)
LANGUAGE SQL STABLE SECURITY DEFINER AS $$
  SELECT branchekode, branchetekst, COUNT(*) AS antal
  FROM   companies
  WHERE  status = 'aktiv' AND branchekode IS NOT NULL
  GROUP  BY branchekode, branchetekst
  HAVING COUNT(*) >= min_count
  ORDER  BY antal DESC
  LIMIT  max_results;
$$;

-- 3. New companies — companies founded in a date range
--    Usage: POST /rest/v1/rpc/new_companies
--    Body:  {"from_date": "2026-01-01", "branche": "el", "max_results": 50}
-- ============================================================
CREATE OR REPLACE FUNCTION new_companies(
  from_date   TEXT DEFAULT NULL,
  to_date     TEXT DEFAULT NULL,
  branche     TEXT DEFAULT NULL,
  kode        TEXT DEFAULT NULL,
  kommune     TEXT DEFAULT NULL,
  max_results INT  DEFAULT 50
)
RETURNS TABLE(
  cvr TEXT, navn TEXT, stiftelsesdato TEXT,
  branchetekst TEXT, branchekode TEXT,
  kommunenavn TEXT, postnummer TEXT, postdistrikt TEXT,
  vejnavn TEXT, husnummer TEXT,
  telefon TEXT, email TEXT
)
LANGUAGE SQL STABLE SECURITY DEFINER AS $$
  SELECT
    cvr, navn, stiftelsesdato,
    branchetekst, branchekode,
    kommunenavn, postnummer, postdistrikt,
    vejnavn, husnummer,
    telefon, email
  FROM   companies
  WHERE  status = 'aktiv'
    AND  stiftelsesdato IS NOT NULL
    AND  (from_date IS NULL OR stiftelsesdato >= from_date)
    AND  (to_date   IS NULL OR stiftelsesdato <= to_date)
    AND  (branche   IS NULL OR branchetekst ILIKE '%' || branche || '%')
    AND  (kode      IS NULL OR branchekode = kode)
    AND  (kommune   IS NULL OR kommunenavn ILIKE '%' || kommune || '%')
  ORDER  BY stiftelsesdato DESC
  LIMIT  max_results;
$$;

-- Grant access
GRANT EXECUTE ON FUNCTION database_stats()     TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION top_branches(INT,INT) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION new_companies(TEXT,TEXT,TEXT,TEXT,TEXT,INT) TO anon, authenticated, service_role;
