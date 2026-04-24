-- ============================================================
-- CVR Companies flat table for Supabase
-- Run this in Supabase SQL Editor before importing
-- ============================================================

-- Enable trigram extension for fast text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Drop and recreate (safe to run multiple times)
DROP TABLE IF EXISTS companies;

CREATE TABLE companies (
  cvr           TEXT PRIMARY KEY,
  navn          TEXT,
  status        TEXT,           -- 'aktiv' | 'ophørt' | ...
  stiftelsesdato TEXT,
  vejnavn       TEXT,
  husnummer     TEXT,
  postnummer    TEXT,
  postdistrikt  TEXT,
  kommunenavn   TEXT,
  kommunekode   TEXT,
  branchekode   TEXT,
  branchetekst  TEXT,
  virksomhedsform TEXT,
  telefon       TEXT,
  email         TEXT,
  antal_ansatte INTEGER,        -- filled in Phase 2
  updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Indexes for MCP query patterns
CREATE INDEX idx_companies_status       ON companies(status);
CREATE INDEX idx_companies_postnummer   ON companies(postnummer);
CREATE INDEX idx_companies_kommunenavn  ON companies(lower(kommunenavn));
CREATE INDEX idx_companies_branchekode  ON companies(branchekode);
CREATE INDEX idx_companies_ansatte      ON companies(antal_ansatte);

-- Trigram indexes for fast ILIKE '%...%' searches
CREATE INDEX idx_companies_navn_trgm        ON companies USING gin(lower(navn) gin_trgm_ops);
CREATE INDEX idx_companies_branchetekst_trgm ON companies USING gin(lower(branchetekst) gin_trgm_ops);

-- Composite index for the most common MCP query pattern
CREATE INDEX idx_companies_status_branche ON companies(status, branchekode);
CREATE INDEX idx_companies_status_postnr  ON companies(status, postnummer);
