-- slatr PostgreSQL playground initialization
-- This runs automatically when the container is first created.

-- ============================================================
-- Firebase JSONB model table (recommended for DDEX XML)
-- ============================================================
CREATE TABLE IF NOT EXISTS ddex_releases (
  _id    SERIAL PRIMARY KEY,
  data   JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GIN index for fast JSONB containment queries (@>)
CREATE INDEX IF NOT EXISTS idx_ddex_data ON ddex_releases USING GIN (data);

-- ============================================================
-- Traditional columnar table (simple example)
-- ============================================================
CREATE TABLE IF NOT EXISTS products (
  _id       SERIAL PRIMARY KEY,
  id        INTEGER NOT NULL,
  name      TEXT NOT NULL,
  price     DOUBLE PRECISION,
  available BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Seed: Firebase model sample rows
-- These mirror the DDEX ERN fields that slatr extracts.
-- ============================================================
INSERT INTO ddex_releases (data) VALUES
(
  '[
    {"name":"MessageId","value":"MSG001"},
    {"name":"MessageSender.PartyName.FullName","value":"Example Label"},
    {"name":"ISRC","value":"USAT20001234"},
    {"name":"ReferenceTitle.TitleText","value":"Summer Breeze"},
    {"name":"DisplayArtistName","value":"The Coastals"},
    {"name":"Genre","value":"Pop"},
    {"name":"PLine.Year","value":"2025"},
    {"name":"PLine.PLineText","value":"(P) 2025 Example Label"}
  ]'::jsonb
),
(
  '[
    {"name":"MessageId","value":"MSG002"},
    {"name":"MessageSender.PartyName.FullName","value":"Another Records"},
    {"name":"ISRC","value":"GBAYE0500001"},
    {"name":"ReferenceTitle.TitleText","value":"Midnight Run"},
    {"name":"DisplayArtistName","value":"Neon Pulse"},
    {"name":"Genre","value":"Electronic"},
    {"name":"Duration","value":"PT3M42S"},
    {"name":"ParentalWarningType","value":"NotExplicit"}
  ]'::jsonb
),
(
  '[
    {"name":"MessageId","value":"MSG003"},
    {"name":"MessageSender.PartyName.FullName","value":"Example Label"},
    {"name":"ISRC","value":"USAT20001235"},
    {"name":"ReferenceTitle.TitleText","value":"Winter Chill"},
    {"name":"DisplayArtistName","value":"The Coastals"},
    {"name":"Genre","value":"Ambient"},
    {"name":"PLine.Year","value":"2026"},
    {"name":"PLine.PLineText","value":"(P) 2026 Example Label"},
    {"name":"TerritoryCode","value":"US"},
    {"name":"ValidityStartDate","value":"2026-03-01"}
  ]'::jsonb
);

-- ============================================================
-- Seed: Traditional model sample rows
-- ============================================================
INSERT INTO products (id, name, price, available) VALUES
(1, 'Wireless Headphones', 79.99, true),
(2, 'USB-C Cable', 12.49, true),
(3, 'Laptop Stand', 45.00, true),
(4, 'Mechanical Keyboard', 149.99, false),
(5, 'Webcam HD', 59.95, true);

-- ============================================================
-- Handy view: flattened DDEX releases
-- Demonstrates how to extract JSONB array fields into columns.
-- ============================================================
CREATE OR REPLACE VIEW ddex_releases_flat AS
SELECT
  _id,
  (SELECT elem->>'value' FROM jsonb_array_elements(data) AS elem WHERE elem->>'name' = 'MessageId'          LIMIT 1) AS message_id,
  (SELECT elem->>'value' FROM jsonb_array_elements(data) AS elem WHERE elem->>'name' = 'ISRC'               LIMIT 1) AS isrc,
  (SELECT elem->>'value' FROM jsonb_array_elements(data) AS elem WHERE elem->>'name' = 'ReferenceTitle.TitleText' LIMIT 1) AS title,
  (SELECT elem->>'value' FROM jsonb_array_elements(data) AS elem WHERE elem->>'name' = 'DisplayArtistName'  LIMIT 1) AS artist,
  (SELECT elem->>'value' FROM jsonb_array_elements(data) AS elem WHERE elem->>'name' = 'Genre'              LIMIT 1) AS genre,
  created_at
FROM ddex_releases;
