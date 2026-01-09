-- ============================================
-- DW Load - dim_tiempo
-- Genera un rango de fechas y lo inserta (sin duplicar)
-- ============================================

BEGIN;

-- Ajusta el rango si quieres. Recomiendo 2023-01-01 a 2026-12-31
WITH fechas AS (
  SELECT d::date AS fecha
  FROM generate_series('2023-01-01'::date, '2026-12-31'::date, interval '1 day') AS gs(d)
)
INSERT INTO dw.dim_tiempo (fecha, anio, mes, dia, trimestre, semana_anio, dia_semana)
SELECT
  f.fecha,
  EXTRACT(YEAR FROM f.fecha)::int  AS anio,
  EXTRACT(MONTH FROM f.fecha)::int AS mes,
  EXTRACT(DAY FROM f.fecha)::int   AS dia,
  EXTRACT(QUARTER FROM f.fecha)::int AS trimestre,
  EXTRACT(WEEK FROM f.fecha)::int  AS semana_anio,
  EXTRACT(ISODOW FROM f.fecha)::int AS dia_semana
FROM fechas f
ON CONFLICT (fecha) DO NOTHING;

COMMIT;
