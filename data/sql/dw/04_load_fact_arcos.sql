-- ============================================
-- DW Load - fact_arcos_registrados (sin columna fecha en arcos)
-- Genera una "fecha derivada" estable a partir de arco_id (reproducible)
-- ============================================

BEGIN;

-- Rango de fechas que existe en dim_tiempo:
-- aquí usamos 2023-01-01 como base y repartimos en 4 años aprox (1461 días).
-- (arco_id % 1461) asegura que caiga dentro del rango.
WITH arcos_con_fecha AS (
  SELECT
    ar.miembro_id,
    ('2023-01-01'::date + ((ar.arco_id % 1461))::int) AS fecha_dw
  FROM public.arcos ar
),
agg AS (
  SELECT
    dt.tiempo_id,
    da.atleta_id,
    COUNT(*)::bigint AS cantidad_arcos
  FROM arcos_con_fecha x
  JOIN dw.dim_atleta da ON da.miembro_id = x.miembro_id
  JOIN dw.dim_tiempo dt ON dt.fecha = x.fecha_dw
  GROUP BY dt.tiempo_id, da.atleta_id
)
INSERT INTO dw.fact_arcos_registrados (tiempo_id, atleta_id, cantidad_arcos)
SELECT tiempo_id, atleta_id, cantidad_arcos
FROM agg
ON CONFLICT (tiempo_id, atleta_id) DO UPDATE
SET cantidad_arcos = EXCLUDED.cantidad_arcos;

COMMIT;
