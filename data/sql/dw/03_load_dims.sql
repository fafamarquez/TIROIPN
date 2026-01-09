-- ============================================
-- DW Load - dimensiones desde OLTP (public)
-- Orden: coach -> clase -> atleta
-- Usa UPSERT por si lo corres varias veces
-- ============================================

BEGIN;

-- 1) dim_coach
-- Suposición del modelo: tabla public.coachs tiene PK miembro_id
INSERT INTO dw.dim_coach (coach_id)
SELECT c.miembro_id
FROM public.coachs c
ON CONFLICT (coach_id) DO NOTHING;

-- 2) dim_clase
-- Suposición del modelo: public.clases tiene:
-- clase_id, dias, hora_inicio, hora_fin, nivel, coach_id
INSERT INTO dw.dim_clase (clase_id, dias, hora_inicio, hora_fin, nivel_clase, coach_id)
SELECT
  cl.clase_id,
  cl.dias,
  cl.hora_inicio,
  cl.hora_fin,
  cl.nivel,
  cl.coach_id
FROM public.clases cl
ON CONFLICT (clase_id) DO UPDATE
SET
  dias = EXCLUDED.dias,
  hora_inicio = EXCLUDED.hora_inicio,
  hora_fin = EXCLUDED.hora_fin,
  nivel_clase = EXCLUDED.nivel_clase,
  coach_id = EXCLUDED.coach_id;

-- 3) dim_atleta
-- Suposición del modelo: public.atletas tiene:
-- miembro_id, alumno_ipn, nivel, clase_id
INSERT INTO dw.dim_atleta (miembro_id, alumno_ipn, nivel_atleta, clase_id)
SELECT
  a.miembro_id,
  a.alumno_ipn,
  a.nivel,
  a.clase_id
FROM public.atletas a
ON CONFLICT (miembro_id) DO UPDATE
SET
  alumno_ipn = EXCLUDED.alumno_ipn,
  nivel_atleta = EXCLUDED.nivel_atleta,
  clase_id = EXCLUDED.clase_id;

COMMIT;
