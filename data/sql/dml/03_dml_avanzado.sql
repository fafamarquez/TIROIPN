/* =========================================================
   Practica 5 - Ejercicio 3: DML Avanzado (PostgreSQL)
   BD: archery_db
   Tablas: miembros, coachs, clases, atletas, arcos, coach_certificacion
   ========================================================= */

-- Recomendación para pgAdmin/psql:
-- \timing on (en psql) o usa la pestaña "Messages" en pgAdmin.

-- =========================================================
-- 3.1 SELECT (mínimo 10) cubriendo varios tipos
-- =========================================================

/* S1) JOIN múltiple (3+ tablas): Atletas con su clase y coach */
SELECT
  a.miembro_id AS atleta_id,
  m.correo AS atleta_correo,
  a.nivel AS atleta_nivel,
  c.clase_id,
  c.dias,
  c.hora_inicio,
  c.hora_fin,
  mc.correo AS coach_correo
FROM atletas a
JOIN miembros m ON m.miembro_id = a.miembro_id
JOIN clases c ON c.clase_id = a.clase_id
JOIN miembros mc ON mc.miembro_id = c.coach_id
ORDER BY c.clase_id, a.miembro_id
LIMIT 50;

/* S2) Agregación + GROUP BY + HAVING: clases con más atletas */
SELECT
  c.clase_id,
  c.nivel,
  c.dias,
  COUNT(*) AS total_atletas
FROM clases c
JOIN atletas a ON a.clase_id = c.clase_id
GROUP BY c.clase_id, c.nivel, c.dias
HAVING COUNT(*) >= 50
ORDER BY total_atletas DESC
LIMIT 20;

/* S3) Subconsulta correlacionada: atletas que tienen más arcos que el promedio */
SELECT
  a.miembro_id,
  m.correo,
  (SELECT COUNT(*) FROM arcos ar WHERE ar.miembro_id = a.miembro_id) AS arcos_del_atleta
FROM atletas a
JOIN miembros m ON m.miembro_id = a.miembro_id
WHERE (SELECT COUNT(*) FROM arcos ar WHERE ar.miembro_id = a.miembro_id) >
      (SELECT AVG(cnt) FROM (
          SELECT COUNT(*) AS cnt
          FROM arcos
          GROUP BY miembro_id
      ) x)
ORDER BY arcos_del_atleta DESC
LIMIT 50;

/* S4) Window function: ranking de coaches por número de atletas atendidos */
SELECT
  c.coach_id,
  mc.correo AS coach_correo,
  COUNT(a.miembro_id) AS atletas_total,
  DENSE_RANK() OVER (ORDER BY COUNT(a.miembro_id) DESC) AS rank_coach
FROM clases c
JOIN miembros mc ON mc.miembro_id = c.coach_id
LEFT JOIN atletas a ON a.clase_id = c.clase_id
GROUP BY c.coach_id, mc.correo
ORDER BY rank_coach, atletas_total DESC
LIMIT 30;

/* S5) CTE: distribución de niveles por clase */
WITH dist AS (
  SELECT
    a.clase_id,
    a.nivel,
    COUNT(*) AS n
  FROM atletas a
  GROUP BY a.clase_id, a.nivel
)
SELECT *
FROM dist
ORDER BY clase_id, nivel
LIMIT 100;

/* S6) CASE: etiqueta de “carga” de clase */
SELECT
  c.clase_id,
  c.nivel,
  COUNT(a.miembro_id) AS inscritos,
  CASE
    WHEN COUNT(a.miembro_id) >= 100 THEN 'ALTA'
    WHEN COUNT(a.miembro_id) >= 40  THEN 'MEDIA'
    ELSE 'BAJA'
  END AS carga
FROM clases c
LEFT JOIN atletas a ON a.clase_id = c.clase_id
GROUP BY c.clase_id, c.nivel
ORDER BY inscritos DESC
LIMIT 30;

/* S7) Operación de conjuntos: correos que son atletas UNION correos que son coachs */
SELECT m.correo, 'atleta' AS rol
FROM miembros m
JOIN atletas a ON a.miembro_id = m.miembro_id
UNION
SELECT m.correo, 'coach' AS rol
FROM miembros m
JOIN coachs c ON c.miembro_id = m.miembro_id
ORDER BY correo
LIMIT 50;

/* S8) EXCEPT: miembros que NO tienen arcos registrados */
SELECT m.miembro_id
FROM miembros m
EXCEPT
SELECT DISTINCT a.miembro_id
FROM arcos a
ORDER BY miembro_id
LIMIT 50;

/* S9) Búsqueda texto/regex: correos demo y coaches con certificación “WA” */
SELECT DISTINCT
  m.miembro_id,
  m.correo
FROM miembros m
JOIN coach_certificacion cc ON cc.miembro_id = m.miembro_id
WHERE m.correo ~* '@demo\.com$'
  AND cc.certificacion ILIKE '%WA%'
LIMIT 50;

/* S10) “Análisis temporal” (con tiempos): duración de clases (minutos) */
SELECT
  clase_id,
  dias,
  (EXTRACT(EPOCH FROM (hora_fin - hora_inicio)) / 60)::int AS duracion_min
FROM clases
ORDER BY duracion_min DESC, clase_id
LIMIT 30;


-- =========================================================
-- 3.2 INSERT (subconsultas, múltiples, calculados, UPSERT)
-- =========================================================

/* I1) INSERT con subconsulta: crear certificación para coaches top */
INSERT INTO coach_certificacion (miembro_id, certificacion)
SELECT coach_id, 'Reconocimiento: Top Coach'
FROM (
  SELECT c.coach_id, COUNT(a.miembro_id) AS atletas_total
  FROM clases c
  LEFT JOIN atletas a ON a.clase_id = c.clase_id
  GROUP BY c.coach_id
  ORDER BY atletas_total DESC
  LIMIT 5
) t
ON CONFLICT DO NOTHING;

/* I2) INSERT múltiple: crea 3 certificaciones a un coach específico (elige el primero) */
INSERT INTO coach_certificacion (miembro_id, certificacion)
SELECT coach_id, certificacion
FROM (
  SELECT (SELECT miembro_id FROM coachs ORDER BY miembro_id LIMIT 1) AS coach_id,
         unnest(ARRAY['Primeros Auxilios','Safe Sport','Entrenador IPN']) AS certificacion
) x
ON CONFLICT DO NOTHING;

/* I3) INSERT con valores calculados: agrega un arco “default” a 20 atletas sin arcos */
INSERT INTO arcos (tipo, librAje, mano, estabilizador, mira, rama, maneral, miembro_id)
SELECT
  'recurvo', 24, 'diestro', FALSE, TRUE, 'WNS', 'Win&Win', m.miembro_id
FROM miembros m
JOIN atletas a ON a.miembro_id = m.miembro_id
LEFT JOIN arcos ar ON ar.miembro_id = m.miembro_id
WHERE ar.miembro_id IS NULL
ORDER BY m.miembro_id
LIMIT 20;

/* I4) UPSERT: asegura que exista una certificación específica */
INSERT INTO coach_certificacion (miembro_id, certificacion)
VALUES ((SELECT miembro_id FROM coachs ORDER BY miembro_id LIMIT 1), 'WA Level 1')
ON CONFLICT (miembro_id, certificacion) DO NOTHING;


-- =========================================================
-- 3.3 UPDATE (join, CASE, masivo, subconsultas)
-- =========================================================

/* U1) UPDATE con JOIN: alinear nivel del atleta con el nivel de su clase (si difiere) */
UPDATE atletas a
SET nivel = c.nivel
FROM clases c
WHERE c.clase_id = a.clase_id
  AND a.nivel <> c.nivel;

/* U2) UPDATE condicional con CASE: marcar alumno_ipn según boleta */
UPDATE atletas
SET alumno_ipn = CASE
  WHEN boleta IS NULL OR boleta = '' THEN FALSE
  ELSE TRUE
END;

/* U3) UPDATE masivo: subir 1 libra a todos los arcos con librAje < 20 */
UPDATE arcos
SET librAje = librAje + 1
WHERE librAje < 20;

/* U4) UPDATE con subconsulta: a los 10 atletas con más arcos, forzarlos a “Avanzado” */
UPDATE atletas
SET nivel = 'Avanzado'
WHERE miembro_id IN (
  SELECT miembro_id
  FROM arcos
  GROUP BY miembro_id
  ORDER BY COUNT(*) DESC
  LIMIT 10
);


-- =========================================================
-- 3.4 DELETE (subconsulta, join, soft delete, archivado)
-- NOTA: No tenemos columna "activo" en el modelo (tú dijiste no agregar atributos).
-- Así que el "soft delete" lo simulamos archivando y luego borrando.
-- =========================================================

/* D1) Crear tabla de archivo (si no existe) para arcos */
CREATE TABLE IF NOT EXISTS arcos_archivo (
  LIKE arcos INCLUDING ALL,
  archivado_en TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

/* D2) Archivado + DELETE: arcos con librAje muy bajo (< 18) */
INSERT INTO arcos_archivo
SELECT a.*, CURRENT_TIMESTAMP
FROM arcos a
WHERE a.librAje < 18;

DELETE FROM arcos
WHERE librAje < 18;

/* D3) DELETE con JOIN (usando USING): borrar certificaciones de coaches sin clases */
DELETE FROM coach_certificacion cc
USING coachs c
WHERE cc.miembro_id = c.miembro_id
  AND NOT EXISTS (SELECT 1 FROM clases cl WHERE cl.coach_id = c.miembro_id);

/* D4) DELETE con subconsulta: borrar arcos de un conjunto limitado (demo) */
DELETE FROM arcos
WHERE arco_id IN (
  SELECT arco_id
  FROM arcos
  ORDER BY arco_id
  LIMIT 10
);


-- =========================================================
-- 3.5 TRANSACCIONES (BEGIN/COMMIT/ROLLBACK, SAVEPOINT, locks)
-- =========================================================

/* T1) Transacción con SAVEPOINT: intentar cambio y revertir parcial */
BEGIN;

-- Bloqueo para evitar cambios concurrentes al mismo atleta
SELECT miembro_id FROM atletas ORDER BY miembro_id LIMIT 1 FOR UPDATE;

-- Cambios tentativos
UPDATE atletas
SET nivel = 'Intermedio'
WHERE miembro_id = (SELECT miembro_id FROM atletas ORDER BY miembro_id LIMIT 1);

SAVEPOINT sp1;

-- Operación "riesgosa" (simulada): intenta poner boleta duplicada (puede fallar)
-- Si falla, hacemos ROLLBACK TO SAVEPOINT.
DO $$
BEGIN
  BEGIN
    UPDATE atletas
    SET boleta = (SELECT boleta FROM atletas WHERE boleta IS NOT NULL LIMIT 1)
    WHERE miembro_id = (SELECT miembro_id FROM atletas ORDER BY miembro_id OFFSET 1 LIMIT 1);
  EXCEPTION WHEN others THEN
    RAISE NOTICE 'Fallo esperado (boleta duplicada). Se revierte a SAVEPOINT sp1.';
    ROLLBACK TO SAVEPOINT sp1;
  END;
END$$;

COMMIT;

/* T2) Nivel de aislamiento (ejemplo): lectura consistente */
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT COUNT(*) AS atletas_total FROM atletas;
SELECT COUNT(*) AS arcos_total FROM arcos;
COMMIT;
