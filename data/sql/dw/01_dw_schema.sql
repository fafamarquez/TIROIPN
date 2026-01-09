-- ============================================
-- DW Schema for ArcheryProject (Star Schema)
-- Objetivo: modelo dimensional para OLAP
-- Nota: OLTP vive en public; DW vive en schema dw
-- ============================================

BEGIN;

-- 1) Crear schema DW
CREATE SCHEMA IF NOT EXISTS dw;

-- 2) Dimensiones
-- 2.1 Dimensión Tiempo
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
  tiempo_id      BIGSERIAL PRIMARY KEY,
  fecha          DATE NOT NULL UNIQUE,
  anio           INT NOT NULL CHECK (anio >= 2000 AND anio <= 2100),
  mes            INT NOT NULL CHECK (mes BETWEEN 1 AND 12),
  dia            INT NOT NULL CHECK (dia BETWEEN 1 AND 31),
  trimestre      INT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
  semana_anio    INT NOT NULL CHECK (semana_anio BETWEEN 1 AND 53),
  dia_semana     INT NOT NULL CHECK (dia_semana BETWEEN 1 AND 7) -- 1=Lun ... 7=Dom
);

-- 2.2 Dimensión Atleta
-- Reutiliza el miembro_id como "natural key" del OLTP
CREATE TABLE IF NOT EXISTS dw.dim_atleta (
  atleta_id      BIGSERIAL PRIMARY KEY,
  miembro_id     BIGINT NOT NULL UNIQUE,   -- clave natural (OLTP)
  alumno_ipn     BOOLEAN NOT NULL,
  nivel_atleta   TEXT NOT NULL,
  clase_id       BIGINT NULL,              -- referencia lógica a OLTP (no FK)
  fecha_alta_dw  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2.3 Dimensión Clase
CREATE TABLE IF NOT EXISTS dw.dim_clase (
  clase_dw_id    BIGSERIAL PRIMARY KEY,
  clase_id       BIGINT NOT NULL UNIQUE,   -- clave natural (OLTP)
  dias           TEXT NOT NULL,
  hora_inicio    TIME NOT NULL,
  hora_fin       TIME NOT NULL,
  nivel_clase    TEXT NOT NULL,
  coach_id       BIGINT NOT NULL,
  fecha_alta_dw  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2.4 Dimensión Coach
CREATE TABLE IF NOT EXISTS dw.dim_coach (
  coach_dw_id    BIGSERIAL PRIMARY KEY,
  coach_id       BIGINT NOT NULL UNIQUE,   -- clave natural (OLTP) (miembro_id del coach)
  fecha_alta_dw  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3) Tabla de Hechos
-- Granularidad: "conteo de arcos registrados por atleta por día"
CREATE TABLE IF NOT EXISTS dw.fact_arcos_registrados (
  tiempo_id      BIGINT NOT NULL REFERENCES dw.dim_tiempo(tiempo_id),
  atleta_id      BIGINT NOT NULL REFERENCES dw.dim_atleta(atleta_id),
  cantidad_arcos BIGINT NOT NULL CHECK (cantidad_arcos >= 0),
  PRIMARY KEY (tiempo_id, atleta_id)
);

-- 4) Índices para consultas OLAP
CREATE INDEX IF NOT EXISTS ix_fact_arcos_atleta ON dw.fact_arcos_registrados(atleta_id);
CREATE INDEX IF NOT EXISTS ix_fact_arcos_tiempo ON dw.fact_arcos_registrados(tiempo_id);

COMMIT;
