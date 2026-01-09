import os
import random
import time
import csv
import io
import resource  # Linux only (dentro del contenedor sí aplica)
from datetime import time as dtime
from sqlalchemy import create_engine, text

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "archery_db")
DB_USER = os.getenv("DB_USER", "archery_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "archery_pass")


def db_url() -> str:
    return f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# === Generadores determinísticos (sin sets gigantes) ===
ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
ALNUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def base26(n: int, width: int) -> str:
    s = []
    for _ in range(width):
        s.append(ALPHA[n % 26])
        n //= 26
    return "".join(reversed(s))


def base36(n: int, width: int) -> str:
    s = []
    for _ in range(width):
        s.append(ALNUM[n % 36])
        n //= 36
    return "".join(reversed(s))


def curp_from_int(i: int) -> str:
    # Formato: 4 letras + 6 dígitos + H/M + 5 letras + 1 alfanum + 1 dígito
    letras = base26(i, 4)
    yy = (i % 100)
    mm = ((i // 100) % 12) + 1
    dd = ((i // 1000) % 28) + 1
    fecha = f"{yy:02d}{mm:02d}{dd:02d}"
    sexo = "H" if (i % 2 == 0) else "M"
    entidad = base26(i * 7 + 13, 5)
    pen = base36(i * 11 + 17, 1)
    ult = str((i * 3 + 7) % 10)
    return letras + fecha + sexo + entidad + pen + ult


def peak_rss_mb() -> float:
    # ru_maxrss: KB en Linux
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def copy_rows(cur, copy_sql: str, rows_iter, batch_rows: int = 50000):
    """
    COPY FROM STDIN usando psycopg3, generando CSV por lotes (chunks) para no comer RAM.
    rows_iter debe producir listas/tuplas (ya en orden de columnas).
    """
    with cur.copy(copy_sql) as copy:
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")

        n = 0
        for row in rows_iter:
            writer.writerow(row)
            n += 1
            if n % batch_rows == 0:
                copy.write(buf.getvalue())
                buf.seek(0)
                buf.truncate(0)

        if buf.tell() > 0:
            copy.write(buf.getvalue())

    return n


def main():
    # ===== NIVEL 3 (MASIVO REALISTA - opción B) =====
    N_COACHS = 1000
    N_CLASES = 3000
    N_ATLETAS = 200_000
    N_ARCOS = 2_000_000
    N_CERTS = 80_000

    # Ajusta si tu equipo se queja:
    # N_ATLETAS = 30_000
    # N_ARCOS   = 300_000

    random.seed(2025)

    niveles = ["Inicial", "Intermedio", "Avanzado"]
    dias_opts = ["L-M-V", "M-J", "S", "L-M", "J-V"]
    tipo_opts = ["recurvo", "compuesto", "barebow", "tradicional"]
    mano_opts = ["diestro", "zurdo"]
    ramas = ["WNS", "Hoyt", "SF", "PSE", "Samick", "Core"]
    manerales = ["Win&Win", "Hoyt", "SF", "PSE", "Kinetic", "Core"]
    cert_pool = ["WA Level 1", "WA Level 2", "Entrenador IPN", "Primeros Auxilios", "Safe Sport", "Juez Local"]

    engine = create_engine(db_url(), future=True)

    t0 = time.time()
    rss0 = peak_rss_mb()

    # Usaremos IDs explícitos para no depender de RETURNING (y acelerar).
    # Recomendación: TRUNCATE RESTART IDENTITY antes de correr.
    coach_ids = list(range(1, N_COACHS + 1))
    atleta_ids = list(range(N_COACHS + 1, N_COACHS + N_ATLETAS + 1))
    clase_ids = list(range(1, N_CLASES + 1))

    # Conexión DBAPI (psycopg3) para usar copy protocol
    dbapi_conn = engine.raw_connection()
    try:
        cur = dbapi_conn.cursor()

        # Acelerar carga masiva
        cur.execute("SET synchronous_commit = off;")
        cur.execute("SET maintenance_work_mem = '256MB';")  # puedes subir/bajar
        cur.execute("SET work_mem = '64MB';")

        # Desactivar trigger ISA durante carga; validamos al final
        cur.execute("ALTER TABLE miembros DISABLE TRIGGER trg_enforce_miembro_has_role;")

        # Quitar índices para carga masiva (los recreamos al final)
        cur.execute("DROP INDEX IF EXISTS idx_clases_coach;")
        cur.execute("DROP INDEX IF EXISTS idx_atletas_clase;")
        cur.execute("DROP INDEX IF EXISTS idx_arcos_miembro;")
        cur.execute("DROP INDEX IF EXISTS idx_miembros_correo;")

        # =========================================================
        # 1) MIEMBROS (coachs)
        # =========================================================
        print("COPY miembros (coachs)...")

        def rows_miembros_coachs():
            for i, mid in enumerate(coach_ids, start=1):
                curp = curp_from_int(mid)
                correo = f"coach{mid}@demo.com"
                edad = random.randint(22, 55)
                celular = "55" + str(random.randint(10000000, 99999999))
                yield (mid, curp, correo, edad, celular)

        n1 = copy_rows(
            cur,
            "COPY miembros (miembro_id, curp, correo, edad, celular) FROM STDIN WITH (FORMAT csv)",
            rows_miembros_coachs(),
            batch_rows=50000
        )
        print(f"  -> {n1} rows")

        # 2) COACHS
        print("COPY coachs...")
        def rows_coachs():
            for mid in coach_ids:
                yield (mid,)

        n2 = copy_rows(
            cur,
            "COPY coachs (miembro_id) FROM STDIN WITH (FORMAT csv)",
            rows_coachs(),
            batch_rows=100000
        )
        print(f"  -> {n2} rows")

        # =========================================================
        # 3) CLASES (IDs explícitos)
        # =========================================================
        print("COPY clases...")

        def rows_clases():
            for cid in clase_ids:
                dias = random.choice(dias_opts)
                hi = dtime(random.choice([6, 7, 8, 9, 10]), 0)
                hf = dtime(random.choice([11, 12, 13, 14, 15]), 0)
                if hf <= hi:
                    hf = dtime(hi.hour + 2, 0)
                nivel = random.choice(niveles)
                coach_id = random.choice(coach_ids)
                yield (cid, dias, hi, hf, nivel, coach_id)

        n3 = copy_rows(
            cur,
            "COPY clases (clase_id, dias, hora_inicio, hora_fin, nivel, coach_id) FROM STDIN WITH (FORMAT csv)",
            rows_clases(),
            batch_rows=50000
        )
        print(f"  -> {n3} rows")

        # =========================================================
        # 4) MIEMBROS (atletas)
        # =========================================================
        print("COPY miembros (atletas)...")

        def rows_miembros_atletas():
            for mid in atleta_ids:
                curp = curp_from_int(mid)
                correo = f"atleta{mid}@demo.com"
                edad = random.randint(15, 45)
                celular = ""  # NULL
                yield (mid, curp, correo, edad, celular)

        n4 = copy_rows(
            cur,
            "COPY miembros (miembro_id, curp, correo, edad, celular) FROM STDIN WITH (FORMAT csv)",
            rows_miembros_atletas(),
            batch_rows=50000
        )
        print(f"  -> {n4} rows")

        # =========================================================
        # 5) ATLETAS (boleta UNIQUE solo si alumno_ipn=True)
        # =========================================================
        print("COPY atletas...")

        def rows_atletas():
            # boletas únicas por construcción: 20000000 + contador
            boleta_counter = 20000000
            for mid in atleta_ids:
                alumno_ipn = (random.random() < 0.6)
                boleta = str(boleta_counter) if alumno_ipn else ""
                if alumno_ipn:
                    boleta_counter += 1
                nivel = random.choice(niveles)
                clase_id = random.choice(clase_ids)
                yield (mid, boleta, alumno_ipn, nivel, clase_id)

        n5 = copy_rows(
            cur,
            "COPY atletas (miembro_id, boleta, alumno_ipn, nivel, clase_id) FROM STDIN WITH (FORMAT csv)",
            rows_atletas(),
            batch_rows=50000
        )
        print(f"  -> {n5} rows")

        # =========================================================
        # 6) ARCOS (500k) - sin arco_id explícito
        # =========================================================
        print("COPY arcos...")

        all_miembros = coach_ids + atleta_ids

        def rows_arcos():
            for _ in range(N_ARCOS):
                tipo = random.choice(tipo_opts)
                libraje = random.randint(18, 60)
                mano = random.choice(mano_opts)
                estabilizador = (random.random() < 0.3)
                mira = (random.random() < 0.5)
                rama = random.choice(ramas)
                maneral = random.choice(manerales)
                miembro_id = random.choice(all_miembros)
                yield (tipo, libraje, mano, estabilizador, mira, rama, maneral, miembro_id)

        n6 = copy_rows(
            cur,
            "COPY arcos (tipo, librAje, mano, estabilizador, mira, rama, maneral, miembro_id) "
            "FROM STDIN WITH (FORMAT csv)",
            rows_arcos(),
            batch_rows=50000
        )
        print(f"  -> {n6} rows")

        # =========================================================
        # 7) CERTIFICACIONES (30k) con ON CONFLICT DO NOTHING
        # COPY no soporta ON CONFLICT, así que cargamos a tabla staging temporal
        # =========================================================
        print("COPY certificaciones (staging) + merge...")

        cur.execute("DROP TABLE IF EXISTS _stg_certs;")
        cur.execute("CREATE TEMP TABLE _stg_certs (miembro_id INT, certificacion TEXT);")

        def rows_certs():
            for _ in range(N_CERTS):
                yield (random.choice(coach_ids), random.choice(cert_pool))

        n7 = copy_rows(
            cur,
            "COPY _stg_certs (miembro_id, certificacion) FROM STDIN WITH (FORMAT csv)",
            rows_certs(),
            batch_rows=50000
        )

        cur.execute("""
            INSERT INTO coach_certificacion (miembro_id, certificacion)
            SELECT miembro_id, certificacion FROM _stg_certs
            ON CONFLICT DO NOTHING;
        """)
        print(f"  -> staging rows: {n7}")

        # =========================================================
        # 8) Validar ISA total + reactivar trigger
        # =========================================================
        print("Validando ISA total...")
        cur.execute("""
            SELECT COUNT(*)
            FROM miembros m
            WHERE NOT EXISTS (SELECT 1 FROM atletas a WHERE a.miembro_id = m.miembro_id)
              AND NOT EXISTS (SELECT 1 FROM coachs  c WHERE c.miembro_id = m.miembro_id);
        """)
        bad = cur.fetchone()[0]
        if bad != 0:
            raise RuntimeError(f"ISA total violada: existen {bad} miembros sin rol.")

        cur.execute("ALTER TABLE miembros ENABLE TRIGGER trg_enforce_miembro_has_role;")

        # Ajustar secuencias (porque usamos IDs explícitos en miembros y clases)
        cur.execute("""
            SELECT setval(pg_get_serial_sequence('miembros','miembro_id'),
                          (SELECT COALESCE(MAX(miembro_id),1) FROM miembros), true);
        """)
        cur.execute("""
            SELECT setval(pg_get_serial_sequence('clases','clase_id'),
                          (SELECT COALESCE(MAX(clase_id),1) FROM clases), true);
        """)

        # Recrear índices
        print("Recreando índices...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_clases_coach    ON clases(coach_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_atletas_clase   ON atletas(clase_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_arcos_miembro   ON arcos(miembro_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_miembros_correo ON miembros(correo);")

        # Métrica: tamaño de BD
        cur.execute("SELECT pg_database_size(current_database());")
        db_size_bytes = cur.fetchone()[0]

        dbapi_conn.commit()

    except Exception:
        dbapi_conn.rollback()
        raise
    finally:
        dbapi_conn.close()

    t1 = time.time()
    rss1 = peak_rss_mb()

    total_rows = (
        (N_COACHS + N_ATLETAS)  # miembros
        + N_COACHS              # coachs
        + N_CLASES              # clases
        + N_ATLETAS             # atletas
        + N_ARCOS               # arcos
        + N_CERTS               # staging certs (final real puede ser menor por conflict)
    )

    secs = t1 - t0
    print("\n✅ Poblado MASIVO (opción B) terminado.")
    print(f"Tiempo: {secs:.2f} s")
    print(f"Filas generadas/cargadas (aprox): {total_rows}")
    print(f"Filas/seg (aprox): {total_rows / secs:.2f}")
    print(f"Peak RSS (MB) (aprox): {max(rss0, rss1):.2f}")
    print(f"Tamaño BD (MB): {db_size_bytes / (1024*1024):.2f}")


if __name__ == "__main__":
    main()
