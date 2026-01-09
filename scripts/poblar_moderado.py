import os
import random
import time
from datetime import time as dtime
from sqlalchemy import create_engine, text
from tqdm import tqdm

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "archery_db")
DB_USER = os.getenv("DB_USER", "archery_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "archery_pass")


def db_url() -> str:
    return f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def gen_curp_valida(unicos: set[str]) -> str:
    # 4 letras + 6 dígitos + H/M + 5 letras + 1 alfanum + 1 dígito
    while True:
        letras = "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(4))
        fecha = f"{random.randint(0, 99):02d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}"
        sexo = random.choice("HM")
        entidad = "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(5))
        pen = random.choice("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        ult = random.choice("0123456789")
        curp = letras + fecha + sexo + entidad + pen + ult
        if curp not in unicos:
            unicos.add(curp)
            return curp


def gen_boleta_unica(unicos: set[str]) -> str:
    # rango ejemplo: 20000000 - 20999999
    while True:
        b = str(random.randint(20000000, 20999999))
        if b not in unicos:
            unicos.add(b)
            return b


def main():
    engine = create_engine(db_url(), future=True)

    # ===== NIVEL 2 (MODERADO) =====
    N_COACHS = 200
    N_CLASES = 400
    N_ATLETAS = 10_000
    N_ARCOS = 100_000
    N_CERTS = 5_000

    BATCH = 2000

    niveles = ["Inicial", "Intermedio", "Avanzado"]
    dias_opts = ["L-M-V", "M-J", "S", "L-M", "J-V"]

    tipo_opts = ["recurvo", "compuesto", "barebow", "tradicional"]
    mano_opts = ["diestro", "zurdo"]
    ramas = ["WNS", "Hoyt", "SF", "PSE", "Samick", "Core"]
    manerales = ["Win&Win", "Hoyt", "SF", "PSE", "Kinetic", "Core"]

    cert_pool = [
        "WA Level 1", "WA Level 2", "Entrenador IPN",
        "Primeros Auxilios", "Safe Sport", "Juez Local"
    ]

    # Para evitar duplicados en el mismo run
    curps_usadas: set[str] = set()
    boletas_usadas: set[str] = set()

    t0 = time.time()

    with engine.begin() as conn:
        # Acelerar la transacción
        conn.execute(text("SET LOCAL synchronous_commit = off;"))

        # Desactivar trigger ISA temporalmente (validamos al final)
        conn.execute(text("ALTER TABLE miembros DISABLE TRIGGER trg_enforce_miembro_has_role;"))

        # Quitar índices temporalmente (los recreamos al final)
        conn.execute(text("DROP INDEX IF EXISTS idx_clases_coach;"))
        conn.execute(text("DROP INDEX IF EXISTS idx_atletas_clase;"))
        conn.execute(text("DROP INDEX IF EXISTS idx_arcos_miembro;"))
        conn.execute(text("DROP INDEX IF EXISTS idx_miembros_correo;"))

        # =========================================================
        # 1) COACHS: miembros + coachs
        # =========================================================
        coach_miembros = []
        for i in range(N_COACHS):
            coach_miembros.append({
                "curp": gen_curp_valida(curps_usadas),
                "correo": f"coach{i}@demo.com",
                "edad": random.randint(22, 55),
                "celular": "55" + str(random.randint(10000000, 99999999)),
            })

        coach_ids = []
        print("Insertando miembros (coachs)...")
        for batch in tqdm(list(chunks(coach_miembros, BATCH))):
            conn.execute(
                text("""
                    INSERT INTO miembros (curp, correo, edad, celular)
                    VALUES (:curp, :correo, :edad, :celular)
                """),
                batch
            )
            correos = [r["correo"] for r in batch]
            res = conn.execute(
                text("SELECT miembro_id, correo FROM miembros WHERE correo = ANY(:correos)"),
                {"correos": correos}
            )
            # Map para no depender del orden
            correo_a_id = {row[1]: row[0] for row in res.fetchall()}
            for r in batch:
                coach_ids.append(correo_a_id[r["correo"]])

        print("Insertando coachs...")
        for batch in tqdm(list(chunks(coach_ids, BATCH))):
            conn.execute(
                text("INSERT INTO coachs (miembro_id) VALUES (:mid)"),
                [{"mid": mid} for mid in batch]
            )

        # =========================================================
        # 2) CLASES
        # =========================================================
        clases_rows = []
        for _ in range(N_CLASES):
            hi = dtime(random.choice([6, 7, 8, 9, 10]), 0)
            hf = dtime(random.choice([11, 12, 13, 14, 15]), 0)
            if hf <= hi:
                hf = dtime(hi.hour + 2, 0)

            clases_rows.append({
                "dias": random.choice(dias_opts),
                "hi": hi,
                "hf": hf,
                "nivel": random.choice(niveles),
                "coach_id": random.choice(coach_ids),
            })

        print("Insertando clases...")
        for batch in tqdm(list(chunks(clases_rows, BATCH))):
            conn.execute(
                text("""
                    INSERT INTO clases (dias, hora_inicio, hora_fin, nivel, coach_id)
                    VALUES (:dias, :hi, :hf, :nivel, :coach_id)
                """),
                batch
            )

        clase_ids = [r[0] for r in conn.execute(text("SELECT clase_id FROM clases")).all()]

        # =========================================================
        # 3) ATLETAS: miembros + atletas (boleta UNIQUE)
        # =========================================================
        atleta_miembros = []
        atleta_info = []

        for i in range(N_ATLETAS):
            alumno_ipn = (random.random() < 0.6)
            boleta = gen_boleta_unica(boletas_usadas) if alumno_ipn else None

            atleta_miembros.append({
                "curp": gen_curp_valida(curps_usadas),
                "correo": f"atleta{i}@demo.com",
                "edad": random.randint(15, 45),
            })

            atleta_info.append({
                "boleta": boleta,
                "ipn": alumno_ipn,
                "nivel": random.choice(niveles),
                "clase_id": random.choice(clase_ids),
            })

        atleta_ids = []
        print("Insertando miembros (atletas) + atletas...")
        idx = 0

        for batch in tqdm(list(chunks(atleta_miembros, BATCH))):
            conn.execute(
                text("""
                    INSERT INTO miembros (curp, correo, edad)
                    VALUES (:curp, :correo, :edad)
                """),
                batch
            )

            correos = [r["correo"] for r in batch]
            res = conn.execute(
                text("SELECT miembro_id, correo FROM miembros WHERE correo = ANY(:correos)"),
                {"correos": correos}
            )
            correo_a_mid = {row[1]: row[0] for row in res.fetchall()}

            atletas_batch = []
            for row in batch:
                mid = correo_a_mid[row["correo"]]
                info = atleta_info[idx]
                atletas_batch.append({
                    "mid": mid,
                    "boleta": info["boleta"],
                    "ipn": info["ipn"],
                    "nivel": info["nivel"],
                    "clase_id": info["clase_id"],
                })
                atleta_ids.append(mid)
                idx += 1

            conn.execute(
                text("""
                    INSERT INTO atletas (miembro_id, boleta, alumno_ipn, nivel, clase_id)
                    VALUES (:mid, :boleta, :ipn, :nivel, :clase_id)
                """),
                atletas_batch
            )

        # =========================================================
        # 4) ARCOS
        # =========================================================
        miembro_ids = coach_ids + atleta_ids

        print("Insertando arcos...")
        remaining = N_ARCOS
        pbar = tqdm(total=N_ARCOS)
        while remaining > 0:
            take = min(BATCH, remaining)
            batch = []
            for _ in range(take):
                batch.append({
                    "tipo": random.choice(tipo_opts),
                    "lib": random.randint(18, 60),
                    "mano": random.choice(mano_opts),
                    "est": (random.random() < 0.3),
                    "mira": (random.random() < 0.5),
                    "rama": random.choice(ramas),
                    "man": random.choice(manerales),
                    "mid": random.choice(miembro_ids),
                })

            conn.execute(
                text("""
                    INSERT INTO arcos (tipo, librAje, mano, estabilizador, mira, rama, maneral, miembro_id)
                    VALUES (:tipo, :lib, :mano, :est, :mira, :rama, :man, :mid)
                """),
                batch
            )
            remaining -= take
            pbar.update(take)
        pbar.close()

        # =========================================================
        # 5) CERTIFICACIONES
        # =========================================================
        print("Insertando certificaciones...")
        cert_rows = []
        for _ in range(N_CERTS):
            cert_rows.append({
                "mid": random.choice(coach_ids),
                "cert": random.choice(cert_pool),
            })

        for batch in tqdm(list(chunks(cert_rows, BATCH))):
            conn.execute(
                text("""
                    INSERT INTO coach_certificacion (miembro_id, certificacion)
                    VALUES (:mid, :cert)
                    ON CONFLICT DO NOTHING
                """),
                batch
            )

        # =========================================================
        # 6) Validación ISA total + reactivar trigger
        # =========================================================
        bad = conn.execute(text("""
            SELECT COUNT(*)
            FROM miembros m
            WHERE NOT EXISTS (SELECT 1 FROM atletas a WHERE a.miembro_id = m.miembro_id)
              AND NOT EXISTS (SELECT 1 FROM coachs  c WHERE c.miembro_id = m.miembro_id)
        """)).scalar_one()

        if bad != 0:
            raise RuntimeError(f"ISA total violada: existen {bad} miembros sin rol.")

        conn.execute(text("ALTER TABLE miembros ENABLE TRIGGER trg_enforce_miembro_has_role;"))

        # Recrear índices
        print("Recreando índices...")
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clases_coach    ON clases(coach_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_atletas_clase   ON atletas(clase_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_arcos_miembro   ON arcos(miembro_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_miembros_correo ON miembros(correo);"))

    t1 = time.time()
    secs = t1 - t0

    total_ops = (N_COACHS * 2) + N_CLASES + (N_ATLETAS * 2) + N_ARCOS + N_CERTS

    print("\n✅ Poblado MODERADO terminado.")
    print(f"Operaciones (aprox): {total_ops}")
    print(f"Tiempo: {secs:.2f} s")
    print(f"Operaciones/seg (aprox): {total_ops / secs:.2f}")


if __name__ == "__main__":
    main()
