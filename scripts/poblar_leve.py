import os
import random
from datetime import time
from faker import Faker
from sqlalchemy import create_engine, text

fake = Faker("es_MX")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "archery_db")
DB_USER = os.getenv("DB_USER", "archery_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "archery_pass")

def db_url() -> str:
    return f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def gen_curp_valida() -> str:
    # 4 letras + 6 dígitos + H/M + 5 letras + 1 alfanum + 1 dígito
    letras = "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(4))
    fecha = f"{random.randint(0,99):02d}{random.randint(1,12):02d}{random.randint(1,28):02d}"
    sexo = random.choice("HM")
    entidad = "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(5))
    pen = random.choice("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    ult = random.choice("0123456789")
    return letras + fecha + sexo + entidad + pen + ult

def main():
    random.seed(123)

    engine = create_engine(db_url(), future=True)

    # Cantidades Nivel 1 (leve)
    n_coachs = 10
    n_clases = 15
    n_atletas = 80
    n_arcos = 150
    n_certs = 30

    # IMPORTANTE: el trigger ISA es DEFERRABLE y se valida al COMMIT
    # Por eso, SIEMPRE insertamos miembro + (coach o atleta) en la misma transacción.
    with engine.begin() as conn:
        # ----------- 1) Crear COACHS (miembro + coach) -----------
        coach_ids = []
        for i in range(n_coachs):
            curp = gen_curp_valida()
            correo = f"coach{i}@demo.com"
            edad = random.randint(22, 55)
            celular = "55" + str(random.randint(10000000, 99999999))

            mid = conn.execute(
                text("""
                    INSERT INTO miembros (curp, correo, edad, celular)
                    VALUES (:curp, :correo, :edad, :celular)
                    RETURNING miembro_id
                """),
                {"curp": curp, "correo": correo, "edad": edad, "celular": celular},
            ).scalar_one()

            conn.execute(
                text("INSERT INTO coachs (miembro_id) VALUES (:mid)"),
                {"mid": mid},
            )
            coach_ids.append(mid)

        # ----------- 2) Crear CLASES -----------
        niveles = ["Inicial", "Intermedio", "Avanzado"]
        dias_opts = ["L-M-V", "M-J", "S"]

        for j in range(n_clases):
            conn.execute(
                text("""
                    INSERT INTO clases (dias, hora_inicio, hora_fin, nivel, coach_id)
                    VALUES (:dias, :hi, :hf, :nivel, :coach_id)
                """),
                {
                    "dias": random.choice(dias_opts),
                    "hi": time(7, 0),
                    "hf": time(9, 0),
                    "nivel": random.choice(niveles),
                    "coach_id": random.choice(coach_ids),
                },
            )

        clase_ids = [r[0] for r in conn.execute(text("SELECT clase_id FROM clases")).all()]

        # ----------- 3) Crear ATLETAS (miembro + atleta) -----------
        atleta_ids = []
        for k in range(n_atletas):
            curp = gen_curp_valida()
            correo = f"atleta{k}@demo.com"
            edad = random.randint(15, 45)

            alumno_ipn = (random.random() < 0.6)
            boleta = str(random.randint(20000000, 20999999)) if alumno_ipn else None

            mid = conn.execute(
                text("""
                    INSERT INTO miembros (curp, correo, edad)
                    VALUES (:curp, :correo, :edad)
                    RETURNING miembro_id
                """),
                {"curp": curp, "correo": correo, "edad": edad},
            ).scalar_one()

            conn.execute(
                text("""
                    INSERT INTO atletas (miembro_id, boleta, alumno_ipn, nivel, clase_id)
                    VALUES (:mid, :boleta, :ipn, :nivel, :clase_id)
                """),
                {
                    "mid": mid,
                    "boleta": boleta,
                    "ipn": alumno_ipn,
                    "nivel": random.choice(niveles),
                    "clase_id": random.choice(clase_ids),
                },
            )
            atleta_ids.append(mid)

        # ----------- 4) Crear ARCOS (secundaria) -----------
        miembro_ids = [r[0] for r in conn.execute(text("SELECT miembro_id FROM miembros")).all()]
        tipo_opts = ["recurvo", "compuesto", "barebow", "tradicional"]
        mano_opts = ["diestro", "zurdo"]
        ramas = ["WNS", "Hoyt", "SF", "PSE"]
        manerales = ["Win&Win", "Hoyt", "SF", "PSE"]

        for a in range(n_arcos):
            conn.execute(
                text("""
                    INSERT INTO arcos (tipo, librAje, mano, estabilizador, mira, rama, maneral, miembro_id)
                    VALUES (:tipo, :lib, :mano, :est, :mira, :rama, :man, :mid)
                """),
                {
                    "tipo": random.choice(tipo_opts),
                    "lib": random.randint(18, 60),
                    "mano": random.choice(mano_opts),
                    "est": (random.random() < 0.3),
                    "mira": (random.random() < 0.5),
                    "rama": random.choice(ramas),
                    "man": random.choice(manerales),
                    "mid": random.choice(miembro_ids),
                },
            )

        # ----------- 5) Certificaciones (secundaria) -----------
        cert_pool = ["WA Level 1", "WA Level 2", "Entrenador IPN", "Primeros Auxilios", "Safe Sport"]
        for c in range(n_certs):
            conn.execute(
                text("""
                    INSERT INTO coach_certificacion (miembro_id, certificacion)
                    VALUES (:mid, :cert)
                    ON CONFLICT DO NOTHING
                """),
                {"mid": random.choice(coach_ids), "cert": random.choice(cert_pool)},
            )

    print("✅ Poblado leve terminado.")
    print(f"Coachs: {n_coachs}, Clases: {n_clases}, Atletas: {n_atletas}, Arcos: {n_arcos}, Certificaciones: {n_certs}")

if __name__ == "__main__":
    main()
