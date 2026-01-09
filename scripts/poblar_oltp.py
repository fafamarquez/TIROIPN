import os
from datetime import datetime, date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v: raise RuntimeError(f"Falta {name} en .env")
    return v

def main():
    load_dotenv()
    # Usamos pool_pre_ping para asegurar que la conexión a Neon esté viva
    engine = create_engine(must_env("DATABASE_URL"), pool_pre_ping=True)

    with engine.begin() as conn:
        # 1. DESACTIVAR TRIGGERS TEMPORALMENTE (Para evitar el error de ISA Total durante la carga)
        conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))

        # 2. LIMPIEZA TOTAL (RESTART IDENTITY vuelve los IDs a 1)
        print("Limpiando tablas...")
        conn.execute(text("""
            TRUNCATE TABLE coach_certificacion, arcos, atletas, clases, coachs, miembros 
            RESTART IDENTITY CASCADE;
        """))

        # 3. INSERTAR MIEMBROS (Datos que cumplen con tu Regex de CURP)
        print("Insertando miembros...")
        miembros = [
            ("AAAA000000HDFRRR01", "Juan", "Perez", "Lopez"),
            ("BBBB000000MDFRRR02", "Ana", "Garcia", "Ruiz"),
            ("CCCC000000HDFRRR03", "Luis", "Hernandez", "Cruz"),
            ("DDDD000000MDFRRR04", "Maria", "Santos", "Mora"),
            ("EEEE000000HDFRRR05", "Carlos", "Martinez", "Vega")
        ]
        for curp, nom, ap, am in miembros:
            conn.execute(text("""
                INSERT INTO miembros (curp, nombre, apellido_paterno, apellido_materno, fecha_registro, edad)
                VALUES (:curp, :nom, :ap, :am, NOW(), 25)
            """), {"curp": curp, "nom": nom, "ap": ap, "am": am})

        # 4. INSERTAR ROLES DE COACH
        print("Asignando coaches...")
        # Juan, Ana y Carlos son coaches
        conn.execute(text("""
            INSERT INTO coachs (miembro_id)
            SELECT miembro_id FROM miembros WHERE curp IN ('AAAA000000HDFRRR01', 'BBBB000000MDFRRR02', 'EEEE000000HDFRRR05');
        """))

        # 5. CREAR CLASES (Cambiamos ::time por CAST)
        print("Creando clases...")
        clases = [
            ('Lun/Mie', '10:00', '12:00', 'Inicial', 'AAAA000000HDFRRR01'),
            ('Mar/Jue', '14:00', '16:00', 'Avanzado', 'EEEE000000HDFRRR05')
        ]
        for dias, inicio, fin, nivel, c_curp in clases:
            conn.execute(text("""
                INSERT INTO clases (dias, hora_inicio, hora_fin, nivel, coach_id)
                SELECT :dias, CAST(:inicio AS TIME), CAST(:fin AS TIME), :nivel, m.miembro_id
                FROM miembros m WHERE m.curp = :curp
            """), {"dias": dias, "inicio": inicio, "fin": fin, "nivel": nivel, "curp": c_curp})

        # 6. INSERTAR ATLETAS (Solución a AmbiguousParameter)
        print("Asignando atletas...")
        atletas_data = [
            ('CCCC000000HDFRRR03', '20240001', True, 'Inicial'),
            ('DDDD000000MDFRRR04', '20240002', False, 'Avanzado'),
            ('EEEE000000HDFRRR05', '20240003', True, 'Avanzado')
        ]
        for curp, boleta, ipn, nivel_atl in atletas_data:
            conn.execute(text("""
                INSERT INTO atletas (miembro_id, boleta, alumno_ipn, nivel, clase_id)
                SELECT 
                    m.miembro_id, 
                    CAST(:boleta AS VARCHAR), 
                    CAST(:ipn AS BOOLEAN), 
                    CAST(:nivel_atl AS VARCHAR), 
                    (SELECT clase_id FROM clases WHERE nivel = CAST(:nivel_atl AS VARCHAR) LIMIT 1)
                FROM miembros m
                WHERE m.curp = CAST(:curp AS VARCHAR)
            """), {"curp": curp, "boleta": boleta, "ipn": ipn, "nivel_atl": nivel_atl})

    print("✔ ¡Éxito! Base de datos poblada correctamente.")

if __name__ == "__main__":
    main()