import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def migrar_a_dw():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))

    with engine.begin() as conn:
        print("Copiando miembros al Data Warehouse...")
        conn.execute(text("""
            INSERT INTO dw.dim_miembros (miembro_id, nombre_completo, curp, es_atleta, es_coach)
            SELECT 
                m.miembro_id, 
                m.nombre || ' ' || m.apellido_paterno, 
                m.curp,
                EXISTS(SELECT 1 FROM atletas a WHERE a.miembro_id = m.miembro_id),
                EXISTS(SELECT 1 FROM coachs c WHERE c.miembro_id = m.miembro_id)
            FROM miembros m
            ON CONFLICT (miembro_id) DO NOTHING;
        """))
        
    print("✔ Datos migrados al DW.")

if __name__ == "__main__":
    migrar_a_dw()