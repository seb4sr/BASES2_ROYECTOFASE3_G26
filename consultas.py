"""
Metodos requeridos por el enunciado Proyecto Fase 3.
  - sp_info_mundial(anio, grupo=None, pais=None, fecha=None)
  - sp_info_pais(pais, anio=None)
"""

from datetime import datetime
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME   = "mundiales_db"

_client = None
_db     = None

def _get_db():
    global _client, _db
    if _db is None:
        _client = MongoClient(MONGO_URI)
        _db     = _client[DB_NAME]
    return _db


# ─── sp_info_mundial ──────────────────────────────────────────────────────────

def sp_info_mundial(anio: int, grupo: str = None, pais: str = None, fecha=None):
    """
    Muestra toda la informacion de un Mundial.
    Parametros opcionales para filtrar partidos: grupo, pais, fecha (str 'DD-Mon-YYYY' o date).
    """
    db  = _get_db()
    doc = db.mundiales.find_one({"anio": anio})

    if not doc:
        print(f"No se encontro informacion del Mundial {anio}.")
        return

    print("=" * 70)
    print(f"  MUNDIAL {doc['anio']} — {doc['nombre']}")
    print(f"  Sede: {doc['sede']}   |   URL: {doc['url']}")
    print("=" * 70)

    # ── Grupos ──────────────────────────────────────────────────────────────
    print("\n--- GRUPOS ---")
    for g in doc.get("grupos", []):
        print(f"  [{g['codigo']}] {g['fase']}: {', '.join(g['selecciones'])}")

    # ── Partidos (con filtros opcionales) ────────────────────────────────────
    partidos = doc.get("partidos", [])

    if grupo:
        partidos = [p for p in partidos if p.get("grupo") == grupo]
    if pais:
        partidos = [p for p in partidos if p["local"] == pais or p["visita"] == pais]
    if fecha:
        if isinstance(fecha, str):
            # intentar parsear el string
            for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    fecha = datetime.strptime(fecha, fmt)
                    break
                except ValueError:
                    pass
        if isinstance(fecha, datetime):
            partidos = [
                p for p in partidos
                if isinstance(p["fecha"], datetime) and p["fecha"].date() == fecha.date()
            ]

    print(f"\n--- PARTIDOS ({len(partidos)}) ---")
    print(f"  {'#':>3}  {'Fecha':<15} {'Fase':<18} {'Gr':<4} {'Local':<22} {'GL':>3} - {'GV':<3} {'Visita':<22} {'Resultado'}")
    print("  " + "-" * 100)
    for p in sorted(partidos, key=lambda x: (x["fecha"] if isinstance(x["fecha"], datetime) else datetime.min, x["numero_partido"])):
        fecha_str = p["fecha"].strftime("%d-%b-%Y") if isinstance(p["fecha"], datetime) else str(p["fecha"])
        gr        = p["grupo"] or "-"
        print(f"  {p['numero_partido']:>3}  {fecha_str:<15} {p['fase']:<18} {gr:<4} "
              f"{p['local']:<22} {p['goles_local']:>3} - {p['goles_visita']:<3} "
              f"{p['visita']:<22} {p['resultado']}")

    # ── Goleadores ───────────────────────────────────────────────────────────
    print("\n--- GOLEADORES ---")
    print(f"  {'Pos':>3}  {'Jugador':<30} {'Seleccion':<22} {'Goles':>6} {'PJ':>4} {'Prom':>6}")
    print("  " + "-" * 75)
    for g in doc.get("goleadores", []):
        print(f"  {g['posicion']:>3}  {g['jugador']:<30} {g['seleccion']:<22} "
              f"{g['goles']:>6} {g['partidos']:>4} {g['promedio_gol']:>6.2f}")

    # ── Posiciones Finales ───────────────────────────────────────────────────
    print("\n--- POSICIONES FINALES ---")
    print(f"  {'Pos':>3}  {'Seleccion':<25} {'Etapa':<20} {'Pts':>4} {'PJ':>3} {'PG':>3} {'PE':>3} {'PP':>3} {'GF':>4} {'GC':>4} {'Dif':>5}")
    print("  " + "-" * 95)
    for p in doc.get("posiciones_finales", []):
        print(f"  {p['posicion']:>3}  {p['seleccion']:<25} {p['etapa']:<20} "
              f"{p['pts']:>4} {p['pj']:>3} {p['pg']:>3} {p['pe']:>3} {p['pp']:>3} "
              f"{p['gf']:>4} {p['gc']:>4} {p['dif']:>5}")

    # ── Premios ──────────────────────────────────────────────────────────────
    print("\n--- PREMIOS ---")
    for p in doc.get("premios", []):
        print(f"  {p['premio']:<20} {p['categoria']:<15} -> {p['ganador']}")

    print()


# ─── sp_info_pais ─────────────────────────────────────────────────────────────

def sp_info_pais(pais: str, anio: int = None):
    """
    Muestra toda la informacion de un pais en Mundiales.
    Si se indica anio filtra solo ese Mundial.
    """
    db  = _get_db()
    doc = db.selecciones.find_one({"nombre": pais})

    if not doc:
        print(f"No se encontro informacion del pais '{pais}'.")
        return

    print("=" * 70)
    print(f"  SELECCION: {doc['nombre']}")
    sedes = doc.get("sedes", [])
    if sedes:
        print(f"  Sede del Mundial en: {', '.join(str(s) for s in sorted(sedes))}")
    else:
        print("  Nunca fue sede del Mundial.")
    print("=" * 70)

    participaciones = doc.get("participaciones", [])
    if anio:
        participaciones = [p for p in participaciones if p["anio"] == anio]

    if not participaciones:
        print(f"  Sin participaciones registradas{f' en {anio}' if anio else ''}.")
        return

    print(f"\n  Participaciones: {len(participaciones)} mundial(es)\n")

    for part in sorted(participaciones, key=lambda x: x["anio"]):
        print(f"  {'─' * 65}")
        print(f"  {part['nombre_mundial']}  |  Sede: {part['sede']}")

        # grupos
        if part.get("grupos"):
            for g in part["grupos"]:
                print(f"    Grupo {g['codigo']} — {g['fase']}")

        # partidos
        partidos = part.get("partidos", [])
        if partidos:
            print(f"\n    Partidos ({len(partidos)}):")
            print(f"    {'Fecha':<15} {'Fase':<18} {'Gr':<4} {'Local':<22} {'GL':>3} - {'GV':<3} {'Visita':<22} {'Resultado'}")
            print("    " + "-" * 95)
            for p in sorted(partidos, key=lambda x: (x["fecha"] if isinstance(x["fecha"], datetime) else datetime.min, x["numero_partido"])):
                fecha_str = p["fecha"].strftime("%d-%b-%Y") if isinstance(p["fecha"], datetime) else str(p["fecha"])
                gr        = p["grupo"] or "-"
                print(f"    {fecha_str:<15} {p['fase']:<18} {gr:<4} "
                      f"{p['local']:<22} {p['goles_local']:>3} - {p['goles_visita']:<3} "
                      f"{p['visita']:<22} {p['resultado']}")

        # posicion final
        pf = part.get("posicion_final")
        if pf:
            print(f"\n    Posicion final: #{pf['posicion']} — {pf['etapa']}")
            print(f"    Pts:{pf['pts']}  PJ:{pf['pj']}  PG:{pf['pg']}  PE:{pf['pe']}  PP:{pf['pp']}  GF:{pf['gf']}  GC:{pf['gc']}  Dif:{pf['dif']}")

        print()


# ─── DEMO ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== DEMO sp_info_mundial ===")
    sp_info_mundial(2022)

    print("\n=== DEMO sp_info_mundial con filtros ===")
    sp_info_mundial(2022, grupo="A")

    print("\n=== DEMO sp_info_pais ===")
    sp_info_pais("Alemania")

    print("\n=== DEMO sp_info_pais con filtro de anio ===")
    sp_info_pais("Argentina", anio=1986)
