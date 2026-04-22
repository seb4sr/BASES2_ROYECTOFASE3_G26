"""
Script de carga de datos de Mundiales de Futbol a MongoDB.
Crea 2 colecciones:
  - mundiales: 1 documento por año, todo embebido
  - selecciones: 1 documento por pais, con toda su historia
"""

import csv
import os
from datetime import datetime
from pymongo import MongoClient, ASCENDING

# ─── CONFIGURACION ────────────────────────────────────────────────────────────
MONGO_URI = "mongodb://localhost:27017"
DB_NAME   = "mundiales_db"
DATA_DIR  = os.path.join(os.path.dirname(__file__), "data", "CSV_s", "output")

SEDES = {
    1930: "Uruguay",    1934: "Italia",        1938: "Francia",
    1950: "Brasil",     1954: "Suiza",         1958: "Suecia",
    1962: "Chile",      1966: "Inglaterra",    1970: "Mexico",
    1974: "Alemania",   1978: "Argentina",     1982: "Espana",
    1986: "Mexico",     1990: "Italia",        1994: "Estados Unidos",
    1998: "Francia",    2002: "JP&KR",         2006: "Alemania",
    2010: "Sudafrica",  2014: "Brasil",        2018: "Rusia",
    2022: "Catar",
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def leer_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_fecha(s):
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    return s.strip()


def resultado(goles_local, goles_visita, local, visita):
    gl, gv = int(goles_local), int(goles_visita)
    if gl > gv:
        return local
    elif gv > gl:
        return visita
    return "Empate"


# ─── LECTURA DE DATOS ─────────────────────────────────────────────────────────

def cargar_datos():
    mundiales_base = leer_csv(os.path.join(DATA_DIR, "mundiales.csv"))
    mundiales_map  = {int(r["anio"]): r for r in mundiales_base}

    mundiales_docs  = {}
    selecciones_map = {}   # nombre_pais -> doc

    for anio, sede in sorted(SEDES.items()):
        folder  = os.path.join(DATA_DIR, f"wb_{anio}")
        base    = mundiales_map.get(anio, {})

        # ── partidos ────────────────────────────────────────────────────────
        partidos_raw = leer_csv(os.path.join(folder, f"partidos_{anio}_unificado.csv"))
        partidos = []
        for r in partidos_raw:
            local  = r["seleccion_local"].strip()
            visita = r["seleccion_visita"].strip()
            gl     = int(r["goles_local"])
            gv     = int(r["goles_visita"])
            p = {
                "numero_partido":   int(r["numero_partido"]),
                "fecha":            parse_fecha(r["fecha"]),
                "fase":             r["fase"].strip(),
                "grupo":            r["grupo"].strip() or None,
                "local":            local,
                "goles_local":      gl,
                "goles_visita":     gv,
                "visita":           visita,
                "resultado":        resultado(gl, gv, local, visita),
            }
            partidos.append(p)

        # ── grupos / seleccion_grupo ─────────────────────────────────────────
        sg_raw  = leer_csv(os.path.join(folder, f"seleccion_grupo_{anio}.csv"))
        grupos_tmp = {}
        for r in sg_raw:
            key = (r["fase"].strip(), r["grupo"].strip())
            grupos_tmp.setdefault(key, []).append(r["seleccion"].strip())

        grupos = [
            {"fase": fase, "codigo": codigo, "selecciones": sels}
            for (fase, codigo), sels in sorted(grupos_tmp.items())
        ]

        # ── goleadores ──────────────────────────────────────────────────────
        goleadores = []
        for r in leer_csv(os.path.join(folder, f"goleadores_{anio}.csv")):
            goleadores.append({
                "posicion":      int(r["posicion"]),
                "jugador":       r["jugador"].strip(),
                "seleccion":     r["seleccion"].strip(),
                "goles":         int(r["goles"]),
                "partidos":      int(r["partidos"]),
                "promedio_gol":  float(r["promedio_gol"]),
            })

        # ── posiciones finales ───────────────────────────────────────────────
        posiciones = []
        for r in leer_csv(os.path.join(folder, f"posiciones_finales_{anio}.csv")):
            posiciones.append({
                "posicion":  int(r["posicion"]),
                "seleccion": r["seleccion"].strip(),
                "etapa":     r["etapa"].strip(),
                "pts":       int(r["pts"]),
                "pj":        int(r["pj"]),
                "pg":        int(r["pg"]),
                "pe":        int(r["pe"]),
                "pp":        int(r["pp"]),
                "gf":        int(r["gf"]),
                "gc":        int(r["gc"]),
                "dif":       int(r["dif"]),
            })

        # ── premios ─────────────────────────────────────────────────────────
        premios = []
        for r in leer_csv(os.path.join(folder, f"premios_{anio}.csv")):
            premios.append({
                "premio":    r["premio"].strip(),
                "categoria": r["categoria"].strip(),
                "ganador":   r["ganador"].strip(),
            })

        # equipo ideal (solo algunos años lo tienen separado)
        ei_path = os.path.join(folder, f"equipo_ideal_{anio}.csv")
        for r in leer_csv(ei_path):
            premios.append({
                "premio":    r["premio"].strip(),
                "categoria": r["categoria"].strip(),
                "ganador":   r["ganador"].strip(),
            })

        # ── documento mundial ────────────────────────────────────────────────
        doc_mundial = {
            "_id":                anio,
            "anio":               anio,
            "nombre":             base.get("nombre", f"Mundial {anio}"),
            "sede":               sede,
            "url":                base.get("url", ""),
            "grupos":             grupos,
            "partidos":           partidos,
            "goleadores":         goleadores,
            "posiciones_finales": posiciones,
            "premios":            premios,
        }
        mundiales_docs[anio] = doc_mundial

        # ── construir selecciones (índice inverso) ───────────────────────────
        paises_en_mundial = set()

        for p in partidos:
            paises_en_mundial.add(p["local"])
            paises_en_mundial.add(p["visita"])
        for r in sg_raw:
            paises_en_mundial.add(r["seleccion"].strip())

        for pais in paises_en_mundial:
            if pais not in selecciones_map:
                selecciones_map[pais] = {
                    "_id":            pais,
                    "nombre":         pais,
                    "sedes":          [],
                    "participaciones": [],
                }

            # si fue sede
            if sede == pais:
                selecciones_map[pais]["sedes"].append(anio)

            # grupos donde participó
            grupos_pais = [
                {"fase": g["fase"], "codigo": g["codigo"]}
                for g in grupos
                if pais in g["selecciones"]
            ]

            # partidos donde jugó
            partidos_pais = [
                p for p in partidos
                if p["local"] == pais or p["visita"] == pais
            ]

            # posicion final
            pos_pais = next((p for p in posiciones if p["seleccion"] == pais), None)

            participacion = {
                "anio":           anio,
                "nombre_mundial": base.get("nombre", f"Mundial {anio}"),
                "sede":           sede,
                "grupos":         grupos_pais,
                "partidos":       partidos_pais,
                "posicion_final": pos_pais,
            }
            selecciones_map[pais]["participaciones"].append(participacion)

    # Marcar sedes para países que son sede pero no jugaron partidos (raro, por seguridad)
    for anio, sede in SEDES.items():
        if sede in selecciones_map and anio not in selecciones_map[sede]["sedes"]:
            selecciones_map[sede]["sedes"].append(anio)

    return list(mundiales_docs.values()), list(selecciones_map.values())


# ─── CARGA A MONGODB ──────────────────────────────────────────────────────────

def main():
    print("Conectando a MongoDB...")
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    print("Leyendo CSVs...")
    mundiales, selecciones = cargar_datos()
    print(f"  Mundiales preparados:  {len(mundiales)}")
    print(f"  Selecciones preparadas: {len(selecciones)}")

    # ── mundiales ────────────────────────────────────────────────────────────
    print("\nCargando coleccion 'mundiales'...")
    db.mundiales.drop()
    db.mundiales.insert_many(mundiales)
    db.mundiales.create_index([("anio", ASCENDING)], unique=True, name="idx_anio")
    db.mundiales.create_index([("sede", ASCENDING)],              name="idx_sede")
    db.mundiales.create_index(
        [("partidos.local", ASCENDING), ("partidos.visita", ASCENDING)],
        name="idx_partidos_paises"
    )
    print(f"  {db.mundiales.count_documents({})} documentos insertados")

    # ── selecciones ──────────────────────────────────────────────────────────
    print("\nCargando coleccion 'selecciones'...")
    db.selecciones.drop()
    db.selecciones.insert_many(selecciones)
    db.selecciones.create_index([("nombre", ASCENDING)], unique=True, name="idx_nombre")
    db.selecciones.create_index([("sedes",  ASCENDING)],              name="idx_sedes")
    print(f"  {db.selecciones.count_documents({})} documentos insertados")

    print("\nListo. Base de datos 'mundiales_db' cargada correctamente.")
    client.close()


if __name__ == "__main__":
    main()
