# Proyecto Fase 3 — Documentación Técnica
## Mundiales de Fútbol — Base de Datos NoSQL (MongoDB)

**Universidad de San Carlos de Guatemala**  
**Sistemas de Bases de Datos 2 — 1er. Semestre 2026**

---

## Índice

1. [Punto B — Creación de colecciones, índices y carga de datos](#punto-b)
2. [Punto C — Método sp_info_mundial](#punto-c)
3. [Punto D — Método sp_info_pais](#punto-d)
4. [Consultas en MongoDB Compass y mongosh](#consultas-mongo)

---

<a name="punto-b"></a>
# PUNTO B — Creación de colecciones, índices y carga de datos

## ¿Qué hace este punto?

Se toman los archivos CSV (que contienen la información de los 22 Mundiales de Fútbol desde 1930 hasta 2022) y se cargan a MongoDB de forma organizada, creando dos colecciones con sus respectivos índices para que las búsquedas sean rápidas.

---

## ¿Por qué solo 2 colecciones y no una por tabla?

En una base de datos relacional (como MySQL) la información se divide en muchas tablas pequeñas conectadas entre sí mediante llaves. En MongoDB, el enfoque correcto es diferente: se prefiere **guardar toda la información relacionada junta dentro de un mismo documento**, en lugar de separarla.

Si se hubiera creado una colección por cada tabla SQL, se habrían necesitado 8 colecciones separadas y cada consulta habría requerido múltiples "uniones" de datos (equivalente a los JOIN de SQL), lo cual es lento e innecesario en NoSQL.

La decisión fue crear **2 colecciones**:

| Colección | Descripción | Documentos |
|---|---|---|
| `mundiales` | Un documento por cada año de Mundial con toda su información embebida | 22 documentos |
| `selecciones` | Un documento por cada país con toda su historia en Mundiales | 86 documentos |

---

## Archivos fuente utilizados

Los datos provienen de archivos CSV organizados en carpetas por año:

```
data/CSV_s/output/
│
├── mundiales.csv                  ← nombre y URL de cada Mundial
│
├── wb_1930/
│   ├── partidos_1930_unificado.csv
│   ├── goleadores_1930.csv
│   ├── posiciones_finales_1930.csv
│   ├── premios_1930.csv
│   ├── equipo_ideal_1930.csv
│   └── seleccion_grupo_1930.csv
│
├── wb_1934/ ...
├── wb_1938/ ...
│  (un folder por cada Mundial hasta 2022)
```

### Ejemplo real del archivo `mundiales.csv`

```
anio,nombre,url
2022,Mundial 2022,https://www.losmundialesdefutbol.com/mundiales/2022_mundial.php
2018,Mundial 2018,https://www.losmundialesdefutbol.com/mundiales/2018_mundial.php
1930,Mundial 1930,https://www.losmundialesdefutbol.com/mundiales/1930_mundial.php
```

### Ejemplo real del archivo `partidos_2022_unificado.csv`

```
anio,numero_partido,fecha,fase,grupo,seleccion_local,goles_local,goles_visita,seleccion_visita
2022,1,20-Nov-2022,1ra Ronda,A,Catar,0,2,Ecuador
2022,2,21-Nov-2022,1ra Ronda,B,Inglaterra,6,2,Iran
2022,5,22-Nov-2022,1ra Ronda,C,Argentina,1,2,Arabia Saudita
2022,57,09-Dec-2022,Cuartos,-,Paises Bajos,2,2,Argentina
2022,63,18-Dec-2022,Final,-,Argentina,3,3,Francia
```

### Ejemplo real del archivo `goleadores_2022.csv`

```
anio,posicion,jugador,goles,partidos,promedio_gol,seleccion
2022,1,Kylian Mbappe,8,7,1.14,Francia
2022,2,Lionel Messi,7,7,1.0,Argentina
2022,3,Olivier Giroud,4,6,0.67,Francia
```

### Ejemplo real del archivo `seleccion_grupo_2022.csv`

```
anio,fase,grupo,seleccion
2022,1ra Ronda,A,Catar
2022,1ra Ronda,A,Ecuador
2022,1ra Ronda,A,Paises Bajos
2022,1ra Ronda,A,Senegal
2022,1ra Ronda,C,Argentina
2022,1ra Ronda,C,Arabia Saudita
```

### Ejemplo real del archivo `posiciones_finales_2022.csv`

```
anio,posicion,seleccion,etapa,pts,pj,pg,pe,pp,gf,gc,dif
2022,1,Argentina,Final,13,7,4,3,0,12,6,6
2022,2,Francia,Final,12,7,4,1,2,16,9,7
2022,3,Croacia,Semifinal,12,7,4,0,3,10,6,4
```

### Ejemplo real del archivo `premios_2022.csv`

```
anio,premio,categoria,ganador
2022,Balon,Oro,Lionel Messi
2022,Balon,Plata,Kylian Mbappe
2022,Botin,Oro,Kylian Mbappe
2022,Guante,Oro,Emiliano Martinez
```

---

## Estructura del documento en MongoDB

### Colección `mundiales` — cómo queda cada documento

Toda la información de un Mundial queda guardada en un solo documento. A continuación se muestra cómo luce el documento del Mundial 2022 ya cargado en MongoDB:

```json
{
  "_id": 2022,
  "anio": 2022,
  "nombre": "Mundial 2022",
  "sede": "Catar",
  "url": "https://www.losmundialesdefutbol.com/mundiales/2022_mundial.php",

  "grupos": [
    {
      "fase": "1ra Ronda",
      "codigo": "A",
      "selecciones": ["Catar", "Ecuador", "Paises Bajos", "Senegal"]
    },
    {
      "fase": "1ra Ronda",
      "codigo": "C",
      "selecciones": ["Argentina", "Arabia Saudita", "Mexico", "Polonia"]
    }
  ],

  "partidos": [
    {
      "numero_partido": 1,
      "fecha": "2022-11-20T00:00:00",
      "fase": "1ra Ronda",
      "grupo": "A",
      "local": "Catar",
      "goles_local": 0,
      "goles_visita": 2,
      "visita": "Ecuador",
      "resultado": "Ecuador"
    },
    {
      "numero_partido": 63,
      "fecha": "2022-12-18T00:00:00",
      "fase": "Final",
      "grupo": null,
      "local": "Argentina",
      "goles_local": 3,
      "goles_visita": 3,
      "visita": "Francia",
      "resultado": "Empate"
    }
  ],

  "goleadores": [
    {
      "posicion": 1,
      "jugador": "Kylian Mbappe",
      "seleccion": "Francia",
      "goles": 8,
      "partidos": 7,
      "promedio_gol": 1.14
    },
    {
      "posicion": 2,
      "jugador": "Lionel Messi",
      "seleccion": "Argentina",
      "goles": 7,
      "partidos": 7,
      "promedio_gol": 1.0
    }
  ],

  "posiciones_finales": [
    {
      "posicion": 1,
      "seleccion": "Argentina",
      "etapa": "Final",
      "pts": 13,
      "pj": 7,
      "pg": 4,
      "pe": 3,
      "pp": 0,
      "gf": 12,
      "gc": 6,
      "dif": 6
    }
  ],

  "premios": [
    { "premio": "Balon",   "categoria": "Oro",   "ganador": "Lionel Messi" },
    { "premio": "Balon",   "categoria": "Plata",  "ganador": "Kylian Mbappe" },
    { "premio": "Botin",   "categoria": "Oro",   "ganador": "Kylian Mbappe" },
    { "premio": "Guante",  "categoria": "Oro",   "ganador": "Emiliano Martinez" }
  ]
}
```

### Colección `selecciones` — cómo queda cada documento

Cada país tiene su propio documento con toda su historia agrupada. Ejemplo de Argentina (fragmento):

```json
{
  "_id": "Argentina",
  "nombre": "Argentina",
  "sedes": [1978],

  "participaciones": [
    {
      "anio": 1978,
      "nombre_mundial": "Mundial 1978",
      "sede": "Argentina",
      "grupos": [
        { "fase": "1ra Ronda", "codigo": "1" }
      ],
      "partidos": [
        {
          "numero_partido": 1,
          "fecha": "1978-06-02T00:00:00",
          "fase": "1ra Ronda",
          "grupo": "1",
          "local": "Argentina",
          "goles_local": 2,
          "goles_visita": 1,
          "visita": "Hungria",
          "resultado": "Argentina"
        }
      ],
      "posicion_final": {
        "posicion": 1,
        "seleccion": "Argentina",
        "etapa": "Final",
        "pts": 12,
        "pj": 7,
        "pg": 5,
        "pe": 1,
        "pp": 1,
        "gf": 15,
        "gc": 4,
        "dif": 11
      }
    },
    {
      "anio": 1986,
      "nombre_mundial": "Mundial 1986",
      "sede": "Mexico",
      "grupos": [
        { "fase": "1ra Ronda", "codigo": "A" }
      ],
      "partidos": [ ... ],
      "posicion_final": {
        "posicion": 1,
        "etapa": "Final",
        ...
      }
    }
  ]
}
```

---

## Explicación del script `cargar_mongodb.py` por bloques

### Bloque 1 — Librerías y configuración inicial

```python
import csv
import os
from datetime import datetime
from pymongo import MongoClient, ASCENDING

MONGO_URI = "mongodb://localhost:27017"
DB_NAME   = "mundiales_db"
DATA_DIR  = os.path.join(os.path.dirname(__file__), "data", "CSV_s", "output")
```

**¿Qué hace?**  
Importa las herramientas necesarias:
- `csv` — permite leer archivos CSV fila por fila
- `os` — permite navegar las carpetas del sistema
- `datetime` — convierte textos de fechas a formato de fecha real
- `pymongo` — es el conector entre Python y MongoDB

Las constantes definen a dónde conectarse (`localhost:27017` es la dirección local donde corre MongoDB) y cuál es la base de datos a usar.

---

### Bloque 2 — Diccionario de sedes

```python
SEDES = {
    1930: "Uruguay",    1934: "Italia",    1938: "Francia",
    1950: "Brasil",     1954: "Suiza",     1958: "Suecia",
    ...
    2018: "Rusia",      2022: "Catar",
}
```

**¿Qué hace?**  
Guarda la sede (país anfitrión) de cada Mundial. Esta información no venía en los CSV principales, así que se tomó directamente del script SQL original que tenía sentencias `UPDATE mundial SET sede = '...'`.

---

### Bloque 3 — Función `leer_csv`

```python
def leer_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))
```

**¿Qué hace?**  
Lee un archivo CSV y convierte cada fila en un diccionario de Python. Si el archivo no existe (por ejemplo, `equipo_ideal_2022.csv` no existe para 2022), simplemente devuelve una lista vacía en lugar de romper el programa.

**Ejemplo:** Para el CSV de goleadores de 1930:
```
anio,posicion,jugador,goles,...
1930,1,Guillermo Stabile,8,...
```
La función devuelve:
```python
[
  {"anio": "1930", "posicion": "1", "jugador": "Guillermo Stabile", "goles": "8", ...}
]
```
Nótese que todo llega como texto; los números se convierten más adelante.

---

### Bloque 4 — Función `parse_fecha`

```python
def parse_fecha(s):
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    return s.strip()
```

**¿Qué hace?**  
Las fechas en los CSV no siempre están en el mismo formato. Esta función intenta tres formatos distintos hasta que uno funcione:

| Formato | Ejemplo |
|---|---|
| `%d-%b-%Y` | `20-Nov-2022` |
| `%Y-%m-%d` | `2022-11-20` |
| `%d/%m/%Y` | `20/11/2022` |

Si ningún formato funciona, guarda la fecha como texto simple. Esto es importante porque MongoDB puede almacenar fechas reales, lo que permite ordenarlas y filtrarlas correctamente.

---

### Bloque 5 — Función `resultado`

```python
def resultado(goles_local, goles_visita, local, visita):
    gl, gv = int(goles_local), int(goles_visita)
    if gl > gv:
        return local
    elif gv > gl:
        return visita
    return "Empate"
```

**¿Qué hace?**  
Calcula el ganador de un partido comparando los goles. En los CSV solo venían los números de goles, no quién ganó. Esta función los interpreta:

| Situación | Resultado |
|---|---|
| Argentina 3 — Francia 3 | `"Empate"` |
| Argentina 2 — Mexico 0 | `"Argentina"` |
| Brasil 0 — Uruguay 2 | `"Uruguay"` |

---

### Bloque 6 — Función principal `cargar_datos`

Esta es la función más larga e importante. Recorre todos los años y construye los documentos. Se divide en sub-pasos:

#### Sub-paso 6.1 — Leer partidos

```python
partidos_raw = leer_csv(os.path.join(folder, f"partidos_{anio}_unificado.csv"))
partidos = []
for r in partidos_raw:
    local  = r["seleccion_local"].strip()
    visita = r["seleccion_visita"].strip()
    gl     = int(r["goles_local"])
    gv     = int(r["goles_visita"])
    p = {
        "numero_partido": int(r["numero_partido"]),
        "fecha":          parse_fecha(r["fecha"]),
        "fase":           r["fase"].strip(),
        "grupo":          r["grupo"].strip() or None,
        "local":          local,
        "goles_local":    gl,
        "goles_visita":   gv,
        "visita":         visita,
        "resultado":      resultado(gl, gv, local, visita),
    }
    partidos.append(p)
```

Lee cada fila del CSV de partidos y la convierte en un diccionario limpio. Convierte los goles de texto a número, las fechas a tipo fecha real, y calcula automáticamente el resultado de cada partido.

**Dato de ejemplo procesado:**
```
CSV:  2022,63,18-Dec-2022,Final,,Argentina,3,3,Francia
↓
{
  "numero_partido": 63,
  "fecha": datetime(2022, 12, 18),
  "fase": "Final",
  "grupo": null,
  "local": "Argentina",
  "goles_local": 3,
  "goles_visita": 3,
  "visita": "Francia",
  "resultado": "Empate"
}
```

#### Sub-paso 6.2 — Construir grupos

```python
sg_raw     = leer_csv(os.path.join(folder, f"seleccion_grupo_{anio}.csv"))
grupos_tmp = {}
for r in sg_raw:
    key = (r["fase"].strip(), r["grupo"].strip())
    grupos_tmp.setdefault(key, []).append(r["seleccion"].strip())

grupos = [
    {"fase": fase, "codigo": codigo, "selecciones": sels}
    for (fase, codigo), sels in sorted(grupos_tmp.items())
]
```

El CSV de selección-grupo tiene una fila por cada combinación país-grupo. Este código agrupa todas las selecciones que pertenecen al mismo grupo y las junta en una lista.

**Transformación visual:**

```
CSV (filas separadas):          →    Documento (agrupado):
1ra Ronda, A, Argentina              { "fase": "1ra Ronda",
1ra Ronda, A, Mexico                    "codigo": "A",
1ra Ronda, A, Polonia                   "selecciones": [
1ra Ronda, A, Arabia Saudita               "Argentina", "Mexico",
                                           "Polonia", "Arabia Saudita"
                                        ]
                                     }
```

#### Sub-paso 6.3 — Construir índice inverso para selecciones

```python
for pais in paises_en_mundial:
    grupos_pais  = [g para cada grupo donde el pais aparece]
    partidos_pais = [p para cada partido donde el pais jugo]
    pos_pais     = posicion final del pais en ese mundial

    participacion = {
        "anio": anio,
        "grupos": grupos_pais,
        "partidos": partidos_pais,
        "posicion_final": pos_pais,
    }
    selecciones_map[pais]["participaciones"].append(participacion)
```

Mientras se construye el documento de cada Mundial, al mismo tiempo se va construyendo el documento de cada selección. Por cada Mundial que se procesa, se revisa qué países participaron y se agrega esa participación al documento del país correspondiente.

Esto se llama un **índice inverso**: se leen los datos orientados a un Mundial, pero se re-organizan para quedar orientados a un país.

---

### Bloque 7 — Función `main` (carga a MongoDB)

```python
def main():
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    mundiales, selecciones = cargar_datos()

    db.mundiales.drop()
    db.mundiales.insert_many(mundiales)
    db.mundiales.create_index([("anio",  ASCENDING)], unique=True, name="idx_anio")
    db.mundiales.create_index([("sede",  ASCENDING)],              name="idx_sede")
    db.mundiales.create_index(
        [("partidos.local", ASCENDING), ("partidos.visita", ASCENDING)],
        name="idx_partidos_paises"
    )

    db.selecciones.drop()
    db.selecciones.insert_many(selecciones)
    db.selecciones.create_index([("nombre", ASCENDING)], unique=True, name="idx_nombre")
    db.selecciones.create_index([("sedes",  ASCENDING)],              name="idx_sedes")
```

**¿Qué hace?**
1. Se conecta a MongoDB en la dirección local
2. Llama a `cargar_datos()` para obtener los 22 documentos de mundiales y los 86 de selecciones
3. **Elimina** las colecciones anteriores si existen (para evitar duplicados al correr el script varias veces)
4. Inserta todos los documentos de una sola vez (`insert_many` es más eficiente que insertar uno por uno)
5. Crea los **índices** para acelerar las búsquedas

### ¿Qué son los índices y para qué sirven?

Un índice es como el índice alfabético al final de un libro: en lugar de leer todo el libro para encontrar un tema, vas directamente a la página. En MongoDB funciona igual: sin índice, buscar `anio = 1986` requiere revisar los 22 documentos uno por uno; con índice, va directo al documento correcto.

| Índice creado | Colección | ¿Para qué sirve? |
|---|---|---|
| `idx_anio` | mundiales | Búsqueda por año del Mundial |
| `idx_sede` | mundiales | Búsqueda por país sede |
| `idx_partidos_paises` | mundiales | Filtrar partidos por país jugando |
| `idx_nombre` | selecciones | Búsqueda por nombre de país |
| `idx_sedes` | selecciones | Ver qué países fueron sede |

---

## Resultado de la carga

Al ejecutar el script (`python cargar_mongodb.py`) se obtiene:

```
Conectando a MongoDB...
Leyendo CSVs...
  Mundiales preparados:   22
  Selecciones preparadas: 86

Cargando coleccion 'mundiales'...
  22 documentos insertados

Cargando coleccion 'selecciones'...
  86 documentos insertados

Listo. Base de datos 'mundiales_db' cargada correctamente.
```

---

<a name="punto-c"></a>
# PUNTO C — Método sp_info_mundial

## ¿Qué hace?

Recibe el año de un Mundial y muestra toda su información: grupos, partidos, goleadores, posiciones finales y premios. Opcionalmente puede filtrar partidos por grupo, país o fecha.

## Firma del método

```python
sp_info_mundial(anio, grupo=None, pais=None, fecha=None)
```

| Parámetro | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `anio` | entero | Sí | Año del Mundial (ej: 2022, 1986) |
| `grupo` | texto | No | Filtra partidos de un grupo específico (ej: "A", "B") |
| `pais` | texto | No | Filtra partidos donde jugó ese país (ej: "Argentina") |
| `fecha` | texto | No | Filtra partidos de esa fecha (ej: "22-Nov-2022") |

## Explicación del código por bloques

### Bloque 1 — Conexión y búsqueda del documento

```python
db  = _get_db()
doc = db.mundiales.find_one({"anio": anio})

if not doc:
    print(f"No se encontro informacion del Mundial {anio}.")
    return
```

**¿Qué hace?**  
Se conecta a la colección `mundiales` y busca el documento cuyo campo `anio` coincida con el parámetro recibido. La operación `find_one` devuelve exactamente un documento (o `None` si no existe).

Gracias al índice `idx_anio`, esta búsqueda es instantánea sin importar cuántos Mundiales haya en la base de datos.

**Ejemplo:**  
`sp_info_mundial(2022)` ejecuta internamente: `db.mundiales.find_one({"anio": 2022})`  
Y obtiene el documento completo del Mundial de Catar 2022.

---

### Bloque 2 — Mostrar encabezado

```python
print("=" * 70)
print(f"  MUNDIAL {doc['anio']} — {doc['nombre']}")
print(f"  Sede: {doc['sede']}   |   URL: {doc['url']}")
print("=" * 70)
```

**Salida real para 2022:**
```
======================================================================
  MUNDIAL 2022 — Mundial 2022
  Sede: Catar   |   URL: https://www.losmundialesdefutbol.com/mundiales/2022_mundial.php
======================================================================
```

---

### Bloque 3 — Mostrar grupos

```python
print("\n--- GRUPOS ---")
for g in doc.get("grupos", []):
    print(f"  [{g['codigo']}] {g['fase']}: {', '.join(g['selecciones'])}")
```

**¿Qué hace?**  
Recorre la lista de grupos del documento y muestra el código del grupo, la fase y los países que lo integran.

**Salida real para 2022:**
```
--- GRUPOS ---
  [A] 1ra Ronda: Catar, Ecuador, Paises Bajos, Senegal
  [B] 1ra Ronda: Estados Unidos, Gales, Inglaterra, Iran
  [C] 1ra Ronda: Arabia Saudita, Argentina, Mexico, Polonia
  [D] 1ra Ronda: Australia, Dinamarca, Francia, Tunez
  [E] 1ra Ronda: Alemania, Costa Rica, Espana, Japon
  [F] 1ra Ronda: Belgica, Canada, Croacia, Marruecos
  [G] 1ra Ronda: Brasil, Camerun, Serbia, Suiza
  [H] 1ra Ronda: Corea del Sur, Ghana, Portugal, Uruguay
```

---

### Bloque 4 — Aplicar filtros a los partidos

```python
partidos = doc.get("partidos", [])

if grupo:
    partidos = [p for p in partidos if p.get("grupo") == grupo]
if pais:
    partidos = [p for p in partidos if p["local"] == pais or p["visita"] == pais]
if fecha:
    if isinstance(fecha, str):
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
```

**¿Qué hace?**  
Obtiene todos los partidos del documento y luego aplica los filtros que el usuario haya indicado. Los filtros se aplican en Python sobre los datos ya recuperados del documento (sin hacer nuevas consultas a MongoDB).

| Si se pasa... | Se filtran los partidos donde... |
|---|---|
| `grupo="C"` | el campo `grupo` sea igual a `"C"` |
| `pais="Argentina"` | Argentina sea el equipo local O el visitante |
| `fecha="22-Nov-2022"` | la fecha del partido coincida con ese día |

**Ejemplo — filtrar por grupo C en 2022:**
```python
sp_info_mundial(2022, grupo="C")
```
Solo mostrará los 6 partidos del Grupo C donde jugaron Argentina, Arabia Saudita, México y Polonia.

**Ejemplo — filtrar por país Argentina en 2022:**
```python
sp_info_mundial(2022, pais="Argentina")
```
Mostrará los 7 partidos de Argentina (3 de fase de grupos + 4 de eliminatorias).

---

### Bloque 5 — Mostrar tabla de partidos

```python
print(f"\n--- PARTIDOS ({len(partidos)}) ---")
print(f"  {'#':>3}  {'Fecha':<15} {'Fase':<18} {'Gr':<4} {'Local':<22} {'GL':>3} - {'GV':<3} {'Visita':<22} {'Resultado'}")
for p in sorted(partidos, key=lambda x: (x["fecha"], x["numero_partido"])):
    fecha_str = p["fecha"].strftime("%d-%b-%Y")
    gr        = p["grupo"] or "-"
    print(f"  {p['numero_partido']:>3}  {fecha_str:<15} {p['fase']:<18} {gr:<4} "
          f"{p['local']:<22} {p['goles_local']:>3} - {p['goles_visita']:<3} "
          f"{p['visita']:<22} {p['resultado']}")
```

**¿Qué hace?**  
Ordena los partidos por fecha y número de partido (para que aparezcan en orden cronológico), luego los imprime en una tabla formateada. La expresión `{:<22}` y `{:>3}` son instrucciones de alineación para que las columnas queden bien alineadas.

**Salida real (fragmento de 2022):**
```
--- PARTIDOS (64) ---
  #   Fecha           Fase               Gr   Local                   GL - GV  Visita                 Resultado
  ----------------------------------------------------------------------------------------------------
    1  20-Nov-2022     1ra Ronda          A    Catar                    0 - 2   Ecuador                Ecuador
    2  21-Nov-2022     1ra Ronda          B    Inglaterra               6 - 2   Iran                   Inglaterra
    5  22-Nov-2022     1ra Ronda          C    Argentina                1 - 2   Arabia Saudita         Arabia Saudita
   63  18-Dec-2022     Final              -    Argentina                3 - 3   Francia                Empate
```

---

### Bloque 6 — Mostrar goleadores

```python
print("\n--- GOLEADORES ---")
print(f"  {'Pos':>3}  {'Jugador':<30} {'Seleccion':<22} {'Goles':>6} {'PJ':>4} {'Prom':>6}")
for g in doc.get("goleadores", []):
    print(f"  {g['posicion']:>3}  {g['jugador']:<30} {g['seleccion']:<22} "
          f"{g['goles']:>6} {g['partidos']:>4} {g['promedio_gol']:>6.2f}")
```

**Salida real para 2022:**
```
--- GOLEADORES ---
  Pos  Jugador                        Seleccion               Goles   PJ    Prom
  ---------------------------------------------------------------------------
    1  Kylian Mbappe                  Francia                     8    7    1.14
    2  Lionel Messi                   Argentina                   7    7    1.00
    3  Olivier Giroud                 Francia                     4    6    0.67
```

---

### Bloque 7 — Mostrar posiciones finales

```python
print("\n--- POSICIONES FINALES ---")
for p in doc.get("posiciones_finales", []):
    print(f"  {p['posicion']:>3}  {p['seleccion']:<25} {p['etapa']:<20} "
          f"{p['pts']:>4} {p['pj']:>3} {p['pg']:>3} {p['pe']:>3} {p['pp']:>3} "
          f"{p['gf']:>4} {p['gc']:>4} {p['dif']:>5}")
```

Las columnas significan:
- **Pts** — Puntos totales acumulados
- **PJ** — Partidos jugados
- **PG** — Partidos ganados
- **PE** — Partidos empatados
- **PP** — Partidos perdidos
- **GF** — Goles a favor
- **GC** — Goles en contra
- **Dif** — Diferencia de goles (GF − GC)

**Salida real para 2022 (primeros 4):**
```
--- POSICIONES FINALES ---
  Pos  Seleccion                 Etapa                Pts  PJ  PG  PE  PP   GF   GC   Dif
  -----------------------------------------------------------------------------------------------
    1  Argentina                 Final                  13   7   4   3   0   12    6     6
    2  Francia                   Final                  12   7   4   1   2   16    9     7
    3  Croacia                   Semifinal              12   7   4   0   3   10    6     4
    4  Marruecos                 Semifinal               8   7   2   2   3    6    5     1
```

---

### Bloque 8 — Mostrar premios

```python
print("\n--- PREMIOS ---")
for p in doc.get("premios", []):
    print(f"  {p['premio']:<20} {p['categoria']:<15} -> {p['ganador']}")
```

**Salida real para 2022:**
```
--- PREMIOS ---
  Balon                Oro             -> Lionel Messi
  Balon                Plata           -> Kylian Mbappe
  Balon                Bronce          -> Luka Modric
  Botin                Oro             -> Kylian Mbappe
  Guante               Oro             -> Emiliano Martinez
```

---

## Ejemplos completos de uso

```python
from consultas import sp_info_mundial

# Ver todo el Mundial 1986 (el de Maradona)
sp_info_mundial(1986)

# Ver solo los partidos del Grupo A en el Mundial 2022
sp_info_mundial(2022, grupo="A")

# Ver todos los partidos de Argentina en el Mundial 2022
sp_info_mundial(2022, pais="Argentina")

# Ver los partidos jugados el 22 de noviembre de 2022
sp_info_mundial(2022, fecha="22-Nov-2022")

# Combinar filtros: partidos de Argentina en Grupo C del 2022
sp_info_mundial(2022, grupo="C", pais="Argentina")
```

---

<a name="punto-d"></a>
# PUNTO D — Método sp_info_pais

## ¿Qué hace?

Recibe el nombre de un país y muestra toda su historia en los Mundiales: en qué años participó, en qué grupos estuvo, todos sus partidos, su posición final en cada torneo y si alguna vez fue sede del Mundial.

## Firma del método

```python
sp_info_pais(pais, anio=None)
```

| Parámetro | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `pais` | texto | Sí | Nombre del país tal como aparece en los datos (ej: "Brasil", "Argentina") |
| `anio` | entero | No | Si se indica, filtra solo la participación en ese año |

## Explicación del código por bloques

### Bloque 1 — Búsqueda del documento del país

```python
db  = _get_db()
doc = db.selecciones.find_one({"nombre": pais})

if not doc:
    print(f"No se encontro informacion del pais '{pais}'.")
    return
```

**¿Qué hace?**  
Busca en la colección `selecciones` el documento cuyo campo `nombre` sea igual al país pedido. Gracias al índice `idx_nombre`, la búsqueda es inmediata.

**Ejemplo:**  
`sp_info_pais("Argentina")` ejecuta: `db.selecciones.find_one({"nombre": "Argentina"})`  
Y devuelve el documento con las 18 participaciones de Argentina en Mundiales.

---

### Bloque 2 — Mostrar encabezado e información de sede

```python
print(f"  SELECCION: {doc['nombre']}")
sedes = doc.get("sedes", [])
if sedes:
    print(f"  Sede del Mundial en: {', '.join(str(s) for s in sorted(sedes))}")
else:
    print("  Nunca fue sede del Mundial.")
```

**¿Qué hace?**  
Muestra el nombre del país y, si estuvo en la lista `sedes` del documento, en qué año(s) fue anfitrión.

**Salida real para Argentina:**
```
======================================================================
  SELECCION: Argentina
  Sede del Mundial en: 1978
======================================================================
```

**Salida real para Mexico:**
```
======================================================================
  SELECCION: Mexico
  Sede del Mundial en: 1970, 1986
======================================================================
```

**Salida real para Uruguay:**
```
======================================================================
  SELECCION: Uruguay
  Sede del Mundial en: 1930
======================================================================
```

---

### Bloque 3 — Filtrado opcional por año

```python
participaciones = doc.get("participaciones", [])
if anio:
    participaciones = [p for p in participaciones if p["anio"] == anio]

if not participaciones:
    print(f"  Sin participaciones registradas.")
    return

print(f"\n  Participaciones: {len(participaciones)} mundial(es)\n")
```

**¿Qué hace?**  
Obtiene la lista completa de participaciones del país. Si el usuario indicó un año específico, filtra solo esa participación. Luego muestra cuántos Mundiales tiene registrados.

**Ejemplo:** `sp_info_pais("Argentina", anio=1986)` mostrará únicamente el Mundial de México 1986.

---

### Bloque 4 — Mostrar cada participación

```python
for part in sorted(participaciones, key=lambda x: x["anio"]):
    print(f"  {part['nombre_mundial']}  |  Sede: {part['sede']}")
    
    if part.get("grupos"):
        for g in part["grupos"]:
            print(f"    Grupo {g['codigo']} — {g['fase']}")
```

**¿Qué hace?**  
Ordena las participaciones de más antigua a más reciente y para cada una muestra el nombre del Mundial, dónde se jugó y en qué grupo participó el país.

**Salida real para Argentina 1986:**
```
  ─────────────────────────────────────────────────────────────────
  Mundial 1986  |  Sede: Mexico
    Grupo A — 1ra Ronda
```

---

### Bloque 5 — Mostrar partidos del país en ese Mundial

```python
partidos = part.get("partidos", [])
if partidos:
    print(f"\n    Partidos ({len(partidos)}):")
    for p in sorted(partidos, key=lambda x: (x["fecha"], x["numero_partido"])):
        fecha_str = p["fecha"].strftime("%d-%b-%Y")
        gr        = p["grupo"] or "-"
        print(f"    {fecha_str:<15} {p['fase']:<18} {gr:<4} "
              f"{p['local']:<22} {p['goles_local']:>3} - {p['goles_visita']:<3} "
              f"{p['visita']:<22} {p['resultado']}")
```

**¿Qué hace?**  
Muestra en orden cronológico todos los partidos que jugó el país en ese Mundial, ya sean de fase de grupos, octavos, cuartos, semifinal o final.

**Salida real para Argentina en 1986:**
```
    Partidos (7):
    Fecha           Fase               Gr   Local                   GL - GV  Visita                 Resultado
    -----------------------------------------------------------------------------------------------
    02-Jun-1986     1ra Ronda          A    Argentina                3 - 1   Corea del Sur          Argentina
    05-Jun-1986     1ra Ronda          A    Italia                   1 - 1   Argentina              Empate
    10-Jun-1986     1ra Ronda          A    Argentina                2 - 0   Bulgaria               Argentina
    16-Jun-1986     Octavos            -    Argentina                1 - 0   Uruguay                Argentina
    22-Jun-1986     Cuartos            -    Argentina                2 - 1   Inglaterra             Argentina
    25-Jun-1986     Semis              -    Argentina                2 - 0   Belgica                Argentina
    29-Jun-1986     Final              -    Argentina                3 - 2   Alemania               Argentina
```

---

### Bloque 6 — Mostrar posición final

```python
pf = part.get("posicion_final")
if pf:
    print(f"\n    Posicion final: #{pf['posicion']} — {pf['etapa']}")
    print(f"    Pts:{pf['pts']}  PJ:{pf['pj']}  PG:{pf['pg']}  PE:{pf['pe']}  PP:{pf['pp']}  GF:{pf['gf']}  GC:{pf['gc']}  Dif:{pf['dif']}")
```

**¿Qué hace?**  
Muestra el puesto que obtuvo el país en ese Mundial y un resumen de su rendimiento estadístico.

**Salida real para Argentina en 1986:**
```
    Posicion final: #1 — Final
    Pts:14  PJ:7  PG:6  PE:1  PP:0  GF:14  GC:5  Dif:9
```

---

## Ejemplos completos de uso

```python
from consultas import sp_info_pais

# Ver toda la historia de Brasil en Mundiales
sp_info_pais("Brasil")

# Ver la historia de Mexico en Mundiales
sp_info_pais("Mexico")

# Ver solo lo que hizo Argentina en el Mundial 2022
sp_info_pais("Argentina", anio=2022)

# Ver solo lo que hizo Uruguay en el primer Mundial (1930)
sp_info_pais("Uruguay", anio=1930)

# Ver si Alemania fue alguna vez sede
sp_info_pais("Alemania")   # mostrará: "Sede del Mundial en: 1974, 2006"
```

**Salida real de `sp_info_pais("Uruguay", anio=1930)`:**
```
======================================================================
  SELECCION: Uruguay
  Sede del Mundial en: 1930
======================================================================

  Participaciones: 1 mundial(es)

  ─────────────────────────────────────────────────────────────────
  Mundial 1930  |  Sede: Uruguay
    Grupo 1 — 1ra Ronda

    Partidos (4):
    Fecha           Fase               Gr   Local                   GL - GV  Visita                 Resultado
    -------------------------------------------------------------------------------------------------------
    13-Jul-1930     1ra Ronda          1    Uruguay                  1 - 0   Peru                   Uruguay
    17-Jul-1930     1ra Ronda          1    Uruguay                  3 - 0   Rumania                Uruguay
    21-Jul-1930     Semis              -    Uruguay                  6 - 1   Yugoslavia             Uruguay
    30-Jul-1930     Final              -    Uruguay                  4 - 2   Argentina              Uruguay

    Posicion final: #1 — Final
    Pts:8  PJ:4  PG:4  PE:0  PP:0  GF:15  GC:3  Dif:12
```

---

## Cómo ejecutar los métodos

### Opción 1 — Modo demo (automático)

```bash
python consultas.py
```

Ejecuta automáticamente cuatro ejemplos de demostración.

### Opción 2 — Desde otro script de Python

```python
from consultas import sp_info_mundial, sp_info_pais

sp_info_mundial(1986)
sp_info_pais("Francia")
```

### Opción 3 — Desde el intérprete interactivo de Python

```bash
python
>>> from consultas import sp_info_mundial, sp_info_pais
>>> sp_info_mundial(2014, pais="Alemania")
>>> sp_info_pais("Italia")
```

---

## Resumen de archivos entregados

| Archivo | Descripción |
|---|---|
| `cargar_mongodb.py` | Script de carga: lee los CSV y crea las 2 colecciones en MongoDB |
| `consultas.py` | Contiene los métodos `sp_info_mundial` y `sp_info_pais` |
| `data/CSV_s/output/` | Carpeta con todos los CSV fuente (22 Mundiales) |
| `Querys/ddl.sql` | DDL original de la base de datos relacional MySQL |
| `Querys/sp_info_mundial.sql` | Stored procedure original en MySQL |
| `Querys/sp_info_pais.sql` | Stored procedure original en MySQL |

---

<a name="consultas-mongo"></a>
# CONSULTAS EN MONGODB COMPASS Y MONGOSH

## ¿Qué es mongosh y cómo se usa?

**mongosh** es la consola interactiva oficial de MongoDB. Se puede abrir directamente desde **MongoDB Compass** haciendo clic en el botón `>_MONGOSH` que aparece en la barra inferior de la aplicación, o ejecutando `mongosh` desde la terminal del sistema.

Todas las consultas de esta sección se escriben en mongosh y retornan exactamente la misma información que los métodos de Python, pero en formato JSON directamente desde la base de datos.

### Antes de ejecutar cualquier consulta

```javascript
// Seleccionar la base de datos del proyecto
use mundiales_db
```

Esto le indica a MongoDB en qué base de datos trabajar. Solo se hace una vez al abrir la sesión.

---

## Equivalente a `sp_info_mundial`

---

### Caso 1 — Todo el Mundial sin filtros

**Python:**
```python
sp_info_mundial(2022)
```

**mongosh / Compass:**
```javascript
db.mundiales.findOne({ anio: 2022 })
```

**¿Qué retorna?**  
El documento completo del Mundial 2022 con grupos, partidos, goleadores, posiciones finales y premios tal como fue guardado.

**Cómo usarlo en Compass (pestaña Documents):**  
En el campo **Filter** escribir:
```json
{ "anio": 2022 }
```
Presionar **Find** y aparece el documento completo.

---

### Caso 2 — Filtrar partidos por grupo

**Python:**
```python
sp_info_mundial(2022, grupo="A")
```

**mongosh / Compass:**
```javascript
db.mundiales.aggregate([
  { $match: { anio: 2022 } },
  {
    $project: {
      anio: 1,
      nombre: 1,
      sede: 1,
      grupos: 1,
      goleadores: 1,
      posiciones_finales: 1,
      premios: 1,
      partidos: {
        $filter: {
          input: "$partidos",
          as: "p",
          cond: { $eq: ["$$p.grupo", "A"] }
        }
      }
    }
  }
])
```

**¿Qué retorna?**  
El documento del Mundial 2022 igual que antes, pero el arreglo `partidos` solo contiene los 6 partidos del Grupo A (Catar, Ecuador, Países Bajos, Senegal).

**Salida esperada en `partidos` (fragmento):**
```json
"partidos": [
  {
    "numero_partido": 1,
    "fecha": "2022-11-20T00:00:00.000Z",
    "fase": "1ra Ronda",
    "grupo": "A",
    "local": "Catar",
    "goles_local": 0,
    "goles_visita": 2,
    "visita": "Ecuador",
    "resultado": "Ecuador"
  },
  {
    "numero_partido": 3,
    "fecha": "2022-11-21T00:00:00.000Z",
    "fase": "1ra Ronda",
    "grupo": "A",
    "local": "Senegal",
    "goles_local": 0,
    "goles_visita": 2,
    "visita": "Paises Bajos",
    "resultado": "Paises Bajos"
  }
]
```

**Cómo usarlo en Compass (pestaña Aggregations):**  
Agregar dos etapas:
- Etapa 1: `$match` → `{ "anio": 2022 }`
- Etapa 2: `$project` → pegar el bloque `$project` de arriba

---

### Caso 3 — Filtrar partidos por país

**Python:**
```python
sp_info_mundial(2022, pais="Argentina")
```

**mongosh / Compass:**
```javascript
db.mundiales.aggregate([
  { $match: { anio: 2022 } },
  {
    $project: {
      anio: 1,
      nombre: 1,
      sede: 1,
      grupos: 1,
      goleadores: 1,
      posiciones_finales: 1,
      premios: 1,
      partidos: {
        $filter: {
          input: "$partidos",
          as: "p",
          cond: {
            $or: [
              { $eq: ["$$p.local",  "Argentina"] },
              { $eq: ["$$p.visita", "Argentina"] }
            ]
          }
        }
      }
    }
  }
])
```

**¿Qué retorna?**  
Solo los 7 partidos donde Argentina fue local o visitante durante el Mundial 2022, desde la fase de grupos hasta la final.

**Salida esperada en `partidos` (fragmento):**
```json
"partidos": [
  {
    "numero_partido": 5,
    "fecha": "2022-11-22T00:00:00.000Z",
    "fase": "1ra Ronda",
    "grupo": "C",
    "local": "Argentina",
    "goles_local": 1,
    "goles_visita": 2,
    "visita": "Arabia Saudita",
    "resultado": "Arabia Saudita"
  },
  {
    "numero_partido": 63,
    "fecha": "2022-12-18T00:00:00.000Z",
    "fase": "Final",
    "grupo": null,
    "local": "Argentina",
    "goles_local": 3,
    "goles_visita": 3,
    "visita": "Francia",
    "resultado": "Empate"
  }
]
```

---

### Caso 4 — Filtrar partidos por fecha

**Python:**
```python
sp_info_mundial(2022, fecha="22-Nov-2022")
```

**mongosh / Compass:**
```javascript
db.mundiales.aggregate([
  { $match: { anio: 2022 } },
  {
    $project: {
      anio: 1,
      nombre: 1,
      sede: 1,
      grupos: 1,
      goleadores: 1,
      posiciones_finales: 1,
      premios: 1,
      partidos: {
        $filter: {
          input: "$partidos",
          as: "p",
          cond: {
            $eq: [
              { $dateToString: { format: "%Y-%m-%d", date: "$$p.fecha" } },
              "2022-11-22"
            ]
          }
        }
      }
    }
  }
])
```

**¿Qué retorna?**  
Solo los partidos jugados el 22 de noviembre de 2022. La expresión `$dateToString` convierte la fecha guardada a texto en formato `YYYY-MM-DD` para poder compararla.

**Salida esperada en `partidos`:**
```json
"partidos": [
  {
    "numero_partido": 5,  "fecha": "2022-11-22T00:00:00.000Z",
    "fase": "1ra Ronda",  "grupo": "C",
    "local": "Argentina", "goles_local": 1,
    "goles_visita": 2,    "visita": "Arabia Saudita",
    "resultado": "Arabia Saudita"
  },
  {
    "numero_partido": 6,  "fecha": "2022-11-22T00:00:00.000Z",
    "fase": "1ra Ronda",  "grupo": "D",
    "local": "Dinamarca", "goles_local": 0,
    "goles_visita": 0,    "visita": "Tunez",
    "resultado": "Empate"
  },
  {
    "numero_partido": 7,  "fecha": "2022-11-22T00:00:00.000Z",
    "fase": "1ra Ronda",  "grupo": "C",
    "local": "Mexico",    "goles_local": 0,
    "goles_visita": 0,    "visita": "Polonia",
    "resultado": "Empate"
  },
  {
    "numero_partido": 8,  "fecha": "2022-11-22T00:00:00.000Z",
    "fase": "1ra Ronda",  "grupo": "D",
    "local": "Francia",   "goles_local": 4,
    "goles_visita": 1,    "visita": "Australia",
    "resultado": "Francia"
  }
]
```

---

### Caso 5 — Combinar filtros: grupo + país

**Python:**
```python
sp_info_mundial(2022, grupo="C", pais="Argentina")
```

**mongosh / Compass:**
```javascript
db.mundiales.aggregate([
  { $match: { anio: 2022 } },
  {
    $project: {
      anio: 1,
      nombre: 1,
      sede: 1,
      grupos: 1,
      goleadores: 1,
      posiciones_finales: 1,
      premios: 1,
      partidos: {
        $filter: {
          input: "$partidos",
          as: "p",
          cond: {
            $and: [
              { $eq: ["$$p.grupo", "C"] },
              {
                $or: [
                  { $eq: ["$$p.local",  "Argentina"] },
                  { $eq: ["$$p.visita", "Argentina"] }
                ]
              }
            ]
          }
        }
      }
    }
  }
])
```

**¿Qué retorna?**  
Solo los partidos de Argentina dentro del Grupo C (los 3 partidos de fase de grupos).

---

## Equivalente a `sp_info_pais`

---

### Caso 1 — Historia completa de un país sin filtros

**Python:**
```python
sp_info_pais("Argentina")
```

**mongosh / Compass:**
```javascript
db.selecciones.findOne({ nombre: "Argentina" })
```

**¿Qué retorna?**  
El documento completo de Argentina con todas sus participaciones en Mundiales, sus partidos por torneo, posiciones finales y los años en que fue sede.

**Cómo usarlo en Compass (pestaña Documents):**  
En el campo **Filter** escribir:
```json
{ "nombre": "Argentina" }
```

**Salida esperada (fragmento del encabezado):**
```json
{
  "_id": "Argentina",
  "nombre": "Argentina",
  "sedes": [1978],
  "participaciones": [
    { "anio": 1930, ... },
    { "anio": 1934, ... },
    ...
    { "anio": 2022, ... }
  ]
}
```

---

### Caso 2 — Historia de un país filtrada por año de Mundial

**Python:**
```python
sp_info_pais("Argentina", anio=1986)
```

**mongosh / Compass:**
```javascript
db.selecciones.aggregate([
  { $match: { nombre: "Argentina" } },
  {
    $project: {
      nombre: 1,
      sedes: 1,
      participaciones: {
        $filter: {
          input: "$participaciones",
          as: "p",
          cond: { $eq: ["$$p.anio", 1986] }
        }
      }
    }
  }
])
```

**¿Qué retorna?**  
El documento de Argentina pero con el arreglo `participaciones` reducido a solo el Mundial de México 1986, mostrando todos sus partidos y posición final.

**Salida esperada:**
```json
{
  "_id": "Argentina",
  "nombre": "Argentina",
  "sedes": [1978],
  "participaciones": [
    {
      "anio": 1986,
      "nombre_mundial": "Mundial 1986",
      "sede": "Mexico",
      "grupos": [
        { "fase": "1ra Ronda", "codigo": "A" }
      ],
      "partidos": [
        {
          "numero_partido": 2,
          "fecha": "1986-06-02T00:00:00.000Z",
          "fase": "1ra Ronda",
          "grupo": "A",
          "local": "Argentina",
          "goles_local": 3,
          "goles_visita": 1,
          "visita": "Corea del Sur",
          "resultado": "Argentina"
        },
        {
          "numero_partido": 63,
          "fecha": "1986-06-29T00:00:00.000Z",
          "fase": "Final",
          "grupo": null,
          "local": "Argentina",
          "goles_local": 3,
          "goles_visita": 2,
          "visita": "Alemania",
          "resultado": "Argentina"
        }
      ],
      "posicion_final": {
        "posicion": 1,
        "seleccion": "Argentina",
        "etapa": "Final",
        "pts": 14,
        "pj": 7,
        "pg": 6,
        "pe": 1,
        "pp": 0,
        "gf": 14,
        "gc": 5,
        "dif": 9
      }
    }
  ]
}
```

---

## Consultas adicionales útiles para la calificación

Estas consultas extra son útiles si el catedrático pide información específica en el momento de la calificación.

### Ver solo los premios de un Mundial

```javascript
db.mundiales.findOne(
  { anio: 2022 },
  { premios: 1, nombre: 1, _id: 0 }
)
```

### Ver solo los goleadores de un Mundial

```javascript
db.mundiales.findOne(
  { anio: 2022 },
  { goleadores: 1, nombre: 1, _id: 0 }
)
```

### Ver solo las posiciones finales de un Mundial

```javascript
db.mundiales.findOne(
  { anio: 2022 },
  { posiciones_finales: 1, nombre: 1, _id: 0 }
)
```

### Ver todos los Mundiales que organizó un país

```javascript
db.mundiales.find(
  { sede: "Brasil" },
  { anio: 1, nombre: 1, sede: 1, _id: 0 }
)
```

**Salida esperada:**
```json
[
  { "anio": 1950, "nombre": "Mundial 1950", "sede": "Brasil" },
  { "anio": 2014, "nombre": "Mundial 2014", "sede": "Brasil" }
]
```

### Ver en qué Mundiales participó un país (solo los años)

```javascript
db.selecciones.aggregate([
  { $match: { nombre: "Mexico" } },
  { $project: {
      nombre: 1,
      sedes: 1,
      anios_participacion: {
        $map: {
          input: "$participaciones",
          as: "p",
          in: "$$p.anio"
        }
      }
  }}
])
```

**Salida esperada:**
```json
{
  "nombre": "Mexico",
  "sedes": [1970, 1986],
  "anios_participacion": [1930, 1950, 1954, 1958, 1962, 1966, 1970, 1978, 1986, 1994, 1998, 2002, 2006, 2010, 2014, 2018, 2022]
}
```

### Ver cuántos goles metió un país en un Mundial específico

```javascript
db.selecciones.aggregate([
  { $match: { nombre: "Brasil" } },
  { $unwind: "$participaciones" },
  { $match: { "participaciones.anio": 2014 } },
  {
    $project: {
      nombre: 1,
      anio: "$participaciones.anio",
      total_goles_favor: {
        $sum: {
          $map: {
            input: "$participaciones.partidos",
            as: "p",
            in: {
              $cond: [
                { $eq: ["$$p.local", "Brasil"] },
                "$$p.goles_local",
                "$$p.goles_visita"
              ]
            }
          }
        }
      }
    }
  }
])
```

**Salida esperada:**
```json
{ "nombre": "Brasil", "anio": 2014, "total_goles_favor": 11 }
```

---

## Resumen de equivalencias Python ↔ mongosh

| Acción | Python | mongosh |
|---|---|---|
| Todo un Mundial | `sp_info_mundial(2022)` | `db.mundiales.findOne({anio: 2022})` |
| Mundial filtrado por grupo | `sp_info_mundial(2022, grupo="A")` | `aggregate` con `$filter` en partidos por `grupo` |
| Mundial filtrado por país | `sp_info_mundial(2022, pais="Argentina")` | `aggregate` con `$filter` por `local` o `visita` |
| Mundial filtrado por fecha | `sp_info_mundial(2022, fecha="22-Nov-2022")` | `aggregate` con `$filter` y `$dateToString` |
| Historia completa de un país | `sp_info_pais("Argentina")` | `db.selecciones.findOne({nombre: "Argentina"})` |
| País en un año específico | `sp_info_pais("Argentina", anio=1986)` | `aggregate` con `$filter` en participaciones por `anio` |
