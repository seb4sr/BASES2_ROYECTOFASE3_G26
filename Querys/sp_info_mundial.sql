USE mundiales_db;

DROP PROCEDURE IF EXISTS sp_info_mundial;
DELIMITER $$

CREATE PROCEDURE sp_info_mundial(
    IN p_anio INT,
    IN p_grupo VARCHAR(10),
    IN p_pais VARCHAR(100),
    IN p_fecha DATE
)
BEGIN

    SELECT 
        m.anio,
        m.nombre,
        m.url
    FROM mundial m
    WHERE m.anio = p_anio;


    SELECT
        pf.anio,
        pf.posicion,
        s.nombre AS seleccion,
        pf.etapa,
        pf.pts,
        pf.pj,
        pf.pg,
        pf.pe,
        pf.pp,
        pf.gf,
        pf.gc,
        pf.dif
    FROM posicion_final pf
    JOIN seleccion s
        ON pf.id_seleccion = s.id_seleccion
    WHERE pf.anio = p_anio
    ORDER BY pf.posicion, s.nombre;

    SELECT
        g.anio,
        g.posicion,
        g.jugador,
        s.nombre AS seleccion,
        g.goles,
        g.partidos,
        g.promedio_gol
    FROM goleador g
    JOIN seleccion s
        ON g.id_seleccion = s.id_seleccion
    WHERE g.anio = p_anio
    ORDER BY g.posicion, g.jugador;


    SELECT
        p.anio,
        p.premio,
        p.categoria,
        p.ganador
    FROM premio p
    WHERE p.anio = p_anio
    ORDER BY p.premio, p.categoria, p.ganador;

    SELECT
        pa.anio,
        pa.numero_partido,
        pa.fecha,
        f.nombre AS fase,
        g.codigo_grupo AS grupo,
        sl.nombre AS seleccion_local,
        pa.goles_local,
        pa.goles_visita,
        sv.nombre AS seleccion_visita,
        CASE
            WHEN pa.goles_local > pa.goles_visita THEN sl.nombre
            WHEN pa.goles_visita > pa.goles_local THEN sv.nombre
            ELSE 'Empate'
        END AS resultado
    FROM partido pa
    JOIN fase f
        ON pa.id_fase = f.id_fase
    LEFT JOIN grupo g
        ON pa.id_grupo = g.id_grupo
    JOIN seleccion sl
        ON pa.id_seleccion_local = sl.id_seleccion
    JOIN seleccion sv
        ON pa.id_seleccion_visita = sv.id_seleccion
    WHERE pa.anio = p_anio
      AND (p_grupo IS NULL OR g.codigo_grupo = p_grupo)
      AND (
            p_pais IS NULL
            OR sl.nombre = p_pais
            OR sv.nombre = p_pais
          )
      AND (p_fecha IS NULL OR pa.fecha = p_fecha)
    ORDER BY pa.fecha, pa.numero_partido;

END $$

DELIMITER ;

