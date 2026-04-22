USE mundiales_db;

DROP PROCEDURE IF EXISTS sp_info_pais;
DELIMITER $$

CREATE PROCEDURE sp_info_pais(
    IN p_pais VARCHAR(100),
    IN p_anio INT
)
BEGIN

    SELECT 
        s.id_seleccion,
        s.nombre AS pais
    FROM seleccion s
    WHERE s.nombre = p_pais;

    SELECT DISTINCT
        p.anio
    FROM partido p
    JOIN seleccion s
        ON s.id_seleccion = p.id_seleccion_local
        OR s.id_seleccion = p.id_seleccion_visita
    WHERE s.nombre = p_pais
      AND (p_anio IS NULL OR p.anio = p_anio)
    ORDER BY p.anio;


    SELECT DISTINCT
        g.anio,
        f.nombre AS fase,
        g.codigo_grupo
    FROM seleccion_grupo sg
    JOIN grupo g ON sg.id_grupo = g.id_grupo
    JOIN fase f ON g.id_fase = f.id_fase
    JOIN seleccion s ON sg.id_seleccion = s.id_seleccion
    WHERE s.nombre = p_pais
      AND (p_anio IS NULL OR g.anio = p_anio)
    ORDER BY g.anio, g.codigo_grupo;


    SELECT
        p.anio,
        p.numero_partido,
        p.fecha,
        f.nombre AS fase,
        g.codigo_grupo AS grupo,
        sl.nombre AS local,
        p.goles_local,
        p.goles_visita,
        sv.nombre AS visita,
        CASE
            WHEN p.goles_local > p.goles_visita THEN sl.nombre
            WHEN p.goles_visita > p.goles_local THEN sv.nombre
            ELSE 'Empate'
        END AS resultado
    FROM partido p
    JOIN fase f ON p.id_fase = f.id_fase
    LEFT JOIN grupo g ON p.id_grupo = g.id_grupo
    JOIN seleccion sl ON p.id_seleccion_local = sl.id_seleccion
    JOIN seleccion sv ON p.id_seleccion_visita = sv.id_seleccion
    WHERE (sl.nombre = p_pais OR sv.nombre = p_pais)
      AND (p_anio IS NULL OR p.anio = p_anio)
    ORDER BY p.anio, p.fecha, p.numero_partido;


    SELECT
        pf.anio,
        pf.posicion,
        pf.etapa,
        pf.pts,
        pf.gf,
        pf.gc,
        pf.dif
    FROM posicion_final pf
    JOIN seleccion s ON pf.id_seleccion = s.id_seleccion
    WHERE s.nombre = p_pais
      AND (p_anio IS NULL OR pf.anio = p_anio)
    ORDER BY pf.anio;


    SELECT
        m.anio,
        m.nombre,
        m.sede
    FROM mundial m
    WHERE m.sede = p_pais
      AND (p_anio IS NULL OR m.anio = p_anio)
    ORDER BY m.anio;

END $$

DELIMITER ;