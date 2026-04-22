-- ======================Script Utilizado==============================
DROP DATABASE IF EXISTS mundiales_db;
CREATE DATABASE mundiales_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_0900_ai_ci;

USE mundiales_db;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- =========================================================
-- 1) TABLAS PRINCIPALES
-- =========================================================

CREATE TABLE mundial (
    anio INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    url VARCHAR(255) NULL,
    sede VARCHAR(100) NOT NULL,
    PRIMARY KEY (anio)
) ENGINE=InnoDB;

CREATE TABLE seleccion (
    id_seleccion INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL,
    PRIMARY KEY (id_seleccion),
    UNIQUE KEY uq_seleccion_nombre (nombre)
) ENGINE=InnoDB;

CREATE TABLE fase (
    id_fase INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL,
    PRIMARY KEY (id_fase),
    UNIQUE KEY uq_fase_nombre (nombre)
) ENGINE=InnoDB;

CREATE TABLE grupo (
    id_grupo INT NOT NULL AUTO_INCREMENT,
    anio INT NOT NULL,
    id_fase INT NOT NULL,
    codigo_grupo VARCHAR(10) NOT NULL,
    PRIMARY KEY (id_grupo),
    UNIQUE KEY uq_grupo (anio, id_fase, codigo_grupo),
    CONSTRAINT fk_grupo_mundial
        FOREIGN KEY (anio) REFERENCES mundial(anio),
    CONSTRAINT fk_grupo_fase
        FOREIGN KEY (id_fase) REFERENCES fase(id_fase)
) ENGINE=InnoDB;

CREATE TABLE seleccion_grupo (
    id_seleccion_grupo INT NOT NULL AUTO_INCREMENT,
    id_grupo INT NOT NULL,
    id_seleccion INT NOT NULL,
    PRIMARY KEY (id_seleccion_grupo),
    UNIQUE KEY uq_seleccion_grupo (id_grupo, id_seleccion),
    CONSTRAINT fk_selgrupo_grupo
        FOREIGN KEY (id_grupo) REFERENCES grupo(id_grupo),
    CONSTRAINT fk_selgrupo_seleccion
        FOREIGN KEY (id_seleccion) REFERENCES seleccion(id_seleccion)
) ENGINE=InnoDB;

CREATE TABLE partido (
    id_partido INT NOT NULL AUTO_INCREMENT,
    anio INT NOT NULL,
    numero_partido INT NOT NULL,
    fecha DATE NOT NULL,
    id_fase INT NOT NULL,
    id_grupo INT NULL,
    id_seleccion_local INT NOT NULL,
    goles_local INT NOT NULL,
    goles_visita INT NOT NULL,
    id_seleccion_visita INT NOT NULL,
    PRIMARY KEY (id_partido),
    UNIQUE KEY uq_partido_anio_numero (anio, numero_partido),
    KEY idx_partido_fecha (anio, fecha),
    KEY idx_partido_fase (id_fase),
    KEY idx_partido_grupo (id_grupo),
    KEY idx_partido_local (id_seleccion_local),
    KEY idx_partido_visita (id_seleccion_visita),
    CONSTRAINT fk_partido_mundial
        FOREIGN KEY (anio) REFERENCES mundial(anio),
    CONSTRAINT fk_partido_fase
        FOREIGN KEY (id_fase) REFERENCES fase(id_fase),
    CONSTRAINT fk_partido_grupo
        FOREIGN KEY (id_grupo) REFERENCES grupo(id_grupo),
    CONSTRAINT fk_partido_local
        FOREIGN KEY (id_seleccion_local) REFERENCES seleccion(id_seleccion),
    CONSTRAINT fk_partido_visita
        FOREIGN KEY (id_seleccion_visita) REFERENCES seleccion(id_seleccion),
    CONSTRAINT chk_equipos_distintos
        CHECK (id_seleccion_local <> id_seleccion_visita)
) ENGINE=InnoDB;

CREATE TABLE goleador (
    id_goleador INT NOT NULL AUTO_INCREMENT,
    anio INT NOT NULL,
    posicion INT NOT NULL,
    jugador VARCHAR(150) NOT NULL,
    goles INT NOT NULL,
    partidos INT NOT NULL,
    promedio_gol DECIMAL(5,2) NOT NULL,
    id_seleccion INT NOT NULL,
    PRIMARY KEY (id_goleador),
    UNIQUE KEY uq_goleador (anio, jugador, id_seleccion),
    KEY idx_goleador_posicion (anio, posicion),
    CONSTRAINT fk_goleador_mundial
        FOREIGN KEY (anio) REFERENCES mundial(anio),
    CONSTRAINT fk_goleador_seleccion
        FOREIGN KEY (id_seleccion) REFERENCES seleccion(id_seleccion)
) ENGINE=InnoDB;

CREATE TABLE posicion_final (
    id_posicion_final INT NOT NULL AUTO_INCREMENT,
    anio INT NOT NULL,
    posicion INT NOT NULL,
    id_seleccion INT NOT NULL,
    etapa VARCHAR(100) NOT NULL,
    pts INT NOT NULL,
    pj INT NOT NULL,
    pg INT NOT NULL,
    pe INT NOT NULL,
    pp INT NOT NULL,
    gf INT NOT NULL,
    gc INT NOT NULL,
    dif INT NOT NULL,
    PRIMARY KEY (id_posicion_final),
    UNIQUE KEY uq_posicion_anio_pos (anio, posicion),
    UNIQUE KEY uq_posicion_anio_sel (anio, id_seleccion),
    CONSTRAINT fk_posicion_mundial
        FOREIGN KEY (anio) REFERENCES mundial(anio),
    CONSTRAINT fk_posicion_seleccion
        FOREIGN KEY (id_seleccion) REFERENCES seleccion(id_seleccion)
) ENGINE=InnoDB;

CREATE TABLE premio (
    id_premio INT NOT NULL AUTO_INCREMENT,
    anio INT NOT NULL,
    premio VARCHAR(100) NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    ganador VARCHAR(150) NOT NULL,
    PRIMARY KEY (id_premio),
    UNIQUE KEY uq_premio (anio, premio, categoria, ganador),
    CONSTRAINT fk_premio_mundial
        FOREIGN KEY (anio) REFERENCES mundial(anio)
) ENGINE=InnoDB;

-- =========================================================
-- 2) TABLAS STAGING
-- =========================================================

CREATE TABLE stg_mundial (
    anio VARCHAR(20),
    nombre VARCHAR(100),
    url VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE stg_goleadores (
    anio VARCHAR(20),
    posicion VARCHAR(20),
    jugador VARCHAR(150),
    goles VARCHAR(20),
    partidos VARCHAR(20),
    promedio_gol VARCHAR(20),
    seleccion VARCHAR(100)
) ENGINE=InnoDB;

CREATE TABLE stg_partidos (
    anio VARCHAR(20),
    numero_partido VARCHAR(20),
    fecha VARCHAR(30),
    fase VARCHAR(100),
    grupo VARCHAR(10),
    seleccion_local VARCHAR(100),
    goles_local VARCHAR(20),
    goles_visita VARCHAR(20),
    seleccion_visita VARCHAR(100)
) ENGINE=InnoDB;

CREATE TABLE stg_posiciones (
    anio VARCHAR(20),
    posicion VARCHAR(20),
    seleccion VARCHAR(100),
    etapa VARCHAR(100),
    pts VARCHAR(20),
    pj VARCHAR(20),
    pg VARCHAR(20),
    pe VARCHAR(20),
    pp VARCHAR(20),
    gf VARCHAR(20),
    gc VARCHAR(20),
    dif VARCHAR(20)
) ENGINE=InnoDB;

CREATE TABLE stg_premios (
    anio VARCHAR(20),
    premio VARCHAR(100),
    categoria VARCHAR(100),
    ganador VARCHAR(150)
) ENGINE=InnoDB;

CREATE TABLE stg_equipo_ideal (
    anio VARCHAR(20),
    premio VARCHAR(100),
    categoria VARCHAR(100),
    ganador VARCHAR(150)
) ENGINE=InnoDB;

CREATE TABLE stg_seleccion_grupos (
    anio VARCHAR(20),
    fase VARCHAR(100),
    grupo VARCHAR(10),
    seleccion VARCHAR(100)
) ENGINE=InnoDB;

SET FOREIGN_KEY_CHECKS = 1;


UPDATE mundial SET sede = 'Uruguay' WHERE anio = 1930;
UPDATE mundial SET sede = 'Italia' WHERE anio = 1934;
UPDATE mundial SET sede = 'Francia' WHERE anio = 1938;
UPDATE mundial SET sede = 'Brasil' WHERE anio = 1950;
UPDATE mundial SET sede = 'Suiza' WHERE anio = 1954;
UPDATE mundial SET sede = 'Suecia' WHERE anio = 1958;
UPDATE mundial SET sede = 'Chile' WHERE anio = 1962;
UPDATE mundial SET sede = 'Inglaterra' WHERE anio = 1966;
UPDATE mundial SET sede = 'Mexico' WHERE anio = 1970;
UPDATE mundial SET sede = 'Alemania' WHERE anio = 1974;
UPDATE mundial SET sede = 'Argentina' WHERE anio = 1978;
UPDATE mundial SET sede = 'Espana' WHERE anio = 1982;
UPDATE mundial SET sede = 'Mexico' WHERE anio = 1986;
UPDATE mundial SET sede = 'Italia' WHERE anio = 1990;
UPDATE mundial SET sede = 'Estados Unidos' WHERE anio = 1994;
UPDATE mundial SET sede = 'Francia' WHERE anio = 1998;
UPDATE mundial SET sede = 'JP&KR' WHERE anio = 2002;
UPDATE mundial SET sede = 'Alemania' WHERE anio = 2006;
UPDATE mundial SET sede = 'Sudafrica' WHERE anio = 2010;
UPDATE mundial SET sede = 'Brasil' WHERE anio = 2014;
UPDATE mundial SET sede = 'Rusia' WHERE anio = 2018;
UPDATE mundial SET sede = 'Catar' WHERE anio = 2022;

