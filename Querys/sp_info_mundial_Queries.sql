-- Ver toda la información de un mundial
CALL sp_info_mundial(2022, NULL, NULL, NULL);

-- Ver toda la información de un grupo de un anio de mundial
CALL sp_info_mundial(1930, '2', NULL, NULL);

-- Ver toda la información de un país en un anio de mundial
CALL sp_info_mundial(1998, NULL, 'Brasil', NULL);

-- Ver los partidos de una fecha especifica de un mundial
CALL sp_info_mundial(1950, NULL, NULL, '1950-07-16');