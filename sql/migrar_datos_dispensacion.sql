-- ============================================================================
-- MIGRACIÓN DE DATOS EXISTENTES A ESTRUCTURA DE DISPENSACIÓN
-- ============================================================================
-- Este script toma los datos YA CARGADOS en la tabla transacciones
-- y los prepara para análisis de dispensación (solo Retiro y Avance)
--
-- VENTAJAS:
-- - No requiere recargar desde Parquet
-- - Usa datos ya validados
-- - Proceso rápido (solo SQL)
--
-- Uso:
--   psql -U fraud_user -d fraud_detection -f 02_migrar_datos_dispensacion.sql
--
-- O desde Python:
--   engine.execute(open('02_migrar_datos_dispensacion.sql').read())
-- ============================================================================

-- ============================================================================
-- PASO 1: Verificar datos existentes
-- ============================================================================

DO $$
DECLARE
    total_registros BIGINT;
    registros_retiro_avance BIGINT;
BEGIN
    -- Contar total
    SELECT COUNT(*) INTO total_registros FROM transacciones;
    RAISE NOTICE 'Total registros en transacciones: %', total_registros;
    
    -- Contar solo Retiro/Avance
    SELECT COUNT(*) INTO registros_retiro_avance 
    FROM transacciones 
    WHERE tipo_operacion IN ('Retiro', 'Avance');
    
    RAISE NOTICE 'Registros Retiro/Avance: % (%.1f%%)', 
        registros_retiro_avance, 
        (registros_retiro_avance::FLOAT / total_registros * 100);
END $$;

-- ============================================================================
-- PASO 2: Crear vista de dispensación (si no existe)
-- ============================================================================

-- Esta vista filtra automáticamente solo Retiro y Avance
CREATE OR REPLACE VIEW v_transacciones_dispensacion AS
SELECT 
    id_tlf,
    fecha_transaccion,
    time_bucket('15 minutes', fecha_transaccion) AS fecha_transaccion_15min,
    cod_terminal,
    tipo_operacion,
    valor_transaccion,
    valor_transaccion_original,
    cod_estado_transaccion,
    adquiriente,
    autorizador,
    cantidad_tx,
    fecha_negocio,
    mes_origen
FROM transacciones
WHERE tipo_operacion IN ('Retiro', 'Avance')
    AND cod_estado_transaccion = 1  -- Solo exitosas
ORDER BY fecha_transaccion DESC;

COMMENT ON VIEW v_transacciones_dispensacion IS 
    'Vista filtrada de transacciones para análisis de dispensación (solo Retiro/Avance exitosos)';

-- ============================================================================
-- PASO 3: Crear índice adicional si no existe (para consultas rápidas)
-- ============================================================================

-- Índice compuesto para consultas de dispensación
CREATE INDEX IF NOT EXISTS idx_transacciones_dispensacion 
    ON transacciones(tipo_operacion, fecha_transaccion DESC, cod_terminal)
    WHERE tipo_operacion IN ('Retiro', 'Avance') 
    AND cod_estado_transaccion = 1;

-- Índice para buckets de 15 minutos
CREATE INDEX IF NOT EXISTS idx_transacciones_bucket_15min
    ON transacciones(fecha_transaccion_15min, cod_terminal)
    WHERE tipo_operacion IN ('Retiro', 'Avance');

-- ============================================================================
-- PASO 4: Crear agregaciones materializadas para análisis rápido
-- ============================================================================

-- Agregación por cajero y 15 minutos (para features)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dispensacion_por_cajero_15min AS
SELECT 
    cod_terminal,
    time_bucket('15 minutes', fecha_transaccion) AS bucket_15min,
    tipo_operacion,
    
    -- Estadísticas de dispensación
    COUNT(*) AS num_transacciones,
    SUM(valor_transaccion) AS monto_total_dispensado,
    AVG(valor_transaccion) AS monto_promedio,
    MAX(valor_transaccion) AS monto_maximo,
    MIN(valor_transaccion) AS monto_minimo,
    STDDEV(valor_transaccion) AS monto_std,
    
    -- Información temporal (promedios)
    AVG(EXTRACT(HOUR FROM fecha_transaccion)) AS hora_promedio,
    MODE() WITHIN GROUP (ORDER BY EXTRACT(DOW FROM fecha_transaccion)) AS dia_semana_mas_comun,
    
    -- Metadata
    COUNT(DISTINCT fecha_negocio) AS dias_unicos,
    MAX(fecha_transaccion) AS ultima_transaccion
    
FROM transacciones
WHERE tipo_operacion IN ('Retiro', 'Avance')
    AND cod_estado_transaccion = 1
GROUP BY cod_terminal, bucket_15min, tipo_operacion
ORDER BY cod_terminal, bucket_15min DESC;

-- Índices para la vista materializada
CREATE INDEX IF NOT EXISTS idx_mv_disp_cajero 
    ON mv_dispensacion_por_cajero_15min(cod_terminal, bucket_15min DESC);

CREATE INDEX IF NOT EXISTS idx_mv_disp_bucket 
    ON mv_dispensacion_por_cajero_15min(bucket_15min);

COMMENT ON MATERIALIZED VIEW mv_dispensacion_por_cajero_15min IS 
    'Agregación de dispensación por cajero cada 15 minutos - para análisis rápido';

-- ============================================================================
-- PASO 5: Crear vista de resumen por cajero (para features ML)
-- ============================================================================

CREATE OR REPLACE VIEW v_resumen_dispensacion_por_cajero AS
SELECT 
    cod_terminal,
    
    -- Estadísticas generales
    COUNT(*) AS total_transacciones,
    SUM(valor_transaccion) AS monto_total_dispensado,
    AVG(valor_transaccion) AS dispensacion_promedio,
    STDDEV(valor_transaccion) AS dispensacion_std,
    MAX(valor_transaccion) AS dispensacion_max,
    MIN(valor_transaccion) AS dispensacion_min,
    
    -- Por franja horaria (madrugada: 0-6, mañana: 6-12, tarde: 12-18, noche: 18-24)
    AVG(CASE WHEN EXTRACT(HOUR FROM fecha_transaccion) BETWEEN 0 AND 5 THEN valor_transaccion END) 
        AS disp_promedio_madrugada,
    AVG(CASE WHEN EXTRACT(HOUR FROM fecha_transaccion) BETWEEN 6 AND 11 THEN valor_transaccion END) 
        AS disp_promedio_manana,
    AVG(CASE WHEN EXTRACT(HOUR FROM fecha_transaccion) BETWEEN 12 AND 17 THEN valor_transaccion END) 
        AS disp_promedio_tarde,
    AVG(CASE WHEN EXTRACT(HOUR FROM fecha_transaccion) BETWEEN 18 AND 23 THEN valor_transaccion END) 
        AS disp_promedio_noche,
    
    -- Por día de semana (0=Domingo, 6=Sábado)
    AVG(CASE WHEN EXTRACT(DOW FROM fecha_transaccion) = 1 THEN valor_transaccion END) AS disp_promedio_lunes,
    AVG(CASE WHEN EXTRACT(DOW FROM fecha_transaccion) = 2 THEN valor_transaccion END) AS disp_promedio_martes,
    AVG(CASE WHEN EXTRACT(DOW FROM fecha_transaccion) = 3 THEN valor_transaccion END) AS disp_promedio_miercoles,
    AVG(CASE WHEN EXTRACT(DOW FROM fecha_transaccion) = 4 THEN valor_transaccion END) AS disp_promedio_jueves,
    AVG(CASE WHEN EXTRACT(DOW FROM fecha_transaccion) = 5 THEN valor_transaccion END) AS disp_promedio_viernes,
    AVG(CASE WHEN EXTRACT(DOW FROM fecha_transaccion) = 6 THEN valor_transaccion END) AS disp_promedio_sabado,
    AVG(CASE WHEN EXTRACT(DOW FROM fecha_transaccion) = 0 THEN valor_transaccion END) AS disp_promedio_domingo,
    
    -- Proporciones
    COUNT(*) FILTER (WHERE tipo_operacion = 'Retiro')::FLOAT / COUNT(*) AS ratio_retiros,
    COUNT(*) FILTER (WHERE tipo_operacion = 'Avance')::FLOAT / COUNT(*) AS ratio_avances,
    
    -- Rangos temporales
    MIN(fecha_transaccion) AS fecha_primera_transaccion,
    MAX(fecha_transaccion) AS fecha_ultima_transaccion,
    
    -- Timestamp de cálculo
    NOW() AS fecha_calculo
    
FROM transacciones
WHERE tipo_operacion IN ('Retiro', 'Avance')
    AND cod_estado_transaccion = 1
GROUP BY cod_terminal
ORDER BY total_transacciones DESC;

COMMENT ON VIEW v_resumen_dispensacion_por_cajero IS 
    'Resumen estadístico de dispensación por cajero - base para features ML';

-- ============================================================================
-- PASO 6: Poblar tabla de features_dispensacion (si existe)
-- ============================================================================

-- Insertar o actualizar features desde la vista de resumen
INSERT INTO features_dispensacion (
    cod_cajero,
    dispensacion_promedio,
    dispensacion_std,
    dispensacion_max,
    dispensacion_min,
    disp_promedio_madrugada,
    disp_promedio_manana,
    disp_promedio_tarde,
    disp_promedio_noche,
    disp_promedio_lunes,
    disp_promedio_martes,
    disp_promedio_miercoles,
    disp_promedio_jueves,
    disp_promedio_viernes,
    disp_promedio_sabado,
    disp_promedio_domingo,
    ratio_billetes_altos,  -- Calculamos después con archivo 15min
    total_transacciones,
    fecha_primer_registro,
    fecha_ultimo_registro,
    fecha_calculo
)
SELECT 
    cod_terminal::VARCHAR,
    dispensacion_promedio,
    dispensacion_std,
    dispensacion_max,
    dispensacion_min,
    disp_promedio_madrugada,
    disp_promedio_manana,
    disp_promedio_tarde,
    disp_promedio_noche,
    disp_promedio_lunes,
    disp_promedio_martes,
    disp_promedio_miercoles,
    disp_promedio_jueves,
    disp_promedio_viernes,
    disp_promedio_sabado,
    disp_promedio_domingo,
    NULL as ratio_billetes_altos,  -- Se calculará con archivos 15min
    total_transacciones,
    fecha_primera_transaccion,
    fecha_ultima_transaccion,
    fecha_calculo
FROM v_resumen_dispensacion_por_cajero
ON CONFLICT (cod_cajero) 
DO UPDATE SET
    dispensacion_promedio = EXCLUDED.dispensacion_promedio,
    dispensacion_std = EXCLUDED.dispensacion_std,
    dispensacion_max = EXCLUDED.dispensacion_max,
    dispensacion_min = EXCLUDED.dispensacion_min,
    disp_promedio_madrugada = EXCLUDED.disp_promedio_madrugada,
    disp_promedio_manana = EXCLUDED.disp_promedio_manana,
    disp_promedio_tarde = EXCLUDED.disp_promedio_tarde,
    disp_promedio_noche = EXCLUDED.disp_promedio_noche,
    total_transacciones = EXCLUDED.total_transacciones,
    fecha_ultimo_registro = EXCLUDED.fecha_ultimo_registro,
    fecha_calculo = EXCLUDED.fecha_calculo;

-- ============================================================================
-- PASO 7: Refrescar vista materializada
-- ============================================================================

REFRESH MATERIALIZED VIEW mv_dispensacion_por_cajero_15min;

-- ============================================================================
-- PASO 8: Estadísticas finales
-- ============================================================================

DO $$
DECLARE
    cajeros_con_features INTEGER;
    total_cajeros INTEGER;
    total_dispensado NUMERIC;
BEGIN
    -- Cajeros con features
    SELECT COUNT(*) INTO cajeros_con_features FROM features_dispensacion;
    RAISE NOTICE '✅ Features calculadas para % cajeros', cajeros_con_features;
    
    -- Total cajeros en transacciones
    SELECT COUNT(DISTINCT cod_terminal) INTO total_cajeros 
    FROM transacciones 
    WHERE tipo_operacion IN ('Retiro', 'Avance');
    RAISE NOTICE '📊 Total de cajeros únicos: %', total_cajeros;
    
    -- Monto total dispensado
    SELECT SUM(valor_transaccion) INTO total_dispensado 
    FROM transacciones 
    WHERE tipo_operacion IN ('Retiro', 'Avance')
    AND cod_estado_transaccion = 1;
    RAISE NOTICE '💰 Monto total dispensado: $%', TO_CHAR(total_dispensado, '999,999,999,999');
    
    RAISE NOTICE '';
    RAISE NOTICE '🎉 MIGRACIÓN COMPLETADA EXITOSAMENTE';
END $$;

-- ============================================================================
-- QUERIES DE VERIFICACIÓN (comentados - descomentarlos para probar)
-- ============================================================================

-- Ver top 10 cajeros con más dispensación
-- SELECT cod_terminal, total_transacciones, 
--        TO_CHAR(monto_total_dispensado, '999,999,999,999') as monto_total
-- FROM v_resumen_dispensacion_por_cajero
-- LIMIT 10;

-- Ver dispensación por franja horaria de un cajero específico
-- SELECT * FROM v_resumen_dispensacion_por_cajero 
-- WHERE cod_terminal = '100';  -- Cambiar por un cajero real

-- Ver agregación de 15 minutos para un cajero
-- SELECT * FROM mv_dispensacion_por_cajero_15min
-- WHERE cod_terminal = 100
-- ORDER BY bucket_15min DESC
-- LIMIT 20;

-- FIN DEL SCRIPT