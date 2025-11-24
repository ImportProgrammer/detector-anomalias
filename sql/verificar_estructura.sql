-- ============================================================================
-- VERIFICACIÓN DE ESTRUCTURA DE DISPENSACIÓN
-- ============================================================================
-- Script para verificar que todas las tablas, vistas e índices
-- fueron creados correctamente después de la migración
-- ============================================================================

\echo '============================================================================'
\echo 'VERIFICACIÓN DE ESTRUCTURA DE DISPENSACIÓN'
\echo '============================================================================'
\echo ''

-- ============================================================================
-- 1. VERIFICAR TABLAS CREADAS
-- ============================================================================

\echo '📋 1. TABLAS CREADAS:'
\echo ''

SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename IN (
    'dispensacion_efectivo',
    'features_dispensacion', 
    'alertas_dispensacion',
    'transacciones'
)
ORDER BY tablename;

\echo ''

-- ============================================================================
-- 2. VERIFICAR VISTAS CREADAS
-- ============================================================================

\echo '👁️  2. VISTAS CREADAS:'
\echo ''

SELECT 
    schemaname,
    viewname,
    definition IS NOT NULL AS has_definition
FROM pg_views
WHERE viewname IN (
    'v_transacciones_dispensacion',
    'v_resumen_dispensacion_por_cajero'
)
ORDER BY viewname;

\echo ''

-- ============================================================================
-- 3. VERIFICAR VISTAS MATERIALIZADAS
-- ============================================================================

\echo '💎 3. VISTAS MATERIALIZADAS:'
\echo ''

SELECT 
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) AS size,
    ispopulated
FROM pg_matviews
WHERE matviewname LIKE '%dispensacion%'
ORDER BY matviewname;

\echo ''

-- ============================================================================
-- 4. VERIFICAR ÍNDICES CREADOS
-- ============================================================================

\echo '🔍 4. ÍNDICES CREADOS:'
\echo ''

SELECT 
    schemaname,
    tablename,
    indexname
FROM pg_indexes
WHERE (indexname LIKE '%disp%' 
   OR indexname LIKE '%dispensacion%')
   AND schemaname = 'public'
ORDER BY tablename, indexname;

\echo ''

-- ============================================================================
-- 5. CONTAR REGISTROS EN CADA ESTRUCTURA (con EXPLAIN para estimados)
-- ============================================================================

\echo '📊 5. REGISTROS POR ESTRUCTURA (estimados rápidos):'
\echo ''

-- Usar estadísticas de PostgreSQL en lugar de COUNT completo
SELECT 
    'transacciones (total)' AS estructura,
    reltuples::BIGINT AS registros_estimados
FROM pg_class
WHERE relname = 'transacciones'

UNION ALL

SELECT 
    'features_dispensacion' AS estructura,
    reltuples::BIGINT AS registros_estimados
FROM pg_class
WHERE relname = 'features_dispensacion'

UNION ALL

SELECT 
    'mv_dispensacion_por_cajero_15min' AS estructura,
    reltuples::BIGINT AS registros_estimados
FROM pg_class
WHERE relname = 'mv_dispensacion_por_cajero_15min';

\echo ''

-- ============================================================================
-- 6. ESTADÍSTICAS BÁSICAS DE FEATURES
-- ============================================================================

\echo '💰 6. ESTADÍSTICAS DE FEATURES CALCULADAS:'
\echo ''

SELECT 
    COUNT(*) AS total_cajeros,
    COUNT(*) FILTER (WHERE dispensacion_promedio IS NOT NULL) AS con_promedio,
    COUNT(*) FILTER (WHERE total_transacciones > 100) AS cajeros_activos,
    TO_CHAR(AVG(dispensacion_promedio), '999,999,999') AS promedio_general,
    TO_CHAR(MAX(dispensacion_max), '999,999,999') AS max_dispensacion
FROM features_dispensacion;

\echo ''

-- ============================================================================
-- 7. TOP 10 CAJEROS CON MÁS DISPENSACIÓN
-- ============================================================================

\echo '🏆 7. TOP 10 CAJEROS CON MÁS DISPENSACIÓN:'
\echo ''

SELECT 
    cod_terminal,
    total_transacciones,
    TO_CHAR(monto_total_dispensado, '999,999,999,999') AS monto_total,
    TO_CHAR(dispensacion_promedio, '999,999,999') AS promedio,
    TO_CHAR(dispensacion_max, '999,999,999') AS maximo,
    fecha_primera_transaccion,
    fecha_ultima_transaccion
FROM v_resumen_dispensacion_por_cajero
ORDER BY total_transacciones DESC
LIMIT 10;

\echo ''

-- ============================================================================
-- 8. DISTRIBUCIÓN POR FRANJA HORARIA (PROMEDIO GENERAL)
-- ============================================================================

\echo '⏰ 8. DISPENSACIÓN PROMEDIO POR FRANJA HORARIA:'
\echo ''

SELECT 
    'Madrugada (0-6h)' AS franja,
    COUNT(*) AS cajeros,
    TO_CHAR(AVG(disp_promedio_madrugada), '999,999,999') AS monto_promedio
FROM v_resumen_dispensacion_por_cajero
WHERE disp_promedio_madrugada IS NOT NULL

UNION ALL

SELECT 
    'Mañana (6-12h)' AS franja,
    COUNT(*) AS cajeros,
    TO_CHAR(AVG(disp_promedio_manana), '999,999,999') AS monto_promedio
FROM v_resumen_dispensacion_por_cajero
WHERE disp_promedio_manana IS NOT NULL

UNION ALL

SELECT 
    'Tarde (12-18h)' AS franja,
    COUNT(*) AS cajeros,
    TO_CHAR(AVG(disp_promedio_tarde), '999,999,999') AS monto_promedio
FROM v_resumen_dispensacion_por_cajero
WHERE disp_promedio_tarde IS NOT NULL

UNION ALL

SELECT 
    'Noche (18-24h)' AS franja,
    COUNT(*) AS cajeros,
    TO_CHAR(AVG(disp_promedio_noche), '999,999,999') AS monto_promedio
FROM v_resumen_dispensacion_por_cajero
WHERE disp_promedio_noche IS NOT NULL;

\echo ''

-- ============================================================================
-- 9. SAMPLE DE AGREGACIÓN DE 15 MINUTOS
-- ============================================================================

\echo '📅 9. MUESTRA DE AGREGACIÓN CADA 15 MINUTOS (últimas 24h):'
\echo ''

SELECT 
    cod_terminal,
    bucket_15min,
    tipo_operacion,
    num_transacciones,
    TO_CHAR(monto_total_dispensado, '999,999,999') AS monto_total,
    TO_CHAR(monto_promedio, '999,999') AS monto_promedio
FROM mv_dispensacion_por_cajero_15min
WHERE bucket_15min >= NOW() - INTERVAL '24 hours'
ORDER BY bucket_15min DESC, num_transacciones DESC
LIMIT 20;

\echo ''

-- ============================================================================
-- 10. VERIFICAR INTEGRIDAD DE FEATURES
-- ============================================================================

\echo '✅ 10. VERIFICACIÓN DE INTEGRIDAD DE FEATURES:'
\echo ''

SELECT 
    'Features completas' AS verificacion,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE dispensacion_promedio IS NOT NULL) AS con_promedio,
    COUNT(*) FILTER (WHERE dispensacion_std IS NOT NULL) AS con_std,
    COUNT(*) FILTER (WHERE total_transacciones > 0) AS con_transacciones,
    COUNT(*) FILTER (WHERE 
        disp_promedio_madrugada IS NOT NULL OR
        disp_promedio_manana IS NOT NULL OR
        disp_promedio_tarde IS NOT NULL OR
        disp_promedio_noche IS NOT NULL
    ) AS con_patron_horario
FROM features_dispensacion;

\echo ''

-- ============================================================================
-- 11. VERIFICAR CAJEROS SIN FEATURES (si hay alguno)
-- ============================================================================

\echo '⚠️  11. CAJEROS SIN FEATURES (debería estar vacío):'
\echo ''

SELECT 
    t.cod_terminal,
    COUNT(*) AS num_transacciones,
    MIN(t.fecha_transaccion) AS primera_tx,
    MAX(t.fecha_transaccion) AS ultima_tx
FROM transacciones t
WHERE t.tipo_operacion IN ('Retiro', 'Avance')
    AND t.cod_estado_transaccion = 1
    AND NOT EXISTS (
        SELECT 1 FROM features_dispensacion f 
        WHERE f.cod_cajero = t.cod_terminal::VARCHAR
    )
GROUP BY t.cod_terminal
LIMIT 10;

\echo ''

-- ============================================================================
-- 12. RESUMEN FINAL
-- ============================================================================

\echo '============================================================================'
\echo '📋 RESUMEN FINAL DE VERIFICACIÓN'
\echo '============================================================================'
\echo ''

DO $$
DECLARE
    v_tablas INTEGER;
    v_vistas INTEGER;
    v_vistas_mat INTEGER;
    v_indices INTEGER;
    v_features INTEGER;
    v_transacciones BIGINT;
BEGIN
    -- Contar estructuras
    SELECT COUNT(*) INTO v_tablas 
    FROM pg_tables 
    WHERE tablename IN ('dispensacion_efectivo', 'features_dispensacion', 'alertas_dispensacion');
    
    SELECT COUNT(*) INTO v_vistas 
    FROM pg_views 
    WHERE viewname LIKE '%dispensacion%';
    
    SELECT COUNT(*) INTO v_vistas_mat 
    FROM pg_matviews 
    WHERE matviewname LIKE '%dispensacion%';
    
    SELECT COUNT(*) INTO v_indices 
    FROM pg_indexes 
    WHERE indexname LIKE '%disp%';
    
    SELECT COUNT(*) INTO v_features 
    FROM features_dispensacion;
    
    SELECT COUNT(*) INTO v_transacciones 
    FROM v_transacciones_dispensacion;
    
    -- Mostrar resumen
    RAISE NOTICE '✅ Tablas creadas: %', v_tablas;
    RAISE NOTICE '✅ Vistas creadas: %', v_vistas;
    RAISE NOTICE '✅ Vistas materializadas: %', v_vistas_mat;
    RAISE NOTICE '✅ Índices creados: %', v_indices;
    RAISE NOTICE '✅ Features calculadas: % cajeros', v_features;
    RAISE NOTICE '✅ Transacciones Retiro/Avance: %', v_transacciones;
    RAISE NOTICE '';
    
    -- Validación
    IF v_tablas >= 3 AND v_vistas >= 2 AND v_vistas_mat >= 1 AND v_features > 0 THEN
        RAISE NOTICE '🎉 TODAS LAS VERIFICACIONES PASARON EXITOSAMENTE';
        RAISE NOTICE '👉 La estructura está lista para el siguiente paso';
    ELSE
        RAISE NOTICE '⚠️  ADVERTENCIA: Algunas estructuras pueden estar faltando';
        RAISE NOTICE '   Revisa los resultados anteriores para más detalles';
    END IF;
END $$;

\echo ''
\echo '============================================================================'
\echo 'FIN DE VERIFICACIÓN'
\echo '============================================================================'