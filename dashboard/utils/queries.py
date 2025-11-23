"""
Queries SQL reutilizables para el dashboard
"""

# ============================================================================
# QUERIES DE KPIs VENTANA APP
# ============================================================================

QUIERY_ALERTAS_CRITICAS_APP = """
SELECT 
    COUNT(*) 
FROM alertas_dispensacion 
WHERE severidad = 'critico'
"""

QUIERY_ALERTAS_ALTAS_APP = """
SELECT 
    COUNT(*) 
FROM alertas_dispensacion 
WHERE severidad = 'alto'
"""

QUIERY_ALERTAS_MEDIAS_APP = """
SELECT 
    COUNT(*) 
FROM alertas_dispensacion 
WHERE severidad = 'medio'
"""

QUIERY_ALERTAS_TOTAL_APP = """
SELECT 
    COUNT(*) 
FROM alertas_dispensacion
"""

# ============================================================================
# QUERIES DE KPIs
# ============================================================================

QUERY_KPIS_GENERALES = """
SELECT 
    COUNT(*) FILTER (WHERE severidad = 'critico') as alertas_criticas,
    COUNT(*) FILTER (WHERE severidad = 'alto') as alertas_altas,
    COUNT(*) FILTER (WHERE severidad = 'medio') as alertas_medias,
    COUNT(*) as total_alertas,
    COUNT(DISTINCT cod_cajero) as cajeros_con_alertas
FROM alertas_dispensacion
WHERE fecha_hora >= %s AND fecha_hora <= %s
"""

QUERY_ALERTAS_POR_DIA = """
SELECT 
    DATE(fecha_hora) as fecha,
    COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
    COUNT(*) FILTER (WHERE severidad = 'alto') as altas
    --,
    --COUNT(*) FILTER (WHERE severidad = 'medio') as medias
FROM alertas_dispensacion
WHERE fecha_hora >= %s AND fecha_hora <= %s
GROUP BY DATE(fecha_hora)
ORDER BY fecha ASC
"""

# ============================================================================
# QUERIES DE ALERTAS
# ============================================================================

QUERY_ALERTAS_RECIENTES = """
SELECT 
    a.id,
    a.cod_cajero,
    a.fecha_hora,
    a.severidad,
    a.score_anomalia,
    a.monto_dispensado,
    a.monto_esperado,
    a.descripcion,
    a.razones
FROM alertas_dispensacion a
ORDER BY a.fecha_hora DESC, a.score_anomalia DESC
LIMIT %s
"""

QUERY_ALERTAS_POR_CAJERO = """
SELECT 
    a.id,
    a.fecha_hora,
    a.severidad,
    a.score_anomalia,
    a.monto_dispensado,
    a.monto_esperado,
    a.desviacion_std,
    a.descripcion,
    a.razones,
    a.tipo_anomalia
FROM alertas_dispensacion a
WHERE a.cod_cajero = %s
ORDER BY a.fecha_hora DESC
LIMIT 100
"""

QUERY_ALERTAS_CON_UBICACION = """
SELECT 
    a.cod_cajero,
    a.fecha_hora,
    a.severidad,
    a.score_anomalia,
    a.monto_dispensado,
    a.descripcion,
    f.latitud,
    f.longitud,
    f.municipio_dane,
    f.departamento
FROM alertas_dispensacion a
INNER JOIN features_ml f ON a.cod_cajero = f.cod_cajero
WHERE a.fecha_hora >= %s 
  AND a.fecha_hora <= %s
  AND a.severidad IN ('critico', 'alto')
  AND f.latitud IS NOT NULL 
  AND f.longitud IS NOT NULL
ORDER BY a.score_anomalia DESC
LIMIT %s
"""

# ============================================================================
# QUERIES DE CAJEROS
# ============================================================================

QUERY_TOP_CAJEROS_PROBLEMATICOS = """
SELECT 
    a.cod_cajero,
    COUNT(*) as num_alertas,
    COUNT(*) FILTER (WHERE severidad = 'critico') as alertas_criticas,
    MAX(a.fecha_hora) as ultima_alerta,
    ROUND(AVG(a.score_anomalia), 2) as score_promedio,
    f.municipio_dane,
    f.departamento
FROM alertas_dispensacion a
LEFT JOIN features_ml f ON a.cod_cajero = f.cod_cajero
WHERE a.fecha_hora >= %s  AND a.fecha_hora <= %s
GROUP BY a.cod_cajero, f.municipio_dane, f.departamento
ORDER BY num_alertas DESC, alertas_criticas DESC
LIMIT %s
"""

QUERY_INFO_CAJERO = """
SELECT 
    f.cod_cajero,
    f.dispensacion_promedio,
    f.dispensacion_std,
    f.dispensacion_max,
    f.coef_variacion,
    f.pct_anomalias_3std,
    f.latitud,
    f.longitud,
    f.municipio_dane,
    f.departamento,
    f.num_periodos_15min,
    f.transacciones_totales
FROM features_ml f
WHERE f.cod_cajero = %s
"""

# ============================================================================
# QUERIES DE PATRONES TEMPORALES
# ============================================================================

QUERY_HEATMAP_HORARIO = """
SELECT 
    EXTRACT(HOUR FROM fecha_hora) as hora,
    EXTRACT(DOW FROM fecha_hora) as dia_semana,
    COUNT(*) as num_alertas,
    AVG(score_anomalia) as score_promedio
FROM alertas_dispensacion
--WHERE fecha_hora >= NOW() - INTERVAL '30 days'
GROUP BY EXTRACT(HOUR FROM fecha_hora), EXTRACT(DOW FROM fecha_hora)
ORDER BY hora, dia_semana
"""

QUERY_TENDENCIA_MENSUAL = """
SELECT 
    DATE_TRUNC('month', fecha_hora) as mes,
    COUNT(*) as num_alertas,
    COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
    COUNT(*) FILTER (WHERE severidad = 'alto') as altas
FROM alertas_dispensacion
--WHERE fecha_hora >= NOW() - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', fecha_hora)
ORDER BY mes
"""

# ============================================================================
# QUERIES DE ESTADÍSTICAS
# ============================================================================

QUERY_DISTRIBUCION_SCORES = """
SELECT 
    CASE 
        WHEN score_anomalia >= 90 THEN '90-100'
        WHEN score_anomalia >= 80 THEN '80-89'
        WHEN score_anomalia >= 70 THEN '70-79'
        WHEN score_anomalia >= 60 THEN '60-69'
        ELSE '50-59'
    END as rango_score,
    COUNT(*) as cantidad
FROM alertas_dispensacion
GROUP BY 
    CASE 
        WHEN score_anomalia >= 90 THEN '90-100'
        WHEN score_anomalia >= 80 THEN '80-89'
        WHEN score_anomalia >= 70 THEN '70-79'
        WHEN score_anomalia >= 60 THEN '60-69'
        ELSE '50-59'
    END
ORDER BY rango_score DESC
"""

QUERY_COMPARACION_PERIODOS = """
WITH periodo_actual AS (
    SELECT 
        COUNT(*) as alertas_actuales,
        AVG(score_anomalia) as score_actual
    FROM alertas_dispensacion
    WHERE fecha_hora >= %s AND fecha_hora <= %s
    AND severidad = 'critico'
),
periodo_anterior AS (
    SELECT 
        COUNT(*) as alertas_anteriores,
        AVG(score_anomalia) as score_anterior
    FROM alertas_dispensacion
    WHERE fecha_hora >= %s AND fecha_hora < %s
    AND severidad = 'critico'
)
SELECT 
    pa.alertas_actuales,
    pa.score_actual,
    pp.alertas_anteriores,
    pp.score_anterior,
    ROUND(((pa.alertas_actuales::numeric - pp.alertas_anteriores) / 
           NULLIF(pp.alertas_anteriores, 0) * 100), 2) as cambio_porcentual
FROM periodo_actual pa, periodo_anterior pp
"""

# ============================================================================
# QUERIES DE ANÁLISIS GEOGRÁFICO
# ============================================================================

QUERY_ALERTAS_POR_DEPARTAMENTO = """
SELECT 
    f.departamento,
    COUNT(*) as num_alertas,
    COUNT(DISTINCT a.cod_cajero) as cajeros_afectados,
    ROUND(AVG(a.score_anomalia), 2) as score_promedio
FROM alertas_dispensacion a
INNER JOIN features_ml f ON a.cod_cajero = f.cod_cajero
WHERE a.fecha_hora >= NOW() - INTERVAL '30 days'
  AND f.departamento IS NOT NULL
GROUP BY f.departamento
ORDER BY num_alertas DESC
"""

QUERY_ALERTAS_POR_MUNICIPIO = """
SELECT 
    f.municipio_dane,
    f.departamento,
    COUNT(*) as num_alertas,
    COUNT(DISTINCT a.cod_cajero) as cajeros_afectados
FROM alertas_dispensacion a
INNER JOIN features_ml f ON a.cod_cajero = f.cod_cajero
WHERE a.fecha_hora >= NOW() - INTERVAL '30 days'
  AND f.municipio_dane IS NOT NULL
GROUP BY f.municipio_dane, f.departamento
ORDER BY num_alertas DESC
LIMIT 20
"""