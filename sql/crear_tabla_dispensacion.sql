-- ============================================================================
-- TABLA DE DISPENSACIÓN DE EFECTIVO - TimescaleDB
-- ============================================================================
-- Script para crear la estructura de tablas para análisis de dispensación
-- Incluye hypertable principal y continuous aggregates
-- ============================================================================

-- ============================================================================
-- 1. TABLA PRINCIPAL: dispensacion_efectivo (HYPERTABLE)
-- ============================================================================

CREATE TABLE IF NOT EXISTS dispensacion_efectivo (
    -- Identificación
    id BIGSERIAL,
    cod_cajero VARCHAR(20) NOT NULL,
    fecha_hora TIMESTAMP NOT NULL,
    
    -- Información de dispensación
    cod_admin_efectivo INTEGER NOT NULL,  -- 1-10 según códigos
    tipo_admin TEXT,                       -- Descripción del código
    monto_total DECIMAL(15, 2) NOT NULL,   -- Total dispensado
    
    -- Desglose por denominación (pares: cantidad, denominación)
    billetes_20k INTEGER DEFAULT 0,
    billetes_50k INTEGER DEFAULT 0,
    billetes_100k INTEGER DEFAULT 0,
    billetes_10k INTEGER DEFAULT 0,
    billetes_5k INTEGER DEFAULT 0,
    billetes_2k INTEGER DEFAULT 0,
    billetes_1k INTEGER DEFAULT 0,
    
    -- Metadata
    archivo_origen TEXT,                   -- Nombre del archivo que originó el registro
    fecha_procesamiento TIMESTAMP DEFAULT NOW(),
    
    -- Constraint
    PRIMARY KEY (id, fecha_hora)
);

-- Comentarios
COMMENT ON TABLE dispensacion_efectivo IS 'Registro de dispensación de efectivo por cajero en intervalos de tiempo';
COMMENT ON COLUMN dispensacion_efectivo.cod_admin_efectivo IS '1=Provisión, 2=Disp.Acum, 3=Disp.antes, 4=Disp.después, 5=Saldo, 6=Arqueo, 8=Reciclado, 9=Rec.antes, 10=Rec.después';

-- ============================================================================
-- 2. CONVERTIR A HYPERTABLE
-- ============================================================================

SELECT create_hypertable(
    'dispensacion_efectivo', 
    'fecha_hora',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ============================================================================
-- 3. ÍNDICES PARA OPTIMIZACIÓN
-- ============================================================================

-- Índice por cajero y fecha (consultas frecuentes)
CREATE INDEX IF NOT EXISTS idx_disp_cajero_fecha 
    ON dispensacion_efectivo(cod_cajero, fecha_hora DESC);

-- Índice por código de administración
CREATE INDEX IF NOT EXISTS idx_disp_cod_admin 
    ON dispensacion_efectivo(cod_admin_efectivo);

-- Índice por monto (para detectar dispensaciones altas)
CREATE INDEX IF NOT EXISTS idx_disp_monto 
    ON dispensacion_efectivo(monto_total DESC);

-- ============================================================================
-- 4. CONTINUOUS AGGREGATE: Agregación cada 15 minutos
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS dispensacion_15min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('15 minutes', fecha_hora) AS bucket_15min,
    cod_cajero,
    cod_admin_efectivo,
    
    -- Agregaciones
    COUNT(*) AS num_registros,
    SUM(monto_total) AS monto_total,
    AVG(monto_total) AS monto_promedio,
    MAX(monto_total) AS monto_maximo,
    MIN(monto_total) AS monto_minimo,
    STDDEV(monto_total) AS monto_std,
    
    -- Billetes por denominación
    SUM(billetes_20k) AS total_billetes_20k,
    SUM(billetes_50k) AS total_billetes_50k,
    SUM(billetes_100k) AS total_billetes_100k,
    SUM(billetes_10k) AS total_billetes_10k,
    SUM(billetes_5k) AS total_billetes_5k,
    SUM(billetes_2k) AS total_billetes_2k,
    SUM(billetes_1k) AS total_billetes_1k,
    
    -- Total de billetes
    SUM(billetes_20k + billetes_50k + billetes_100k + 
        billetes_10k + billetes_5k + billetes_2k + billetes_1k) AS total_billetes
        
FROM dispensacion_efectivo
GROUP BY bucket_15min, cod_cajero, cod_admin_efectivo;

-- Política de refresh automático (cada 5 minutos)
SELECT add_continuous_aggregate_policy('dispensacion_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- ============================================================================
-- 5. CONTINUOUS AGGREGATE: Agregación cada 30 minutos
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS dispensacion_30min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('30 minutes', fecha_hora) AS bucket_30min,
    cod_cajero,
    cod_admin_efectivo,
    
    COUNT(*) AS num_registros,
    SUM(monto_total) AS monto_total,
    AVG(monto_total) AS monto_promedio,
    MAX(monto_total) AS monto_maximo,
    MIN(monto_total) AS monto_minimo,
    STDDEV(monto_total) AS monto_std,
    
    SUM(billetes_20k) AS total_billetes_20k,
    SUM(billetes_50k) AS total_billetes_50k,
    SUM(billetes_100k) AS total_billetes_100k,
    SUM(billetes_10k) AS total_billetes_10k,
    SUM(billetes_5k) AS total_billetes_5k,
    SUM(billetes_2k) AS total_billetes_2k,
    SUM(billetes_1k) AS total_billetes_1k,
    
    SUM(billetes_20k + billetes_50k + billetes_100k + 
        billetes_10k + billetes_5k + billetes_2k + billetes_1k) AS total_billetes
        
FROM dispensacion_efectivo
GROUP BY bucket_30min, cod_cajero, cod_admin_efectivo;

SELECT add_continuous_aggregate_policy('dispensacion_30min',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes',
    if_not_exists => TRUE
);

-- ============================================================================
-- 6. CONTINUOUS AGGREGATE: Agregación cada 1 hora
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS dispensacion_1hora
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', fecha_hora) AS bucket_1hora,
    cod_cajero,
    cod_admin_efectivo,
    
    COUNT(*) AS num_registros,
    SUM(monto_total) AS monto_total,
    AVG(monto_total) AS monto_promedio,
    MAX(monto_total) AS monto_maximo,
    MIN(monto_total) AS monto_minimo,
    STDDEV(monto_total) AS monto_std,
    
    SUM(billetes_20k) AS total_billetes_20k,
    SUM(billetes_50k) AS total_billetes_50k,
    SUM(billetes_100k) AS total_billetes_100k,
    SUM(billetes_10k) AS total_billetes_10k,
    SUM(billetes_5k) AS total_billetes_5k,
    SUM(billetes_2k) AS total_billetes_2k,
    SUM(billetes_1k) AS total_billetes_1k,
    
    SUM(billetes_20k + billetes_50k + billetes_100k + 
        billetes_10k + billetes_5k + billetes_2k + billetes_1k) AS total_billetes
        
FROM dispensacion_efectivo
GROUP BY bucket_1hora, cod_cajero, cod_admin_efectivo;

SELECT add_continuous_aggregate_policy('dispensacion_1hora',
    start_offset => INTERVAL '6 hours',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);

-- ============================================================================
-- 7. TABLA DE FEATURES DE DISPENSACIÓN (para ML)
-- ============================================================================

CREATE TABLE IF NOT EXISTS features_dispensacion (
    cod_cajero VARCHAR(20) PRIMARY KEY,
    
    -- Estadísticas generales
    dispensacion_promedio DECIMAL(15, 2),
    dispensacion_std DECIMAL(15, 2),
    dispensacion_max DECIMAL(15, 2),
    dispensacion_min DECIMAL(15, 2),
    
    -- Por franja horaria (00-06, 06-12, 12-18, 18-24)
    disp_promedio_madrugada DECIMAL(15, 2),
    disp_promedio_manana DECIMAL(15, 2),
    disp_promedio_tarde DECIMAL(15, 2),
    disp_promedio_noche DECIMAL(15, 2),
    
    -- Por día de semana
    disp_promedio_lunes DECIMAL(15, 2),
    disp_promedio_martes DECIMAL(15, 2),
    disp_promedio_miercoles DECIMAL(15, 2),
    disp_promedio_jueves DECIMAL(15, 2),
    disp_promedio_viernes DECIMAL(15, 2),
    disp_promedio_sabado DECIMAL(15, 2),
    disp_promedio_domingo DECIMAL(15, 2),
    
    -- Patrones de billetes
    ratio_billetes_altos DECIMAL(5, 4),  -- (50k+100k) / total
    billetes_promedio_20k DECIMAL(10, 2),
    billetes_promedio_50k DECIMAL(10, 2),
    billetes_promedio_100k DECIMAL(10, 2),
    
    -- Metadata
    total_transacciones INTEGER,
    fecha_primer_registro TIMESTAMP,
    fecha_ultimo_registro TIMESTAMP,
    fecha_calculo TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE features_dispensacion IS 'Features históricas por cajero para entrenamiento de modelo ML';

-- ============================================================================
-- 8. TABLA DE ALERTAS DE DISPENSACIÓN
-- ============================================================================

CREATE TABLE IF NOT EXISTS alertas_dispensacion (
    id SERIAL PRIMARY KEY,
    cod_cajero VARCHAR(20) NOT NULL,
    fecha_hora TIMESTAMP NOT NULL,
    
    -- Información de la alerta
    tipo_anomalia TEXT NOT NULL,  -- 'monto_alto', 'horario_inusual', 'patron_billetes', etc.
    severidad VARCHAR(20) NOT NULL,  -- 'Crítico', 'Advertencia', 'Sospechoso'
    score_anomalia DECIMAL(5, 3),
    
    -- Detalles
    monto_dispensado DECIMAL(15, 2),
    monto_esperado DECIMAL(15, 2),
    desviacion_std DECIMAL(5, 2),
    
    -- Razones
    descripcion TEXT,
    razones JSONB,  -- Array de razones detalladas
    
    -- Metadata
    modelo_usado TEXT,
    fecha_deteccion TIMESTAMP DEFAULT NOW(),
    validado BOOLEAN DEFAULT FALSE,
    validado_por VARCHAR(100),
    fecha_validacion TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alertas_cajero_fecha 
    ON alertas_dispensacion(cod_cajero, fecha_hora DESC);

CREATE INDEX IF NOT EXISTS idx_alertas_severidad 
    ON alertas_dispensacion(severidad);

COMMENT ON TABLE alertas_dispensacion IS 'Alertas de anomalías detectadas en dispensación de efectivo';

-- ============================================================================
-- 9. FUNCIÓN HELPER: Obtener patrón histórico de un cajero
-- ============================================================================

CREATE OR REPLACE FUNCTION get_patron_cajero(
    p_cod_cajero VARCHAR(20),
    p_hora INTEGER DEFAULT NULL
)
RETURNS TABLE (
    dispensacion_promedio DECIMAL,
    dispensacion_std DECIMAL,
    dispensacion_max DECIMAL,
    num_transacciones BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        AVG(monto_total)::DECIMAL AS dispensacion_promedio,
        STDDEV(monto_total)::DECIMAL AS dispensacion_std,
        MAX(monto_total)::DECIMAL AS dispensacion_max,
        COUNT(*) AS num_transacciones
    FROM dispensacion_efectivo
    WHERE cod_cajero = p_cod_cajero
        AND cod_admin_efectivo IN (2, 3, 4)  -- Solo códigos relevantes
        AND (p_hora IS NULL OR EXTRACT(HOUR FROM fecha_hora) = p_hora);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 10. VISTA: Resumen de dispensación por cajero (últimas 24 horas)
-- ============================================================================

CREATE OR REPLACE VIEW v_dispensacion_ultima_24h AS
SELECT 
    cod_cajero,
    COUNT(*) AS num_dispensaciones,
    SUM(monto_total) AS monto_total_24h,
    AVG(monto_total) AS monto_promedio_24h,
    MAX(monto_total) AS monto_maximo_24h,
    MIN(fecha_hora) AS primera_dispensacion,
    MAX(fecha_hora) AS ultima_dispensacion
FROM dispensacion_efectivo
WHERE fecha_hora >= NOW() - INTERVAL '24 hours'
    AND cod_admin_efectivo IN (2, 3, 4)
GROUP BY cod_cajero;

COMMENT ON VIEW v_dispensacion_ultima_24h IS 'Resumen de dispensación por cajero en las últimas 24 horas';

-- ============================================================================
-- FIN DEL SCRIPT
-- ============================================================================

-- Verificación
SELECT 'Estructura de dispensación creada exitosamente' AS status;