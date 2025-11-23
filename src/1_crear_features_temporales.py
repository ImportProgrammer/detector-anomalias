#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
PASO 1: CREAR FEATURES TEMPORALES
============================================================================

Genera features con contexto temporal para cada ventana de 15 minutos.
Estas features son CRÍTICAS para que el Isolation Forest detecte anomalías
correctamente considerando:
- Estacionalidad (diciembre vs febrero)
- Patrones semanales (fin de semana vs días laborales)
- Patrones horarios (madrugada vs hora pico)
- Cambios y tendencias

Uso:
    uv run crear_features_temporales.py --config ../config.yaml

Tiempo estimado: 30-60 minutos para 37.7M registros
============================================================================
"""

import pandas as pd
import numpy as np
import yaml
import argparse
import logging
import sys
import os
from datetime import datetime
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

def setup_logging(log_path):
    """Configura logging"""
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# ============================================================================
# CREAR TABLA
# ============================================================================

def crear_tabla_features_temporales(engine, logger):
    """Crea tabla de features temporales si no existe"""
    
    logger.info("📋 Reiniciando tabla features_temporales...")
    
    # Reinica la cracion de la tabla si es necesario
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS features_temporales CASCADE"))
        conn.commit()
    logger.info("🗑️ Tabla anterior eliminada")
    
    logger.info("📋 Creando tabla features_temporales...")
    
    query = """
    CREATE TABLE IF NOT EXISTS features_temporales (
        id SERIAL PRIMARY KEY,
        bucket_15min TIMESTAMP NOT NULL,
        cod_terminal VARCHAR(50) NOT NULL,
        
        -- Features básicas
        monto_total_dispensado NUMERIC,
        num_transacciones INTEGER,
        
        -- Features temporales
        hora_del_dia INTEGER,
        dia_semana INTEGER,
        mes INTEGER,
        es_fin_de_semana BOOLEAN,
        es_fin_de_mes BOOLEAN,
        es_quincena BOOLEAN,
        
        -- Features de desviación
        z_score_vs_cajero NUMERIC,
        z_score_vs_hora NUMERIC,
        z_score_vs_dia_semana NUMERIC,
        percentil_vs_mes NUMERIC,
        
        -- Features de tendencia
        cambio_vs_anterior NUMERIC,
        cambio_vs_ayer NUMERIC,
        tendencia_24h NUMERIC,
        volatilidad_reciente NUMERIC,
        
        fecha_calculo TIMESTAMP DEFAULT NOW(),
        
        CONSTRAINT unique_bucket_terminal UNIQUE(bucket_15min, cod_terminal)
    );
    
    CREATE INDEX IF NOT EXISTS idx_ft_bucket ON features_temporales(bucket_15min);
    CREATE INDEX IF NOT EXISTS idx_ft_terminal ON features_temporales(cod_terminal);
    CREATE INDEX IF NOT EXISTS idx_ft_hora ON features_temporales(hora_del_dia);
    CREATE INDEX IF NOT EXISTS idx_ft_dia ON features_temporales(dia_semana);
    """
    
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
    
    logger.info("✅ Tabla features_temporales creada")

# ============================================================================
# CALCULAR FEATURES
# ============================================================================

def calcular_features_chunk(df_chunk, features_cajeros, logger):
    """Calcula features temporales para un chunk de datos"""
    
    # Asegurar tipos de datos compatibles para el merge
    df_chunk['cod_terminal'] = df_chunk['cod_terminal'].astype(str)
    # Aseguramos también que features_cajeros sea string (por si acaso no se hizo en main)
    features_cajeros['cod_cajero'] = features_cajeros['cod_cajero'].astype(str)
    
    # Features temporales básicas
    df_chunk['hora_del_dia'] = df_chunk['bucket_15min'].dt.hour
    df_chunk['dia_semana'] = df_chunk['bucket_15min'].dt.dayofweek + 1  # 1=lunes
    df_chunk['mes'] = df_chunk['bucket_15min'].dt.month
    
    # Día del mes para detectar fin de mes y quincena
    dia_mes = df_chunk['bucket_15min'].dt.day
    df_chunk['es_fin_de_semana'] = df_chunk['dia_semana'].isin([6, 7])
    df_chunk['es_fin_de_mes'] = dia_mes >= 28
    df_chunk['es_quincena'] = ((dia_mes >= 14) & (dia_mes <= 16)) | ((dia_mes >= 29) | (dia_mes <= 1))
    
    # Merge con features de cajeros
    df_chunk = df_chunk.merge(
        features_cajeros[['cod_cajero', 'dispensacion_promedio', 'dispensacion_std']],
        left_on='cod_terminal',
        right_on='cod_cajero',
        how='left'
    )
    
    # Z-score vs promedio del cajero
    df_chunk['z_score_vs_cajero'] = (
        (df_chunk['monto_total_dispensado'] - df_chunk['dispensacion_promedio']) / 
        df_chunk['dispensacion_std'].replace(0, np.nan)
    ).fillna(0)
    
    return df_chunk

def calcular_features_avanzadas(engine, logger):
    """Calcula features avanzadas usando ventanas de tiempo"""
    
    logger.info("📊 Calculando features avanzadas con SQL...")
    
    # Features que requieren LAG/LEAD windows
    query = """
    WITH datos_agregados AS (
        -- NUEVO: Primero agregamos por bucket + terminal
        SELECT 
            bucket_15min,
            cod_terminal,
            SUM(monto_total_dispensado) as monto_total_dispensado,
            SUM(num_transacciones) as num_transacciones
        FROM mv_dispensacion_por_cajero_15min
        GROUP BY bucket_15min, cod_terminal
    ),
    datos_ordenados AS (
        SELECT 
            bucket_15min,
            cod_terminal,
            monto_total_dispensado,
            EXTRACT(HOUR FROM bucket_15min) as hora_del_dia,
            EXTRACT(DOW FROM bucket_15min) + 1 as dia_semana,
            LAG(monto_total_dispensado, 1) OVER (
                PARTITION BY cod_terminal ORDER BY bucket_15min
            ) as monto_anterior,
            LAG(monto_total_dispensado, 96) OVER (
                PARTITION BY cod_terminal ORDER BY bucket_15min
            ) as monto_ayer,
            AVG(monto_total_dispensado) OVER (
                PARTITION BY cod_terminal, EXTRACT(HOUR FROM bucket_15min)
                ORDER BY bucket_15min
                ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
            ) as promedio_misma_hora,
            STDDEV(monto_total_dispensado) OVER (
                PARTITION BY cod_terminal, EXTRACT(HOUR FROM bucket_15min)
                ORDER BY bucket_15min
                ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
            ) as std_misma_hora,
            AVG(monto_total_dispensado) OVER (
                PARTITION BY cod_terminal, EXTRACT(DOW FROM bucket_15min) + 1
                ORDER BY bucket_15min
                ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as promedio_mismo_dia,
            STDDEV(monto_total_dispensado) OVER (
                PARTITION BY cod_terminal, EXTRACT(DOW FROM bucket_15min) + 1
                ORDER BY bucket_15min
                ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as std_mismo_dia,
            STDDEV(monto_total_dispensado) OVER (
                PARTITION BY cod_terminal
                ORDER BY bucket_15min
                ROWS BETWEEN 96 PRECEDING AND 1 PRECEDING
            ) as volatilidad_24h,
            REGR_SLOPE(
                monto_total_dispensado, 
                EXTRACT(EPOCH FROM bucket_15min)
            ) OVER (
                PARTITION BY cod_terminal
                ORDER BY bucket_15min
                ROWS BETWEEN 96 PRECEDING AND CURRENT ROW
            ) as tendencia_24h
        FROM datos_agregados
    )
    UPDATE features_temporales ft
    SET 
        z_score_vs_hora = CASE 
            WHEN d.std_misma_hora > 0 
            THEN (ft.monto_total_dispensado - d.promedio_misma_hora) / d.std_misma_hora
            ELSE 0 
        END,
        z_score_vs_dia_semana = CASE 
            WHEN d.std_mismo_dia > 0 
            THEN (ft.monto_total_dispensado - d.promedio_mismo_dia) / d.std_mismo_dia
            ELSE 0 
        END,
        cambio_vs_anterior = CASE 
            WHEN d.monto_anterior > 0 
            THEN ((ft.monto_total_dispensado - d.monto_anterior) / d.monto_anterior) * 100
            ELSE 0 
        END,
        cambio_vs_ayer = CASE 
            WHEN d.monto_ayer > 0 
            THEN ((ft.monto_total_dispensado - d.monto_ayer) / d.monto_ayer) * 100
            ELSE 0 
        END,
        volatilidad_reciente = d.volatilidad_24h,
        tendencia_24h = d.tendencia_24h
    FROM datos_ordenados d
    WHERE ft.bucket_15min = d.bucket_15min 
      AND ft.cod_terminal = d.cod_terminal;
    """
    
    logger.info("   Ejecutando query de ventanas temporales...")
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
    
    logger.info("✅ Features avanzadas calculadas")

def calcular_percentiles_mensuales(engine, logger):
    """Calcula percentiles por mes para cada cajero"""
    
    logger.info("📊 Calculando percentiles mensuales...")
    
    query = """
    WITH datos_agregados AS (
        SELECT 
            bucket_15min,
            cod_terminal,
            SUM(monto_total_dispensado) as monto_total_dispensado,
            EXTRACT(MONTH FROM bucket_15min) as mes
        FROM mv_dispensacion_por_cajero_15min
        GROUP BY bucket_15min, cod_terminal
    ),
    percentiles_mes AS (
        SELECT 
            cod_terminal,
            mes,
            bucket_15min,
            monto_total_dispensado,
            PERCENT_RANK() OVER (
                PARTITION BY cod_terminal, mes 
                ORDER BY monto_total_dispensado
            ) as percentil
        FROM datos_agregados
    )
    UPDATE features_temporales ft
    SET percentil_vs_mes = pm.percentil * 100
    FROM percentiles_mes pm
    WHERE ft.cod_terminal = pm.cod_terminal 
      AND ft.bucket_15min = pm.bucket_15min;
    """
    
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
    
    logger.info("✅ Percentiles mensuales calculados")

# ============================================================================
# PROCESO PRINCIPAL
# ============================================================================

def procesar_features_temporales(engine, features_cajeros, batch_size, logger):
    """Procesa todas las ventanas y genera features temporales"""
    
    logger.info("="*70)
    logger.info("📈 PROCESANDO FEATURES TEMPORALES")
    logger.info("="*70)
    
    # Contar registros totales
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM mv_dispensacion_por_cajero_15min"))
        total_registros = result.scalar()
    
    logger.info(f"📊 Total de ventanas a procesar: {total_registros:,}")
    
    # Procesar en chunks
    offset = 0
    total_insertados = 0
    
    with tqdm(total=total_registros, desc="Procesando ventanas") as pbar:
        while offset < total_registros:
            # Leer chunk
            query = f"""
            SELECT 
                bucket_15min,
                cod_terminal,
                SUM(monto_total_dispensado) as monto_total_dispensado,
                SUM(num_transacciones) as num_transacciones
            FROM mv_dispensacion_por_cajero_15min
            GROUP BY bucket_15min, cod_terminal
            ORDER BY bucket_15min, cod_terminal
            LIMIT {batch_size} OFFSET {offset}
            """
            
            df_chunk = pd.read_sql(query, engine)
            
            if len(df_chunk) == 0:
                break
            
            # Calcular features temporales básicas
            df_chunk = calcular_features_chunk(df_chunk, features_cajeros, logger)
            
            # Seleccionar columnas para insertar
            columnas_insert = [
                'bucket_15min', 'cod_terminal', 'monto_total_dispensado', 'num_transacciones',
                'hora_del_dia', 'dia_semana', 'mes', 'es_fin_de_semana', 'es_fin_de_mes',
                'es_quincena', 'z_score_vs_cajero'
            ]
            
            df_insert = df_chunk[columnas_insert].copy()
            
            # Insertar en tabla
            try:
                df_insert.to_sql(
                    'features_temporales',
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                total_insertados += len(df_insert)
                pbar.update(len(df_chunk))
            except Exception as e:
                logger.error(f"❌ Error insertando chunk en offset {offset}: {e}")
            
            offset += batch_size
    
    logger.info(f"\n✅ Total de registros insertados: {total_insertados:,}")
    
    # Calcular features avanzadas
    calcular_features_avanzadas(engine, logger)
    calcular_percentiles_mensuales(engine, logger)

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Crear features temporales para ML')
    parser.add_argument('--config', type=str, default='../config.yaml')
    parser.add_argument('--batch-size', type=int, default=50000, help='Tamaño de chunks')
    args = parser.parse_args()
    
    # Cargar configuración
    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    paths = config['paths']
    postgres_config = config['postgres']
    
    # Setup logging
    log_path = os.path.join(paths['logs'], 'features_temporales.log')
    logger = setup_logging(log_path)
    
    logger.info("="*70)
    logger.info("🚀 CREACIÓN DE FEATURES TEMPORALES")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now()}")
    logger.info(f"Batch size: {args.batch_size:,}")
    logger.info("")
    
    # Conectar a PostgreSQL
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    engine = create_engine(connection_string, poolclass=NullPool)
    
    # Crear tabla
    crear_tabla_features_temporales(engine, logger)
    
    # Cargar features de cajeros
    logger.info("📊 Cargando features de cajeros...")
    features_cajeros = pd.read_sql("SELECT * FROM features_ml", engine)
    logger.info(f"✅ Cargadas features de {len(features_cajeros):,} cajeros")
    
    # Procesar features temporales
    procesar_features_temporales(engine, features_cajeros, args.batch_size, logger)
    
    # Verificar
    logger.info("\n" + "="*70)
    logger.info("🔍 VERIFICACIÓN FINAL")
    logger.info("="*70)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM features_temporales"))
        count = result.scalar()
        logger.info(f"✅ Total registros en features_temporales: {count:,}")
        
        # Muestra de features
        result = conn.execute(text("""
            SELECT 
                cod_terminal,
                bucket_15min,
                monto_total_dispensado,
                z_score_vs_cajero,
                hora_del_dia,
                dia_semana,
                es_fin_de_semana
            FROM features_temporales
            ORDER BY z_score_vs_cajero DESC NULLS LAST
            LIMIT 5
        """))
        
        logger.info("\n📊 Top 5 anomalías por z-score:")
        for row in result:
            logger.info(f"   Cajero: {row[0]} | Fecha: {row[1]} | Monto: ${row[2]:,.0f} | Z-score: {row[3]:.2f}")
    
    logger.info("\n" + "="*70)
    logger.info("🎉 FEATURES TEMPORALES COMPLETADAS")
    logger.info("="*70)

if __name__ == "__main__":
    main()