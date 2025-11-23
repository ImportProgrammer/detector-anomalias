#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
ARREGLAR FEATURES AVANZADAS
============================================================================

Calcula las features que faltaron en features_temporales procesando
por cajero para evitar problemas de memoria.

Para ejecutar leer las instrucciones al final

Uso:
    uv run arreglar_features_avanzadas.py --config ../config.yaml

Tiempo estimado: 30-60 minutos
============================================================================
"""

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
# CALCULAR FEATURES POR CAJERO
# ============================================================================

def calcular_features_cajero(engine, cod_cajero, logger):
    """Calcula features avanzadas para un cajero específico"""
    
    query = """
    WITH datos_ordenados AS (
        SELECT 
            bucket_15min,
            cod_terminal,
            monto_total_dispensado,
            EXTRACT(HOUR FROM bucket_15min) as hora_del_dia,
            EXTRACT(DOW FROM bucket_15min) + 1 as dia_semana,
            
            -- LAG para cambios
            LAG(monto_total_dispensado, 1) OVER (ORDER BY bucket_15min) as monto_anterior,
            LAG(monto_total_dispensado, 96) OVER (ORDER BY bucket_15min) as monto_ayer,
            
            -- Volatilidad 24h (últimas 96 ventanas de 15min)
            STDDEV(monto_total_dispensado) OVER (
                ORDER BY bucket_15min
                ROWS BETWEEN 96 PRECEDING AND 1 PRECEDING
            ) as volatilidad_24h,
            
            -- Tendencia 24h
            CASE 
                WHEN COUNT(*) OVER (
                    ORDER BY bucket_15min
                    ROWS BETWEEN 96 PRECEDING AND CURRENT ROW
                ) >= 10
                THEN REGR_SLOPE(
                    monto_total_dispensado, 
                    EXTRACT(EPOCH FROM bucket_15min)
                ) OVER (
                    ORDER BY bucket_15min
                    ROWS BETWEEN 96 PRECEDING AND CURRENT ROW
                )
                ELSE NULL
            END as tendencia_24h
        FROM features_temporales
        WHERE cod_terminal = :cod_cajero
        ORDER BY bucket_15min
    ),
    promedios_hora AS (
        SELECT 
            hora_del_dia,
            AVG(monto_total_dispensado) as promedio_hora,
            STDDEV(monto_total_dispensado) as std_hora
        FROM features_temporales
        WHERE cod_terminal = :cod_cajero
        GROUP BY hora_del_dia
    ),
    promedios_dia AS (
        SELECT 
            dia_semana,
            AVG(monto_total_dispensado) as promedio_dia,
            STDDEV(monto_total_dispensado) as std_dia
        FROM features_temporales
        WHERE cod_terminal = :cod_cajero
        GROUP BY dia_semana
    )
    UPDATE features_temporales ft
    SET 
        -- Cambios
        cambio_vs_anterior = CASE 
            WHEN d.monto_anterior > 0 AND d.monto_anterior IS NOT NULL
            THEN ((d.monto_total_dispensado - d.monto_anterior) / d.monto_anterior) * 100
            ELSE 0 
        END,
        cambio_vs_ayer = CASE 
            WHEN d.monto_ayer > 0 AND d.monto_ayer IS NOT NULL
            THEN ((d.monto_total_dispensado - d.monto_ayer) / d.monto_ayer) * 100
            ELSE 0 
        END,
        
        -- Z-scores vs hora
        z_score_vs_hora = CASE 
            WHEN ph.std_hora > 0 AND ph.std_hora IS NOT NULL
            THEN (d.monto_total_dispensado - ph.promedio_hora) / ph.std_hora
            ELSE 0 
        END,
        
        -- Z-scores vs día
        z_score_vs_dia_semana = CASE 
            WHEN pd.std_dia > 0 AND pd.std_dia IS NOT NULL
            THEN (d.monto_total_dispensado - pd.promedio_dia) / pd.std_dia
            ELSE 0 
        END,
        
        -- Volatilidad y tendencia
        volatilidad_reciente = d.volatilidad_24h,
        tendencia_24h = d.tendencia_24h
    FROM datos_ordenados d
    LEFT JOIN promedios_hora ph ON d.hora_del_dia = ph.hora_del_dia
    LEFT JOIN promedios_dia pd ON d.dia_semana = pd.dia_semana
    WHERE ft.bucket_15min = d.bucket_15min 
      AND ft.cod_terminal = d.cod_terminal
      AND ft.cod_terminal = :cod_cajero;
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(query), {'cod_cajero': cod_cajero})
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error en cajero {cod_cajero}: {e}")
        return False

def calcular_percentiles_mensuales_cajero(engine, cod_cajero, logger):
    """Calcula percentiles mensuales para un cajero"""
    
    query = """
    WITH percentiles_mes AS (
        SELECT 
            cod_terminal,
            EXTRACT(MONTH FROM bucket_15min) as mes,
            bucket_15min,
            monto_total_dispensado,
            PERCENT_RANK() OVER (
                PARTITION BY EXTRACT(MONTH FROM bucket_15min)
                ORDER BY monto_total_dispensado
            ) as percentil
        FROM features_temporales
        WHERE cod_terminal = :cod_cajero
    )
    UPDATE features_temporales ft
    SET percentil_vs_mes = pm.percentil * 100
    FROM percentiles_mes pm
    WHERE ft.cod_terminal = pm.cod_terminal 
      AND ft.bucket_15min = pm.bucket_15min
      AND ft.cod_terminal = :cod_cajero;
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(query), {'cod_cajero': cod_cajero})
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error calculando percentiles para cajero {cod_cajero}: {e}")
        return False

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Arreglar features avanzadas')
    parser.add_argument('--config', type=str, default='../config.yaml')
    args = parser.parse_args()
    
    # Cargar configuración
    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    paths = config['paths']
    postgres_config = config['postgres']
    
    # Setup logging
    log_path = os.path.join(paths['logs'], 'arreglar_features.log')
    logger = setup_logging(log_path)
    
    logger.info("="*70)
    logger.info("🔧 ARREGLANDO FEATURES AVANZADAS")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now()}")
    logger.info("")
    
    # Conectar a PostgreSQL
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    engine = create_engine(connection_string, poolclass=NullPool)
    
    # Obtener lista de cajeros
    logger.info("📋 Obteniendo lista de cajeros...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT cod_terminal FROM features_temporales ORDER BY cod_terminal"))
        cajeros = [row[0] for row in result]
    
    logger.info(f"✅ Total de cajeros a procesar: {len(cajeros)}")
    logger.info("")
    
    # Procesar cada cajero
    logger.info("🔄 Procesando cajeros...")
    exitos = 0
    fallos = 0
    
    with tqdm(total=len(cajeros), desc="Procesando cajeros") as pbar:
        for cajero in cajeros:
            # Calcular features avanzadas
            if calcular_features_cajero(engine, cajero, logger):
                # Calcular percentiles
                if calcular_percentiles_mensuales_cajero(engine, cajero, logger):
                    exitos += 1
                else:
                    fallos += 1
            else:
                fallos += 1
            
            pbar.update(1)
    
    logger.info("\n" + "="*70)
    logger.info("📊 RESUMEN")
    logger.info("="*70)
    logger.info(f"✅ Cajeros procesados exitosamente: {exitos}")
    logger.info(f"❌ Cajeros con errores: {fallos}")
    
    # Verificar una muestra
    logger.info("\n🔍 Verificando resultado...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                cod_terminal,
                bucket_15min,
                monto_total_dispensado,
                z_score_vs_cajero,
                z_score_vs_hora,
                z_score_vs_dia_semana,
                cambio_vs_anterior,
                percentil_vs_mes
            FROM features_temporales
            WHERE z_score_vs_hora IS NOT NULL
            ORDER BY ABS(z_score_vs_cajero) DESC
            LIMIT 5
        """))
        
        logger.info("\n📊 Top 5 anomalías por z-score:")
        for row in result:
            logger.info(f"   Cajero: {row[0]} | Monto: ${row[2]:,.0f} | Z-cajero: {row[3]:.2f} | Z-hora: {row[4]:.2f if row[4] else 'N/A'}")
    
    logger.info("\n" + "="*70)
    logger.info("🎉 FEATURES AVANZADAS COMPLETADAS")
    logger.info("="*70)
    logger.info("✅ Ahora puedes ejecutar el Paso 2: entrenar modelo")

if __name__ == "__main__":
    main()
    
    
# ============================================================================
# APLICACIÓN DE FEATURES AVANZADAS
# ============================================================================  
    
# # 1. Calcular features avanzadas (30-60 min)
# uv run arreglar_features_avanzadas.py --config ../config.yaml

# # 2. Re-entrenar modelo con TODAS las features (10-15 min)
# uv run 2_entrenar_modelo.py --config ../config.yaml --contamination 0.01

# # 3. Re-detectar anomalías con nuevo modelo (30-40 min)
# # Primero limpiar alertas anteriores
# # SQL: TRUNCATE TABLE alertas_dispensacion;
# uv run 3_detectar_anomalias.py --config ../config.yaml --chunk-size 100000
# ```

# ---

# ## 💡 **¿Vale la pena?**

# ### **Beneficios de agregar features avanzadas:**
# - ✅ Detecta cambios bruscos mejor
# - ✅ Considera patrones horarios
# - ✅ Considera patrones del día de la semana
# - ✅ Detecta tendencias sostenidas
# - ✅ Mayor precisión (~5-10% más anomalías detectadas)

# ### **Costo:**
# - ⏱️ ~30-60 minutos para calcular
# - ⏱️ ~10-15 minutos para re-entrenar
# - ⏱️ ~30-40 minutos para re-detectar
# - **Total: ~1.5-2 horas**

# ---

# ## 🎯 **Mi recomendación:**

# ### **Para el MVP:**
# **NO lo hagas ahora** - ya tienes un modelo funcional con 9 features buenas.

# ### **Para mejora futura:**
# Después de mostrar el dashboard y validar con el cliente, ejecutas:
# 1. `arreglar_features_avanzadas.py`
# 2. Re-entrenar
# 3. Re-detectar

# Y le muestras: "Mira, ahora con 16 features en lugar de 9, detectamos X% más anomalías"

# ---

# ## ✅ **Tu orden está perfecto:**
# ```
# arreglar_features_avanzadas.py  →  2_entrenar_modelo.py  →  3_detectar_anomalias.py
#         (calcula)                        (re-entrena)              (re-detecta)