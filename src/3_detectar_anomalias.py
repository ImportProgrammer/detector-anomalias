#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
PASO 3: DETECTAR ANOMALÍAS Y GENERAR ALERTAS
============================================================================

Aplica el modelo Isolation Forest entrenado a todas las ventanas
de 15 minutos y genera alertas en la tabla alertas_dispensacion.

Uso:
    uv run detectar_anomalias.py --config ../config.yaml

Tiempo estimado: 20-40 minutos para 37.7M registros
============================================================================
"""

import pandas as pd
import numpy as np
import yaml
import argparse
import logging
import sys
import os
import joblib
from datetime import datetime
from tqdm import tqdm
from psycopg2.extras import execute_batch
import psycopg2
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
# CARGAR MODELO
# ============================================================================

def cargar_modelo(model_path, logger):
    """Carga modelo entrenado"""
    
    logger.info("📦 Cargando modelo entrenado...")
    
    if not os.path.exists(model_path):
        logger.error(f"❌ ERROR: No se encontró el modelo en {model_path}")
        logger.error("   Ejecuta primero: uv run 2_entrenar_modelo.py")
        sys.exit(1)
    
    model_data = joblib.load(model_path)
    
    logger.info(f"✅ Modelo cargado:")
    logger.info(f"   Versión: {model_data['version']}")
    logger.info(f"   Fecha entrenamiento: {model_data['fecha_entrenamiento']}")
    logger.info(f"   Features: {len(model_data['feature_names'])}")
    
    if 'metadata' in model_data:
        metadata = model_data['metadata']
        logger.info(f"   Registros entrenamiento: {metadata['total_registros_entrenamiento']:,}")
        logger.info(f"   Contamination: {metadata['contamination']}")
    
    return model_data['modelo'], model_data['scaler'], model_data['feature_names']

# ============================================================================
# DETECTAR ANOMALÍAS
# ============================================================================

def determinar_severidad(score_anomalia, z_score_vs_cajero):
    """Determina severidad basada en score y z-score"""
    
    # Score alto Y z-score alto = CRÍTICO
    if score_anomalia >= 80 and abs(z_score_vs_cajero) >= 4:
        return 'critico'
    # Score muy alto O z-score muy alto
    elif score_anomalia >= 85 or abs(z_score_vs_cajero) >= 5:
        return 'critico'
    # Score alto
    elif score_anomalia >= 70 or abs(z_score_vs_cajero) >= 3:
        return 'alto'
    # Score medio
    else:
        return 'medio'

def generar_razones(row, score_anomalia):
    """Genera lista de razones de la anomalía"""
    
    razones = []
    
    # Z-score vs cajero
    if abs(row['z_score_vs_cajero']) >= 3:
        razones.append(f"Z-score vs cajero: {row['z_score_vs_cajero']:.2f} std")
    
    # Z-score vs hora
    if pd.notna(row.get('z_score_vs_hora')) and abs(row['z_score_vs_hora']) >= 3:
        razones.append(f"Z-score vs misma hora: {row['z_score_vs_hora']:.2f} std")
    
    # Z-score vs día semana
    if pd.notna(row.get('z_score_vs_dia_semana')) and abs(row['z_score_vs_dia_semana']) >= 2.5:
        razones.append(f"Z-score vs mismo dia: {row['z_score_vs_dia_semana']:.2f} std")
    
    # Percentil mensual
    if pd.notna(row.get('percentil_vs_mes')) and row['percentil_vs_mes'] >= 95:
        razones.append(f"Percentil mensual: {row['percentil_vs_mes']:.1f}%")
    
    # Cambio vs anterior
    if pd.notna(row.get('cambio_vs_anterior')) and abs(row['cambio_vs_anterior']) >= 200:
        razones.append(f"Cambio vs anterior: {row['cambio_vs_anterior']:+.1f}%")
    
    # Isolation Forest score
    if score_anomalia >= 70:
        razones.append(f"Isolation Forest: {score_anomalia:.0f}/100")
    
    return ' | '.join(razones) if razones else 'Anomalia detectada por modelo'

def generar_descripcion(row, severidad, monto_esperado):
    """Genera descripción legible de la anomalía"""
    
    monto = row['monto_total_dispensado']
    desviacion_pct = ((monto - monto_esperado) / monto_esperado * 100) if monto_esperado > 0 else 0
    
    # Día de la semana
    dias = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo']
    dia_nombre = dias[row['dia_semana'] - 1] if 1 <= row['dia_semana'] <= 7 else 'Dia desconocido'
    
    descripcion = (
        f"Dispensacion: ${monto:,.0f} ({desviacion_pct:+.1f}% vs esperado) | "
        f"{dia_nombre} {row['hora_del_dia']:02d}:XX | "
        f"Z-score: {row['z_score_vs_cajero']:.2f} std"
    )
    
    # Contexto adicional
    if row.get('es_fin_de_semana'):
        descripcion += " | Fin de semana"
    if row.get('es_quincena'):
        descripcion += " | Quincena"
    
    return descripcion

def detectar_anomalias_chunk(df_chunk, modelo, scaler, feature_names, logger):
    """Detecta anomalías en un chunk de datos"""
    
    # Preparar features en el mismo orden que el entrenamiento
    X_chunk = df_chunk[feature_names].copy()
    
    # Convertir booleanos a int
    for col in X_chunk.columns:
        if X_chunk[col].dtype == 'bool':
            X_chunk[col] = X_chunk[col].astype(int)
    
    # Manejar NaN e infinitos
    X_chunk.replace([np.inf, -np.inf], np.nan, inplace=True)
    for col in X_chunk.columns:
        if X_chunk[col].isna().any():
            median_val = X_chunk[col].median()
            X_chunk[col] = X_chunk[col].fillna(median_val)
    
    # Normalizar
    X_scaled = scaler.transform(X_chunk)
    
    # Predecir
    predictions = modelo.predict(X_scaled)
    scores = modelo.score_samples(X_scaled)
    
    # Convertir scores a 0-100 (invertido, 100 = más anómalo)
    scores_normalized = (scores - scores.min()) / (scores.max() - scores.min())
    scores_normalized = (1 - scores_normalized) * 100
    
    # Filtrar solo anomalías
    df_chunk['is_anomaly'] = predictions
    df_chunk['score_anomalia'] = scores_normalized
    
    anomalias = df_chunk[df_chunk['is_anomaly'] == -1].copy()
    
    if len(anomalias) == 0:
        return []
    
    # Generar alertas
    alertas = []
    for idx, row in anomalias.iterrows():
        
        # Calcular monto esperado (promedio del cajero)
        # Esto debería venir de features_ml, pero usamos el inverso del z-score como aprox
        if row['z_score_vs_cajero'] != 0:
            monto_esperado = row['monto_total_dispensado'] / (1 + row['z_score_vs_cajero'])
        else:
            monto_esperado = row['monto_total_dispensado']
        
        # Determinar severidad
        severidad = determinar_severidad(row['score_anomalia'], row['z_score_vs_cajero'])
        
        # Generar razones y descripción
        razones = generar_razones(row, row['score_anomalia'])
        descripcion = generar_descripcion(row, severidad, monto_esperado)
        
        alerta = {
            'cod_cajero': row['cod_terminal'],
            'fecha_hora': row['bucket_15min'],
            'tipo_anomalia': 'isolation_forest',
            'severidad': severidad,
            'score_anomalia': float(row['score_anomalia']),
            'monto_dispensado': float(row['monto_total_dispensado']),
            'monto_esperado': float(monto_esperado),
            'desviacion_std': float(abs(row['z_score_vs_cajero'])),
            'descripcion': descripcion,
            'razones': razones,
            'modelo_usado': 'isolation_forest_v2',
            'fecha_deteccion': datetime.now()
        }
        
        alertas.append(alerta)
    
    return alertas

def insertar_alertas_batch(conn, alertas, batch_size):
    """Inserta alertas en la base de datos"""
    
    if not alertas:
        return 0
    
    query = """
    INSERT INTO alertas_dispensacion (
        cod_cajero, fecha_hora, tipo_anomalia, severidad,
        score_anomalia, monto_dispensado, monto_esperado, desviacion_std,
        descripcion, razones, modelo_usado, fecha_deteccion
    ) VALUES (
        %(cod_cajero)s, %(fecha_hora)s, %(tipo_anomalia)s, %(severidad)s,
        %(score_anomalia)s, %(monto_dispensado)s, %(monto_esperado)s, %(desviacion_std)s,
        %(descripcion)s, %(razones)s, %(modelo_usado)s, %(fecha_deteccion)s
    )
    ON CONFLICT (cod_cajero, fecha_hora) DO UPDATE SET
        severidad = EXCLUDED.severidad,
        score_anomalia = EXCLUDED.score_anomalia,
        desviacion_std = EXCLUDED.desviacion_std,
        descripcion = EXCLUDED.descripcion,
        razones = EXCLUDED.razones,
        fecha_deteccion = EXCLUDED.fecha_deteccion
    """
    
    try:
        cursor = conn.cursor()
        execute_batch(cursor, query, alertas, page_size=batch_size)
        conn.commit()
        cursor.close()
        return len(alertas)
    except Exception as e:
        print(f"❌ Error insertando alertas: {e}")
        conn.rollback()
        return 0

# ============================================================================
# PROCESO PRINCIPAL
# ============================================================================

def procesar_deteccion(engine, db_config, modelo, scaler, feature_names, batch_size, chunk_size, logger):
    """Procesa detección de anomalías en todos los datos"""
    
    logger.info("="*70)
    logger.info("🔍 DETECTANDO ANOMALÍAS")
    logger.info("="*70)
    
    # Contar registros
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM features_temporales"))
        total_registros = result.scalar()
    
    logger.info(f"📊 Total de ventanas a analizar: {total_registros:,}")
    
    # Conectar para inserts (psycopg2 para mejor rendimiento)
    conn_insert = psycopg2.connect(**db_config)
    
    # Procesar en chunks
    offset = 0
    total_alertas = 0
    
    with tqdm(total=total_registros, desc="Analizando ventanas") as pbar:
        while offset < total_registros:
            # Leer chunk
            query = f"""
            SELECT *
            FROM features_temporales
            ORDER BY bucket_15min, cod_terminal
            LIMIT {chunk_size} OFFSET {offset}
            """
            
            df_chunk = pd.read_sql(query, engine)
            
            if len(df_chunk) == 0:
                break
            
            # Detectar anomalías
            alertas = detectar_anomalias_chunk(df_chunk, modelo, scaler, feature_names, logger)
            
            # Insertar alertas
            if alertas:
                insertadas = insertar_alertas_batch(conn_insert, alertas, batch_size)
                total_alertas += insertadas
            
            pbar.update(len(df_chunk))
            offset += chunk_size
            
            # Log cada 10 chunks
            if (offset // chunk_size) % 10 == 0:
                logger.info(f"\n📊 Progreso: {offset:,}/{total_registros:,} | Alertas: {total_alertas:,}")
    
    conn_insert.close()
    
    logger.info(f"\n✅ Total de alertas generadas: {total_alertas:,}")
    
    return total_alertas

def mostrar_estadisticas(engine, logger):
    """Muestra estadísticas de alertas generadas"""
    
    logger.info("\n" + "="*70)
    logger.info("📊 ESTADÍSTICAS DE ALERTAS")
    logger.info("="*70)
    
    with engine.connect() as conn:
        # Por severidad
        result = conn.execute(text("""
            SELECT 
                severidad,
                COUNT(*) as cantidad,
                ROUND(AVG(score_anomalia), 2) as score_promedio,
                ROUND(AVG(desviacion_std), 2) as desv_std_promedio
            FROM alertas_dispensacion
            GROUP BY severidad
            ORDER BY 
                CASE severidad
                    WHEN 'critico' THEN 1
                    WHEN 'alto' THEN 2
                    WHEN 'medio' THEN 3
                END
        """))
        
        logger.info("\n🔔 Por Severidad:")
        for row in result:
            logger.info(f"   {row[0]:10s}: {row[1]:>8,} alertas | Score prom: {row[2]:>6.2f} | Desv prom: {row[3]:>5.2f}σ")
        
        # Top cajeros
        result = conn.execute(text("""
            SELECT 
                cod_cajero,
                COUNT(*) as num_alertas,
                MAX(severidad) as severidad_max,
                ROUND(AVG(score_anomalia), 2) as score_promedio
            FROM alertas_dispensacion
            GROUP BY cod_cajero
            ORDER BY num_alertas DESC
            LIMIT 10
        """))
        
        logger.info("\n🏆 Top 10 Cajeros con Más Alertas:")
        for row in result:
            logger.info(f"   Cajero {row[0]:>6s}: {row[1]:>5,} alertas | Severidad máx: {row[2]:8s} | Score: {row[3]:>6.2f}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Detectar anomalías con Isolation Forest')
    parser.add_argument('--config', type=str, default='../config.yaml')
    parser.add_argument('--chunk-size', type=int, default=100000)
    parser.add_argument('--batch-size', type=int, default=1000)
    args = parser.parse_args()
    
    # Cargar configuración
    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    paths = config['paths']
    postgres_config = config['postgres']
    
    # Setup logging
    log_path = os.path.join(paths['logs'], 'detectar_anomalias.log')
    logger = setup_logging(log_path)
    
    logger.info("="*70)
    logger.info("🚀 DETECCIÓN DE ANOMALÍAS")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now()}")
    logger.info(f"Chunk size: {args.chunk_size:,}")
    logger.info(f"Batch size: {args.batch_size:,}")
    logger.info("")
    
    # Cargar modelo
    model_path = os.path.join(paths['models'], 'isolation_forest_dispensacion_v2.pkl')
    modelo, scaler, feature_names = cargar_modelo(model_path, logger)
    
    # Conectar a PostgreSQL
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    engine = create_engine(connection_string, poolclass=NullPool)
    
    db_config = {
        'dbname': postgres_config['database'],
        'user': postgres_config['user'],
        'password': postgres_config['password'],
        'host': postgres_config['host'],
        'port': postgres_config['port']
    }
    
    # Procesar detección
    total_alertas = procesar_deteccion(
        engine, db_config, modelo, scaler, feature_names,
        args.batch_size, args.chunk_size, logger
    )
    
    # Mostrar estadísticas
    mostrar_estadisticas(engine, logger)
    
    logger.info("\n" + "="*70)
    logger.info("🎉 DETECCIÓN COMPLETADA")
    logger.info("="*70)
    logger.info(f"✅ Total de alertas generadas: {total_alertas:,}")
    logger.info(f"✅ Próximo paso: Lanzar dashboard")

if __name__ == "__main__":
    main()