#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
PASO 2: ENTRENAR MODELO ISOLATION FOREST
============================================================================

Entrena el modelo Isolation Forest con features temporales correctas.

El modelo aprenderá patrones normales y detectará anomalías considerando:
- Contexto temporal (hora, día, mes)
- Desviaciones vs múltiples baseline
- Cambios y tendencias

Uso:
    uv run entrenar_modelo.py --config ../config.yaml --contamination 0.01

Tiempo estimado: 10-30 minutos
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
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
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
# PREPARAR FEATURES
# ============================================================================

def preparar_features_ml(df, logger):
    """Prepara features para entrenamiento"""
    
    logger.info("🔧 Preparando features para ML...")
    
    # Lista de features a usar
    feature_columns = [
        # Features básicas
        'monto_total_dispensado',
        'num_transacciones',
        
        # Features temporales
        'hora_del_dia',
        'dia_semana',
        'mes',
        'es_fin_de_semana',
        'es_fin_de_mes',
        'es_quincena',
        
        # Features de desviación
        'z_score_vs_cajero',
        'z_score_vs_hora',
        'z_score_vs_dia_semana',
        'percentil_vs_mes',
        
        # Features de tendencia
        'cambio_vs_anterior',
        'cambio_vs_ayer',
        'tendencia_24h',
        'volatilidad_reciente'
    ]
    
    # Verificar columnas disponibles
    columnas_disponibles = [col for col in feature_columns if col in df.columns]
    columnas_faltantes = set(feature_columns) - set(columnas_disponibles)
    
    if columnas_faltantes:
        logger.warning(f"⚠️  Columnas faltantes: {columnas_faltantes}")
    
    logger.info(f"✅ Features disponibles: {len(columnas_disponibles)}/{len(feature_columns)}")
    
    # Extraer features
    X = df[columnas_disponibles].copy()
    
    # Convertir booleanos a int
    for col in X.columns:
        if X[col].dtype == 'bool':
            X[col] = X[col].astype(int)
    
    # Manejar valores infinitos y NaN
    X.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # Imputar NaN con mediana
    for col in X.columns:
        if X[col].isna().any():
            median_val = X[col].median()
            X[col].fillna(median_val, inplace=True)
            logger.info(f"   Imputados {X[col].isna().sum()} NaN en '{col}' con mediana: {median_val:.2f}")
    
    logger.info(f"✅ Shape final: {X.shape}")
    logger.info(f"✅ Features: {list(X.columns)}")
    
    return X, columnas_disponibles

# ============================================================================
# ENTRENAMIENTO
# ============================================================================

def entrenar_modelo(X, contamination, random_state, logger):
    """Entrena Isolation Forest"""
    
    logger.info("="*70)
    logger.info("🤖 ENTRENANDO ISOLATION FOREST")
    logger.info("="*70)
    
    logger.info(f"Hiperparámetros:")
    logger.info(f"  - contamination: {contamination} ({contamination*100}% anomalías esperadas)")
    logger.info(f"  - n_estimators: 200")
    logger.info(f"  - random_state: {random_state}")
    logger.info(f"  - max_features: 0.8")
    
    # Normalizar features
    logger.info("\n📊 Normalizando features con StandardScaler...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    logger.info(f"   Media después de normalización: {X_scaled.mean():.6f}")
    logger.info(f"   Std después de normalización: {X_scaled.std():.6f}")
    
    # Entrenar modelo
    logger.info("\n🏋️  Entrenando modelo...")
    modelo = IsolationForest(
        contamination=contamination,
        n_estimators=200,
        max_samples='auto',
        max_features=0.8,
        random_state=random_state,
        n_jobs=-1,
        verbose=0
    )
    
    modelo.fit(X_scaled)
    
    logger.info("✅ Modelo entrenado exitosamente")
    
    # Validación básica
    logger.info("\n📊 Validación del modelo:")
    predictions = modelo.predict(X_scaled)
    n_anomalias = (predictions == -1).sum()
    pct_anomalias = (n_anomalias / len(predictions)) * 100
    
    logger.info(f"   Anomalías detectadas en entrenamiento: {n_anomalias:,} ({pct_anomalias:.2f}%)")
    logger.info(f"   Esperado (contamination): {contamination*100:.2f}%")
    
    return modelo, scaler

def guardar_modelo(modelo, scaler, feature_names, model_path, metadata, logger):
    """Guarda modelo entrenado"""
    
    logger.info("="*70)
    logger.info("💾 GUARDANDO MODELO")
    logger.info("="*70)
    
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    
    model_data = {
        'modelo': modelo,
        'scaler': scaler,
        'feature_names': feature_names,
        'metadata': metadata,
        'fecha_entrenamiento': datetime.now(),
        'version': '2.0'
    }
    
    joblib.dump(model_data, model_path)
    
    file_size = os.path.getsize(model_path) / (1024 * 1024)
    logger.info(f"✅ Modelo guardado:")
    logger.info(f"   Ruta: {model_path}")
    logger.info(f"   Tamaño: {file_size:.2f} MB")
    logger.info(f"   Features: {len(feature_names)}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Entrenar Isolation Forest')
    parser.add_argument('--config', type=str, default='../config.yaml')
    parser.add_argument('--contamination', type=float, default=0.01, 
                        help='Proporción esperada de anomalías (0-1)')
    parser.add_argument('--sample-size', type=int, default=2000000,
                        help='Número de registros para entrenar (0=todos)')
    parser.add_argument('--random-state', type=int, default=42)
    args = parser.parse_args()
    
    # Cargar configuración
    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    paths = config['paths']
    postgres_config = config['postgres']
    
    # Setup logging
    log_path = os.path.join(paths['logs'], 'entrenar_modelo.log')
    logger = setup_logging(log_path)
    
    logger.info("="*70)
    logger.info("🚀 ENTRENAMIENTO DE MODELO ISOLATION FOREST")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now()}")
    logger.info(f"Contamination: {args.contamination}")
    logger.info(f"Sample size: {args.sample_size:,}" if args.sample_size > 0 else "Sample size: TODOS")
    logger.info("")
    
    # Conectar a PostgreSQL
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    engine = create_engine(connection_string, poolclass=NullPool)
    
    # Verificar tabla de features
    logger.info("🔍 Verificando tabla features_temporales...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM features_temporales"))
        total_features = result.scalar()
        logger.info(f"✅ Total de registros disponibles: {total_features:,}")
    
    if total_features == 0:
        logger.error("❌ ERROR: Tabla features_temporales está vacía")
        logger.error("   Ejecuta primero: uv run 1_crear_features_temporales.py")
        sys.exit(1)
    
    # Cargar datos para entrenamiento
    logger.info("\n📖 Cargando datos para entrenamiento...")
    
    if args.sample_size > 0 and args.sample_size < total_features:
        # Muestreo aleatorio estratificado por mes y cajero
        query = f"""
        SELECT *
        FROM features_temporales
        WHERE random() < {args.sample_size / total_features}
        LIMIT {args.sample_size}
        """
        logger.info(f"   Muestreando {args.sample_size:,} registros...")
    else:
        query = "SELECT * FROM features_temporales"
        logger.info(f"   Cargando TODOS los registros ({total_features:,})...")
    
    df = pd.read_sql(query, engine)
    logger.info(f"✅ Registros cargados: {len(df):,}\n")
    
    # Preparar features
    X, feature_names = preparar_features_ml(df, logger)
    
    # Entrenar modelo
    modelo, scaler = entrenar_modelo(X, args.contamination, args.random_state, logger)
    
    # Guardar modelo
    metadata = {
        'total_registros_entrenamiento': len(X),
        'contamination': args.contamination,
        'random_state': args.random_state,
        'fecha_datos_desde': df['bucket_15min'].min(),
        'fecha_datos_hasta': df['bucket_15min'].max(),
        'cajeros_unicos': df['cod_terminal'].nunique()
    }
    
    model_path = os.path.join(paths['models'], 'isolation_forest_dispensacion_v2.pkl')
    guardar_modelo(modelo, scaler, feature_names, model_path, metadata, logger)
    
    logger.info("\n" + "="*70)
    logger.info("🎉 ENTRENAMIENTO COMPLETADO")
    logger.info("="*70)
    logger.info(f"✅ Modelo listo para usar en detección de anomalías")
    logger.info(f"✅ Próximo paso: uv run 3_detectar_anomalias.py --config ../config.yaml")

if __name__ == "__main__":
    main()