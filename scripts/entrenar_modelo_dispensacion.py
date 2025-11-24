#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
ENTRENAMIENTO DE MODELO + REGLAS DE NEGOCIO - Detección Fraudes Dispensación
============================================================================

Este script:
1. Entrena Isolation Forest con features históricos
2. Define reglas de negocio para detección de anomalías
3. Guarda modelo entrenado
4. Exporta funciones de reglas para usar en producción

Input:  features_ml (tabla con features calculados)
Output: modelo_isolation_forest_dispensacion.joblib

Uso:
    python entrenar_modelo_dispensacion.py --config config.yaml

Autor: Sistema de Detección de Fraudes - Módulo Dispensación
Fecha: 2025-11-20
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
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# ============================================================================
# LOGGING
# ============================================================================

def setup_logging(log_path, log_level='INFO'):
    """Configura el sistema de logging"""
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    handlers = [
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )
    
    return logging.getLogger(__name__)

# ============================================================================
# CARGAR FEATURES
# ============================================================================

def cargar_features(engine, logger):
    """Carga features desde PostgreSQL"""
    
    logger.info("="*70)
    logger.info("📂 CARGANDO FEATURES DESDE POSTGRESQL")
    logger.info("="*70)
    
    query = """
        SELECT * FROM features_ml
        ORDER BY cod_cajero
    """
    
    logger.info("🔄 Cargando features...")
    df = pd.read_sql(query, engine)
    
    logger.info(f"✅ Features cargados: {len(df):,} cajeros")
    logger.info(f"📊 Columnas: {len(df.columns)}")
    logger.info("")
    
    return df

# ============================================================================
# PREPARAR DATOS PARA ML
# ============================================================================

def preparar_datos_ml(df, logger):
    """Prepara features para Isolation Forest"""
    
    logger.info("="*70)
    logger.info("🔧 PREPARANDO DATOS PARA ML")
    logger.info("="*70)
    
    # Seleccionar features numéricos relevantes
    features_ml = [
        'dispensacion_promedio',
        'dispensacion_std',
        'coef_variacion',
        'transacciones_promedio_15min',
        'iqr',
        'disp_madrugada',
        'disp_manana',
        'disp_tarde',
        'disp_noche',
        'hora_pico',
        'ratio_fds_laboral',
        'std_por_hora',
        'tendencia_slope',
        'tendencia_r2',
        'volatilidad_reciente',
        'volatilidad_promedio',
        'cambio_reciente_pct',
        'pct_anomalias_2std',
        'pct_anomalias_3std',
        'max_z_score_historico',
        'cajeros_cercanos_1km',
        'ratio_vs_zona'
    ]
    
    logger.info(f"📊 Features seleccionados para ML: {len(features_ml)}")
    
    # Extraer features
    X = df[features_ml].copy()
    
    # Manejar valores faltantes
    logger.info("🧹 Limpiando datos...")
    
    # Reemplazar infinitos con NaN
    X = X.replace([np.inf, -np.inf], np.nan)
    
    # Imputar NaN con mediana
    for col in X.columns:
        if X[col].isna().any():
            median_val = X[col].median()
            X[col].fillna(median_val, inplace=True)
            logger.info(f"   Imputados NaN en '{col}' con mediana: {median_val:.2f}")
    
    logger.info(f"✅ Datos preparados: {X.shape}")
    logger.info(f"   Cajeros: {X.shape[0]:,}")
    logger.info(f"   Features: {X.shape[1]}")
    logger.info("")
    
    return X, features_ml, df['cod_cajero'].values

# ============================================================================
# ENTRENAR ISOLATION FOREST
# ============================================================================

def entrenar_isolation_forest(X, contamination, logger):
    """Entrena Isolation Forest"""
    
    logger.info("="*70)
    logger.info("🤖 ENTRENANDO ISOLATION FOREST")
    logger.info("="*70)
    
    logger.info(f"⚙️  Hiperparámetros:")
    logger.info(f"   • contamination: {contamination} ({contamination*100}% anomalías esperadas)")
    logger.info(f"   • n_estimators: 100")
    logger.info(f"   • max_samples: auto")
    logger.info(f"   • random_state: 42")
    logger.info("")
    
    # Normalizar features
    logger.info("📊 Normalizando features con StandardScaler...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    logger.info("✅ Features normalizados")
    logger.info("")
    
    # Entrenar modelo
    logger.info("🏋️  Entrenando modelo...")
    logger.info("   (Esto puede tomar varios minutos)")
    logger.info("")
    
    modelo = IsolationForest(
        contamination=contamination,
        n_estimators=100,
        max_samples='auto',
        random_state=42,
        n_jobs=-1,
        verbose=1
    )
    
    modelo.fit(X_scaled)
    
    logger.info("")
    logger.info("✅ Modelo entrenado exitosamente")
    logger.info("")
    
    # Evaluar en training set
    logger.info("📈 Evaluando modelo en datos de entrenamiento...")
    predictions = modelo.predict(X_scaled)
    scores = modelo.score_samples(X_scaled)
    
    # Convertir scores a rango [0, 1] donde 1 = más anómalo
    scores_normalized = (scores - scores.min()) / (scores.max() - scores.min())
    scores_normalized = 1 - scores_normalized  # Invertir: 1 = anómalo
    
    anomalias = (predictions == -1).sum()
    normales = (predictions == 1).sum()
    
    logger.info(f"📊 Resultados:")
    logger.info(f"   • Normales: {normales:,} ({normales/len(predictions)*100:.2f}%)")
    logger.info(f"   • Anomalías: {anomalias:,} ({anomalias/len(predictions)*100:.2f}%)")
    logger.info(f"   • Score promedio: {scores_normalized.mean():.3f}")
    logger.info(f"   • Score std: {scores_normalized.std():.3f}")
    logger.info(f"   • Score mín: {scores_normalized.min():.3f}")
    logger.info(f"   • Score máx: {scores_normalized.max():.3f}")
    logger.info("")
    
    return modelo, scaler, scores_normalized

# ============================================================================
# REGLAS DE NEGOCIO
# ============================================================================

def definir_reglas_negocio(logger):
    """
    Define reglas de negocio para detección de anomalías.
    
    Estas funciones se exportarán y usarán en producción.
    """
    
    logger.info("="*70)
    logger.info("📏 DEFINIENDO REGLAS DE NEGOCIO")
    logger.info("="*70)
    
    reglas_doc = """
    REGLAS DE NEGOCIO DEFINIDAS:
    
    1. DISPENSACIÓN EXTREMA (peso: 0.30)
       • Si dispensación > 3σ del promedio histórico
       • Severidad aumenta con desviación
    
    2. HORARIO SOSPECHOSO (peso: 0.25)
       • Dispensación en madrugada (0-6am)
       • Especialmente si el cajero normalmente no opera en esa franja
    
    3. CAMBIO DRÁSTICO (peso: 0.20)
       • Dispensación actual vs reciente > 200%
       • Indica posible cambio súbito de comportamiento
    
    4. MÚLTIPLES ANOMALÍAS (peso: 0.15)
       • Cajero con historial de anomalías frecuentes
       • Sugiere cajero problemático o ubicación de riesgo
    
    5. PATRÓN GEOGRÁFICO (peso: 0.10)
       • Dispensación muy diferente a cajeros cercanos
       • Ratio vs zona > 3 o < 0.3
    
    SCORE FINAL = suma ponderada de reglas activadas
    """
    
    logger.info(reglas_doc)
    logger.info("")
    
    # Las funciones reales están definidas más abajo
    # Aquí solo documentamos
    
    return reglas_doc

def aplicar_reglas_negocio(
    dispensacion_actual,
    features_historicos,
    hora_actual,
    es_madrugada=False,
    dispensacion_reciente_promedio=None
):
    """
    Aplica reglas de negocio a una dispensación nueva.
    
    Esta función se usará en producción (procesar_archivo_15min.py)
    
    Args:
        dispensacion_actual: Monto dispensado en el período actual
        features_historicos: Dict con features del cajero (desde features_ml)
        hora_actual: Hora del día (0-23)
        es_madrugada: Bool indicando si es madrugada
        dispensacion_reciente_promedio: Promedio de últimos períodos
    
    Returns:
        score_reglas: Float [0-1] indicando nivel de anomalía
        razones: List de strings explicando por qué es anómalo
        reglas_activadas: Dict con detalle de cada regla
    """
    
    score = 0.0
    razones = []
    reglas_activadas = {}
    
    # Extraer features históricos
    promedio = features_historicos.get('dispensacion_promedio', 0)
    std = features_historicos.get('dispensacion_std', 1)
    disp_madrugada_hist = features_historicos.get('disp_madrugada', 0)
    ratio_vs_zona = features_historicos.get('ratio_vs_zona', 1)
    pct_anomalias_hist = features_historicos.get('pct_anomalias_3std', 0)
    
    # REGLA 1: Dispensación extrema (peso: 0.30)
    if std > 0:
        z_score = abs((dispensacion_actual - promedio) / std)
        
        if z_score > 3:
            score_regla1 = min(z_score / 10, 1.0)  # Normalizar a [0, 1]
            score += 0.30 * score_regla1
            
            reglas_activadas['regla_1_dispensacion_extrema'] = {
                'activada': True,
                'z_score': z_score,
                'score_parcial': 0.30 * score_regla1
            }
            
            razones.append(
                f"Dispensación extrema: ${dispensacion_actual:,.0f} "
                f"({z_score:.1f}σ del promedio histórico ${promedio:,.0f})"
            )
    
    # REGLA 2: Horario sospechoso (peso: 0.25)
    if es_madrugada or (0 <= hora_actual <= 5):
        # Verificar si normalmente opera en madrugada
        ratio_madrugada = disp_madrugada_hist / promedio if promedio > 0 else 0
        
        if ratio_madrugada < 0.1:  # Normalmente no opera en madrugada
            score_regla2 = 1.0
            score += 0.25 * score_regla2
            
            reglas_activadas['regla_2_horario_sospechoso'] = {
                'activada': True,
                'hora': hora_actual,
                'score_parcial': 0.25 * score_regla2
            }
            
            razones.append(
                f"Dispensación en madrugada ({hora_actual}:00h) "
                f"cuando normalmente no opera en este horario"
            )
    
    # REGLA 3: Cambio drástico reciente (peso: 0.20)
    if dispensacion_reciente_promedio and dispensacion_reciente_promedio > 0:
        cambio_pct = ((dispensacion_actual - dispensacion_reciente_promedio) / 
                      dispensacion_reciente_promedio) * 100
        
        if abs(cambio_pct) > 200:
            score_regla3 = min(abs(cambio_pct) / 500, 1.0)
            score += 0.20 * score_regla3
            
            reglas_activadas['regla_3_cambio_drastico'] = {
                'activada': True,
                'cambio_pct': cambio_pct,
                'score_parcial': 0.20 * score_regla3
            }
            
            direccion = "aumento" if cambio_pct > 0 else "disminución"
            razones.append(
                f"Cambio drástico: {direccion} de {abs(cambio_pct):.0f}% "
                f"respecto al promedio reciente"
            )
    
    # REGLA 4: Historial de anomalías (peso: 0.15)
    if pct_anomalias_hist > 5:  # Más del 5% de períodos históricos fueron anómalos
        score_regla4 = min(pct_anomalias_hist / 20, 1.0)
        score += 0.15 * score_regla4
        
        reglas_activadas['regla_4_historial_anomalias'] = {
            'activada': True,
            'pct_anomalias': pct_anomalias_hist,
            'score_parcial': 0.15 * score_regla4
        }
        
        razones.append(
            f"Cajero con historial problemático: "
            f"{pct_anomalias_hist:.1f}% de períodos con anomalías"
        )
    
    # REGLA 5: Patrón geográfico anómalo (peso: 0.10)
    if ratio_vs_zona > 3 or ratio_vs_zona < 0.3:
        score_regla5 = 1.0 if ratio_vs_zona > 3 else 0.7
        score += 0.10 * score_regla5
        
        reglas_activadas['regla_5_patron_geografico'] = {
            'activada': True,
            'ratio_vs_zona': ratio_vs_zona,
            'score_parcial': 0.10 * score_regla5
        }
        
        tipo = "mucho mayor" if ratio_vs_zona > 3 else "mucho menor"
        razones.append(
            f"Dispensación {tipo} que cajeros cercanos "
            f"(ratio: {ratio_vs_zona:.2f})"
        )
    
    # Normalizar score final a [0, 1]
    score = min(score, 1.0)
    
    return score, razones, reglas_activadas

# ============================================================================
# GUARDAR MODELO
# ============================================================================

def guardar_modelo(modelo, scaler, features_ml, model_path, logger):
    """Guarda modelo, scaler y metadata"""
    
    logger.info("="*70)
    logger.info("💾 GUARDANDO MODELO")
    logger.info("="*70)
    
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    
    model_data = {
        'modelo': modelo,
        'scaler': scaler,
        'feature_names': features_ml,
        'fecha_entrenamiento': datetime.now(),
        'version': '1.0',
        'tipo': 'isolation_forest_dispensacion',
        'contamination': modelo.contamination,
        'n_estimators': modelo.n_estimators
    }
    
    joblib.dump(model_data, model_path)
    
    file_size = os.path.getsize(model_path) / (1024 * 1024)
    logger.info(f"✅ Modelo guardado:")
    logger.info(f"   Ruta: {model_path}")
    logger.info(f"   Tamaño: {file_size:.2f} MB")
    logger.info("")

# ============================================================================
# EXPORTAR FUNCIONES DE REGLAS
# ============================================================================

def exportar_funciones_reglas(export_path, logger):
    """Exporta funciones de reglas a un archivo Python reutilizable"""
    
    logger.info("="*70)
    logger.info("📤 EXPORTANDO FUNCIONES DE REGLAS")
    logger.info("="*70)
    
    codigo_reglas = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funciones de Reglas de Negocio - Sistema de Detección de Fraudes
Generado automáticamente por entrenar_modelo_dispensacion.py
"""

def aplicar_reglas_negocio(
    dispensacion_actual,
    features_historicos,
    hora_actual,
    es_madrugada=False,
    dispensacion_reciente_promedio=None
):
    """
    Aplica reglas de negocio a una dispensación nueva.
    
    Args:
        dispensacion_actual: Monto dispensado en el período actual
        features_historicos: Dict con features del cajero
        hora_actual: Hora del día (0-23)
        es_madrugada: Bool indicando si es madrugada
        dispensacion_reciente_promedio: Promedio de últimos períodos
    
    Returns:
        score_reglas, razones, reglas_activadas
    """
    
    score = 0.0
    razones = []
    reglas_activadas = {}
    
    promedio = features_historicos.get('dispensacion_promedio', 0)
    std = features_historicos.get('dispensacion_std', 1)
    disp_madrugada_hist = features_historicos.get('disp_madrugada', 0)
    ratio_vs_zona = features_historicos.get('ratio_vs_zona', 1)
    pct_anomalias_hist = features_historicos.get('pct_anomalias_3std', 0)
    
    # REGLA 1: Dispensación extrema (peso: 0.30)
    if std > 0:
        z_score = abs((dispensacion_actual - promedio) / std)
        if z_score > 3:
            score_regla1 = min(z_score / 10, 1.0)
            score += 0.30 * score_regla1
            reglas_activadas['regla_1_dispensacion_extrema'] = {
                'activada': True, 'z_score': z_score,
                'score_parcial': 0.30 * score_regla1
            }
            razones.append(
                f"Dispensación extrema: ${dispensacion_actual:,.0f} "
                f"({z_score:.1f}σ del promedio ${promedio:,.0f})"
            )
    
    # REGLA 2: Horario sospechoso (peso: 0.25)
    if es_madrugada or (0 <= hora_actual <= 5):
        ratio_madrugada = disp_madrugada_hist / promedio if promedio > 0 else 0
        if ratio_madrugada < 0.1:
            score_regla2 = 1.0
            score += 0.25 * score_regla2
            reglas_activadas['regla_2_horario_sospechoso'] = {
                'activada': True, 'hora': hora_actual,
                'score_parcial': 0.25 * score_regla2
            }
            razones.append(
                f"Dispensación en madrugada ({hora_actual}:00h) "
                f"cuando normalmente no opera"
            )
    
    # REGLA 3: Cambio drástico (peso: 0.20)
    if dispensacion_reciente_promedio and dispensacion_reciente_promedio > 0:
        cambio_pct = ((dispensacion_actual - dispensacion_reciente_promedio) / 
                      dispensacion_reciente_promedio) * 100
        if abs(cambio_pct) > 200:
            score_regla3 = min(abs(cambio_pct) / 500, 1.0)
            score += 0.20 * score_regla3
            reglas_activadas['regla_3_cambio_drastico'] = {
                'activada': True, 'cambio_pct': cambio_pct,
                'score_parcial': 0.20 * score_regla3
            }
            direccion = "aumento" if cambio_pct > 0 else "disminución"
            razones.append(f"Cambio drástico: {direccion} de {abs(cambio_pct):.0f}%")
    
    # REGLA 4: Historial de anomalías (peso: 0.15)
    if pct_anomalias_hist > 5:
        score_regla4 = min(pct_anomalias_hist / 20, 1.0)
        score += 0.15 * score_regla4
        reglas_activadas['regla_4_historial_anomalias'] = {
            'activada': True, 'pct_anomalias': pct_anomalias_hist,
            'score_parcial': 0.15 * score_regla4
        }
        razones.append(
            f"Historial problemático: {pct_anomalias_hist:.1f}% anomalías"
        )
    
    # REGLA 5: Patrón geográfico (peso: 0.10)
    if ratio_vs_zona > 3 or ratio_vs_zona < 0.3:
        score_regla5 = 1.0 if ratio_vs_zona > 3 else 0.7
        score += 0.10 * score_regla5
        reglas_activadas['regla_5_patron_geografico'] = {
            'activada': True, 'ratio_vs_zona': ratio_vs_zona,
            'score_parcial': 0.10 * score_regla5
        }
        tipo = "mucho mayor" if ratio_vs_zona > 3 else "mucho menor"
        razones.append(f"Dispensación {tipo} que cajeros cercanos")
    
    score = min(score, 1.0)
    return score, razones, reglas_activadas
'''
    
    with open(export_path, 'w', encoding='utf-8') as f:
        f.write(codigo_reglas)
    
    logger.info(f"✅ Funciones exportadas a: {export_path}")
    logger.info("")

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal"""
    
    parser = argparse.ArgumentParser(
        description='Entrenar modelo y definir reglas de negocio'
    )
    parser.add_argument(
        '--config', 
        type=str, 
        default='config.yaml',
        help='Ruta al archivo de configuración'
    )
    parser.add_argument(
        '--contamination',
        type=float,
        default=0.01,
        help='Contaminación esperada (default: 0.01 = 1%%)'
    )
    
    args = parser.parse_args()
    
    # Cargar configuración
    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró config.yaml")
        sys.exit(1)
    
    paths = config['paths']
    postgres_config = config['postgres']
    
    # Setup logging
    log_path = os.path.join(paths['logs'], 'entrenar_modelo_dispensacion.log')
    logger = setup_logging(log_path, config['logging']['level'])
    
    logger.info("="*70)
    logger.info("🚀 ENTRENAMIENTO DE MODELO - DISPENSACIÓN")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    logger.info("")
    
    # Conectar a PostgreSQL
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    
    logger.info("🔌 Conectando a PostgreSQL...")
    engine = create_engine(connection_string, poolclass=NullPool)
    logger.info("✅ Conexión exitosa\n")
    
    # 1. Cargar features
    df_features = cargar_features(engine, logger)
    
    # 2. Preparar datos
    X, features_ml, cajeros = preparar_datos_ml(df_features, logger)
    
    # 3. Entrenar Isolation Forest
    modelo, scaler, scores = entrenar_isolation_forest(X, args.contamination, logger)
    
    # 4. Definir reglas de negocio
    reglas_doc = definir_reglas_negocio(logger)
    
    # 5. Guardar modelo
    model_path = os.path.join(paths['models'], 'modelo_isolation_forest_dispensacion.joblib')
    guardar_modelo(modelo, scaler, features_ml, model_path, logger)
    
    # 6. Exportar funciones de reglas
    reglas_path = os.path.join(paths['root'],'scripts', 'reglas_negocio.py')
    exportar_funciones_reglas(reglas_path, logger)
    
    # Finalizar
    logger.info("="*70)
    logger.info("🎉 ENTRENAMIENTO COMPLETADO")
    logger.info("="*70)
    logger.info("")
    logger.info("📋 Archivos generados:")
    logger.info(f"   • Modelo: {model_path}")
    logger.info(f"   • Reglas: {reglas_path}")
    logger.info("")
    logger.info("📋 Próximo paso:")
    logger.info("   python procesar_archivo_15min.py archivo.txt")
    logger.info("")

if __name__ == "__main__":
    main()