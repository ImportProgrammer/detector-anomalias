#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
PROCESAMIENTO DE ARCHIVOS 15 MINUTOS - Detección en Tiempo Real
============================================================================

Script principal que procesa archivos de dispensación cada 15 minutos,
aplica ambos modelos (Isolation Forest + Reglas), y genera alertas.

Formato del archivo esperado:
    01,20251027094500,8796
    02,100,2,5280000,20251027094500,4,20,104,50
    ...

Input:  Archivo de texto con datos de 15 minutos
Output: Alertas guardadas en alertas_dispensacion

Uso:
    python procesar_archivo_15min.py archivo_202511201500.txt

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
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# Importar reglas de negocio
import sys
sys.path.insert(0, '/dados/avc')  # Agregar raíz del proyecto al path

try:
    from reglas_negocio import aplicar_reglas_negocio
except ImportError:
    print("⚠️  Advertencia: No se encontró reglas_negocio.py")
    print("   Ejecuta primero: uv run scripts/entrenar_modelo_dispensacion.py --config config.yaml")
    aplicar_reglas_negocio = None

# ============================================================================
# LOGGING
# ============================================================================

def setup_logging(log_path, log_level='INFO'):
    """Configura el sistema de logging"""
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
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
# PARSEAR ARCHIVO
# ============================================================================

def parsear_archivo_15min(archivo_path, logger):
    """
    Parsea archivo de dispensación de 15 minutos.
    
    Formato:
        Línea 1: 01,YYYYMMDDHHMMSS,num_registros
        Líneas siguientes: 02,cod_cajero,cod_admin,monto,fecha,cant1,denom1,cant2,denom2,...
    """
    
    logger.info("="*70)
    logger.info("📂 PARSEANDO ARCHIVO")
    logger.info("="*70)
    logger.info(f"Archivo: {archivo_path}")
    
    # Mapeo de códigos de administración
    codigos_admin = {
        1: 'Provision',
        2: 'Dispensacion acumulada',
        3: 'Dispensacion antes de provision',
        4: 'Dispensacion despues de provision',
        5: 'Saldo actual',
        6: 'Arqueo de la maquina',
        8: 'Efectivo reciclado',
        9: 'Efectivo reciclado antes de provision',
        10: 'Efectivo reciclado despues de provision'
    }
    
    # Códigos relevantes para análisis (solo dispensación)
    codigos_relevantes = [2, 3, 4]
    
    registros = []
    
    with open(archivo_path, 'r') as f:
        primera_linea = f.readline().strip()
        partes = primera_linea.split(',')
        
        fecha_envio_str = partes[1]
        fecha_envio = datetime.strptime(fecha_envio_str, '%Y%m%d%H%M%S')
        num_registros_esperados = int(partes[2])
        
        logger.info(f"Fecha/hora envío: {fecha_envio}")
        logger.info(f"Registros esperados: {num_registros_esperados:,}")
        
        # Leer registros
        for linea in f:
            partes = linea.strip().split(',')
            
            if len(partes) < 5:
                continue
            
            cod_cajero = partes[1]
            cod_admin = int(partes[2])
            
            # Filtrar solo códigos relevantes
            if cod_admin not in codigos_relevantes:
                continue
            
            monto_total = float(partes[3])
            fecha_str = partes[4]
            fecha_transaccion = datetime.strptime(fecha_str, '%Y%m%d%H%M%S')
            
            # Parsear billetes (parejas: cantidad, denominación)
            billetes = {}
            for i in range(5, len(partes), 2):
                if i+1 < len(partes):
                    cantidad = int(partes[i])
                    denominacion = int(partes[i+1])
                    billetes[f'billetes_{denominacion}k'] = cantidad
            
            registros.append({
                'cod_cajero': cod_cajero,
                'cod_admin_efectivo': cod_admin,
                'tipo_admin': codigos_admin.get(cod_admin, 'Desconocido'),
                'monto_total': monto_total,
                'fecha_hora': fecha_transaccion,
                'fecha_envio': fecha_envio,
                **billetes
            })
    
    df = pd.DataFrame(registros)
    
    logger.info(f"✅ Registros parseados: {len(df):,}")
    logger.info(f"   Cajeros únicos: {df['cod_cajero'].nunique():,}")
    logger.info(f"   Monto total: ${df['monto_total'].sum():,.0f}")
    logger.info("")
    
    return df, fecha_envio

# ============================================================================
# CARGAR MODELO Y FEATURES HISTÓRICOS
# ============================================================================

def cargar_modelo(model_path, logger):
    """Carga modelo Isolation Forest entrenado"""
    
    logger.info("🤖 Cargando modelo...")
    
    if not os.path.exists(model_path):
        logger.error(f"❌ No se encontró el modelo en: {model_path}")
        logger.error("   Ejecuta primero: python entrenar_modelo_dispensacion.py")
        sys.exit(1)
    
    model_data = joblib.load(model_path)
    
    logger.info(f"✅ Modelo cargado:")
    logger.info(f"   Tipo: {model_data['tipo']}")
    logger.info(f"   Versión: {model_data['version']}")
    logger.info(f"   Entrenado: {model_data['fecha_entrenamiento']}")
    logger.info("")
    
    return model_data

def cargar_features_historicos(engine, logger):
    """Carga features históricos de todos los cajeros"""
    
    logger.info("📊 Cargando features históricos...")
    
    query = "SELECT * FROM features_ml"
    df_features = pd.read_sql(query, engine)
    
    # Convertir a dict para acceso rápido
    features_dict = df_features.set_index('cod_cajero').to_dict('index')
    
    logger.info(f"✅ Features cargados: {len(features_dict):,} cajeros")
    logger.info("")
    
    return features_dict

# ============================================================================
# DETECTAR ANOMALÍAS
# ============================================================================

def detectar_anomalias(df_archivo, model_data, features_dict, logger):
    """
    Detecta anomalías aplicando ambos modelos:
    1. Isolation Forest
    2. Reglas de Negocio
    
    Combina scores y genera alertas.
    """
    
    logger.info("="*70)
    logger.info("🔍 DETECTANDO ANOMALÍAS")
    logger.info("="*70)
    
    modelo = model_data['modelo']
    scaler = model_data['scaler']
    feature_names = model_data['feature_names']
    
    alertas = []
    
    # Agrupar por cajero (puede haber múltiples códigos de admin)
    cajeros_unicos = df_archivo['cod_cajero'].unique()
    
    logger.info(f"Analizando {len(cajeros_unicos):,} cajeros...")
    logger.info("")
    
    for cod_cajero in cajeros_unicos:
        df_cajero = df_archivo[df_archivo['cod_cajero'] == cod_cajero]
        
        # Obtener features históricos
        features_hist = features_dict.get(str(cod_cajero))
        
        if not features_hist:
            logger.warning(f"⚠️  Cajero {cod_cajero} sin features históricos - OMITIDO")
            continue
        
        # Calcular dispensación total del período
        dispensacion_actual = df_cajero['monto_total'].sum()
        fecha_hora = df_cajero['fecha_hora'].iloc[0]
        hora_actual = fecha_hora.hour
        
        # ====================================================================
        # MODELO 1: ISOLATION FOREST
        # ====================================================================
        
        # Preparar features para el modelo
        features_modelo = []
        for fname in feature_names:
            valor = features_hist.get(fname, 0)
            # Manejar valores especiales
            if pd.isna(valor) or np.isinf(valor):
                valor = 0
            features_modelo.append(valor)
        
        X = np.array([features_modelo])
        X_scaled = scaler.transform(X)
        
        # Predecir
        score_raw = modelo.score_samples(X_scaled)[0]
        prediccion = modelo.predict(X_scaled)[0]
        
        # Normalizar score a [0, 1] donde 1 = más anómalo
        # (aproximación - en producción usarías los rangos del training)
        score_IF = 1 / (1 + np.exp(score_raw))  # Sigmoide
        
        # ====================================================================
        # MODELO 2: REGLAS DE NEGOCIO
        # ====================================================================
        
        es_madrugada = 0 <= hora_actual <= 5
        
        if aplicar_reglas_negocio:
            score_reglas, razones_reglas, reglas_activadas = aplicar_reglas_negocio(
                dispensacion_actual=dispensacion_actual,
                features_historicos=features_hist,
                hora_actual=hora_actual,
                es_madrugada=es_madrugada,
                dispensacion_reciente_promedio=features_hist.get('dispensacion_promedio')
            )
        else:
            score_reglas = 0
            razones_reglas = []
            reglas_activadas = {}
        
        # ====================================================================
        # COMBINAR SCORES
        # ====================================================================
        
        # Pesos: 60% Isolation Forest, 40% Reglas
        score_final = 0.6 * score_IF + 0.4 * score_reglas
        
        # Clasificar severidad
        if score_final >= 0.9:
            severidad = 'Crítico'
        elif score_final >= 0.7:
            severidad = 'Advertencia'
        elif score_final >= 0.5:
            severidad = 'Sospechoso'
        else:
            severidad = 'Normal'
        
        # Solo guardar si es anómalo
        if score_final >= 0.5:
            
            # Generar descripción
            descripcion_partes = [
                f"Cajero {cod_cajero} dispensó ${dispensacion_actual:,.0f} "
                f"(promedio histórico: ${features_hist.get('dispensacion_promedio', 0):,.0f})"
            ]
            
            if razones_reglas:
                descripcion_partes.append("Razones: " + "; ".join(razones_reglas))
            
            descripcion = ". ".join(descripcion_partes)
            
            # Preparar razones como JSON
            razones_json = {
                'score_isolation_forest': float(score_IF),
                'score_reglas': float(score_reglas),
                'score_final': float(score_final),
                'razones_texto': razones_reglas,
                'reglas_activadas': {k: {
                    'activada': v['activada'],
                    'score_parcial': float(v['score_parcial'])
                } for k, v in reglas_activadas.items()}
            }
            
            alerta = {
                'cod_cajero': cod_cajero,
                'fecha_hora': fecha_hora,
                'tipo_anomalia': 'dispensacion_anomala',
                'severidad': severidad,
                'score_anomalia': score_final,
                'monto_dispensado': dispensacion_actual,
                'monto_esperado': features_hist.get('dispensacion_promedio', 0),
                'desviacion_std': (dispensacion_actual - features_hist.get('dispensacion_promedio', 0)) / 
                                 features_hist.get('dispensacion_std', 1) if features_hist.get('dispensacion_std', 0) > 0 else 0,
                'descripcion': descripcion,
                'razones': json.dumps(razones_json),
                'modelo_usado': 'isolation_forest+reglas',
                'fecha_deteccion': datetime.now()
            }
            
            alertas.append(alerta)
    
    logger.info(f"✅ Análisis completado:")
    logger.info(f"   • Cajeros analizados: {len(cajeros_unicos):,}")
    logger.info(f"   • Anomalías detectadas: {len(alertas):,}")
    
    if len(alertas) > 0:
        df_alertas = pd.DataFrame(alertas)
        severidades = df_alertas['severidad'].value_counts()
        logger.info(f"   • Por severidad:")
        for sev, count in severidades.items():
            logger.info(f"     - {sev}: {count}")
    
    logger.info("")
    
    return alertas

# ============================================================================
# GUARDAR ALERTAS
# ============================================================================

def guardar_alertas(alertas, engine, logger):
    """Guarda alertas en PostgreSQL"""
    
    if len(alertas) == 0:
        logger.info("ℹ️  No hay alertas que guardar")
        return
    
    logger.info("="*70)
    logger.info("💾 GUARDANDO ALERTAS")
    logger.info("="*70)
    
    df_alertas = pd.DataFrame(alertas)
    
    logger.info(f"Guardando {len(df_alertas):,} alertas...")
    
    df_alertas.to_sql(
        'alertas_dispensacion',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )
    
    logger.info("✅ Alertas guardadas exitosamente")
    logger.info("")

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal"""
    
    parser = argparse.ArgumentParser(
        description='Procesar archivo de dispensación de 15 minutos'
    )
    parser.add_argument(
        'archivo',
        type=str,
        help='Ruta al archivo de 15 minutos'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Ruta al archivo de configuración'
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
    log_path = os.path.join(paths['logs'], 'procesar_archivo_15min.log')
    logger = setup_logging(log_path, config['logging']['level'])
    
    logger.info("="*70)
    logger.info("🚀 PROCESAMIENTO ARCHIVO 15 MINUTOS")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    logger.info("")
    
    # Verificar archivo
    if not os.path.exists(args.archivo):
        logger.error(f"❌ No se encontró el archivo: {args.archivo}")
        sys.exit(1)
    
    # Conectar a PostgreSQL
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    
    logger.info("🔌 Conectando a PostgreSQL...")
    engine = create_engine(connection_string, poolclass=NullPool)
    logger.info("✅ Conexión exitosa\n")
    
    # 1. Parsear archivo
    df_archivo, fecha_envio = parsear_archivo_15min(args.archivo, logger)
    
    # 2. Cargar modelo
    model_path = os.path.join(paths['models'], 'modelo_isolation_forest_dispensacion.joblib')
    model_data = cargar_modelo(model_path, logger)
    
    # 3. Cargar features históricos
    features_dict = cargar_features_historicos(engine, logger)
    
    # 4. Detectar anomalías
    alertas = detectar_anomalias(df_archivo, model_data, features_dict, logger)
    
    # 5. Guardar alertas
    guardar_alertas(alertas, engine, logger)
    
    # Finalizar
    logger.info("="*70)
    logger.info("🎉 PROCESAMIENTO COMPLETADO")
    logger.info("="*70)
    logger.info(f"Archivo: {os.path.basename(args.archivo)}")
    logger.info(f"Alertas generadas: {len(alertas)}")
    logger.info("")

if __name__ == "__main__":
    main()