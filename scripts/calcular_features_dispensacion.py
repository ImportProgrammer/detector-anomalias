#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
CÁLCULO DE FEATURES DE DISPENSACIÓN - Sistema Detección de Fraudes
============================================================================

Calcula features avanzados de dispensación desde la vista materializada
para entrenar el modelo de Machine Learning.

Features calculados:
- Estadísticas por cajero (promedio, std, max, min)
- Patrones temporales (hora pico, día pico)
- Volatilidad y tendencias
- Comparación con cajeros cercanos (geográfica)
- Ratios y proporciones

Input:  mv_dispensacion_por_cajero_15min (vista materializada)
Output: features_ml (tabla optimizada para ML)

Uso:
    python calcular_features_dispensacion.py --config config.yaml

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
from datetime import datetime
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from scipy import stats

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
# CARGAR DATOS
# ============================================================================

def cargar_datos_dispensacion(engine, logger):
    """Carga datos de dispensación desde vista materializada"""
    
    logger.info("="*70)
    logger.info("📂 CARGANDO DATOS DE DISPENSACIÓN")
    logger.info("="*70)
    
    # Query optimizado - agregamos por cajero directamente
    query = """
        SELECT 
            cod_terminal,
            bucket_15min,
            tipo_operacion,
            num_transacciones,
            monto_total_dispensado,
            monto_promedio,
            monto_maximo,
            monto_minimo,
            monto_std,
            hora_promedio,
            ultima_transaccion
        FROM mv_dispensacion_por_cajero_15min
        ORDER BY cod_terminal, bucket_15min
    """
    
    logger.info("🔄 Ejecutando query (puede tomar varios minutos)...")
    logger.info("   Vista materializada contiene ~37M registros")
    
    # Leer con chunks para no saturar memoria
    chunks = []
    chunk_size = 1000000  # 1M registros por chunk
    
    for chunk in tqdm(
        pd.read_sql(query, engine, chunksize=chunk_size),
        desc="Cargando datos",
        unit="chunk"
    ):
        chunks.append(chunk)
    
    df = pd.concat(chunks, ignore_index=True)
    
    logger.info(f"✅ Datos cargados: {len(df):,} registros")
    logger.info(f"📊 Cajeros únicos: {df['cod_terminal'].nunique():,}")
    logger.info(f"💾 Memoria usada: {df.memory_usage(deep=True).sum() / (1024**3):.2f} GB")
    logger.info("")
    
    return df

# ============================================================================
# CALCULAR FEATURES POR CAJERO
# ============================================================================

def calcular_features_basicos(df_cajero, logger):
    """Calcula features estadísticos básicos por cajero"""
    
    features = {}
    
    # Estadísticas de monto dispensado
    features['dispensacion_promedio'] = df_cajero['monto_total_dispensado'].mean()
    features['dispensacion_std'] = df_cajero['monto_total_dispensado'].std()
    features['dispensacion_max'] = df_cajero['monto_total_dispensado'].max()
    features['dispensacion_min'] = df_cajero['monto_total_dispensado'].min()
    features['dispensacion_mediana'] = df_cajero['monto_total_dispensado'].median()
    
    # Coeficiente de variación (volatilidad relativa)
    if features['dispensacion_promedio'] > 0:
        features['coef_variacion'] = features['dispensacion_std'] / features['dispensacion_promedio']
    else:
        features['coef_variacion'] = 0
    
    # Número de transacciones
    features['num_periodos_15min'] = len(df_cajero)
    features['transacciones_totales'] = df_cajero['num_transacciones'].sum()
    features['transacciones_promedio_15min'] = df_cajero['num_transacciones'].mean()
    
    # Rangos intercuartiles
    features['q25'] = df_cajero['monto_total_dispensado'].quantile(0.25)
    features['q75'] = df_cajero['monto_total_dispensado'].quantile(0.75)
    features['iqr'] = features['q75'] - features['q25']
    
    return features

def calcular_features_temporales(df_cajero, logger):
    """Calcula features basados en patrones temporales"""
    
    features = {}
    
    # Convertir bucket a datetime
    df_cajero['bucket_dt'] = pd.to_datetime(df_cajero['bucket_15min'])
    df_cajero['hora'] = df_cajero['bucket_dt'].dt.hour
    df_cajero['dia_semana'] = df_cajero['bucket_dt'].dt.dayofweek
    df_cajero['es_fin_semana'] = df_cajero['dia_semana'].isin([5, 6])
    
    # Dispensación por franja horaria
    df_cajero['franja'] = pd.cut(
        df_cajero['hora'],
        bins=[0, 6, 12, 18, 24],
        labels=['madrugada', 'manana', 'tarde', 'noche'],
        right=False
    )
    
    dispensacion_por_franja = df_cajero.groupby('franja', observed=True)['monto_total_dispensado'].mean()
    
    features['disp_madrugada'] = dispensacion_por_franja.get('madrugada', 0)
    features['disp_manana'] = dispensacion_por_franja.get('manana', 0)
    features['disp_tarde'] = dispensacion_por_franja.get('tarde', 0)
    features['disp_noche'] = dispensacion_por_franja.get('noche', 0)
    
    # Hora pico (hora con más dispensación promedio)
    disp_por_hora = df_cajero.groupby('hora')['monto_total_dispensado'].mean()
    if len(disp_por_hora) > 0:
        features['hora_pico'] = disp_por_hora.idxmax()
        features['dispensacion_hora_pico'] = disp_por_hora.max()
    else:
        features['hora_pico'] = 12
        features['dispensacion_hora_pico'] = 0
    
    # Dispensación fin de semana vs laboral
    disp_fds = df_cajero[df_cajero['es_fin_semana']]['monto_total_dispensado'].mean()
    disp_laboral = df_cajero[~df_cajero['es_fin_semana']]['monto_total_dispensado'].mean()
    
    features['disp_fin_semana'] = disp_fds if not pd.isna(disp_fds) else 0
    features['disp_laboral'] = disp_laboral if not pd.isna(disp_laboral) else 0
    
    if features['disp_laboral'] > 0:
        features['ratio_fds_laboral'] = features['disp_fin_semana'] / features['disp_laboral']
    else:
        features['ratio_fds_laboral'] = 1
    
    # Consistencia temporal (¿qué tan regular es?)
    features['std_por_hora'] = df_cajero.groupby('hora')['monto_total_dispensado'].std().mean()
    
    return features

def calcular_features_tendencia(df_cajero, logger):
    """Calcula features de tendencia temporal"""
    
    features = {}
    
    # Ordenar por fecha
    df_sorted = df_cajero.sort_values('bucket_15min').copy()
    df_sorted['periodo'] = range(len(df_sorted))
    
    # Calcular tendencia lineal
    if len(df_sorted) > 5:  # Mínimo 5 puntos para calcular tendencia
        try:
            # Regresión lineal simple
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                df_sorted['periodo'],
                df_sorted['monto_total_dispensado']
            )
            
            features['tendencia_slope'] = slope  # Pendiente (+ = creciente, - = decreciente)
            features['tendencia_r2'] = r_value ** 2  # R² (qué tan fuerte es la tendencia)
            features['tendencia_pvalue'] = p_value  # Significancia estadística
            
        except:
            features['tendencia_slope'] = 0
            features['tendencia_r2'] = 0
            features['tendencia_pvalue'] = 1
    else:
        features['tendencia_slope'] = 0
        features['tendencia_r2'] = 0
        features['tendencia_pvalue'] = 1
    
    # Volatilidad en ventana móvil (últimos 7 días)
    df_sorted['volatilidad_7d'] = df_sorted['monto_total_dispensado'].rolling(
        window=min(7*4*24, len(df_sorted)),  # 7 días * 4 (15min) * 24h
        min_periods=1
    ).std()
    
    features['volatilidad_reciente'] = df_sorted['volatilidad_7d'].iloc[-1] if len(df_sorted) > 0 else 0
    features['volatilidad_promedio'] = df_sorted['volatilidad_7d'].mean()
    
    # Cambio reciente (últimos 7 días vs promedio histórico)
    if len(df_sorted) > 7*4*24:  # Si hay más de 7 días de datos
        ultimos_7d = df_sorted.iloc[-7*4*24:]['monto_total_dispensado'].mean()
        historico = df_sorted.iloc[:-7*4*24]['monto_total_dispensado'].mean()
        
        if historico > 0:
            features['cambio_reciente_pct'] = ((ultimos_7d - historico) / historico) * 100
        else:
            features['cambio_reciente_pct'] = 0
    else:
        features['cambio_reciente_pct'] = 0
    
    return features

def calcular_features_anomalias_historicas(df_cajero, logger):
    """Calcula cuántas veces el cajero ha tenido anomalías históricas"""
    
    features = {}
    
    # Calcular z-scores
    mean = df_cajero['monto_total_dispensado'].mean()
    std = df_cajero['monto_total_dispensado'].std()
    
    if std > 0:
        df_cajero['z_score'] = (df_cajero['monto_total_dispensado'] - mean) / std
        
        # Contar anomalías históricas (|z| > threshold)
        features['anomalias_2std'] = (df_cajero['z_score'].abs() > 2).sum()
        features['anomalias_3std'] = (df_cajero['z_score'].abs() > 3).sum()
        features['anomalias_4std'] = (df_cajero['z_score'].abs() > 4).sum()
        
        # Porcentaje de anomalías
        total_periodos = len(df_cajero)
        features['pct_anomalias_2std'] = (features['anomalias_2std'] / total_periodos) * 100
        features['pct_anomalias_3std'] = (features['anomalias_3std'] / total_periodos) * 100
        
        # Máximo z-score histórico
        features['max_z_score_historico'] = df_cajero['z_score'].abs().max()
    else:
        features['anomalias_2std'] = 0
        features['anomalias_3std'] = 0
        features['anomalias_4std'] = 0
        features['pct_anomalias_2std'] = 0
        features['pct_anomalias_3std'] = 0
        features['max_z_score_historico'] = 0
    
    return features

def calcular_features_por_cajero(df, logger):
    """Calcula todos los features para cada cajero"""
    
    logger.info("="*70)
    logger.info("🧮 CALCULANDO FEATURES POR CAJERO")
    logger.info("="*70)
    
    cajeros_unicos = df['cod_terminal'].unique()
    logger.info(f"📊 Total de cajeros a procesar: {len(cajeros_unicos):,}")
    logger.info("")
    
    features_list = []
    
    for cajero in tqdm(cajeros_unicos, desc="Calculando features", unit="cajero"):
        df_cajero = df[df['cod_terminal'] == cajero].copy()
        
        # Combinar todos los features
        features = {'cod_cajero': str(cajero)}
        
        # Features básicos
        features.update(calcular_features_basicos(df_cajero, logger))
        
        # Features temporales
        features.update(calcular_features_temporales(df_cajero, logger))
        
        # Features de tendencia
        features.update(calcular_features_tendencia(df_cajero, logger))
        
        # Features de anomalías históricas
        features.update(calcular_features_anomalias_historicas(df_cajero, logger))
        
        # Metadata
        features['fecha_calculo'] = datetime.now()
        features['fecha_primer_dato'] = df_cajero['bucket_15min'].min()
        features['fecha_ultimo_dato'] = df_cajero['bucket_15min'].max()
        
        features_list.append(features)
    
    df_features = pd.DataFrame(features_list)
    
    logger.info("")
    logger.info(f"✅ Features calculados para {len(df_features):,} cajeros")
    logger.info(f"📊 Total de features por cajero: {len(df_features.columns) - 1}")
    logger.info("")
    
    return df_features

# ============================================================================
# AGREGAR FEATURES GEOGRÁFICOS
# ============================================================================

# def agregar_features_geograficos(df_features, engine, logger):
#     """Agrega features basados en ubicación geográfica"""
    
#     logger.info("="*70)
#     logger.info("🗺️  AGREGANDO FEATURES GEOGRÁFICOS")
#     logger.info("="*70)
    
#     # Cargar datos de cajeros con ubicación
#     query = """
#         SELECT 
#             codigo,
#             latitud,
#             longitud,
#             municipio_dane,
#             departamento
#         FROM cajeros
#         WHERE latitud IS NOT NULL 
#         AND longitud IS NOT NULL
#     """
    
#     logger.info("📍 Cargando ubicaciones de cajeros...")
#     df_ubicaciones = pd.read_sql(query, engine)
    
#     # Convertimos 'codigo' a string para que coincida con 'cod_cajero' de df_features
#     df_ubicaciones['codigo'] = df_ubicaciones['codigo'].astype(str)
    
#     logger.info(f"   Cajeros con ubicación: {len(df_ubicaciones):,}")
    
#     # Merge con features
#     df_features = df_features.merge(
#         df_ubicaciones,
#         left_on='cod_cajero',
#         right_on='codigo',
#         how='left'
#     )
    
#     # Para cada cajero, encontrar cajeros cercanos (radio 1km)
#     logger.info("🔍 Calculando cajeros cercanos (puede tardar)...")
    
#     def calcular_distancia_haversine(lat1, lon1, lat2, lon2):
#         """Calcula distancia en km usando fórmula de Haversine"""
#         from math import radians, sin, cos, sqrt, atan2
        
#         R = 6371  # Radio de la Tierra en km
        
#         lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
#         dlat = lat2 - lat1
#         dlon = lon2 - lon1
        
#         a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
#         c = 2 * atan2(sqrt(a), sqrt(1-a))
        
#         return R * c
    
#     # Calcular features de proximidad
#     cajeros_cercanos = []
#     dispensacion_zona = []
    
#     # Optimizamos un poco el loop convirtiendo a diccionarios/arrays para velocidad
#     df_ubic_dict = df_ubicaciones.to_dict('records')
    
#     for idx, row in tqdm(df_features.iterrows(), total=len(df_features), desc="Analizando proximidad"):
#         if pd.notna(row['latitud']) and pd.notna(row['longitud']):
#             # Contar cajeros en radio de 1km
#             count = 0
#             disp_promedio_zona = []
            
#             # Usamos la lista de diccionarios que es ligeramente más rápido que iterrows anidado
#             for row2 in df_ubic_dict:
#                 # Comparamos strings (ya que convertimos arriba)
#                 if row['cod_cajero'] != row2['codigo']: 
#                     dist = calcular_distancia_haversine(
#                         row['latitud'], row['longitud'],
#                         row2['latitud'], row2['longitud']
#                     )
                    
#                     if dist <= 1:  # 1 km
#                         count += 1
#                         # Buscar dispensación promedio de ese cajero
#                         # Nota: Esto es lento, una optimización futura sería pre-calcular un mapa {cod: disp}
#                         disp = df_features[df_features['cod_cajero'] == str(row2['codigo'])]['dispensacion_promedio']
#                         if len(disp) > 0:
#                             disp_promedio_zona.append(disp.iloc[0])
            
#             cajeros_cercanos.append(count)
#             dispensacion_zona.append(np.mean(disp_promedio_zona) if disp_promedio_zona else row['dispensacion_promedio'])
#         else:
#             cajeros_cercanos.append(0)
#             dispensacion_zona.append(row['dispensacion_promedio'])
    
#     df_features['cajeros_cercanos_1km'] = cajeros_cercanos
#     df_features['dispensacion_promedio_zona'] = dispensacion_zona
    
#     # Ratio vs zona
#     df_features['ratio_vs_zona'] = df_features['dispensacion_promedio'] / df_features['dispensacion_promedio_zona']
#     df_features['ratio_vs_zona'] = df_features['ratio_vs_zona'].fillna(1)
    
#     logger.info(f"✅ Features geográficos agregados")
#     logger.info(f"   Cajeros con ubicación: {df_features['latitud'].notna().sum():,}")
#     logger.info("")
    
#     return df_features

# Todos: Versión optimizada para mejorar rendimiento

def agregar_features_geograficos(df_features, engine, logger):
    """Agrega features basados en ubicación geográfica (Optimizada con NumPy Puro)"""
    
    logger.info("="*70)
    logger.info("🗺️  AGREGANDO FEATURES GEOGRÁFICOS (NUMPY)")
    logger.info("="*70)
    
    # Cargar datos de cajeros con ubicación
    query = """
        SELECT 
            codigo,
            latitud,
            longitud,
            municipio_dane,
            departamento
        FROM cajeros
        WHERE latitud IS NOT NULL 
        AND longitud IS NOT NULL
    """
    
    logger.info("📍 Cargando ubicaciones de cajeros...")
    df_ubicaciones = pd.read_sql(query, engine)
    
    df_ubicaciones['codigo'] = df_ubicaciones['codigo'].astype(str)
    df_ubicaciones['latitud'] = pd.to_numeric(df_ubicaciones['latitud'], errors='coerce')
    df_ubicaciones['longitud'] = pd.to_numeric(df_ubicaciones['longitud'], errors='coerce')
    
    logger.info(f"   Cajeros con ubicación: {len(df_ubicaciones):,}")
    
    # Merge con features
    df_features = df_features.merge(
        df_ubicaciones,
        left_on='cod_cajero',
        right_on='codigo',
        how='left'
    )
    
    logger.info("🔍 Calculando matriz de distancias y métricas zonales...")

    # 1. Filtrar válidos
    mask_validos = df_features['latitud'].notna() & df_features['longitud'].notna()
    df_validos = df_features[mask_validos].copy()
    
    if len(df_validos) > 0:
        try:
            # 2. Coordenadas en Radianes
            # Shape: (N, 2) -> columna 0: lat, columna 1: lon
            coords = np.radians(df_validos[['latitud', 'longitud']].astype(float).values)
            
            lat = coords[:, 0]
            lon = coords[:, 1]
            
            # 3. Calcular Matriz de Distancias (Haversine Vectorizado)
            # Usamos broadcasting de NumPy para generar la matriz (N, N)
            # lat[:, np.newaxis] es un vector columna, lat[np.newaxis, :] es un vector fila
            dlat = lat[:, np.newaxis] - lat[np.newaxis, :]
            dlon = lon[:, np.newaxis] - lon[np.newaxis, :]
            
            # Fórmula: a = sin²(Δlat/2) + cos(lat1) * cos(lat2) * sin²(Δlon/2)
            a = np.sin(dlat / 2)**2 + np.cos(lat[:, np.newaxis]) * np.cos(lat[np.newaxis, :]) * np.sin(dlon / 2)**2
            
            # Fórmula: c = 2 * arcsin(√a)
            # Usamos np.clip para evitar errores numéricos menores que 0 o mayores que 1
            c = 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
            
            # Distancia en km
            dist_matrix = c * 6371
            
            # 4. Procesar resultados
            dispensaciones = df_validos['dispensacion_promedio'].values
            cajeros_cercanos_list = []
            disp_zona_list = []
            
            for i in tqdm(range(len(df_validos)), desc="Procesando matriz"):
                distancias_i = dist_matrix[i]
                
                # Máscara: cercanos (<= 1km) Y excluirse a sí mismo (> 0)
                # Nota: > 0.001 evita el propio cajero (distancia 0)
                mask_cercanos = (distancias_i <= 1.0) & (distancias_i > 0.001)
                
                count = mask_cercanos.sum()
                cajeros_cercanos_list.append(count)
                
                if count > 0:
                    avg_zona = dispensaciones[mask_cercanos].mean()
                    disp_zona_list.append(avg_zona)
                else:
                    disp_zona_list.append(dispensaciones[i])
            
            # 5. Asignar resultados
            df_features.loc[mask_validos, 'cajeros_cercanos_1km'] = cajeros_cercanos_list
            df_features.loc[mask_validos, 'dispensacion_promedio_zona'] = disp_zona_list
            
        except Exception as e:
            logger.error(f"❌ Error matemático al procesar: {e}")
            df_features['cajeros_cercanos_1km'] = 0
            df_features['dispensacion_promedio_zona'] = df_features['dispensacion_promedio']
    else:
        logger.warning("⚠️ No hay cajeros con coordenadas válidas.")
        
    # Llenar nulos
    df_features['cajeros_cercanos_1km'] = df_features['cajeros_cercanos_1km'].fillna(0).astype(int)
    df_features['dispensacion_promedio_zona'] = df_features['dispensacion_promedio_zona'].fillna(df_features['dispensacion_promedio'])
    
    # --- FIN OPTIMIZACIÓN ---
    
    # Ratio vs zona
    df_features['ratio_vs_zona'] = df_features.apply(
        lambda x: x['dispensacion_promedio'] / x['dispensacion_promedio_zona'] 
                  if x['dispensacion_promedio_zona'] > 0 else 1, 
        axis=1
    )
    
    logger.info(f"✅ Features geográficos agregados")
    logger.info(f"   Cajeros procesados: {mask_validos.sum():,}")
    logger.info("")
    
    return df_features

# ============================================================================
# GUARDAR FEATURES
# ============================================================================

def guardar_features(df_features, engine, logger):
    """Guarda features en PostgreSQL"""
    
    logger.info("="*70)
    logger.info("💾 GUARDANDO FEATURES EN POSTGRESQL")
    logger.info("="*70)
    
    # Crear tabla si no existe
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS features_ml (
            cod_cajero VARCHAR(20) PRIMARY KEY,
            
            -- Estadísticas básicas
            dispensacion_promedio DECIMAL(15, 2),
            dispensacion_std DECIMAL(15, 2),
            dispensacion_max DECIMAL(15, 2),
            dispensacion_min DECIMAL(15, 2),
            dispensacion_mediana DECIMAL(15, 2),
            coef_variacion DECIMAL(10, 4),
            
            -- Transacciones
            num_periodos_15min INTEGER,
            transacciones_totales INTEGER,
            transacciones_promedio_15min DECIMAL(10, 2),
            
            -- Cuartiles
            q25 DECIMAL(15, 2),
            q75 DECIMAL(15, 2),
            iqr DECIMAL(15, 2),
            
            -- Temporales
            disp_madrugada DECIMAL(15, 2),
            disp_manana DECIMAL(15, 2),
            disp_tarde DECIMAL(15, 2),
            disp_noche DECIMAL(15, 2),
            hora_pico INTEGER,
            dispensacion_hora_pico DECIMAL(15, 2),
            disp_fin_semana DECIMAL(15, 2),
            disp_laboral DECIMAL(15, 2),
            ratio_fds_laboral DECIMAL(10, 4),
            std_por_hora DECIMAL(15, 2),
            
            -- Tendencias
            tendencia_slope DECIMAL(15, 6),
            tendencia_r2 DECIMAL(10, 6),
            tendencia_pvalue DECIMAL(10, 6),
            volatilidad_reciente DECIMAL(15, 2),
            volatilidad_promedio DECIMAL(15, 2),
            cambio_reciente_pct DECIMAL(10, 2),
            
            -- Anomalías históricas
            anomalias_2std INTEGER,
            anomalias_3std INTEGER,
            anomalias_4std INTEGER,
            pct_anomalias_2std DECIMAL(10, 4),
            pct_anomalias_3std DECIMAL(10, 4),
            max_z_score_historico DECIMAL(10, 4),
            
            -- Geográficos
            latitud DECIMAL(10, 6),
            longitud DECIMAL(10, 6),
            municipio_dane VARCHAR(100),
            departamento VARCHAR(100),
            cajeros_cercanos_1km INTEGER,
            dispensacion_promedio_zona DECIMAL(15, 2),
            ratio_vs_zona DECIMAL(10, 4),
            
            -- Metadata
            fecha_calculo TIMESTAMP,
            fecha_primer_dato TIMESTAMP,
            fecha_ultimo_dato TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_features_ml_dispensacion 
            ON features_ml(dispensacion_promedio);
    """
    
    logger.info("🏗️  Creando tabla features_ml...")
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    
    # Limpiar tabla existente
    logger.info("🧹 Limpiando datos anteriores...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE features_ml;"))
    
    # Insertar datos
    logger.info(f"💾 Insertando {len(df_features):,} registros...")
    
    # Seleccionar solo las columnas que existen en la tabla
    columnas_tabla = [
        'cod_cajero', 'dispensacion_promedio', 'dispensacion_std', 'dispensacion_max',
        'dispensacion_min', 'dispensacion_mediana', 'coef_variacion',
        'num_periodos_15min', 'transacciones_totales', 'transacciones_promedio_15min',
        'q25', 'q75', 'iqr', 'disp_madrugada', 'disp_manana', 'disp_tarde', 'disp_noche',
        'hora_pico', 'dispensacion_hora_pico', 'disp_fin_semana', 'disp_laboral',
        'ratio_fds_laboral', 'std_por_hora', 'tendencia_slope', 'tendencia_r2',
        'tendencia_pvalue', 'volatilidad_reciente', 'volatilidad_promedio',
        'cambio_reciente_pct', 'anomalias_2std', 'anomalias_3std', 'anomalias_4std',
        'pct_anomalias_2std', 'pct_anomalias_3std', 'max_z_score_historico',
        'latitud', 'longitud', 'municipio_dane', 'departamento', 'cajeros_cercanos_1km',
        'dispensacion_promedio_zona', 'ratio_vs_zona', 'fecha_calculo',
        'fecha_primer_dato', 'fecha_ultimo_dato'
    ]
    
    df_to_save = df_features[columnas_tabla].copy()
    
    # Convertir tipos numéricos para evitar problemas
    for col in df_to_save.columns:
        if df_to_save[col].dtype == 'float64':
            df_to_save[col] = df_to_save[col].astype('float32')
    
    df_to_save.to_sql(
        'features_ml',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    logger.info("✅ Features guardados exitosamente")
    logger.info("")
    
    # Verificar
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM features_ml"))
        count = result.scalar()
        logger.info(f"🔍 Verificación: {count:,} registros en features_ml")

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal"""
    
    parser = argparse.ArgumentParser(
        description='Calcular features de dispensación para ML'
    )
    parser.add_argument(
        '--config', 
        type=str, 
        default='config.yaml',
        help='Ruta al archivo de configuración YAML'
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
    log_path = os.path.join(paths['logs'], 'calcular_features_dispensacion.log')
    logger = setup_logging(log_path, config['logging']['level'])
    
    logger.info("="*70)
    logger.info("🚀 CÁLCULO DE FEATURES DE DISPENSACIÓN")
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
    
    # 1. Cargar datos
    df = cargar_datos_dispensacion(engine, logger)
    
    # 2. Calcular features por cajero
    df_features = calcular_features_por_cajero(df, logger)
    
    # 3. Agregar features geográficos
    df_features = agregar_features_geograficos(df_features, engine, logger)
    
    # 4. Guardar features
    guardar_features(df_features, engine, logger)
    
    # Finalizar
    logger.info("="*70)
    logger.info("🎉 CÁLCULO DE FEATURES COMPLETADO")
    logger.info("="*70)
    logger.info("")
    logger.info("📋 Próximo paso:")
    logger.info("   python entrenar_modelo_dispensacion.py")
    logger.info("")

if __name__ == "__main__":
    main()