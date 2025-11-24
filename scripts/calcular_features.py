#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
CÁLCULO DE FEATURES PARA MODELOS ML - Sistema Detección de Fraudes
============================================================================

Calcula features de Machine Learning desde la tabla transacciones
y las guarda en la tabla features de PostgreSQL.

Features calculadas (20+):
- Temporales: hora, día_semana, es_fin_de_semana, etc.
- Transaccionales: diferencia_valor, es_retiro_maximo, etc.
- Agregadas por cajero: tx_por_hora, monto_promedio, etc.
- Velocidad: tiempo_desde_anterior, es_transaccion_rapida

Uso:
    python calcular_features.py --config config.yaml

Autor: Sistema de Detección de Fraudes
============================================================================
"""

import pandas as pd
import yaml
import argparse
import logging
import sys
import os
import io
from datetime import datetime
from tqdm import tqdm
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
# CÁLCULO DE FEATURES
# ============================================================================

def calcular_features_temporales(df, logger):
    """Calcula features basadas en fecha/hora"""
    
    logger.info("🕐 Calculando features temporales...")
    
    # Convertir a datetime si no lo es
    if not pd.api.types.is_datetime64_any_dtype(df['fecha_transaccion']):
        df['fecha_transaccion'] = pd.to_datetime(df['fecha_transaccion'])
    
    # Extraer componentes temporales
    df['hora'] = df['fecha_transaccion'].dt.hour
    df['dia_semana'] = df['fecha_transaccion'].dt.dayofweek  # 0=Lunes, 6=Domingo
    df['es_fin_de_semana'] = df['dia_semana'].isin([5, 6])
    df['es_horario_nocturno'] = df['hora'].between(22, 23) | df['hora'].between(0, 6)
    df['es_madrugada'] = df['hora'].between(0, 6)
    
    logger.info("   ✅ Features temporales calculadas")
    return df

def calcular_features_transaccionales(df, logger):
    """Calcula features de la transacción individual"""
    
    logger.info("💰 Calculando features transaccionales...")
    
    # Diferencia entre valor transacción y original
    df['diferencia_valor'] = df['valor_transaccion'] - df['valor_transaccion_original']
    
    # Identificar retiros máximos (>= $2,000,000)
    df['es_retiro_maximo'] = (df['valor_transaccion'] >= 2000000) & \
                             (df['tipo_operacion'].isin(['Retiro', 'Avance']))
    
    # Tipo de operación codificado
    tipo_op_map = {
        'Cambio De Pin': 1,
        'Avance': 2,
        'Retiro': 3,
        'Depositos': 4,
        'Transferencias': 5
    }
    df['tipo_operacion_encoded'] = df['tipo_operacion'].map(tipo_op_map)
    
    # Estados de transacción
    df['transaccion_exitosa'] = (df['cod_estado_transaccion'] == 1)
    df['transaccion_rechazada'] = (df['cod_estado_transaccion'] == 2)
    
    # Cambio de PIN
    df['es_cambio_pin'] = (df['tipo_operacion'] == 'Cambio De Pin')
    
    logger.info("   ✅ Features transaccionales calculadas")
    return df

def calcular_features_cajero(df, logger):
    """Calcula features agregadas por cajero"""
    
    logger.info("🏧 Calculando features por cajero...")
    
    # Ordenar por cajero y fecha
    df = df.sort_values(['cod_terminal', 'fecha_transaccion'])
    
    # Calcular tiempo desde transacción anterior (por cajero)
    df['tiempo_desde_anterior_seg'] = df.groupby('cod_terminal')['fecha_transaccion'].diff().dt.total_seconds()
    
    # Transacciones rápidas (< 10 segundos)
    df['es_transaccion_rapida'] = (df['tiempo_desde_anterior_seg'] < 10) & \
                                   (df['tiempo_desde_anterior_seg'].notna())
    
    # Estadísticas por cajero (ventana móvil de 24 horas)
    logger.info("   📊 Calculando estadísticas agregadas por cajero...")
    
    # Agrupar por cajero y calcular métricas
    cajero_stats = df.groupby('cod_terminal').agg({
        'id_tlf': 'count',  # Total de transacciones
        'valor_transaccion': ['mean', 'std'],
        'transaccion_rechazada': 'mean',
        'tiempo_desde_anterior_seg': 'mean'
    }).reset_index()
    
    cajero_stats.columns = [
        'cod_terminal',
        'tx_total_cajero',
        'monto_promedio_cajero',
        'monto_std_cajero',
        'tasa_rechazo_cajero',
        'velocidad_promedio_cajero'
    ]
    
    # Merge con DataFrame principal
    df = df.merge(cajero_stats, on='cod_terminal', how='left')
    
    # Calcular desviación del monto respecto al promedio del cajero
    df['desviacion_monto_cajero'] = (df['valor_transaccion'] - df['monto_promedio_cajero']) / \
                                     (df['monto_std_cajero'] + 1)  # +1 para evitar división por 0
    
    # Transacciones por hora en este cajero (aproximado)
    df['tx_por_hora_cajero'] = df['tx_total_cajero'] / (24 * 30)  # Asumiendo 30 días
    
    logger.info("   ✅ Features por cajero calculadas")
    return df

def calcular_features_metadata_cajero(df, engine, logger):
    """Agrega features desde la tabla cajeros (metadata)"""
    
    logger.info("📍 Agregando metadata de cajeros...")
    
    try:
        # Leer tabla de cajeros
        query = "SELECT codigo, cajero_adyacente_oficina, cierre_nocturno FROM cajeros"
        df_cajeros = pd.read_sql(query, engine)
        
        if len(df_cajeros) == 0:
            logger.warning("   ⚠️  Tabla cajeros está vacía, usando valores por defecto")
            df['cajero_adyacente_encoded'] = 0
            df['cierre_nocturno_encoded'] = 0
            return df
        
        # Convertir cod_terminal a string para hacer merge
        df['cod_terminal_str'] = df['cod_terminal'].astype(str)
        df_cajeros['codigo'] = df_cajeros['codigo'].astype(str)
        
        # Merge
        df = df.merge(
            df_cajeros,
            left_on='cod_terminal_str',
            right_on='codigo',
            how='left'
        )
        
        # Encodear booleanos
        df['cajero_adyacente_encoded'] = df['cajero_adyacente_oficina'].fillna(False).astype(int)
        df['cierre_nocturno_encoded'] = df['cierre_nocturno'].fillna(False).astype(int)
        
        # Limpiar columnas temporales
        df = df.drop(columns=['codigo', 'cajero_adyacente_oficina', 'cierre_nocturno', 'cod_terminal_str'])
        
        logger.info("   ✅ Metadata de cajeros agregada")
        
    except Exception as e:
        logger.warning(f"   ⚠️  No se pudo cargar metadata de cajeros: {e}")
        logger.warning("   Usando valores por defecto")
        df['cajero_adyacente_encoded'] = 0
        df['cierre_nocturno_encoded'] = 0
    
    return df

def guardar_features(df, engine, batch_size, logger):
    """Guarda features calculadas en PostgreSQL usando COPY en chunks"""
    
    logger.info("="*70)
    logger.info("💾 GUARDANDO FEATURES EN POSTGRESQL (OPTIMIZADO - CHUNKS)")
    logger.info("="*70)
    
    logger.info(f"Columnas disponibles antes de renombrar: {df.columns}")

    # Normalizar ID
    if 'id_tlf' in df.columns:
        logger.info("🔄 Renombrando id_tlf → id_transaccion")
        df.rename(columns={'id_tlf': 'id_transaccion'}, inplace=True)
    elif 'id_transaccion' not in df.columns:
        logger.error("❌ ERROR: No existe columna id_tlf ni id_transaccion en el DF final")
        logger.error(f"Columnas disponibles: {df.columns.tolist()}")
        return False
    
    # Seleccionar solo columnas de features
    feature_columns = [
        'id_transaccion',
        'hora',
        'dia_semana',
        'es_fin_de_semana',
        'es_horario_nocturno',
        'es_madrugada',
        'diferencia_valor',
        'es_retiro_maximo',
        'tiempo_desde_anterior_seg',
        'es_transaccion_rapida',
        'es_cambio_pin',
        'tipo_operacion_encoded',
        'transaccion_exitosa',
        'transaccion_rechazada',
        'tx_por_hora_cajero',
        'monto_promedio_cajero',
        'tasa_rechazo_cajero',
        'desviacion_monto_cajero',
        'velocidad_promedio_cajero',
        'cajero_adyacente_encoded',
        'cierre_nocturno_encoded'
    ]
    
    # Verificar que todas las columnas existan
    columnas_disponibles = [col for col in feature_columns if col in df.columns]
    df_features = df[columnas_disponibles].copy()
    
    # Convertir tipos de datos para match con PostgreSQL
    logger.info("🔧 Convirtiendo tipos de datos...")
    
    # Convertir columnas INTEGER (PostgreSQL no acepta 3.0, debe ser 3)
    int_columns = ['hora', 'dia_semana', 'tipo_operacion_encoded', 
                   'cajero_adyacente_encoded', 'cierre_nocturno_encoded']
    for col in int_columns:
        if col in df_features.columns:
            df_features[col] = df_features[col].fillna(0).astype(int)
    
    # Convertir columnas BIGINT
    if 'id_transaccion' in df_features.columns:
        df_features['id_transaccion'] = df_features['id_transaccion'].astype('int64')
    
    # Convertir columnas REAL (float32)
    float_columns = ['diferencia_valor', 'tiempo_desde_anterior_seg', 
                     'tx_por_hora_cajero', 'monto_promedio_cajero',
                     'tasa_rechazo_cajero', 'desviacion_monto_cajero', 
                     'velocidad_promedio_cajero']
    for col in float_columns:
        if col in df_features.columns:
            df_features[col] = df_features[col].astype('float32')
    
    # Convertir columnas BOOLEAN
    bool_columns = ['es_fin_de_semana', 'es_horario_nocturno', 'es_madrugada',
                    'es_retiro_maximo', 'es_transaccion_rapida', 'es_cambio_pin',
                    'transaccion_exitosa', 'transaccion_rechazada']
    for col in bool_columns:
        if col in df_features.columns:
            df_features[col] = df_features[col].fillna(False).astype(bool)
    
    logger.info(f"📋 Features a guardar: {len(columnas_disponibles)}")
    logger.info(f"📊 Registros totales: {len(df_features):,}")
    
    # Limpiar tabla existente
    logger.info("🧹 Limpiando tabla features existente...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE features;"))
        conn.commit()
    
    # Guardar usando COPY en chunks
    logger.info("💾 Insertando datos usando COPY en chunks...")
    
    chunk_size = 5_000_000  # 5M registros por chunk
    total_registros = len(df_features)
    num_chunks = (total_registros // chunk_size) + 1
    
    logger.info(f"📦 Procesando en {num_chunks} chunks de {chunk_size:,} registros")
    
    connection = engine.raw_connection()
    cursor = connection.cursor()
    
    try:
        registros_insertados = 0
        
        for i in tqdm(range(0, total_registros, chunk_size), desc="Guardando features"):
            chunk = df_features.iloc[i:i+chunk_size]
            
            # Crear buffer para este chunk
            output = io.StringIO()
            chunk.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
            output.seek(0)
            
            # COPY este chunk
            cursor.copy_from(
                output,
                'features',
                sep='\t',
                null='\\N',
                columns=columnas_disponibles
            )
            
            # Commit cada chunk
            connection.commit()
            
            registros_insertados += len(chunk)
            logger.info(f"   ✅ Chunk {i//chunk_size + 1}/{num_chunks}: {registros_insertados:,}/{total_registros:,}")
        
        logger.info(f"\n✅ Features guardadas: {registros_insertados:,}")
        return True
        
    except Exception as e:
        connection.rollback()
        logger.error(f"❌ Error al insertar: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        cursor.close()
        connection.close()

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal"""
    
    # Parsear argumentos
    parser = argparse.ArgumentParser(
        description='Calcular features de ML desde transacciones'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='../config.yaml',
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
    log_path = os.path.join(paths['logs'], 'features.log')
    logger = setup_logging(log_path, config['logging']['level'])
    
    logger.info("="*70)
    logger.info("🚀 INICIANDO CÁLCULO DE FEATURES")
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
    try:
        engine = create_engine(connection_string, poolclass=NullPool)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexión exitosa\n")
    except Exception as e:
        logger.error(f"❌ Error de conexión: {e}")
        sys.exit(1)
    
    # Leer transacciones desde PostgreSQL
    logger.info("="*70)
    logger.info("📖 LEYENDO TRANSACCIONES DESDE POSTGRESQL")
    logger.info("="*70)
    
    query = """
        SELECT 
            id_tlf,
            fecha_transaccion,
            cod_terminal,
            tipo_operacion,
            cod_estado_transaccion,
            valor_transaccion,
            valor_transaccion_original
        FROM transacciones
        ORDER BY cod_terminal, fecha_transaccion
    """
    
    logger.info("Ejecutando query...")
    df = pd.read_sql(query, engine)
    logger.info(f"✅ Transacciones cargadas: {len(df):,}\n")
    
    # Calcular features
    df = calcular_features_temporales(df, logger)
    df = calcular_features_transaccionales(df, logger)
    df = calcular_features_cajero(df, logger)
    df = calcular_features_metadata_cajero(df, engine, logger)
    
    # Guardar en PostgreSQL
    guardar_features(
        df,
        engine,
        postgres_config['batch_size'],
        logger
    )
    
    # Verificar
    logger.info("="*70)
    logger.info("🔍 VERIFICANDO FEATURES")
    logger.info("="*70)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM features"))
        count = result.scalar()
        logger.info(f"✅ Total de features en BD: {count:,}")
    
    logger.info("")
    logger.info("="*70)
    logger.info("🎉 CÁLCULO DE FEATURES COMPLETADO")
    logger.info("="*70)

if __name__ == "__main__":
    main()