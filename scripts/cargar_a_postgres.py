#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
CARGA DE DATOS A POSTGRESQL + TIMESCALEDB
============================================================================

Script para cargar datos desde Parquet a PostgreSQL con TimescaleDB.
Solo carga los últimos N meses (configurables) para el frontend.

Funcionalidades:
1. Crea las tablas necesarias con TimescaleDB
2. Carga últimos N meses desde Parquet
3. Crea índices optimizados
4. Configura compresión automática

Uso:
    python cargar_a_postgres.py
    
    # O con configuración custom:
    python cargar_a_postgres.py --config /ruta/config.yaml

Requisitos previos:
    - PostgreSQL instalado
    - TimescaleDB instalado
    - Base de datos creada
    - Usuario con permisos

Autor: Sistema de Detección de Fraudes
Fecha: 2025-11-06
============================================================================
"""

import pandas as pd
import yaml
import argparse
import logging
import sys
import os
import unicodedata
from datetime import datetime, timedelta
from tqdm import tqdm
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# ============================================================================
# CONFIGURACIÓN DE LOGGING
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
# CREACIÓN DE TABLAS
# ============================================================================

def crear_tablas(engine, chunk_interval, logger):
    """
    Crea las tablas necesarias con TimescaleDB
    
    Args:
        engine: SQLAlchemy engine
        chunk_interval: Intervalo de chunks para TimescaleDB
        logger: Logger
    """
    
    logger.info("="*70)
    logger.info("🏗️  CREANDO ESTRUCTURA DE BASE DE DATOS")
    logger.info("="*70)
    
    # SQL para crear extensión TimescaleDB
    sql_extension = """
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    """
    
    # SQL para tabla de cajeros (metadata de ATMs)
    sql_cajeros = """
    CREATE TABLE IF NOT EXISTS cajeros (
        codigo VARCHAR(20) PRIMARY KEY,
        longitud DECIMAL(10, 6),
        latitud DECIMAL(10, 6),
        municipio VARCHAR(100),
        departamento VARCHAR(100),
        tipo_funcion VARCHAR(50),
        horario_atencion_oficina VARCHAR(50),
        hora_apertura_atm TIME,
        hora_cierre_atm TIME,
        cajero_adyacente_oficina BOOLEAN,
        estado_cajero VARCHAR(50),
        fecha_instalacion DATE,
        centro_comercial BOOLEAN,
        grandes_superficies BOOLEAN,
        regional VARCHAR(100),
        nivel_polucion VARCHAR(50),
        temperatura_promedio DECIMAL(5, 2),
        tipo_site VARCHAR(50),
        vip BOOLEAN,
        uniplaza BOOLEAN,
        cierre_nocturno BOOLEAN,
        capacidad_max_aprovisionamiento DECIMAL(15, 2),
        marca VARCHAR(50),
        modelo VARCHAR(50),
        tecnico_servicio VARCHAR(100),
        mas_cajeros_mismo_site BOOLEAN
    );
    
    -- Índices geográficos y de búsqueda
    CREATE INDEX IF NOT EXISTS idx_cajeros_ubicacion ON cajeros(departamento, municipio);
    CREATE INDEX IF NOT EXISTS idx_cajeros_tipo ON cajeros(tipo_funcion);
    CREATE INDEX IF NOT EXISTS idx_cajeros_estado ON cajeros(estado_cajero);
    """
    
    # SQL para tabla de transacciones (hypertable principal)
    sql_transacciones = """
    CREATE TABLE IF NOT EXISTS transacciones (
        id_tlf BIGINT,
        fecha_transaccion TIMESTAMP NOT NULL,
        fecha_transaccion_15min TIMESTAMP NOT NULL,
        cod_terminal INTEGER,
        autorizador TEXT,
        tipo_operacion TEXT,
        cod_estado_transaccion INTEGER,
        cod_tipo_operacion INTEGER,
        operacion TEXT,
        canal TEXT,
        tipo_convenio TEXT,
        adquiriente TEXT,
        valor_transaccion REAL,
        valor_transaccion_original REAL,
        cantidad_tx INTEGER,
        duplicado INTEGER,
        fecha_negocio DATE,
        archivo_origen TEXT,
        mes_origen TEXT,
        fecha_procesamiento TIMESTAMP,
        PRIMARY KEY (id_tlf, fecha_transaccion)
    );
    
    -- Convertir a hypertable si no lo es
    SELECT create_hypertable('transacciones', 'fecha_transaccion',
                            chunk_time_interval => INTERVAL '""" + chunk_interval + """',
                            if_not_exists => TRUE);
    
    -- Crear índice adicional por bucket de 15 minutos
    CREATE INDEX IF NOT EXISTS idx_transacciones_15min 
        ON transacciones(fecha_transaccion_15min, cod_terminal);
    """
    
    # SQL para tabla de features
    sql_features = """
    CREATE TABLE IF NOT EXISTS features (
        id_transaccion BIGINT PRIMARY KEY,
        hora INTEGER,
        dia_semana INTEGER,
        es_fin_de_semana BOOLEAN,
        es_horario_nocturno BOOLEAN,
        es_madrugada BOOLEAN,
        diferencia_valor REAL,
        es_retiro_maximo BOOLEAN,
        tiempo_desde_anterior_seg REAL,
        es_transaccion_rapida BOOLEAN,
        es_cambio_pin BOOLEAN,
        tipo_operacion_encoded INTEGER,
        transaccion_exitosa BOOLEAN,
        transaccion_rechazada BOOLEAN,
        tx_por_hora_cajero REAL,
        monto_promedio_cajero REAL,
        tasa_rechazo_cajero REAL,
        desviacion_monto_cajero REAL,
        velocidad_promedio_cajero REAL,
        cajero_adyacente_encoded INTEGER,
        cierre_nocturno_encoded INTEGER
    );
    """
    
    # SQL para tabla de scores
    sql_scores = """
    CREATE TABLE IF NOT EXISTS scores (
        id_transaccion BIGINT PRIMARY KEY,
        score_reglas REAL,
        score_isolation_forest REAL,
        score_supervised REAL,
        score_final REAL,
        nivel_anomalia VARCHAR(20),
        fecha_scoring TIMESTAMP DEFAULT NOW()
    );
    """
    
    # SQL para razones de anomalías
    sql_razones = """
    CREATE TABLE IF NOT EXISTS razones_anomalias (
        id SERIAL PRIMARY KEY,
        id_transaccion BIGINT,
        tipo_razon VARCHAR(50),
        detalle TEXT,
        severidad VARCHAR(20),
        FOREIGN KEY (id_transaccion) REFERENCES scores(id_transaccion)
    );
    """
    
    # SQL para feedback (Fase 2)
    sql_feedback = """
    CREATE TABLE IF NOT EXISTS feedback (
        id SERIAL PRIMARY KEY,
        id_transaccion BIGINT,
        es_fraude_real BOOLEAN,
        validado_por VARCHAR(100),
        fecha_validacion TIMESTAMP DEFAULT NOW(),
        comentarios TEXT
    );
    """
    
    # SQL para modelos
    sql_modelos = """
    CREATE TABLE IF NOT EXISTS modelos (
        id SERIAL PRIMARY KEY,
        tipo_modelo VARCHAR(50),
        version VARCHAR(20),
        fecha_entrenamiento TIMESTAMP,
        metricas JSONB,
        path_archivo VARCHAR(255),
        activo BOOLEAN DEFAULT FALSE
    );
    """
    
    # Ejecutar SQLs
    sqls = [
        # ("Extensión TimescaleDB", sql_extension),
        ("Tabla cajeros", sql_cajeros),
        # ("Tabla transacciones", sql_transacciones),
        # ("Tabla features", sql_features),
        # ("Tabla scores", sql_scores),
        # ("Tabla razones_anomalias", sql_razones),
        # ("Tabla feedback", sql_feedback),
        # ("Tabla modelos", sql_modelos)
    ]
    
    with engine.connect() as conn:
        for nombre, sql in sqls:
            try:
                logger.info(f"📝 Creando: {nombre}")
                conn.execute(text(sql))
                conn.commit()
                logger.info(f"   ✅ {nombre} creada/verificada")
            except Exception as e:
                logger.error(f"   ❌ Error en {nombre}: {e}")
                raise
    
    logger.info("\n✅ Todas las tablas creadas exitosamente\n")

# ============================================================================
# CREACIÓN DE ÍNDICES
# ============================================================================

def crear_indices(engine, logger):
    """Crea índices optimizados para las tablas"""
    
    logger.info("="*70)
    logger.info("🔍 CREANDO ÍNDICES")
    logger.info("="*70)
    
    indices = [
        # Índices en transacciones
        "CREATE INDEX IF NOT EXISTS idx_transacciones_terminal ON transacciones(cod_terminal, fecha_transaccion DESC);",
        "CREATE INDEX IF NOT EXISTS idx_transacciones_adquiriente ON transacciones(adquiriente, fecha_transaccion DESC);",
        "CREATE INDEX IF NOT EXISTS idx_transacciones_tipo_op ON transacciones(tipo_operacion);",
        "CREATE INDEX IF NOT EXISTS idx_transacciones_mes ON transacciones(mes_origen);",
        
        # Índices en scores
        "CREATE INDEX IF NOT EXISTS idx_scores_nivel ON scores(nivel_anomalia, fecha_scoring DESC);",
        "CREATE INDEX IF NOT EXISTS idx_scores_final ON scores(score_final DESC);",
        
        # Índices en razones
        "CREATE INDEX IF NOT EXISTS idx_razones_transaccion ON razones_anomalias(id_transaccion);",
        "CREATE INDEX IF NOT EXISTS idx_razones_tipo ON razones_anomalias(tipo_razon);",
        
        # Índices en feedback
        "CREATE INDEX IF NOT EXISTS idx_feedback_transaccion ON feedback(id_transaccion);",
        "CREATE INDEX IF NOT EXISTS idx_feedback_fraude ON feedback(es_fraude_real);",
    ]
    
    with engine.connect() as conn:
        for i, sql_index in enumerate(indices, 1):
            try:
                logger.info(f"📝 Creando índice {i}/{len(indices)}")
                conn.execute(text(sql_index))
                conn.commit()
            except Exception as e:
                logger.warning(f"   ⚠️  Advertencia en índice {i}: {e}")
    
    logger.info("\n✅ Índices creados exitosamente\n")

# ============================================================================
# CONFIGURAR COMPRESIÓN
# ============================================================================

def configurar_compresion(engine, logger):
    """Configura compresión automática de chunks antiguos"""
    
    logger.info("="*70)
    logger.info("🗜️  CONFIGURANDO COMPRESIÓN AUTOMÁTICA")
    logger.info("="*70)
    
    sql_compression = """
    -- Comprimir chunks más antiguos de 1 mes automáticamente
    SELECT add_compression_policy('transacciones', INTERVAL '30 days');
    """
    
    try:
        with engine.connect() as conn:
            logger.info("📝 Configurando política de compresión")
            conn.execute(text(sql_compression))
            conn.commit()
            logger.info("   ✅ Compresión configurada")
    except Exception as e:
        logger.warning(f"   ⚠️  No se pudo configurar compresión: {e}")
        logger.warning("   (Esto es normal si ya existe la política)")
    
    logger.info("")

# ============================================================================
# CARGA DE METADATA DE CAJEROS
# ============================================================================

def normalize_bool(value):
    if value is None:
        return None

    # Convertir a string
    value = str(value).strip().upper()

    # Quitar acentos
    value = ''.join(
        c for c in unicodedata.normalize('NFD', value)
        if unicodedata.category(c) != 'Mn'
    )

    # Mapas simples
    if value in ("SI", "S", "TRUE", "1"):
        return True
    if value in ("NO", "N", "FALSE", "0"):
        return False

    # Si no coincide, lo dejamos como None (NULL)
    return None


def cargar_metadata_cajeros(metadata_path, engine, logger):
    """
    Carga metadata de cajeros desde archivo Excel a PostgreSQL
    
    Args:
        metadata_path: Ruta al archivo Excel con metadata
        engine: SQLAlchemy engine
        logger: Logger
        
    Returns:
        bool: True si se cargó exitosamente, False si no existe el archivo
    """
    
    logger.info("="*70)
    logger.info("🏧 CARGANDO METADATA DE CAJEROS")
    logger.info("="*70)
    
    # Verificar si existe el archivo
    if not os.path.exists(metadata_path):
        logger.warning(f"⚠️  No se encontró archivo de metadata: {metadata_path}")
        logger.warning("   La tabla 'cajeros' quedará vacía")
        return False
    
    try:
        logger.info(f"📖 Leyendo archivo: {metadata_path}")
        
        # Leer Excel (ajustar según el formato real del archivo)
        df_cajeros = pd.read_excel(metadata_path)
        logger.info(f"   Cajeros encontrados: {len(df_cajeros):,}")
        
        # Mapeo de columnas (ajustar según nombres reales del Excel)
        # Este es un mapeo aproximado basado en tu Roadmap.md
        column_mapping = {
            'Código (*)': 'codigo',
            'Longitud (X)': 'longitud',
            'Latitud (Y)': 'latitud',
            'Municipio': 'municipio',
            'Departamento': 'departamento',
            'Tipo De Función': 'tipo_funcion',
            'Horario De Atención Oficina': 'horario_atencion_oficina',
            'Hora Apertura ATM': 'hora_apertura_atm',
            'Hora Cierre ATM': 'hora_cierre_atm',
            'Cajero Adyacente A Oficina': 'cajero_adyacente_oficina',
            'Estado De Cajero': 'estado_cajero',
            'Fecha De Instalación': 'fecha_instalacion',
            'Centro Comercial': 'centro_comercial',
            'Grandes Superficies': 'grandes_superficies',
            'Regional Ath/Banco': 'regional',
            'Nivel De Polución': 'nivel_polucion',
            'Temperatura': 'temperatura_promedio',
            'Tipo Site': 'tipo_site',
            'VIP': 'vip',
            'Uniplaza': 'uniplaza',
            'Cierre Nocturno': 'cierre_nocturno',
            'Capacidad Máx. De Aprovisionamiento': 'capacidad_max_aprovisionamiento',
            'Marca': 'marca',
            'Modelo': 'modelo',
            'Técnico De Servicio': 'tecnico_servicio',
            'Más Cajeros En El Mismo Site': 'mas_cajeros_mismo_site'
        }
        
        # Renombrar columnas que existan
        for old_name, new_name in column_mapping.items():
            if old_name in df_cajeros.columns:
                df_cajeros.rename(columns={old_name: new_name}, inplace=True)
        
        # Limpiar datos
        logger.info("🧹 Limpiando datos de cajeros...")
        
        # Convertir booleanos
        bool_columns = ['cajero_adyacente_oficina', 'centro_comercial', 
                       'grandes_superficies', 'vip', 'uniplaza', 
                       'cierre_nocturno', 'mas_cajeros_mismo_site']
        for col in bool_columns:
            if col in df_cajeros.columns:
                df_cajeros[col] = df_cajeros[col].apply(normalize_bool)
        
        # Insertar en PostgreSQL
        logger.info(f"💾 Insertando {len(df_cajeros):,} cajeros en PostgreSQL...")
        
        df_cajeros.to_sql(
            'cajeros',
            engine,
            if_exists='replace',  # Reemplazar si ya existe
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"✅ Metadata de cajeros cargada exitosamente")
        logger.info(f"   Total de cajeros: {len(df_cajeros):,}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al cargar metadata de cajeros: {e}")
        return False

# ============================================================================
# CARGA DE DATOS
# ============================================================================

def cargar_datos_a_postgres(parquet_path, engine, meses_a_cargar, batch_size, logger):
    """
    Carga datos desde Parquet a PostgreSQL
    
    Args:
        parquet_path: Ruta al archivo Parquet consolidado
        engine: SQLAlchemy engine
        meses_a_cargar: Número de meses a cargar (desde más reciente)
        batch_size: Tamaño de lote para INSERT
        logger: Logger
    """
    
    logger.info("="*70)
    logger.info("📥 CARGANDO DATOS A POSTGRESQL")
    logger.info("="*70)
    
    # Verificar que existe el archivo
    if not os.path.exists(parquet_path):
        logger.error(f"❌ No se encontró el archivo: {parquet_path}")
        return False
    
    logger.info(f"📖 Leyendo archivo Parquet: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    logger.info(f"   Total de registros en Parquet: {len(df):,}")
    
    # FILTRO 1: Por fecha (últimos N meses)
    if 'Fecha Transacción' in df.columns:
        df['Fecha Transacción'] = pd.to_datetime(df['Fecha Transacción'])
        fecha_limite = datetime.now() - timedelta(days=meses_a_cargar * 30)
        
        df_filtrado = df[df['Fecha Transacción'] >= fecha_limite].copy()
        
        logger.info(f"📅 Filtrando últimos {meses_a_cargar} meses")
        logger.info(f"   Fecha límite: {fecha_limite.strftime('%Y-%m-%d')}")
        logger.info(f"   Registros después de filtro temporal: {len(df_filtrado):,}")
        
        df = df_filtrado
    else:
        logger.warning("⚠️  No se encontró columna 'Fecha Transacción', cargando todo")
    
    # FILTRO 2: Por Tipo Operación (solo operaciones relevantes)
    # !Para validación grande
    # tipos_operacion_validos = ['Cambio De Pin', 'Avance', 'Retiro', 'Depositos', 'Transferencias']
    # !Para validación fundamental
    tipos_operacion_validos = ['Avance', 'Retiro']
    
    
    if 'Tipo Operación' in df.columns:
        logger.info(f"🔍 Filtrando por Tipo Operación...")
        registros_antes = len(df)
        df = df[df['Tipo Operación'].isin(tipos_operacion_validos)].copy()
        registros_despues = len(df)
        registros_filtrados = registros_antes - registros_despues
        
        logger.info(f"   Tipos válidos: {', '.join(tipos_operacion_validos)}")
        logger.info(f"   Registros filtrados: {registros_filtrados:,}")
        logger.info(f"   Registros después de filtro: {registros_despues:,}")
    else:
        logger.warning("⚠️  No se encontró columna 'Tipo Operación'")
    
    # FILTRO 3: Por Autorizador (debe tener información)
    if 'Autorizador' in df.columns:
        logger.info(f"🔍 Filtrando por Autorizador...")
        registros_antes = len(df)
        df = df[df['Autorizador'].notna() & (df['Autorizador'] != '')].copy()
        registros_despues = len(df)
        registros_filtrados = registros_antes - registros_despues
        
        logger.info(f"   Registros sin autorizador: {registros_filtrados:,}")
        logger.info(f"   Registros después de filtro: {registros_despues:,}")
    else:
        logger.warning("⚠️  No se encontró columna 'Autorizador'")
    
    # FILTRO 4: Eliminar duplicados
    if 'Id Tlf' in df.columns and 'Fecha Transacción' in df.columns:
        logger.info(f"🧹 Eliminando duplicados...")
        registros_antes = len(df)
        df = df.drop_duplicates(subset=['Id Tlf', 'Fecha Transacción'], keep='first')
        registros_despues = len(df)
        duplicados_eliminados = registros_antes - registros_despues
        
        logger.info(f"   Duplicados eliminados: {duplicados_eliminados:,}")
        logger.info(f"   Registros únicos finales: {registros_despues:,}")
    
    logger.info(f"\n✅ RESUMEN DE FILTROS:")
    logger.info(f"   Registros a cargar en PostgreSQL: {len(df):,}")
    
    if len(df) == 0:
        logger.error("❌ No hay registros para cargar después de aplicar filtros")
        return False
    
    # Preparar columnas para PostgreSQL (normalizar nombres)
    df_postgres = df.copy()
    
    # Renombrar columnas para match con schema
    column_mapping = {
        'Id Tlf': 'id_tlf',
        'Fecha Transacción': 'fecha_transaccion',
        'Cod Terminal': 'cod_terminal',
        'Autorizador': 'autorizador',
        'Tipo Operación': 'tipo_operacion',
        'Cod Estado Transacción': 'cod_estado_transaccion',
        'Cod Tipo Operación': 'cod_tipo_operacion',
        'Operación': 'operacion',
        'Canal': 'canal',
        'Tipo Convenio': 'tipo_convenio',
        'Adquiriente': 'adquiriente',
        'Valor Transacción': 'valor_transaccion',
        'Valor Transacción Original': 'valor_transaccion_original',
        'Cantidad Tx': 'cantidad_tx',
        'Duplicado': 'duplicado',
        'Fecha Negocio': 'fecha_negocio'
    }
    
    # Renombrar columnas que existan
    for old_name, new_name in column_mapping.items():
        if old_name in df_postgres.columns:
            df_postgres.rename(columns={old_name: new_name}, inplace=True)
    
    # Seleccionar solo columnas de la tabla
    columnas_tabla = [
        'id_tlf', 'fecha_transaccion', 'cod_terminal', 'autorizador',
        'tipo_operacion', 'cod_estado_transaccion', 'cod_tipo_operacion',
        'operacion', 'canal', 'tipo_convenio', 'adquiriente',
        'valor_transaccion', 'valor_transaccion_original', 'cantidad_tx',
        'duplicado', 'fecha_negocio', 'archivo_origen', 'mes_origen',
        'fecha_procesamiento'
    ]
    
    columnas_disponibles = [col for col in columnas_tabla if col in df_postgres.columns]
    df_postgres = df_postgres[columnas_disponibles]
    
    # NUEVO: Agregar columna de timestamp redondeado a 15 minutos
    if 'fecha_transaccion' in df_postgres.columns:
        logger.info("📊 Calculando timestamps de 15 minutos...")
        df_postgres['fecha_transaccion_15min'] = df_postgres['fecha_transaccion'].dt.floor('15min')
        logger.info("   ✓ Columna fecha_transaccion_15min agregada")
    
    logger.info(f"📋 Columnas a insertar: {len(df_postgres.columns)}")
    
    # Insertar en batches con manejo de duplicados
    total_registros = len(df_postgres)
    num_batches = (total_registros // batch_size) + 1
    
    logger.info(f"\n💾 Insertando datos en {num_batches} batches de {batch_size:,}")
    logger.info("🔄 Modo: ON CONFLICT DO NOTHING (omite duplicados automáticamente)")
    
    registros_insertados = 0
    registros_omitidos = 0
    
    with tqdm(total=total_registros, desc="Insertando en PostgreSQL") as pbar:
        for i in range(0, total_registros, batch_size):
            batch = df_postgres.iloc[i:i+batch_size]
            
            try:
                # Generar nombres de columnas
                columnas = list(batch.columns)
                columnas_str = ', '.join(columnas)
                placeholders = ', '.join([f':{col}' for col in columnas])
                
                # SQL con ON CONFLICT DO NOTHING
                sql = f"""
                    INSERT INTO transacciones ({columnas_str})
                    VALUES ({placeholders})
                    ON CONFLICT (id_tlf, fecha_transaccion) DO NOTHING
                """
                
                # Ejecutar batch
                with engine.begin() as conn:
                    result = conn.execute(
                        text(sql),
                        batch.to_dict('records')
                    )
                    # rowcount indica cuántos se insertaron (los duplicados no cuentan)
                    insertados_batch = result.rowcount if result.rowcount > 0 else 0
                    registros_insertados += insertados_batch
                    registros_omitidos += len(batch) - insertados_batch
                
                pbar.update(len(batch))
                
            except Exception as e:
                logger.error(f"❌ Error en batch {i//batch_size + 1}: {e}")
                logger.error(f"   Intentando insertar uno por uno...")
                
                # Fallback: uno por uno
                for idx, row in batch.iterrows():
                    try:
                        row_dict = row.to_dict()
                        with engine.begin() as conn:
                            result = conn.execute(text(sql), [row_dict])
                            if result.rowcount > 0:
                                registros_insertados += 1
                            else:
                                registros_omitidos += 1
                        pbar.update(1)
                    except:
                        registros_omitidos += 1
                        pbar.update(1)
    
    logger.info(f"\n✅ Carga completada:")
    logger.info(f"   Registros insertados: {registros_insertados:,}")
    logger.info(f"   Registros omitidos (duplicados): {registros_omitidos:,}")
    logger.info(f"   Total procesados: {total_registros:,}")
    
    return True

# ============================================================================
# VERIFICACIÓN
# ============================================================================

def verificar_carga(engine, logger):
    """Verifica que los datos se cargaron correctamente"""
    
    logger.info("="*70)
    logger.info("🔍 VERIFICANDO CARGA DE DATOS")
    logger.info("="*70)
    
    queries = [
        ("Total de transacciones", "SELECT COUNT(*) FROM transacciones;"),
        ("Fecha mínima", "SELECT MIN(fecha_transaccion) FROM transacciones;"),
        ("Fecha máxima", "SELECT MAX(fecha_transaccion) FROM transacciones;"),
        ("Cajeros únicos", "SELECT COUNT(DISTINCT cod_terminal) FROM transacciones;"),
        ("Distribución por mes", "SELECT mes_origen, COUNT(*) FROM transacciones GROUP BY mes_origen ORDER BY mes_origen;"),
    ]
    
    with engine.connect() as conn:
        for nombre, query in queries:
            try:
                result = conn.execute(text(query))
                
                if "GROUP BY" in query:
                    logger.info(f"\n📊 {nombre}:")
                    for row in result:
                        logger.info(f"   {row[0]}: {row[1]:,}")
                else:
                    value = result.scalar()
                    logger.info(f"✅ {nombre}: {value}")
                    
            except Exception as e:
                logger.error(f"❌ Error en {nombre}: {e}")
    
    logger.info("")

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal del script"""
    
    # Parsear argumentos
    parser = argparse.ArgumentParser(
        description='Cargar datos a PostgreSQL + TimescaleDB'
    )
    parser.add_argument(
        '--config', 
        type=str, 
        default='config.yaml',
        help='Ruta al archivo de configuración YAML'
    )
    parser.add_argument(
    '--solo-cajeros',
    action='store_true',
    help='Solo crear/actualizar la tabla cajeros y salir'
    )

    args = parser.parse_args()
    
    # Cargar configuración
    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el archivo de configuración: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR al leer configuración: {e}")
        sys.exit(1)
    
    # Extraer configuraciones
    paths = config['paths']
    postgres_config = config['postgres']
    
    # Configurar logging
    log_path = os.path.join(
        paths['logs'], 
        config['logging']['files']['postgres']
    )
    logger = setup_logging(log_path, config['logging']['level'])
    
    # Inicio del proceso
    logger.info("="*70)
    logger.info("🚀 INICIANDO CARGA A POSTGRESQL + TIMESCALEDB")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Host: {postgres_config['host']}")
    logger.info(f"Database: {postgres_config['database']}")
    logger.info(f"Meses a cargar: {postgres_config['meses_a_cargar']}")
    logger.info("="*70)
    logger.info("")
    
    # Crear connection string
    connection_string = (
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )
    
    # Crear engine
    logger.info("🔌 Conectando a PostgreSQL...")
    try:
        engine = create_engine(
            connection_string,
            poolclass=NullPool,
            echo=False
        )
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexión exitosa\n")
    except Exception as e:
        logger.error(f"❌ Error al conectar: {e}")
        sys.exit(1)
    
    # Crear tablas
    crear_tablas(engine, postgres_config['chunk_interval'], logger)
    
    # Cargar metadata de cajeros (si existe)
    metadata_path = os.path.join(
        paths['root'],
        'data/Inventario_General_Disp_ATM_Centro_de_Efectivo.xlsx'
    )
    cargar_metadata_cajeros(metadata_path, engine, logger)
    
    if args.solo_cajeros:
        logger.info("🛑 Se ejecutó solamente la creación y carga de la tabla cajeros.")
        sys.exit(0)
    
    # Crear índices
    crear_indices(engine, logger)
    
    # Configurar compresión
    configurar_compresion(engine, logger)
    
    # Cargar datos de transacciones
    parquet_path = os.path.join(
        paths['parquet'],
        config['consolidacion']['archivo_final']
    )
    
    success = cargar_datos_a_postgres(
        parquet_path=parquet_path,
        engine=engine,
        meses_a_cargar=postgres_config['meses_a_cargar'],
        batch_size=postgres_config['batch_size'],
        logger=logger
    )
    
    if not success:
        logger.error("❌ Error en la carga de datos")
        sys.exit(1)
    
    # Verificar carga
    verificar_carga(engine, logger)
    
    # Finalizar
    logger.info("="*70)
    logger.info("🎉 PROCESO COMPLETADO EXITOSAMENTE")
    logger.info("="*70)
    logger.info("La base de datos está lista para uso en producción")
    logger.info("")

# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    main()