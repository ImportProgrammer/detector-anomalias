#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
CONSOLIDACIÓN DE CSVs A PARQUET - Sistema de Detección de Fraudes ATM
============================================================================

Script de producción para servidor que:
1. Lee archivos CSV mensuales de /dados/avc/data/
2. Optimiza tipos de datos
3. Guarda archivos Parquet mensuales en /dados/avc/parquet/
4. Consolida todo en un archivo final: transacciones_consolidadas.parquet

Uso:
    python consolidar_a_parquet.py
    
    # O con configuración custom:
    python consolidar_a_parquet.py --config /ruta/custom/config.yaml

Autor: Sistema de Detección de Fraudes
Fecha: 2025-11-06
============================================================================
"""

import pandas as pd
import os
import gc
import sys
import yaml
import argparse
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

def setup_logging(log_path, log_level='INFO'):
    """Configura el sistema de logging"""
    
    # Crear directorio de logs si no existe
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    # Configurar formato
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configurar handlers
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
# FUNCIONES DE OPTIMIZACIÓN
# ============================================================================

def optimizar_tipos_datos(df, config):
    """
    Optimiza tipos de datos del DataFrame según configuración
    
    Args:
        df: DataFrame a optimizar
        config: Diccionario de configuración con columnas
        
    Returns:
        DataFrame optimizado
    """
    logger = logging.getLogger(__name__)
    
    # Columnas enteras
    columnas_int = config.get('int_columns', [])
    for col in columnas_int:
        if col in df.columns and df[col].notna().any():
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if df[col].max() < 32767 and df[col].min() > -32768:
                    df[col] = df[col].astype('Int16')
                else:
                    df[col] = df[col].astype('Int32')
            except Exception as e:
                logger.warning(f"No se pudo optimizar columna {col}: {e}")
    
    # Columnas flotantes
    columnas_float = config.get('float_columns', [])
    for col in columnas_float:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')
            except Exception as e:
                logger.warning(f"No se pudo convertir {col} a float32: {e}")
    
    # Columnas categóricas
    columnas_categoricas = config.get('categorical_columns', [])
    for col in columnas_categoricas:
        if col in df.columns:
            try:
                df[col] = df[col].astype('category')
            except Exception as e:
                logger.warning(f"No se pudo convertir {col} a category: {e}")
    
    # Columnas de fecha
    columnas_datetime = config.get('datetime_columns', [])
    for col in columnas_datetime:
        if col in df.columns:
            try:
                if col == 'Fecha Transacción':
                    df[col] = pd.to_datetime(df[col], errors='coerce',
                                           format='%Y/%m/%d %H:%M:%S')
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"No se pudo convertir {col} a datetime: {e}")
    
    return df

# ============================================================================
# PROCESAMIENTO POR MES
# ============================================================================

def procesar_mes_individual(ruta_base, carpeta_mes, ruta_salida, 
                           chunk_size, config, logger):
    """
    Procesa un mes completo de CSVs y lo guarda como Parquet
    
    Args:
        ruta_base: Ruta base donde están las carpetas mensuales
        carpeta_mes: Nombre de la carpeta del mes (ej: '02. Febrero')
        ruta_salida: Ruta donde guardar el Parquet
        chunk_size: Número de archivos a procesar por lote
        config: Configuración de columnas
        logger: Logger para mensajes
        
    Returns:
        tuple: (archivos_procesados, lista_errores)
    """
    
    logger.info("="*70)
    logger.info(f"📁 PROCESANDO: {carpeta_mes}")
    logger.info("="*70)
    
    ruta_carpeta = os.path.join(ruta_base, carpeta_mes)
    
    # Validar que existe la carpeta
    if not os.path.exists(ruta_carpeta):
        logger.error(f"❌ Carpeta no encontrada: {carpeta_mes}")
        return 0, [(carpeta_mes, "Carpeta no encontrada")]
    
    # Listar archivos CSV
    archivos_csv = sorted([
        f for f in os.listdir(ruta_carpeta) 
        if f.endswith('.csv') and not f.startswith('.')
    ])
    
    if not archivos_csv:
        logger.warning(f"⚠️  No se encontraron archivos CSV en {carpeta_mes}")
        return 0, [(carpeta_mes, "Sin archivos CSV")]
    
    logger.info(f"Total de archivos CSV encontrados: {len(archivos_csv)}")
    
    # Inicializar variables
    lista_dfs = []
    archivos_procesados = 0
    archivos_con_error = []
    primera_escritura = True
    
    # Procesar con barra de progreso
    with tqdm(total=len(archivos_csv), desc=f"Procesando {carpeta_mes}") as pbar:
        for i, archivo in enumerate(archivos_csv, 1):
            ruta_completa = os.path.join(ruta_carpeta, archivo)
            
            try:
                # Leer CSV
                df = pd.read_csv(ruta_completa, index_col=0, low_memory=False)
                
                # Optimizar tipos de datos inmediatamente
                df = optimizar_tipos_datos(df, config)
                
                # Agregar metadata
                df['archivo_origen'] = archivo
                df['mes_origen'] = carpeta_mes
                df['fecha_procesamiento'] = datetime.now()
                
                lista_dfs.append(df)
                archivos_procesados += 1
                
                # Actualizar barra de progreso
                pbar.update(1)
                
                # Guardar cada chunk_size archivos
                if len(lista_dfs) >= chunk_size:
                    logger.info(f"💾 Guardando lote de {len(lista_dfs)} archivos...")
                    
                    # Consolidar lote
                    df_lote = pd.concat(lista_dfs, ignore_index=True)
                    lista_dfs = []
                    gc.collect()
                    
                    # Guardar o appendear
                    if primera_escritura:
                        df_lote.to_parquet(
                            ruta_salida, 
                            engine='pyarrow',
                            compression='snappy', 
                            index=False
                        )
                        primera_escritura = False
                        logger.info(f"   ✓ Archivo inicial creado")
                    else:
                        # Leer existente, concatenar y guardar
                        df_existente = pd.read_parquet(ruta_salida)
                        df_final = pd.concat([df_existente, df_lote], 
                                            ignore_index=True)
                        
                        del df_existente, df_lote
                        gc.collect()
                        
                        df_final.to_parquet(
                            ruta_salida, 
                            engine='pyarrow',
                            compression='snappy', 
                            index=False
                        )
                        del df_final
                        gc.collect()
                        logger.info(f"   ✓ Lote agregado al archivo")
                
            except Exception as e:
                error_msg = str(e)[:100]
                logger.error(f"❌ Error en {archivo}: {error_msg}")
                archivos_con_error.append((archivo, error_msg))
                pbar.update(1)
                continue
    
    # Guardar archivos restantes
    if lista_dfs:
        logger.info(f"💾 Guardando últimos {len(lista_dfs)} archivos...")
        df_lote = pd.concat(lista_dfs, ignore_index=True)
        
        if primera_escritura:
            df_lote.to_parquet(
                ruta_salida, 
                engine='pyarrow',
                compression='snappy', 
                index=False
            )
        else:
            df_existente = pd.read_parquet(ruta_salida)
            df_final = pd.concat([df_existente, df_lote], ignore_index=True)
            
            del df_existente, df_lote
            gc.collect()
            
            df_final.to_parquet(
                ruta_salida, 
                engine='pyarrow',
                compression='snappy', 
                index=False
            )
            del df_final
            gc.collect()
    
    # Mostrar información del archivo generado
    if os.path.exists(ruta_salida):
        tamanio_mb = os.path.getsize(ruta_salida) / (1024 * 1024)
        df_mes = pd.read_parquet(ruta_salida)
        
        logger.info(f"\n✅ {carpeta_mes} completado:")
        logger.info(f"   Archivo: {os.path.basename(ruta_salida)}")
        logger.info(f"   Tamaño: {tamanio_mb:.2f} MB")
        logger.info(f"   Registros: {len(df_mes):,}")
        logger.info(f"   Columnas: {len(df_mes.columns)}")
        
        del df_mes
        gc.collect()
    
    logger.info("")
    
    return archivos_procesados, archivos_con_error

# ============================================================================
# CONSOLIDACIÓN FINAL
# ============================================================================

def consolidar_todos_los_meses(ruta_parquet, meses, logger):
    """
    Consolida todos los archivos Parquet mensuales en uno solo
    
    Args:
        ruta_parquet: Directorio donde están los Parquet mensuales
        meses: Diccionario de meses y archivos
        logger: Logger
        
    Returns:
        Path del archivo consolidado final
    """
    
    logger.info("="*70)
    logger.info("🔗 CONSOLIDANDO TODOS LOS MESES")
    logger.info("="*70)
    
    # Encontrar archivos existentes
    archivos_mensuales = [
        os.path.join(ruta_parquet, nombre_archivo)
        for nombre_archivo in meses.values()
        if os.path.exists(os.path.join(ruta_parquet, nombre_archivo))
    ]
    
    if not archivos_mensuales:
        logger.error("❌ No se encontraron archivos Parquet mensuales para consolidar")
        return None
    
    logger.info(f"Archivos a consolidar: {len(archivos_mensuales)}\n")
    
    # Leer cada mes con progreso
    dfs_mensuales = []
    
    for archivo in tqdm(archivos_mensuales, desc="Leyendo archivos"):
        logger.info(f"📖 Leyendo {os.path.basename(archivo)}...")
        df = pd.read_parquet(archivo)
        logger.info(f"   Registros: {len(df):,}")
        dfs_mensuales.append(df)
    
    # Consolidar
    logger.info("\n🔗 Consolidando todos los DataFrames...")
    df_consolidado = pd.concat(dfs_mensuales, ignore_index=True)
    
    # Liberar memoria
    del dfs_mensuales
    gc.collect()
    
    logger.info(f"✅ Total de registros consolidados: {len(df_consolidado):,}")
    
    # Guardar archivo final
    ruta_final = os.path.join(ruta_parquet, 'transacciones_consolidadas.parquet')
    logger.info(f"\n💾 Guardando archivo consolidado final...")
    logger.info(f"   Ruta: {ruta_final}")
    
    df_consolidado.to_parquet(
        ruta_final, 
        engine='pyarrow',
        compression='snappy', 
        index=False
    )
    
    tamanio_final_mb = os.path.getsize(ruta_final) / (1024 * 1024)
    
    # Estadísticas finales
    logger.info("")
    logger.info("="*70)
    logger.info("🎉 CONSOLIDACIÓN COMPLETADA EXITOSAMENTE")
    logger.info("="*70)
    logger.info(f"📊 Archivo final: transacciones_consolidadas.parquet")
    logger.info(f"📦 Tamaño: {tamanio_final_mb:.2f} MB")
    logger.info(f"📈 Registros totales: {len(df_consolidado):,}")
    logger.info(f"📋 Columnas: {len(df_consolidado.columns)}")
    
    # Distribución por mes
    if 'mes_origen' in df_consolidado.columns:
        logger.info("\n📊 Distribución por mes:")
        dist_meses = df_consolidado['mes_origen'].value_counts().sort_index()
        for mes, count in dist_meses.items():
            logger.info(f"   {mes}: {count:,} transacciones")
    
    # Rango de fechas
    if 'Fecha Transacción' in df_consolidado.columns:
        fecha_min = df_consolidado['Fecha Transacción'].min()
        fecha_max = df_consolidado['Fecha Transacción'].max()
        logger.info(f"\n📅 Rango de fechas:")
        logger.info(f"   Desde: {fecha_min}")
        logger.info(f"   Hasta: {fecha_max}")
    
    logger.info("="*70)
    
    del df_consolidado
    gc.collect()
    
    return ruta_final

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal del script"""
    
    # Parsear argumentos
    parser = argparse.ArgumentParser(
        description='Consolidar archivos CSV a Parquet'
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
        print(f"❌ ERROR: No se encontró el archivo de configuración: {args.config}")
        print("   Crea el archivo config.yaml en el directorio actual")
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR al leer configuración: {e}")
        sys.exit(1)
    
    # Extraer configuraciones
    paths = config['paths']
    meses = config['meses']
    consolidacion = config['consolidacion']
    columnas = config['columnas']
    
    # Crear directorios si no existen
    os.makedirs(paths['parquet'], exist_ok=True)
    os.makedirs(paths['logs'], exist_ok=True)
    
    # Configurar logging
    log_path = os.path.join(
        paths['logs'], 
        config['logging']['files']['consolidacion']
    )
    logger = setup_logging(log_path, config['logging']['level'])
    
    # Inicio del proceso
    logger.info("="*70)
    logger.info("🚀 INICIANDO CONSOLIDACIÓN DE CSV A PARQUET")
    logger.info("="*70)
    logger.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Configuración: {args.config}")
    logger.info(f"Datos CSV: {paths['data_csv']}")
    logger.info(f"Salida Parquet: {paths['parquet']}")
    logger.info(f"Meses a procesar: {len(meses)}")
    logger.info("="*70)
    logger.info("")
    
    # Procesar cada mes
    total_exitosos = 0
    total_errores = []
    
    for carpeta_mes, nombre_archivo in meses.items():
        ruta_salida = os.path.join(paths['parquet'], nombre_archivo)
        
        # Procesar mes
        exitosos, errores = procesar_mes_individual(
            ruta_base=paths['data_csv'],
            carpeta_mes=carpeta_mes,
            ruta_salida=ruta_salida,
            chunk_size=consolidacion['chunk_size'],
            config=columnas,
            logger=logger
        )
        
        total_exitosos += exitosos
        total_errores.extend(errores)
        
        # Liberar memoria entre meses
        gc.collect()
        logger.info("🧹 Memoria limpiada\n")
    
    # Resumen de procesamiento mensual
    logger.info("="*70)
    logger.info("✅ FASE 1 COMPLETADA: Todos los meses procesados")
    logger.info("="*70)
    logger.info(f"Total de archivos procesados: {total_exitosos}")
    logger.info(f"Total de errores: {len(total_errores)}")
    
    if total_errores:
        logger.warning("\n⚠️  Archivos con errores:")
        for archivo, error in total_errores[:10]:  # Mostrar solo primeros 10
            logger.warning(f"   {archivo}: {error}")
        if len(total_errores) > 10:
            logger.warning(f"   ... y {len(total_errores) - 10} más")
    
    logger.info("")
    
    # Consolidar todos los meses
    ruta_final = consolidar_todos_los_meses(
        ruta_parquet=paths['parquet'],
        meses=meses,
        logger=logger
    )
    
    if ruta_final:
        logger.info(f"\n✅ Proceso completado exitosamente")
        logger.info(f"📄 Archivo final disponible en: {ruta_final}")
    else:
        logger.error(f"\n❌ No se pudo crear el archivo consolidado final")
        sys.exit(1)

# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    main()