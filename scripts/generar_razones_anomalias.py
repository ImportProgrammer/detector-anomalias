#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
GENERADOR DE RAZONES DE ANOMALÍAS - Sistema Detección de Fraudes
============================================================================

Lee scores de anomalías y genera explicaciones detalladas de por qué
cada transacción fue marcada como anómala.

Input:  scores + transacciones + features
Output: razones_anomalias (explicaciones detalladas)

Uso:
    python generar_razones_anomalias.py --config config.yaml

============================================================================
"""

import pandas as pd
import yaml
import argparse
import logging
import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from tqdm import tqdm

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
# GENERACIÓN DE RAZONES
# ============================================================================

def generar_razon_temporal(row):
    """Genera razón relacionada con anomalías temporales"""
    razones = []
    
    if row.get('es_horario_nocturno') and row.get('cierre_nocturno_encoded'):
        razones.append(f"Transacción a las {row['hora']:02d}:00 en cajero que cierra de noche")
    
    if row.get('es_madrugada'):
        razones.append(f"Transacción en madrugada ({row['hora']:02d}:00)")
    
    if row.get('es_fin_de_semana'):
        dias = ['Lun', 'Mar', 'Mie', 'Jue', 'Vie', 'Sáb', 'Dom']
        razones.append(f"Transacción en fin de semana ({dias[row['dia_semana']]})")
    
    return razones

def generar_razon_monto(row):
    """Genera razón relacionada con montos anómalos"""
    razones = []
    
    if row.get('es_retiro_maximo'):
        razones.append(f"Retiro máximo: ${row['valor_transaccion']:,.0f}")
    
    desv = row.get('desviacion_monto_cajero')
    if desv and abs(desv) > 3:
        direccion = "por encima" if desv > 0 else "por debajo"
        razones.append(f"Monto {abs(desv):.1f}σ {direccion} del promedio del cajero")
    
    if row.get('diferencia_valor') and abs(row['diferencia_valor']) > 1000:
        razones.append(f"Diferencia entre valor original y final: ${row['diferencia_valor']:,.0f}")
    
    return razones

def generar_razon_velocidad(row):
    """Genera razón relacionada con velocidad de transacciones"""
    razones = []
    
    if row.get('es_transaccion_rapida'):
        seg = row.get('tiempo_desde_anterior_seg', 0)
        razones.append(f"Transacción {seg:.0f} segundos después de la anterior")
    
    tx_hora = row.get('tx_por_hora_cajero')
    if tx_hora and tx_hora > 30:
        razones.append(f"Alta frecuencia en cajero: {tx_hora:.0f} tx/hora")
    
    return razones

def generar_razon_tipo_operacion(row):
    """Genera razón relacionada con tipo de operación"""
    razones = []
    
    if row.get('es_cambio_pin'):
        razones.append("Cambio de PIN detectado")
    
    if row.get('transaccion_rechazada'):
        razones.append("Transacción rechazada")
    
    tasa_rechazo = row.get('tasa_rechazo_cajero')
    if tasa_rechazo and tasa_rechazo > 0.3:
        razones.append(f"Cajero con alta tasa de rechazo: {tasa_rechazo*100:.1f}%")
    
    return razones

def generar_razon_cajero(row):
    """Genera razón relacionada con características del cajero"""
    razones = []
    
    if not row.get('cajero_adyacente_encoded'):
        razones.append("Cajero aislado (no adyacente a oficina)")
    
    return razones

def generar_razon_isolation_forest(row):
    """Genera razón para anomalías detectadas por Isolation Forest"""
    razones = []
    
    score = row.get('score_final')
    if score and score > 0.7:
        razones.append(f"Patrón anómalo detectado por ML (score: {score:.3f})")
    
    return razones

def generar_razones_completas(df, logger):
    """Genera todas las razones para cada anomalía"""

    logger.info("🔍 Generando razones detalladas...")

    razones_list = []

    def safe_scalar(x):
        """Si x es una Series, devuelve el primer valor; si no, devuelve el escalar."""
        if isinstance(x, pd.Series):
            return x.iloc[0]
        return x

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Generando razones"):
        razones = []

        # Agregar razones de cada categoría
        razones.extend(generar_razon_temporal(row))
        razones.extend(generar_razon_monto(row))
        razones.extend(generar_razon_velocidad(row))
        razones.extend(generar_razon_tipo_operacion(row))
        razones.extend(generar_razon_cajero(row))
        razones.extend(generar_razon_isolation_forest(row))

        # Obtener id_transaccion de manera segura
        id_tx = safe_scalar(row['id_transaccion'])

        # Crear entrada para cada razón
        for i, razon in enumerate(razones, 1):
            razones_list.append({
                'id_transaccion': id_tx,   
                'tipo_razon': clasificar_tipo_razon(razon),
                'descripcion': razon,
                'severidad': int(calcular_severidad(razon, row)),
                'orden': i
            })

    logger.info(f"   ✅ Razones generadas: {len(razones_list):,}")

    return pd.DataFrame(razones_list)

# def generar_razones_completas(df, logger):
#     """Genera todas las razones para cada anomalía"""
    
#     logger.info("🔍 Generando razones detalladas...")
    
#     razones_list = []
    
#     for idx, row in tqdm(df.iterrows(), total=len(df), desc="Generando razones"):
#         razones = []
        
#         # Agregar razones de cada categoría
#         razones.extend(generar_razon_temporal(row))
#         razones.extend(generar_razon_monto(row))
#         razones.extend(generar_razon_velocidad(row))
#         razones.extend(generar_razon_tipo_operacion(row))
#         razones.extend(generar_razon_cajero(row))
#         razones.extend(generar_razon_isolation_forest(row))
        
#         # Crear entrada para cada razón
#         for i, razon in enumerate(razones, 1):
#             razones_list.append({
#                 'id_transaccion': row['id_transaccion'],
#                 'tipo_razon': clasificar_tipo_razon(razon),
#                 'descripcion': razon,
#                 'severidad': calcular_severidad(razon, row),
#                 'orden': i
#             })
    
#     logger.info(f"   ✅ Razones generadas: {len(razones_list):,}")
    
#     return pd.DataFrame(razones_list)

def clasificar_tipo_razon(razon):
    """Clasifica el tipo de razón"""
    razon_lower = razon.lower()
    
    if 'horario' in razon_lower or 'madrugada' in razon_lower:
        return 'temporal'
    elif 'monto' in razon_lower or 'retiro' in razon_lower or 'σ' in razon:
        return 'monto'
    elif 'velocidad' in razon_lower or 'segundos' in razon_lower or 'frecuencia' in razon_lower:
        return 'velocidad'
    elif 'pin' in razon_lower or 'rechazada' in razon_lower:
        return 'operacion'
    elif 'cajero' in razon_lower or 'aislado' in razon_lower:
        return 'ubicacion'
    elif 'patrón' in razon_lower or 'ml' in razon_lower:
        return 'ml'
    else:
        return 'otro'

def calcular_severidad(razon, row):
    """Calcula severidad de la razón (1-10)"""
    razon_lower = razon.lower()
    
    # Severidades altas
    if 'madrugada' in razon_lower and 'cerrado' in razon_lower:
        return 9
    if 'retiro máximo' in razon_lower:
        return 8
    if 'segundos después' in razon_lower:
        return 8
    
    # Severidades medias
    if 'cambio de pin' in razon_lower and not row.get('es_transaccion_rapida'):
        return 5
    if 'σ' in razon and abs(row.get('desviacion_monto_cajero', 0)) > 3:
        return 7
    if 'alta frecuencia' in razon_lower:
        return 6
    if 'aislado' in razon_lower:
        return 5
    
    # Severidad baja
    return 4

# ============================================================================
# GUARDAR RAZONES
# ============================================================================

def guardar_razones(df_razones, engine, batch_size, logger):
    """Guarda razones en PostgreSQL"""
    
    logger.info("="*70)
    logger.info("💾 GUARDANDO RAZONES EN POSTGRESQL")
    logger.info("="*70)
    
    # Limpiar tabla existente
    logger.info("🧹 Limpiando tabla razones_anomalias...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE razones_anomalias;"))
        conn.commit()
    
    # Insertar en batches
    total_registros = len(df_razones)
    logger.info(f"💾 Insertando {total_registros:,} razones...")
    
    for i in tqdm(range(0, total_registros, batch_size), desc="Guardando razones"):
        batch = df_razones.iloc[i:i+batch_size]
        
        try:
            batch.to_sql(
                'razones_anomalias',
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )
        except Exception as e:
            logger.error(f"❌ Error en batch {i//batch_size + 1}: {e}")
    
    logger.info(f"\n✅ Razones guardadas exitosamente")

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal"""
    
    parser = argparse.ArgumentParser(description='Generar razones de anomalías')
    parser.add_argument('--config', type=str, default='../config.yaml', help='Ruta al archivo de configuración')
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
    log_path = os.path.join(paths['logs'], 'razones_anomalias.log')
    logger = setup_logging(log_path, config['logging']['level'])
    
    logger.info("="*70)
    logger.info("🚀 GENERANDO RAZONES DE ANOMALÍAS")
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
    
    # Leer datos
    logger.info("="*70)
    logger.info("📖 LEYENDO DATOS DE ANOMALÍAS")
    logger.info("="*70)
    
    query = """
        SELECT 
            s.id_transaccion,
            s.score_final,
            s.nivel_anomalia,
            t.fecha_transaccion,
            t.cod_terminal,
            t.tipo_operacion,
            t.valor_transaccion,
            f.*
        FROM scores s
        JOIN transacciones t ON s.id_transaccion = t.id_tlf
        JOIN features f ON t.id_tlf = f.id_transaccion
        WHERE s.nivel_anomalia IN ('Crítico', 'Advertencia')
        ORDER BY s.score_final DESC
    """
    
    logger.info("Ejecutando query...")
    df = pd.read_sql(query, engine)
    logger.info(f"✅ Anomalías cargadas: {len(df):,}\n")
    
    if len(df) == 0:
        logger.warning("⚠️  No hay anomalías para procesar")
        return
    
    # Generar razones
    df_razones = generar_razones_completas(df, logger)
    
    # Guardar en PostgreSQL
    guardar_razones(df_razones, engine, postgres_config['batch_size'], logger)
    
    # Verificar
    logger.info("="*70)
    logger.info("🔍 VERIFICANDO RAZONES")
    logger.info("="*70)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM razones_anomalias"))
        count = result.scalar()
        logger.info(f"✅ Total de razones en BD: {count:,}")
        
        result = conn.execute(text("SELECT tipo_razon, COUNT(*) FROM razones_anomalias GROUP BY tipo_razon"))
        logger.info("\n📊 Distribución por tipo:")
        for row in result:
            logger.info(f"   {row[0]}: {row[1]:,}")
    
    logger.info("")
    logger.info("="*70)
    logger.info("🎉 GENERACIÓN DE RAZONES COMPLETADA")
    logger.info("="*70)

if __name__ == "__main__":
    main()