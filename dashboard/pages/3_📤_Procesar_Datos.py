"""
📤 Procesar Datos - Cargar archivos nuevos y detectar anomalías con ML
"""

import streamlit as st
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import joblib
import plotly.graph_objects as go

# Agregar path del dashboard
dashboard_path = Path(__file__).parent.parent
sys.path.append(str(dashboard_path))

from utils.db import execute_query, test_connection
from components.mapa import crear_mapa_alertas

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Procesar Datos - Detección de Fraudes",
    page_icon="📤",
    layout="wide"
)

st.title("📤 Procesar Nuevos Datos de Dispensación")
st.markdown("Cargue archivos de dispensación ATM para detectar anomalías usando ML")

# ============================================================================
# CARGAR MODELO ML
# ============================================================================

@st.cache_resource
def cargar_modelo():
    """Cargar modelo de Isolation Forest entrenado"""
    try:
        model_path = Path(dashboard_path).parent / 'models' / 'isolation_forest_dispensacion_v2.pkl'
        
        if not model_path.exists():
            st.error(f"❌ Modelo no encontrado en: {model_path}")
            return None, None
        
        loaded_obj = joblib.load(model_path)

        if isinstance(loaded_obj, dict):
            model = loaded_obj['modelo']
            feature_names = loaded_obj.get('feature_names', None)
            
            # st.success(f"✅ Modelo cargado: {model_path.name}")
            st.success("✅ Modelo entrenado cargado.")
            
            # if 'metadata' in loaded_obj:
            #     st.info(f"📊 Metadata: {loaded_obj['metadata']}")
            if 'fecha_entrenamiento' in loaded_obj:
                st.info(f"📅 Entrenado: {loaded_obj['fecha_entrenamiento']}")
            
            return model, feature_names
        else:
            # st.success(f"✅ Modelo cargado: {model_path.name}")
            st.success("✅ Modelo entrenado cargado.")
            return loaded_obj, None
        
    except Exception as e:
        st.error(f"❌ Error al cargar modelo: {str(e)}")
        return None, None

# Cargar modelo al inicio
modelo_ml, feature_names_modelo = cargar_modelo()

# ============================================================================
# VERIFICAR CONEXIÓN
# ============================================================================

if not test_connection():
    st.error("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
    st.stop()

# ============================================================================
# INFORMACIÓN DEL FORMATO
# ============================================================================

# with st.expander("ℹ️ Información sobre el formato del archivo"):
#     st.markdown("""
#     ### Formato del Archivo de Dispensación
    
#     **Línea 1 (Header):** Se ignora
#     ```
#     01,20251027094500,1000
#     ```
    
#     **Líneas de Transacciones:**
#     ```
#     02,115,2,7290000,20251027094500,7,20,143,50,0,100
#     ```
    
#     **Estructura:**
#     - Posición 0: `02` (tipo registro)
#     - Posición 1: `115` (código cajero)
#     - Posición 2: `2` (código operación: 2=Retiro, 3=Consulta, 4=Avance)
#     - Posición 3: `7290000` (monto dispensado)
#     - Posición 4: `20251027094500` (timestamp - final ventana 15min)
#     - Posiciones 5+: Billetes (cantidad, denominación, ...)
    
#     **Solo se procesan operaciones con código 2, 3 y 4**
#     """)

# ============================================================================
# UPLOAD DE ARCHIVO
# ============================================================================

st.markdown("### 1️⃣ Cargar Archivo de Dispensación")

uploaded_file = st.file_uploader(
    "Seleccione el archivo de dispensación ATM",
    type=['txt', 'csv'],
    help="Archivos en formato TXT o CSV con estructura ATH"
)

if uploaded_file is None:
    st.info("👆 Por favor, cargue un archivo para comenzar")
    st.stop()

# ============================================================================
# FUNCIÓN DE FEATURE ENGINEERING
# ============================================================================

def calcular_features_ml(df_agregado, df_trans_original):
    """
    Calcula features necesarias para el modelo ML
    VERSIÓN CORREGIDA: Usa features_ml (estadísticas agregadas por cajero)
    """
    
    df_features = df_agregado.copy()
    
    # Renombrar para coincidir con el modelo
    df_features = df_features.rename(columns={
        'monto_dispensado': 'monto_total_dispensado'
    })
    
    # ========== FEATURES TEMPORALES ==========
    df_features['hora_del_dia'] = df_features['bucket_15min'].dt.hour
    df_features['dia_semana'] = df_features['bucket_15min'].dt.dayofweek
    df_features['mes'] = df_features['bucket_15min'].dt.month
    df_features['es_fin_de_semana'] = df_features['dia_semana'].isin([5, 6]).astype(int)
    
    # Fin de mes: últimos 3 días del mes
    df_features['dia_mes'] = df_features['bucket_15min'].dt.day
    df_features['dias_en_mes'] = df_features['bucket_15min'].dt.days_in_month
    df_features['es_fin_de_mes'] = (df_features['dias_en_mes'] - df_features['dia_mes'] <= 3).astype(int)
    
    # Quincena: días 14-16 o 29-31
    df_features['es_quincena'] = (
        ((df_features['dia_mes'] >= 14) & (df_features['dia_mes'] <= 16)) |
        (df_features['dia_mes'] >= 29)
    ).astype(int)
    
    # ========== CONSULTAR ESTADÍSTICAS DE BD ==========
    st.info("🔍 Consultando estadísticas históricas de cajeros (features_ml)...")
    
    cajeros_unicos = df_features['cod_cajero'].unique().tolist()
    
    # Query para obtener estadísticas agregadas por cajero
    query_stats_cajero = """
    SELECT 
        cod_cajero,
        dispensacion_promedio as cajero_mean,
        dispensacion_std as cajero_std,
        dispensacion_max,
        dispensacion_min,
        coef_variacion,
        num_periodos_15min,
        anomalias_3std,
        pct_anomalias_3std
    FROM features_ml
    WHERE cod_cajero = ANY(%s)
    """
    
    df_stats_cajero_bd = execute_query(query_stats_cajero, params=(cajeros_unicos,))
    
    if not df_stats_cajero_bd.empty:
        st.success(f"✅ Estadísticas encontradas para {len(df_stats_cajero_bd)} cajeros")
        
        # Merge con datos históricos
        df_features = df_features.merge(
            df_stats_cajero_bd[['cod_cajero', 'cajero_mean', 'cajero_std']], 
            on='cod_cajero', 
            how='left'
        )
        
        # Para cajeros SIN historial, usar stats del archivo actual
        cajeros_sin_historial = df_features[df_features['cajero_mean'].isna()]['cod_cajero'].unique()
        
        if len(cajeros_sin_historial) > 0:
            st.warning(f"⚠️ {len(cajeros_sin_historial)} cajeros sin historial, usando stats del archivo actual")
            
            # Calcular stats solo para cajeros sin historial
            stats_archivo = df_features[df_features['cod_cajero'].isin(cajeros_sin_historial)].groupby('cod_cajero')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
            stats_archivo.columns = ['cod_cajero', 'mean_archivo', 'std_archivo']
            
            # Rellenar los faltantes
            df_features = df_features.merge(stats_archivo, on='cod_cajero', how='left')
            df_features['cajero_mean'] = df_features['cajero_mean'].fillna(df_features['mean_archivo'])
            df_features['cajero_std'] = df_features['cajero_std'].fillna(df_features['std_archivo'])
            df_features = df_features.drop(columns=['mean_archivo', 'std_archivo'])
    else:
        st.warning("⚠️ No hay estadísticas en features_ml. Usando stats del archivo actual.")
        
        # Fallback: calcular del archivo actual
        stats_cajero = df_features.groupby('cod_cajero')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
        stats_cajero.columns = ['cod_cajero', 'cajero_mean', 'cajero_std']
        df_features = df_features.merge(stats_cajero, on='cod_cajero', how='left')
    
    # Calcular z_score_vs_cajero con datos históricos
    df_features['z_score_vs_cajero'] = (
        (df_features['monto_total_dispensado'] - df_features['cajero_mean']) / 
        df_features['cajero_std'].replace(0, 1)
    ).fillna(0)
    
    # ========== ESTADÍSTICAS POR HORA (calcular de alertas_dispensacion) ==========
    query_stats_hora = """
    SELECT 
        EXTRACT(HOUR FROM fecha_hora) as hora_del_dia,
        AVG(monto_dispensado) as hora_mean,
        STDDEV(monto_dispensado) as hora_std
    FROM alertas_dispensacion
    --WHERE fecha_hora >= NOW() - INTERVAL '90 days'
    GROUP BY EXTRACT(HOUR FROM fecha_hora)
    """
    
    df_stats_hora_bd = execute_query(query_stats_hora)
    
    if not df_stats_hora_bd.empty:
        df_stats_hora_bd['hora_del_dia'] = df_stats_hora_bd['hora_del_dia'].astype(int)
        df_features = df_features.merge(df_stats_hora_bd, on='hora_del_dia', how='left')
        
        # Rellenar horas sin historial con stats del archivo
        mask_sin_hora = df_features['hora_mean'].isna()
        if mask_sin_hora.any():
            stats_hora_archivo = df_features[mask_sin_hora].groupby('hora_del_dia')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
            stats_hora_archivo.columns = ['hora_del_dia', 'hora_mean_archivo', 'hora_std_archivo']
            df_features = df_features.merge(stats_hora_archivo, on='hora_del_dia', how='left')
            df_features['hora_mean'] = df_features['hora_mean'].fillna(df_features['hora_mean_archivo'])
            df_features['hora_std'] = df_features['hora_std'].fillna(df_features['hora_std_archivo'])
            df_features = df_features.drop(columns=['hora_mean_archivo', 'hora_std_archivo'])
    else:
        # Fallback
        stats_hora = df_features.groupby('hora_del_dia')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
        stats_hora.columns = ['hora_del_dia', 'hora_mean', 'hora_std']
        df_features = df_features.merge(stats_hora, on='hora_del_dia', how='left')
    
    df_features['z_score_vs_hora'] = (
        (df_features['monto_total_dispensado'] - df_features['hora_mean']) / 
        df_features['hora_std'].replace(0, 1)
    ).fillna(0)
    
    # ========== ESTADÍSTICAS POR DÍA DE SEMANA (calcular de alertas_dispensacion) ==========
    query_stats_dia = """
    SELECT 
        EXTRACT(DOW FROM fecha_hora) as dia_semana,
        AVG(monto_dispensado) as dia_mean,
        STDDEV(monto_dispensado) as dia_std
    FROM alertas_dispensacion
    --WHERE fecha_hora >= NOW() - INTERVAL '90 days'
    GROUP BY EXTRACT(DOW FROM fecha_hora)
    """
    
    df_stats_dia_bd = execute_query(query_stats_dia)
    
    if not df_stats_dia_bd.empty:
        df_stats_dia_bd['dia_semana'] = df_stats_dia_bd['dia_semana'].astype(int)
        df_features = df_features.merge(df_stats_dia_bd, on='dia_semana', how='left')
        
        # Rellenar días sin historial
        mask_sin_dia = df_features['dia_mean'].isna()
        if mask_sin_dia.any():
            stats_dia_archivo = df_features[mask_sin_dia].groupby('dia_semana')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
            stats_dia_archivo.columns = ['dia_semana', 'dia_mean_archivo', 'dia_std_archivo']
            df_features = df_features.merge(stats_dia_archivo, on='dia_semana', how='left')
            df_features['dia_mean'] = df_features['dia_mean'].fillna(df_features['dia_mean_archivo'])
            df_features['dia_std'] = df_features['dia_std'].fillna(df_features['dia_std_archivo'])
            df_features = df_features.drop(columns=['dia_mean_archivo', 'dia_std_archivo'])
    else:
        # Fallback
        stats_dia = df_features.groupby('dia_semana')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
        stats_dia.columns = ['dia_semana', 'dia_mean', 'dia_std']
        df_features = df_features.merge(stats_dia, on='dia_semana', how='left')
    
    df_features['z_score_vs_dia_semana'] = (
        (df_features['monto_total_dispensado'] - df_features['dia_mean']) / 
        df_features['dia_std'].replace(0, 1)
    ).fillna(0)
    
    # ========== PERCENTIL VS MES (del archivo actual es OK) ==========
    df_features['percentil_vs_mes'] = df_features.groupby('mes')['monto_total_dispensado'].rank(pct=True)
    
    # ========== FEATURES DE TENDENCIA ==========
    st.info("🔍 Consultando historial para tendencias temporales...")
    
    df_features = df_features.sort_values(['cod_cajero', 'bucket_15min'])
    
    # Query para obtener historial reciente (últimas 48h) de alertas_dispensacion
    query_historial_tendencias = """
    SELECT 
        cod_cajero,
        fecha_hora as bucket_15min,
        monto_dispensado as monto_total_dispensado
    FROM alertas_dispensacion
    WHERE cod_cajero = ANY(%s)
      --AND fecha_hora >= NOW() - INTERVAL '48 hours'
    ORDER BY cod_cajero, fecha_hora
    """
    
    df_historial_bd = execute_query(query_historial_tendencias, params=(cajeros_unicos,))
    
    if not df_historial_bd.empty:
        st.success(f"✅ {len(df_historial_bd):,} ventanas históricas cargadas para tendencias")
        
        # Combinar historial de BD con datos nuevos
        df_combinado = pd.concat([df_historial_bd, df_features[['cod_cajero', 'bucket_15min', 'monto_total_dispensado']]], ignore_index=True)
        df_combinado = df_combinado.sort_values(['cod_cajero', 'bucket_15min'])
        df_combinado = df_combinado.drop_duplicates(subset=['cod_cajero', 'bucket_15min'], keep='last')
        
        # Calcular features de tendencia con datos completos
        df_combinado['cambio_vs_anterior'] = df_combinado.groupby('cod_cajero')['monto_total_dispensado'].diff().fillna(0)
        df_combinado['cambio_vs_ayer'] = df_combinado.groupby('cod_cajero')['monto_total_dispensado'].diff(96).fillna(0)
        df_combinado['tendencia_24h'] = (
            df_combinado.groupby('cod_cajero')['monto_total_dispensado']
            .transform(lambda x: x.rolling(window=96, min_periods=1).mean())
        )
        df_combinado['volatilidad_reciente'] = (
            df_combinado.groupby('cod_cajero')['monto_total_dispensado']
            .transform(lambda x: x.rolling(window=96, min_periods=1).std())
        ).fillna(0)
        
        # Extraer solo las ventanas del archivo nuevo
        df_tendencias = df_combinado[df_combinado['bucket_15min'].isin(df_features['bucket_15min'])]
        
        # Merge con df_features
        df_features = df_features.merge(
            df_tendencias[['cod_cajero', 'bucket_15min', 'cambio_vs_anterior', 'cambio_vs_ayer', 'tendencia_24h', 'volatilidad_reciente']],
            on=['cod_cajero', 'bucket_15min'],
            how='left'
        )
    else:
        st.warning("⚠️ No hay historial reciente. Features de tendencia limitadas.")
        
        # Fallback: calcular solo con datos del archivo (limitado)
        df_features['cambio_vs_anterior'] = df_features.groupby('cod_cajero')['monto_total_dispensado'].diff().fillna(0)
        df_features['cambio_vs_ayer'] = 0  # No disponible sin historial
        df_features['tendencia_24h'] = df_features['monto_total_dispensado']  # Usar valor actual
        df_features['volatilidad_reciente'] = 0  # No disponible sin historial
    
    # ========== LIMPIAR ==========
    df_features = df_features.replace([np.inf, -np.inf], 0)
    df_features = df_features.fillna(0)
    
    # Eliminar columnas auxiliares
    cols_to_drop = ['cajero_mean', 'cajero_std', 'hora_mean', 'hora_std', 
                    'dia_mean', 'dia_std', 'dia_mes', 'dias_en_mes']
    df_features = df_features.drop(columns=[c for c in cols_to_drop if c in df_features.columns])
    
    # Mostrar resumen de features calculadas
    with st.expander("📊 Resumen de Features Calculadas"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Ventanas", f"{len(df_features):,}")
        with col2:
            z_score_max = df_features['z_score_vs_cajero'].abs().max()
            st.metric("Max |z-score|", f"{z_score_max:.1f}")
        with col3:
            anomalos = (df_features['z_score_vs_cajero'].abs() > 3).sum()
            st.metric("Ventanas > 3σ", f"{anomalos:,}")
    
    return df_features

# def calcular_features_ml(df_agregado, df_trans_original):
#     """Calcula features necesarias para el modelo ML"""
    
#     df_features = df_agregado.copy()
    
#     # Renombrar para coincidir con el modelo
#     df_features = df_features.rename(columns={
#         'monto_dispensado': 'monto_total_dispensado'
#     })
    
#     # ========== FEATURES TEMPORALES ==========
#     df_features['hora_del_dia'] = df_features['bucket_15min'].dt.hour
#     df_features['dia_semana'] = df_features['bucket_15min'].dt.dayofweek
#     df_features['mes'] = df_features['bucket_15min'].dt.month
#     df_features['es_fin_de_semana'] = df_features['dia_semana'].isin([5, 6]).astype(int)
    
#     # Fin de mes: últimos 3 días del mes
#     df_features['dia_mes'] = df_features['bucket_15min'].dt.day
#     df_features['dias_en_mes'] = df_features['bucket_15min'].dt.days_in_month
#     df_features['es_fin_de_mes'] = (df_features['dias_en_mes'] - df_features['dia_mes'] <= 3).astype(int)
    
#     # Quincena: días 14-16 o 29-31
#     df_features['es_quincena'] = (
#         ((df_features['dia_mes'] >= 14) & (df_features['dia_mes'] <= 16)) |
#         (df_features['dia_mes'] >= 29)
#     ).astype(int)
    
#     # ========== FEATURES DE DESVIACIÓN ==========
#     # Por cajero
#     stats_cajero = df_features.groupby('cod_cajero')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
#     stats_cajero.columns = ['cod_cajero', 'cajero_mean', 'cajero_std']
#     df_features = df_features.merge(stats_cajero, on='cod_cajero', how='left')
#     df_features['z_score_vs_cajero'] = (
#         (df_features['monto_total_dispensado'] - df_features['cajero_mean']) / 
#         df_features['cajero_std'].replace(0, 1)
#     ).fillna(0)
    
#     # Por hora del día
#     stats_hora = df_features.groupby('hora_del_dia')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
#     stats_hora.columns = ['hora_del_dia', 'hora_mean', 'hora_std']
#     df_features = df_features.merge(stats_hora, on='hora_del_dia', how='left')
#     df_features['z_score_vs_hora'] = (
#         (df_features['monto_total_dispensado'] - df_features['hora_mean']) / 
#         df_features['hora_std'].replace(0, 1)
#     ).fillna(0)
    
#     # Por día de semana
#     stats_dia = df_features.groupby('dia_semana')['monto_total_dispensado'].agg(['mean', 'std']).reset_index()
#     stats_dia.columns = ['dia_semana', 'dia_mean', 'dia_std']
#     df_features = df_features.merge(stats_dia, on='dia_semana', how='left')
#     df_features['z_score_vs_dia_semana'] = (
#         (df_features['monto_total_dispensado'] - df_features['dia_mean']) / 
#         df_features['dia_std'].replace(0, 1)
#     ).fillna(0)
    
#     # Percentil vs mes
#     df_features['percentil_vs_mes'] = df_features.groupby('mes')['monto_total_dispensado'].rank(pct=True)
    
#     # ========== FEATURES DE TENDENCIA ==========
#     df_features = df_features.sort_values(['cod_cajero', 'bucket_15min'])
    
#     # Cambio vs anterior (mismo cajero)
#     df_features['cambio_vs_anterior'] = df_features.groupby('cod_cajero')['monto_total_dispensado'].diff().fillna(0)
    
#     # Cambio vs mismo momento ayer (96 períodos = 24h en ventanas de 15min)
#     df_features['cambio_vs_ayer'] = df_features.groupby('cod_cajero')['monto_total_dispensado'].diff(96).fillna(0)
    
#     # Tendencia últimas 24h (promedio móvil)
#     df_features['tendencia_24h'] = (
#         df_features.groupby('cod_cajero')['monto_total_dispensado']
#         .transform(lambda x: x.rolling(window=96, min_periods=1).mean())
#     )
    
#     # Volatilidad reciente (std últimas 24h)
#     df_features['volatilidad_reciente'] = (
#         df_features.groupby('cod_cajero')['monto_total_dispensado']
#         .transform(lambda x: x.rolling(window=96, min_periods=1).std())
#     ).fillna(0)
    
#     # ========== LIMPIAR ==========
#     df_features = df_features.replace([np.inf, -np.inf], 0)
#     df_features = df_features.fillna(0)
    
#     # Eliminar columnas auxiliares
#     cols_to_drop = ['cajero_mean', 'cajero_std', 'hora_mean', 'hora_std', 
#                     'dia_mean', 'dia_std', 'dia_mes', 'dias_en_mes']
#     df_features = df_features.drop(columns=[c for c in cols_to_drop if c in df_features.columns])
    
#     return df_features

def seleccionar_features_modelo(df_features):
    """Selecciona las columnas necesarias para el modelo"""
    
    # Usar feature_names del modelo
    if feature_names_modelo is not None:
        features_modelo = feature_names_modelo
    else:
        # Fallback: lista manual de las 16 features
        features_modelo = [
            'monto_total_dispensado',
            'num_transacciones',
            'hora_del_dia',
            'dia_semana',
            'mes',
            'es_fin_de_semana',
            'es_fin_de_mes',
            'es_quincena',
            'z_score_vs_cajero',
            'z_score_vs_hora',
            'z_score_vs_dia_semana',
            'percentil_vs_mes',
            'cambio_vs_anterior',
            'cambio_vs_ayer',
            'tendencia_24h',
            'volatilidad_reciente'
        ]
    
    # Verificar
    features_disponibles = [f for f in features_modelo if f in df_features.columns]
    features_faltantes = [f for f in features_modelo if f not in df_features.columns]
    
    if features_faltantes:
        st.error(f"❌ Features faltantes: {features_faltantes}")
        return None, None
    
    return df_features[features_disponibles], features_disponibles

def generar_razon_anomalia(row):
    """Genera explicación de por qué es anómala"""
    razones = []
    
    if row.get('z_score_vs_cajero', 0) > 3:
        razones.append(f"Monto {row['z_score_vs_cajero']:.1f}σ sobre promedio del cajero")
    
    if row.get('z_score_vs_hora', 0) > 3:
        razones.append(f"Inusual para esta hora del día")
    
    if row.get('es_fin_de_semana', 0) == 1 and row.get('monto_total_dispensado', 0) > 10000000:
        razones.append("Monto alto en fin de semana")
    
    if row.get('num_transacciones', 0) > 15:
        razones.append(f"Alta frecuencia: {row['num_transacciones']} tx en 15min")
    
    if not razones:
        razones.append(f"Score de anomalía: {row.get('score_normalizado', 0):.1f}")
    
    return " | ".join(razones)

def mostrar_tabla_anomalias(df_anomalias, key_suffix=""):
    """Muestra tabla de anomalías con formato"""
    
    df_display = df_anomalias[[
        'cod_cajero', 'bucket_15min', 'monto_total_dispensado', 
        'num_transacciones', 'severidad', 'score_normalizado', 'razon'
    ]].sort_values('score_normalizado', ascending=False)
    
    def resaltar_severidad(row):
        if row['severidad'] == 'critico':
            return ['background-color: #ffebee'] * len(row)
        elif row['severidad'] == 'alto':
            return ['background-color: #fff3e0'] * len(row)
        elif row['severidad'] == 'medio':
            return ['background-color: #e8f5e9'] * len(row)
        return [''] * len(row)
    
    st.dataframe(
        df_display.style.apply(resaltar_severidad, axis=1),
        column_config={
            'cod_cajero': 'Cajero',
            'bucket_15min': st.column_config.DatetimeColumn('Fecha/Hora', format='DD/MM/YYYY HH:mm'),
            'monto_total_dispensado': st.column_config.NumberColumn('Monto', format='$%.0f'),
            'num_transacciones': 'Tx',
            'severidad': 'Severidad',
            'score_normalizado': st.column_config.NumberColumn('Score', format='%.1f'),
            'razon': st.column_config.TextColumn('Razón', width='large')
        },
        hide_index=True,
        height=400,
        width='stretch'
    )
    
    # Exportar
    if st.button("📥 Exportar", key=f"export_{key_suffix}"):
        csv = df_display.to_csv(index=False)
        st.download_button(
            "⬇️ Descargar CSV",
            csv,
            f"anomalias_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv",
            key=f"download_{key_suffix}"
        )

def mostrar_analisis_cajeros(df_anomalias, key_suffix=""):
    """Muestra mapa, historial y análisis de cajeros con anomalías"""
    
    st.markdown("---")
    st.markdown("#### 🗺️ Ubicación de Cajeros con Anomalías")
    
    # Obtener ubicaciones de los cajeros
    cajeros_anomalos = df_anomalias['cod_cajero'].unique().tolist()
    
    query_ubicaciones = """
    SELECT DISTINCT
        cod_cajero,
        latitud,
        longitud,
        municipio_dane,
        departamento
    FROM features_ml
    WHERE cod_cajero = ANY(%s)
      AND latitud IS NOT NULL
      AND longitud IS NOT NULL
    """
    
    df_ubicaciones = execute_query(query_ubicaciones, params=(cajeros_anomalos,))
    
    if not df_ubicaciones.empty:
        # Agregar conteo de anomalías por cajero y severidad
        conteo_anomalias = df_anomalias.groupby('cod_cajero').agg({
            'severidad': lambda x: (x == 'critico').sum(),
            'score_normalizado': 'max'
        }).reset_index()
        conteo_anomalias.columns = ['cod_cajero', 'num_criticas', 'max_score']
        
        df_ubicaciones = df_ubicaciones.merge(conteo_anomalias, on='cod_cajero', how='left')
        
        # Determinar severidad del cajero
        df_ubicaciones['severidad'] = df_ubicaciones['num_criticas'].apply(
            lambda x: 'critico' if x > 0 else 'alto'
        )
        
        df_ubicaciones['score_anomalia'] = df_ubicaciones['max_score']
        df_ubicaciones['monto_dispensado'] = 0
        
        # Crear descripción para hover
        df_ubicaciones['descripcion'] = df_ubicaciones.apply(
            lambda row: f"Cajero {row['cod_cajero']} | {int(row['num_criticas'])} críticas | Score: {row['max_score']:.1f}",
            axis=1
        )
        
        # Crear mapa
        fig_mapa = crear_mapa_alertas(df_ubicaciones)
        
        if fig_mapa:
            st.plotly_chart(fig_mapa, config={'displayModeBar': False})
            st.caption(f"🔴 Rojo = Cajeros con anomalías críticas | 🟠 Naranja = Cajeros con anomalías altas/medias")
        else:
            st.warning("No se pudo crear el mapa")
    else:
        st.info("No hay coordenadas disponibles para los cajeros")
    
    # ========== HISTORIAL DE ALERTAS ==========
    st.markdown("---")
    st.markdown("#### 📈 Historial de Alertas Acumuladas.")
    
    # Query para obtener historial
    query_historial = """
    SELECT 
        cod_cajero,
        DATE(fecha_hora) as fecha,
        COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
        COUNT(*) FILTER (WHERE severidad = 'alto') as altas,
        COUNT(*) FILTER (WHERE severidad = 'medio') as medias
    FROM alertas_dispensacion
    WHERE cod_cajero = ANY(%s)
      --AND fecha_hora >= NOW() - INTERVAL '30 days'
    GROUP BY cod_cajero, DATE(fecha_hora)
    ORDER BY fecha ASC
    """
    
    df_historial = execute_query(query_historial, params=(cajeros_anomalos,))
    
    if not df_historial.empty:
        # Crear gráfico acumulado por cajero
        fig_historial = go.Figure()
        
        # Limitar a top 10 cajeros con más críticas para legibilidad
        top_cajeros = df_historial.groupby('cod_cajero')['criticas'].sum().nlargest(10).index.tolist()
        
        for cajero in top_cajeros:
            df_cajero = df_historial[df_historial['cod_cajero'] == cajero].copy()
            
            if not df_cajero.empty:
                # Acumular alertas críticas
                df_cajero = df_cajero.sort_values('fecha')
                df_cajero['criticas_acumuladas'] = df_cajero['criticas'].cumsum()
                
                fig_historial.add_trace(go.Scatter(
                    x=df_cajero['fecha'],
                    y=df_cajero['criticas_acumuladas'],
                    mode='lines+markers',
                    name=f'Cajero {cajero}',
                    hovertemplate='<b>Cajero %{fullData.name}</b><br>' +
                                  'Fecha: %{x|%d/%m/%Y}<br>' +
                                  'Acumuladas: %{y}<br>' +
                                  '<extra></extra>'
                ))
        
        fig_historial.update_layout(
            title='Alertas Críticas Acumuladas por Cajero',
            xaxis_title='Fecha',
            yaxis_title='Alertas Críticas Acumuladas',
            height=450,
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            plot_bgcolor='white',
            xaxis=dict(gridcolor='lightgray'),
            yaxis=dict(gridcolor='lightgray')
        )
        
        st.plotly_chart(fig_historial, config={'displayModeBar': False})
        
        # Tabla resumen por cajero
        st.markdown("##### 📊 Resumen Histórico por Cajero")
        
        resumen_cajeros = df_historial.groupby('cod_cajero').agg({
            'criticas': 'sum',
            'altas': 'sum',
            'medias': 'sum'
        }).reset_index()
        resumen_cajeros['total'] = (
            resumen_cajeros['criticas'] + 
            resumen_cajeros['altas'] + 
            resumen_cajeros['medias']
        )
        resumen_cajeros = resumen_cajeros.sort_values('criticas', ascending=False)
        
        col_tabla, col_metricas = st.columns([3, 1])
        
        with col_tabla:
            st.dataframe(
                resumen_cajeros,
                column_config={
                    'cod_cajero': 'Cajero',
                    'criticas': st.column_config.NumberColumn('🔴 Críticas', format='%d'),
                    'altas': st.column_config.NumberColumn('🟠 Altas', format='%d'),
                    'medias': st.column_config.NumberColumn('🟡 Medias', format='%d'),
                    'total': st.column_config.NumberColumn('📊 Total', format='%d')
                },
                hide_index=True,
                width='stretch',
                height=300
            )
        
        with col_metricas:
            st.metric("📍 Cajeros analizados", len(cajeros_anomalos))
            st.metric("🔴 Total críticas", resumen_cajeros['criticas'].sum())
            st.metric("📊 Total alertas", resumen_cajeros['total'].sum())
    else:
        st.info("ℹ️ No hay historial previo en la base de datos para estos cajeros")
        st.caption("💡 Las alertas se registran cuando se procesa el pipeline completo de detección")

# ============================================================================
# PROCESAMIENTO DEL ARCHIVO
# ============================================================================

st.markdown("---")
st.markdown("### 2️⃣ Análisis del Archivo")

try:
    # Leer archivo
    content = uploaded_file.read().decode('utf-8')
    lines = content.strip().split('\n')
    
    st.success(f"✅ Archivo cargado: {uploaded_file.name}")
    
    # Parsear archivo
    header_info = None
    transacciones = []
    
    codigos_operacion = {
        '2': 'Retiro',
        '3': 'Consulta',
        '4': 'Avance',
        '5': 'Avance',
        '8': 'Otros'
    }
    
    ## TODOS: CODIGOS PARA FILTRAR LOS ARCHIVOS DE 15 MIN
    # codigos_procesar = {'2', '3', '4'}
    codigos_procesar = {'2'}
    
    for i, line in enumerate(lines):
        if not line.strip():
            continue
            
        parts = line.split(',')
        tipo_registro = parts[0]
        
        if tipo_registro == '01':
            try:
                header_info = {
                    'fecha_hora': datetime.strptime(parts[1], '%Y%m%d%H%M%S'),
                    'identificador': parts[2] if len(parts) > 2 else 'N/A'
                }
            except:
                pass
                
        elif tipo_registro == '02':
            try:
                cod_cajero = parts[1]
                cod_operacion = parts[2]
                
                if cod_operacion not in codigos_procesar:
                    continue
                
                monto = int(parts[3]) if parts[3] else 0
                timestamp_str = parts[4]
                
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
                except:
                    continue
                
                transaccion = {
                    'cod_cajero': cod_cajero,
                    'cod_operacion': cod_operacion,
                    'tipo_operacion': codigos_operacion.get(cod_operacion, 'Desconocido'),
                    'monto_dispensado': monto,
                    'timestamp': timestamp
                }
                
                transacciones.append(transaccion)
                
            except:
                continue
    
    if not transacciones:
        st.error("❌ No se encontraron transacciones válidas")
        st.stop()
    
    df_trans = pd.DataFrame(transacciones)
    
    # Métricas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Transacciones", f"{len(df_trans):,}")
    
    with col2:
        cajeros_unicos = df_trans['cod_cajero'].nunique()
        st.metric("🏧 Cajeros", f"{cajeros_unicos:,}")
    
    with col3:
        monto_total = df_trans['monto_dispensado'].sum()
        st.metric("💰 Monto Total", f"${monto_total:,.0f}")
    
    with col4:
        if header_info:
            st.metric("📅 Reporte", header_info['fecha_hora'].strftime('%Y-%m-%d %H:%M'))

except Exception as e:
    st.error(f"❌ Error al procesar archivo: {str(e)}")
    st.exception(e)
    st.stop()

st.markdown("---")

# ============================================================================
# AGREGACIÓN Y FEATURE ENGINEERING
# ============================================================================

st.markdown("### 3️⃣ Preparación de Datos para ML")

with st.spinner("🔄 Calculando features..."):
    
    # Crear bucket de 15 minutos
    df_trans['bucket_15min'] = df_trans['timestamp'].dt.floor('15min')
    
    # Agregar por cajero y ventana
    df_agregado = df_trans.groupby(['cod_cajero', 'bucket_15min']).agg({
        'monto_dispensado': 'sum',
        'timestamp': 'count'
    }).reset_index()
    
    df_agregado.columns = ['cod_cajero', 'bucket_15min', 'monto_dispensado', 'num_transacciones']
    
    st.info(f"📦 {len(df_agregado):,} ventanas de 15 minutos agregadas")
    
    # Calcular features para ML
    df_features = calcular_features_ml(df_agregado, df_trans)
    
    st.success(f"✅ Features calculadas: {len(df_features.columns)} columnas")
    
    # Mostrar algunas features
    with st.expander("🔍 Ver Features Calculadas"):
        st.write("**Primeras 5 filas con features:**")
        st.dataframe(df_features.head())

st.markdown("---")

# ============================================================================
# TABS: PRODUCCIÓN vs DEMO
# ============================================================================

st.markdown("### 4️⃣ Detección de Anomalías con Machine Learning")

tab1, tab2 = st.tabs(["🔬 Modo Producción", "🎯 Modo Demo"])

# ============================================================================
# TAB 1: MODO PRODUCCIÓN
# ============================================================================

with tab1:
    st.info("""
    **Modo Producción:** Usa el umbral del modelo entrenado (`contamination=0.01`).
    Solo detecta anomalías que superan este umbral estricto.
    """)
    
    if modelo_ml is None:
        st.error("❌ Modelo no disponible")
        st.stop()
    
    col_det1, col_det2 = st.columns([1, 4])
    
    with col_det1:
        detectar_prod = st.button(
            "🤖 Detectar",
            type="primary",
            width='stretch',
            key="detectar_prod"
        )
    
    if detectar_prod:
        with st.spinner("🤖 Aplicando modelo ML..."):
            try:
                # Seleccionar features
                X_predict, features_usadas = seleccionar_features_modelo(df_features)
                
                if X_predict is None:
                    st.stop()
                
                st.info(f"📊 Usando {len(features_usadas)} features")
                
                # Normalizar con scaler del modelo
                loaded_obj = joblib.load(Path(dashboard_path).parent / 'models' / 'isolation_forest_dispensacion_v2.pkl')
                if 'scaler' in loaded_obj:
                    scaler = loaded_obj['scaler']
                    X_scaled = scaler.transform(X_predict)
                else:
                    X_scaled = X_predict
                
                # Predicciones
                predictions = modelo_ml.predict(X_scaled)
                scores = modelo_ml.score_samples(X_scaled)
                
                # Agregar a DataFrame
                df_features['prediccion'] = predictions
                df_features['score_anomalia'] = scores
                df_features['es_anomalia'] = predictions == -1
                
                # Normalizar scores
                min_score = scores.min()
                max_score = scores.max()
                df_features['score_normalizado'] = (
                    ((scores - min_score) / (max_score - min_score)) * 100
                ).round(1)
                df_features['score_normalizado'] = 100 - df_features['score_normalizado']
                
                # Clasificar severidad
                def clasificar_severidad(row):
                    if row['es_anomalia']:
                        if row['score_normalizado'] >= 90:
                            return 'critico'
                        elif row['score_normalizado'] >= 75:
                            return 'alto'
                        else:
                            return 'medio'
                    return 'normal'
                
                df_features['severidad'] = df_features.apply(clasificar_severidad, axis=1)
                df_features['razon'] = df_features.apply(generar_razon_anomalia, axis=1)
                
                # Resultados
                anomalias_detectadas = df_features['es_anomalia'].sum()
                
                st.success("✅ Detección completada")
                
                # Métricas
                col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                
                with col_m1:
                    criticas = (df_features['severidad'] == 'critico').sum()
                    st.metric("🔴 Críticas", f"{criticas:,}")
                
                with col_m2:
                    altas = (df_features['severidad'] == 'alto').sum()
                    st.metric("🟠 Altas", f"{altas:,}")
                
                with col_m3:
                    medias = (df_features['severidad'] == 'medio').sum()
                    st.metric("🟡 Medias", f"{medias:,}")
                
                with col_m4:
                    pct = (anomalias_detectadas / len(df_features) * 100) if len(df_features) > 0 else 0
                    st.metric("🚨 Total", f"{anomalias_detectadas:,}", f"{pct:.2f}%")
                
                # Mostrar tabla si hay anomalías
                if anomalias_detectadas > 0:
                    st.markdown("#### 🚨 Anomalías Detectadas")
                    
                    df_anomalias = df_features[df_features['es_anomalia']].copy()
                    mostrar_tabla_anomalias(df_anomalias, key_suffix="prod")
                    
                    # Mostrar análisis adicional
                    mostrar_analisis_cajeros(df_anomalias, key_suffix="prod")
                else:
                    st.info("✅ No se detectaron anomalías con el umbral de producción")
                    st.caption("💡 Prueba el Modo Demo para ver las ventanas más sospechosas")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)

# ============================================================================
# TAB 2: MODO DEMO
# ============================================================================

with tab2:
    st.info("""
    **Modo Demo:** Muestra las ventanas más sospechosas aunque no superen el umbral.
    Útil para demostraciones y análisis exploratorio.
    """)
    
    if modelo_ml is None:
        st.error("❌ Modelo no disponible")
        st.stop()
    
    # Control de sensibilidad
    col_sens1, col_sens2 = st.columns([2, 1])
    
    with col_sens1:
        percentil_demo = st.slider(
            "Sensibilidad de detección",
            min_value=1,
            max_value=5,
            value=2,
            help="% de ventanas más sospechosas a marcar"
        )
    
    with col_sens2:
        st.metric("Ventanas a revisar", f"~{int(len(df_features) * percentil_demo / 100):,}")
    
    col_det_demo1, col_det_demo2 = st.columns([1, 4])
    
    with col_det_demo1:
        detectar_demo = st.button(
            "🎯 Detectar Demo",
            type="primary",
            width='stretch',
            key="detectar_demo"
        )
    
    if detectar_demo:
        with st.spinner("🎯 Aplicando detección demo..."):
            try:
                # Seleccionar features
                X_predict, features_usadas = seleccionar_features_modelo(df_features)
                
                if X_predict is None:
                    st.stop()
                
                # Normalizar
                loaded_obj = joblib.load(Path(dashboard_path).parent / 'models' / 'isolation_forest_dispensacion_v2.pkl')
                if 'scaler' in loaded_obj:
                    X_scaled = loaded_obj['scaler'].transform(X_predict)
                else:
                    X_scaled = X_predict
                
                # Scores
                scores = modelo_ml.score_samples(X_scaled)
                
                df_features['score_anomalia'] = scores
                
                # Normalizar scores
                min_score = scores.min()
                max_score = scores.max()
                df_features['score_normalizado'] = (
                    100 - ((scores - min_score) / (max_score - min_score)) * 100
                ).round(1)
                
                # Marcar top N% como sospechosas
                n_sospechosas = max(1, int(len(df_features) * percentil_demo / 100))
                df_sorted = df_features.nlargest(n_sospechosas, 'score_normalizado')
                
                df_features['es_anomalia'] = False
                df_features.loc[df_sorted.index, 'es_anomalia'] = True
                
                # Clasificar severidad
                def clasificar_demo(score):
                    if score >= 90:
                        return 'critico'
                    elif score >= 75:
                        return 'alto'
                    else:
                        return 'medio'
                
                df_features['severidad'] = 'normal'
                df_features.loc[df_features['es_anomalia'], 'severidad'] = (
                    df_features.loc[df_features['es_anomalia'], 'score_normalizado'].apply(clasificar_demo)
                )
                
                # Generar razones
                df_features['razon'] = df_features.apply(generar_razon_anomalia, axis=1)
                
                # Resultados
                st.success("✅ Detección demo completada")
                
                col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                
                with col_m1:
                    criticas = (df_features['severidad'] == 'critico').sum()
                    st.metric("🔴 Críticas", f"{criticas:,}")
                
                with col_m2:
                    altas = (df_features['severidad'] == 'alto').sum()
                    st.metric("🟠 Altas", f"{altas:,}")
                
                with col_m3:
                    medias = (df_features['severidad'] == 'medio').sum()
                    st.metric("🟡 Medias", f"{medias:,}")
                
                with col_m4:
                    st.metric("🎯 Sospechosas", f"{n_sospechosas:,}")
                
                # Tabla
                st.markdown("#### 🎯 Ventanas Más Sospechosas")
                
                df_sospechosas = df_features[df_features['es_anomalia']].copy()
                mostrar_tabla_anomalias(df_sospechosas, key_suffix="demo")
                
                # Mostrar análisis adicional
                mostrar_analisis_cajeros(df_sospechosas, key_suffix="demo")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p><strong>Sistema de Detección de Fraudes ATM</strong></p>
    <p>Usando Machine Learning (Isolation Forest)</p>
</div>
""", unsafe_allow_html=True)