#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
DASHBOARD DE DETECCIÓN DE FRAUDES - Streamlit
============================================================================

Dashboard interactivo para visualizar anomalías detectadas en tiempo real.

Características:
- Vista general de anomalías
- Filtros por fecha, nivel, cajero
- Mapa de anomalías (si hay coordenadas)
- Tabla de alertas con razones
- Gráficos de tendencias

Uso:
    streamlit run dashboard.py

============================================================================
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import yaml
from datetime import datetime, timedelta

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

st.set_page_config(
    page_title="🚨 Detección de Fraudes ATM",
    page_icon="🏧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CONEXIÓN A BD
# ============================================================================

@st.cache_resource
def get_connection():
    """Conecta a PostgreSQL"""
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    pg = config['postgres']
    connection_string = (
        f"postgresql://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['database']}"
    )
    return create_engine(connection_string)

engine = get_connection()

# ============================================================================
# FUNCIONES DE DATOS
# ============================================================================

@st.cache_data(ttl=300)  # Cache 5 minutos
def load_anomalias(fecha_inicio, fecha_fin, nivel_filter):
    """Carga anomalías desde PostgreSQL"""
    
    query = f"""
        SELECT 
            s.id_transaccion,
            t.fecha_transaccion,
            t.cod_terminal,
            t.tipo_operacion,
            t.valor_transaccion,
            s.score_final,
            s.nivel_anomalia,
            s.modelo_usado,
            c.municipio_dane as municipio,
            c.departamento,
            c.latitud,
            c.longitud
        FROM scores s
        JOIN transacciones t ON s.id_transaccion = t.id_tlf
        LEFT JOIN cajeros c ON t.cod_terminal::BIGINT = c.codigo
        WHERE t.fecha_transaccion >= '{fecha_inicio}' 
        AND  t.fecha_transaccion < '{fecha_fin}'
        {'AND s.nivel_anomalia = ' + "'" + nivel_filter + "'" if nivel_filter != 'Todos' else ''}
        ORDER BY s.score_final DESC
        LIMIT 10000
    """
    
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_razones(id_tlf):
    """Carga razones de una anomalía específica"""
    
    query = f"""
        SELECT 
            tipo_razon,
            descripcion,
            severidad::int AS severidad
        FROM razones_anomalias
        WHERE id_transaccion = {id_tlf}
        ORDER BY severidad DESC, orden
    """
    
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_stats():
    """Carga estadísticas generales"""
    
    query = """
        SELECT 
            COUNT(*) as total_anomalias,
            COUNT(CASE WHEN nivel_anomalia = 'Crítico' THEN 1 END) as criticas,
            COUNT(CASE WHEN nivel_anomalia = 'Advertencia' THEN 1 END) as advertencias,
            COUNT(CASE WHEN nivel_anomalia = 'Sospechoso' THEN 1 END) as sospechosas,
            COUNT(DISTINCT s.id_transaccion) as transacciones_unicas
        FROM scores s
        JOIN transacciones t ON s.id_transaccion = t.id_tlf
    """
    
    return pd.read_sql(query, engine).iloc[0]

# ============================================================================
# INTERFAZ
# ============================================================================

# Título
st.title("🚨 Sistema de Detección de Fraudes en ATMs")
st.markdown("---")

# Sidebar - Filtros
st.sidebar.header("🔍 Filtros")

fecha_inicio = st.sidebar.date_input(
    "Fecha inicio",
    value=datetime.now() - timedelta(days=7),
    max_value=datetime.now()
)

fecha_fin = st.sidebar.date_input(
    "Fecha fin",
    value=datetime.now(),
    max_value=datetime.now()
)

nivel_filter = st.sidebar.selectbox(
    "Nivel de anomalía",
    ["Todos", "Crítico", "Advertencia", "Sospechoso"]
)

# Botón de actualizar
if st.sidebar.button("🔄 Actualizar datos", type="primary"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info(f"Última actualización: {datetime.now().strftime('%H:%M:%S')}")

# ============================================================================
# MÉTRICAS GENERALES
# ============================================================================

try:
    stats = load_stats()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🔴 Críticas",
            f"{stats['criticas']:,}",
            delta=None
        )
    
    with col2:
        st.metric(
            "🟡 Advertencias",
            f"{stats['advertencias']:,}",
            delta=None
        )
    
    with col3:
        st.metric(
            "🟢 Sospechosas",
            f"{stats['sospechosas']:,}",
            delta=None
        )
    
    with col4:
        st.metric(
            "📊 Total",
            f"{stats['total_anomalias']:,}",
            delta=None
        )
    
    st.markdown("---")
    
except Exception as e:
    st.error(f"Error al cargar estadísticas: {e}")

# ============================================================================
# CARGAR DATOS
# ============================================================================

with st.spinner("Cargando anomalías..."):
    df = load_anomalias(fecha_inicio, fecha_fin, nivel_filter)

if len(df) == 0:
    st.warning("No se encontraron anomalías con los filtros seleccionados.")
    st.stop()

st.success(f"✅ {len(df):,} anomalías cargadas")

# ============================================================================
# GRÁFICOS
# ============================================================================

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📊 Distribución por Nivel")
    
    fig_nivel = px.pie(
        df,
        names='nivel_anomalia',
        title='Anomalías por Nivel de Severidad',
        color='nivel_anomalia',
        color_discrete_map={
            'Crítico': '#FF4B4B',
            'Advertencia': '#FFA500',
            'Sospechoso': '#FFD700'
        }
    )
    st.plotly_chart(fig_nivel, use_container_width=True)

with col_right:
    st.subheader("📈 Tendencia Temporal")
    
    df['fecha'] = pd.to_datetime(df['fecha_transaccion']).dt.date
    tendencia = df.groupby(['fecha', 'nivel_anomalia']).size().reset_index(name='count')
    
    fig_tendencia = px.line(
        tendencia,
        x='fecha',
        y='count',
        color='nivel_anomalia',
        title='Anomalías por Día',
        color_discrete_map={
            'Crítico': '#FF4B4B',
            'Advertencia': '#FFA500',
            'Sospechoso': '#FFD700'
        }
    )
    st.plotly_chart(fig_tendencia, use_container_width=True)

# ============================================================================
# MAPA (si hay coordenadas)
# ============================================================================
#st.write("DEBUG COORDENADAS")
#st.write(df[['cod_terminal', 'latitud', 'longitud']].head(50))
#st.write(df[['latitud', 'longitud']].dtypes)

df['latitud'] = pd.to_numeric(df['latitud'], errors='coerce')
df['longitud'] = pd.to_numeric(df['longitud'], errors='coerce')


if df['latitud'].notna().any() and df['longitud'].notna().any():
    st.subheader("🗺️ Mapa de Anomalías")
    
    df_mapa = df.dropna(subset=['latitud', 'longitud'])
    
    fig_mapa = px.scatter_map(
        df_mapa,
        lat='latitud',
        lon='longitud',
        color='nivel_anomalia',
        size='score_final',
        hover_data=['cod_terminal', 'municipio', 'tipo_operacion', 'valor_transaccion'],
        color_discrete_map={
            'Crítico': '#FF4B4B',
            'Advertencia': '#FFA500',
            'Sospechoso': '#FFD700'
        },
        zoom=5,
        height=500
    )
    
    fig_mapa.update_layout(mapbox_style="open-street-map")
    st.plotly_chart(fig_mapa, use_container_width=True)

# ============================================================================
# TOP CAJEROS PROBLEMÁTICOS
# ============================================================================

st.subheader("🏧 Top 10 Cajeros con Más Anomalías")

top_cajeros = df.groupby(['cod_terminal', 'municipio']).agg({
    'id_transaccion': 'count',
    'score_final': 'mean'
}).reset_index()

top_cajeros.columns = ['Cajero', 'Municipio', 'Anomalías', 'Score Promedio']
top_cajeros = top_cajeros.sort_values('Anomalías', ascending=False).head(10)

st.dataframe(
    top_cajeros.style.background_gradient(subset=['Anomalías'], cmap='Reds'),
    use_container_width=True
)

# ============================================================================
# TABLA DE ANOMALÍAS
# ============================================================================

st.subheader("📋 Detalle de Anomalías")

# Formatear para display
df_display = df.copy()
df_display['fecha_transaccion'] = pd.to_datetime(df_display['fecha_transaccion']).dt.strftime('%Y-%m-%d %H:%M')
df_display['valor_transaccion'] = df_display['valor_transaccion'].apply(lambda x: f"${x:,.0f}")
df_display['score_final'] = df_display['score_final'].apply(lambda x: f"{x:.3f}")

# Seleccionar columnas a mostrar
columnas_display = [
    'id_transaccion', 'fecha_transaccion', 'cod_terminal', 'municipio',
    'tipo_operacion', 'valor_transaccion', 'score_final', 'nivel_anomalia'
]

st.dataframe(
    df_display[columnas_display],
    use_container_width=True,
    height=400
)

# ============================================================================
# DETALLE DE ANOMALÍA SELECCIONADA
# ============================================================================

st.markdown("---")
st.subheader("🔍 Detalle de Anomalía")

id_tlf_selected = st.selectbox(
    "Seleccionar transacción:",
    df['id_transaccion'].tolist(),
    format_func=lambda x: f"ID: {x} - {df[df['id_transaccion']==x]['tipo_operacion'].iloc[0]} - ${df[df['id_transaccion']==x]['valor_transaccion'].iloc[0]:,.0f}"
)

if id_tlf_selected:
    row = df[df['id_transaccion'] == id_tlf_selected].iloc[0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Fecha", row['fecha_transaccion'].strftime('%Y-%m-%d %H:%M'))
        st.metric("Cajero", row['cod_terminal'])
    
    with col2:
        st.metric("Tipo", row['tipo_operacion'])
        st.metric("Monto", f"${row['valor_transaccion']:,.0f}")
    
    with col3:
        st.metric("Score", f"{row['score_final']:.3f}")
        st.metric("Nivel", row['nivel_anomalia'])
    
    # Cargar razones
    st.markdown("#### 📝 Razones de la Anomalía:")
    
    try:
        razones = load_razones(id_tlf_selected)
        
        if len(razones) > 0:
            for idx, razon in razones.iterrows():
                severity_emoji = "🔴" if razon['severidad'] >= 8 else "🟡" if razon['severidad'] >= 6 else "🟢"
                st.markdown(f"{severity_emoji} **[{razon['tipo_razon']}]** {razon['descripcion']} (Severidad: {razon['severidad']}/10)")
        else:
            st.info("No hay razones detalladas disponibles para esta anomalía.")
    
    except Exception as e:
        st.warning(f"No se pudieron cargar las razones: {e}")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.caption("Sistema de Detección de Fraudes en ATMs | Powered by Python + PostgreSQL + Streamlit")