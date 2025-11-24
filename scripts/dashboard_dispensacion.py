#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
DASHBOARD DE DETECCIÓN DE ANOMALÍAS EN DISPENSACIÓN - Streamlit
============================================================================

Correcciones aplicadas:
1. Ajuste de nombres de columnas (municipio -> municipio_dane).
2. Eliminación de filtros de tiempo restrictivos para datos de junio.
3. Mejora visual en gráficas de barras (ejes categóricos).
4. Actualización de parámetros deprecados de Streamlit.

============================================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yaml
import os
import sys
import subprocess
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

st.set_page_config(
    page_title="🏧 Detección de Fraudes - Dispensación",
    page_icon="🏧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS personalizados
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .alert-critico {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .alert-advertencia {
        background-color: #fff8e1;
        border-left: 4px solid #ff9800;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .alert-sospechoso {
        background-color: #f1f8e9;
        border-left: 4px solid #8bc34a;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONEXIÓN A BD
# ============================================================================

@st.cache_resource
def get_connection():
    """Conecta a PostgreSQL"""
    try:
        config_path = 'config.yaml'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            pg = config['postgres']
        else:
            st.error("❌ No se encontró config.yaml")
            st.stop()
        
        connection_string = (
            f"postgresql://{pg['user']}:{pg['password']}"
            f"@{pg['host']}:{pg['port']}/{pg['database']}"
        )
        return create_engine(connection_string, poolclass=NullPool)
    except Exception as e:
        st.error(f"❌ Error al conectar a PostgreSQL: {e}")
        st.stop()

engine = get_connection()

# ============================================================================
# FUNCIONES DE DATOS
# ============================================================================

@st.cache_data(ttl=300)
def load_datos_historicos_agregados():
    """Carga estadísticas agregadas de datos históricos"""
    # Se eliminó el HAVING COUNT > 100 por si hay pocos datos en pruebas
    query = """
        SELECT 
            cod_terminal,
            COUNT(*) as num_periodos,
            AVG(monto_total_dispensado) as dispensacion_promedio,
            STDDEV(monto_total_dispensado) as dispensacion_std,
            MAX(monto_total_dispensado) as dispensacion_max,
            MIN(bucket_15min) as fecha_inicio,
            MAX(bucket_15min) as fecha_fin
        FROM mv_dispensacion_por_cajero_15min
        GROUP BY cod_terminal
        ORDER BY AVG(monto_total_dispensado) DESC
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error al cargar datos históricos: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datos_mapa_riesgo(dias=180):
    """Carga datos para el mapa de calor de riesgo"""
    # Score: Crítico=5, Advertencia=1, Sospechoso=0.2
    query = f"""
        SELECT 
            a.cod_cajero,
            c.latitud, 
            c.longitud, 
            c.municipio_dane, 
            COUNT(*) as total_eventos,
            SUM(CASE 
                WHEN a.severidad = 'Crítico' THEN 5 
                WHEN a.severidad = 'Advertencia' THEN 1 
                ELSE 0.2 
            END) as risk_score
        FROM alertas_dispensacion a
        JOIN cajeros c ON a.cod_cajero = c.codigo::VARCHAR
        WHERE a.fecha_hora >= NOW() - INTERVAL '{dias} days'
        AND c.latitud IS NOT NULL
        GROUP BY a.cod_cajero, c.latitud, c.longitud, c.municipio_dane
        ORDER BY risk_score DESC
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error mapa: {e}")
        return pd.DataFrame()
    
@st.cache_data(ttl=300)
def load_dispensacion_por_hora():
    """Carga patrón de dispensación por hora del día"""
    # CORRECCIÓN: Se eliminó el filtro WHERE bucket_15min >= NOW() - 90 days
    # para permitir ver datos antiguos (ej. Junio 2025)
    query = """
        SELECT 
            EXTRACT(HOUR FROM bucket_15min) as hora,
            AVG(monto_total_dispensado) as dispensacion_promedio,
            COUNT(*) as num_transacciones,
            STDDEV(monto_total_dispensado) as dispensacion_std
        FROM mv_dispensacion_por_cajero_15min
        GROUP BY EXTRACT(HOUR FROM bucket_15min)
        ORDER BY hora
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error al cargar patrón horario: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_cajeros_volatiles(limit=20):
    """Carga cajeros con mayor volatilidad histórica"""
    # CORRECCIÓN: c.municipio -> c.municipio_dane
    query = f"""
        SELECT 
            f.cod_cajero,
            f.dispensacion_promedio,
            f.dispensacion_std,
            f.coef_variacion,
            f.pct_anomalias_3std,
            c.municipio_dane as municipio,
            c.departamento
        FROM features_ml f
        LEFT JOIN cajeros c ON f.cod_cajero = c.codigo::VARCHAR
        WHERE f.coef_variacion IS NOT NULL
        ORDER BY f.coef_variacion DESC
        LIMIT {limit}
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error al cargar cajeros volátiles: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_alertas_historicas(fecha_inicio, fecha_fin, severidad_filter):
    """Carga alertas históricas desde PostgreSQL"""
    # Corrección: Se usa cast explícito a fecha para evitar problemas de comparación
    severidad_sql = ""
    if severidad_filter != 'Todos':
        severidad_sql = f"AND severidad = '{severidad_filter}'"

    query = f"""
        SELECT 
            id,
            cod_cajero,
            fecha_hora,
            tipo_anomalia,
            severidad,
            score_anomalia,
            monto_dispensado,
            monto_esperado,
            desviacion_std,
            descripcion,
            fecha_deteccion
        FROM alertas_dispensacion
        WHERE fecha_hora::DATE >= '{fecha_inicio}' 
        AND fecha_hora::DATE <= '{fecha_fin}'
        {severidad_sql}
        ORDER BY fecha_deteccion DESC, score_anomalia DESC
        LIMIT 1000
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error al cargar alertas: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_cajeros_ubicacion():
    """Carga ubicación de cajeros"""
    # CORRECCIÓN: c.municipio -> c.municipio_dane
    query = """
        SELECT 
            codigo,
            latitud,
            longitud,
            municipio_dane as municipio,
            departamento
        FROM cajeros
        WHERE latitud IS NOT NULL 
        AND longitud IS NOT NULL
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error al cargar cajeros: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_stats_generales():
    """Carga estadísticas generales"""
    # Eliminamos filtro de fecha estricto para mostrar datos si es una prueba antigua
    query = """
        SELECT 
            COUNT(*) as total_alertas,
            COUNT(CASE WHEN severidad = 'Crítico' THEN 1 END) as criticas,
            COUNT(CASE WHEN severidad = 'Advertencia' THEN 1 END) as advertencias,
            COUNT(CASE WHEN severidad = 'Sospechoso' THEN 1 END) as sospechosas,
            COUNT(DISTINCT cod_cajero) as cajeros_afectados,
            MAX(fecha_deteccion) as ultima_actualizacion
        FROM alertas_dispensacion
    """
    try:
        df = pd.read_sql(query, engine)
        return df.iloc[0]
    except Exception as e:
        st.error(f"Error al cargar estadísticas: {e}")
        return None

@st.cache_data(ttl=300)
def load_top_cajeros_problematicos(limit=10):
    """Carga top cajeros con más alertas"""
    # CORRECCIÓN: c.municipio -> c.municipio_dane
    query = f"""
        SELECT 
            a.cod_cajero,
            c.municipio_dane as municipio,
            c.departamento,
            COUNT(*) as num_alertas,
            COUNT(CASE WHEN a.severidad = 'Crítico' THEN 1 END) as criticas,
            AVG(a.score_anomalia) as score_promedio,
            MAX(a.fecha_hora) as ultima_alerta
        FROM alertas_dispensacion a
        LEFT JOIN cajeros c ON a.cod_cajero = c.codigo::VARCHAR
        GROUP BY a.cod_cajero, c.municipio_dane, c.departamento
        ORDER BY num_alertas DESC
        LIMIT {limit}
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error al cargar top cajeros: {e}")
        return pd.DataFrame()

# ============================================================================
# PÁGINA 1: HOME - RESUMEN GENERAL
# ============================================================================

def page_home():
    """Página principal con resumen general"""
    st.markdown('<div class="main-header">🏧 Sistema de Detección de Anomalías en Dispensación</div>', 
                unsafe_allow_html=True)
    st.markdown("---")
    
    stats = load_stats_generales()
    tiene_alertas = stats is not None and stats['total_alertas'] > 0
    
    if tiene_alertas:
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1: st.metric("📊 Total Alertas", f"{int(stats['total_alertas']):,}")
        with col2: st.metric("🔴 Críticas", f"{int(stats['criticas']):,}")
        with col3: st.metric("🟡 Advertencias", f"{int(stats['advertencias']):,}")
        with col4: st.metric("🟢 Sospechosas", f"{int(stats['sospechosas']):,}")
        with col5: st.metric("🏧 Cajeros Afectados", f"{int(stats['cajeros_afectados']):,}")
    else:
        st.info("ℹ️  No hay alertas detectadas aún. Mostrando análisis de datos históricos.")
        df_historico = load_datos_historicos_agregados()
        if not df_historico.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric("🏧 Cajeros Totales", f"{len(df_historico):,}")
            with col2: st.metric("💰 Promedio Global", f"${df_historico['dispensacion_promedio'].mean():,.0f}")
            with col3: st.metric("📊 Registros", f"{df_historico['num_periodos'].sum():,.0f}")
            with col4: 
                dias = (df_historico['fecha_fin'].max() - df_historico['fecha_inicio'].min()).days
                st.metric("📅 Días Historia", f"{dias}")
    
    st.markdown("---")
    col_izq, col_der = st.columns(2)
    
    with col_izq:
        if tiene_alertas:
            st.subheader("📈 Tendencia de Alertas")
            query_tendencia = """
                SELECT DATE(fecha_hora) as fecha, severidad, COUNT(*) as cantidad
                FROM alertas_dispensacion
                GROUP BY DATE(fecha_hora), severidad
                ORDER BY fecha
            """
            try:
                df_tendencia = pd.read_sql(query_tendencia, engine)
                if not df_tendencia.empty:
                    fig = px.line(df_tendencia, x='fecha', y='cantidad', color='severidad',
                                 color_discrete_map={'Crítico': '#f44336', 'Advertencia': '#ff9800', 'Sospechoso': '#8bc34a'})
                    st.plotly_chart(fig, use_container_width=True)
            except: pass
        else:
            st.subheader("📊 Patrón Horario de Dispensación")
            df_horario = load_dispensacion_por_hora()
            if not df_horario.empty:
                fig = px.bar(df_horario, x='hora', y='dispensacion_promedio',
                           title='Dispensación Promedio por Hora del Día')
                # Asegurar que el eje X muestre todas las horas
                fig.update_xaxes(tickmode='linear', dtick=1)
                st.plotly_chart(fig, use_container_width=True)
    
    with col_der:
        if tiene_alertas:
            st.subheader("🏆 Top 10 Cajeros con Alertas")
            df_top = load_top_cajeros_problematicos(10)
            if not df_top.empty:
                # Convertir cajero a string para evitar huecos en la gráfica
                df_top['cod_cajero'] = df_top['cod_cajero'].astype(str)
                fig = px.bar(df_top, x='num_alertas', y='cod_cajero', orientation='h',
                           color='criticas', color_continuous_scale='Reds')
                fig.update_yaxes(type='category', categoryorder='total ascending')
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.subheader("⚠️ Top 20 Cajeros Volátiles")
            df_volatiles = load_cajeros_volatiles(20)
            if not df_volatiles.empty:
                df_volatiles['cod_cajero'] = df_volatiles['cod_cajero'].astype(str)
                fig = px.bar(df_volatiles, x='coef_variacion', y='cod_cajero', orientation='h',
                           title='Cajeros con Mayor Variabilidad', color='pct_anomalias_3std',
                           color_continuous_scale='Reds')
                fig.update_yaxes(type='category', categoryorder='total ascending')
                st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# PÁGINA 2: ANÁLISIS HISTÓRICO
# ============================================================================

def page_analisis_historico():
    """Página de análisis de datos históricos con Mapa Integrado"""
    st.title("📈 Análisis Histórico de Dispensación")
    
    # 1. Carga de Datos
    df_historico = load_datos_historicos_agregados()
    
    if df_historico.empty:
        st.warning("No hay datos históricos de dispensación disponibles")
        return
    
    # 2. KPIs Generales
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("🏧 Total Cajeros", f"{len(df_historico):,}")
    with col2: st.metric("💰 Dispensación Promedio", f"${df_historico['dispensacion_promedio'].mean():,.0f}")
    with col3: st.metric("📊 Períodos Registrados", f"{df_historico['num_periodos'].sum():,.0f}")
    with col4: 
        dias = (df_historico['fecha_fin'].max() - df_historico['fecha_inicio'].min()).days
        st.metric("📅 Días de Historia", f"{dias}")
    
    st.markdown("---")

    # ============================================================
    # 3. NUEVO: MAPA DE RIESGO HISTÓRICO INTEGRADO
    # ============================================================
    st.subheader("🗺️ Mapa de Riesgo Histórico (Últimos 180 días)")
    st.markdown("Cajeros coloreados según su **Score de Riesgo** acumulado (Rojo = Alto historial de anomalías).")
    
    df_mapa = load_datos_mapa_riesgo(dias=180)
    
    if not df_mapa.empty:
        # Definir centro del mapa basado en promedio de datos o default (Colombia)
        lat_center = df_mapa['latitud'].mean()
        lon_center = df_mapa['longitud'].mean()

        fig_mapa = px.scatter_mapbox(
            df_mapa,
            lat='latitud',
            lon='longitud',
            color='risk_score',  # Color por riesgo acumulado
            size='total_eventos', # Tamaño por cantidad de alertas
            color_continuous_scale="RdYlGn_r", # Rojo (Alto) a Verde (Bajo)
            # Ajustamos el rango para que los puntos rojos resalten
            range_color=[0, df_mapa['risk_score'].quantile(0.95)],
            zoom=5,
            center={"lat": lat_center, "lon": lon_center},
            height=500,
            hover_data={'municipio_dane': True, 'risk_score': ':.1f', 'latitud': False, 'longitud': False},
            title="Distribución Geográfica de Anomalías"
        )
        fig_mapa.update_layout(mapbox_style="open-street-map", margin={"r":0,"t":30,"l":0,"b":0})
        st.plotly_chart(fig_mapa, use_container_width=True)
    else:
        st.info("No hay suficientes alertas históricas para generar el mapa de riesgo.")

    st.markdown("---")
    
    # 4. Gráficos de Barras (Volumen y Volatilidad)
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Top 20 Cajeros por Volumen")
        df_top20 = df_historico.head(20).copy()
        df_top20['cod_terminal'] = df_top20['cod_terminal'].astype(str)
        
        fig = px.bar(df_top20, x='dispensacion_promedio', y='cod_terminal', orientation='h',
                    title='Mayor Dispensación Promedio', text_auto='.2s')
        fig.update_yaxes(type='category', categoryorder='total ascending')
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("⚠️ Top 20 Cajeros Más Volátiles")
        df_volatiles = load_cajeros_volatiles(20)
        if not df_volatiles.empty:
            df_volatiles['cod_cajero'] = df_volatiles['cod_cajero'].astype(str)
            fig = px.bar(df_volatiles, x='coef_variacion', y='cod_cajero', orientation='h',
                       title='Mayor Coeficiente de Variación', color='pct_anomalias_3std',
                       color_continuous_scale='Reds')
            fig.update_yaxes(type='category', categoryorder='total ascending')
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)
    
    # 5. Patrón Horario
    st.markdown("---")
    st.subheader("⏰ Patrón de Dispensación por Hora")
    df_horario = load_dispensacion_por_hora()
    
    if not df_horario.empty:
        fig = px.line(df_horario, x='hora', y='dispensacion_promedio',
                     title='Promedio Global por Hora')
        
        # Bandas de confianza (Std Dev)
        if 'dispensacion_std' in df_horario.columns:
            fig.add_scatter(x=df_horario['hora'], y=df_horario['dispensacion_promedio'] + df_horario['dispensacion_std'],
                           mode='lines', name='+1 std', line=dict(dash='dash', color='gray', width=1))
            fig.add_scatter(x=df_horario['hora'], y=df_horario['dispensacion_promedio'] - df_horario['dispensacion_std'],
                           mode='lines', name='-1 std', line=dict(dash='dash', color='gray', width=1), showlegend=False)
        
        fig.update_xaxes(tickmode='linear', dtick=1)
        st.plotly_chart(fig, use_container_width=True)
    
    # 6. Tabla de Datos
    st.markdown("---")
    st.subheader("📋 Detalle por Cajero")
    
    buscar = st.text_input("🔍 Buscar cajero en histórico:", "")
    if buscar:
        df_display = df_historico[df_historico['cod_terminal'].astype(str).str.contains(buscar, case=False)]
    else:
        df_display = df_historico.head(100)
    
    st.dataframe(df_display, width=None, use_container_width=True)

# ============================================================================
# PÁGINA 3: ALERTAS DETECTADAS
# ============================================================================

def page_alertas_detectadas():
    st.title("🚨 Alertas Detectadas por el Modelo")
    
    stats = load_stats_generales()
    if stats is None or stats['total_alertas'] == 0:
        st.warning("⚠️ No se han detectado alertas aún.")
        return
    
    col1, col2, col3 = st.columns(3)
    # Permitir fechas más amplias por defecto
    fi = col1.date_input("Desde", datetime.now() - timedelta(days=180))
    ff = col2.date_input("Hasta", datetime.now())
    sev = col3.selectbox("Severidad", ["Todos", "Crítico", "Advertencia", "Sospechoso"])
    
    if st.button("🔄 Actualizar datos", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    df_alertas = load_alertas_historicas(fi, ff, sev)
    
    if df_alertas.empty:
        st.warning("⚠️ No se encontraron alertas con los filtros seleccionados")
        return
    
    st.success(f"✅ {len(df_alertas):,} alertas cargadas")
    
    col1, col2 = st.columns(2)
    with col1:
        severidad_counts = df_alertas['severidad'].value_counts()
        fig_pie = px.pie(values=severidad_counts.values, names=severidad_counts.index,
            title='Alertas por Severidad',
            color=severidad_counts.index,
            color_discrete_map={'Crítico': '#f44336', 'Advertencia': '#ff9800', 'Sospechoso': '#8bc34a'})
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        fig_hist = px.histogram(df_alertas, x='score_anomalia', nbins=50,
            title='Distribución de Scores de Anomalía',
            color='severidad',
            color_discrete_map={'Crítico': '#f44336', 'Advertencia': '#ff9800', 'Sospechoso': '#8bc34a'})
        st.plotly_chart(fig_hist, use_container_width=True)
    
    st.markdown("---")
    st.subheader("📋 Detalle de Alertas")
    
    buscar_cajero = st.text_input("🔍 Buscar por código de cajero:", "")
    if buscar_cajero:
        df_display = df_alertas[df_alertas['cod_cajero'].astype(str).str.contains(buscar_cajero, case=False)]
    else:
        df_display = df_alertas
    
    st.dataframe(df_display, width=None, use_container_width=True)
    
    # Detalle individual
    if not df_display.empty:
        st.markdown("---")
        st.subheader("🔍 Inspección Rápida")
        alerta_id = st.selectbox(
            "Seleccionar Alerta:",
            df_display['id'].tolist(),
            format_func=lambda x: f"ID: {x} | {df_display[df_display['id']==x]['cod_cajero'].iloc[0]} | Score: {df_display[df_display['id']==x]['score_anomalia'].iloc[0]:.3f}"
        )
        
        if alerta_id:
            row = df_display[df_display['id'] == alerta_id].iloc[0]
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown(f"**Cajero:** {row['cod_cajero']}")
                st.markdown(f"**Severidad:** {row['severidad']}")
            with c2:
                st.markdown(f"**Dispensado:** ${row['monto_dispensado']:,.0f}")
                st.markdown(f"**Esperado:** ${row['monto_esperado']:,.0f}")
            with c3:
                st.markdown(f"**Score:** {row['score_anomalia']:.4f}")
                st.markdown(f"**Desviación:** {row['desviacion_std']:.2f}σ")
            st.info(f"**Descripción:** {row['descripcion']}")

# ============================================================================
# PÁGINA 3: PROCESAR ARCHIVO
# ============================================================================

def page_procesar_archivo():
    st.title("⚡ Procesar Archivo de 15 Minutos")
    st.markdown("Sube un archivo `.txt` para ejecutar el pipeline de detección.")
    
    uploaded_file = st.file_uploader("📁 Seleccionar archivo", type=['txt'])
    
    if uploaded_file is not None:
        st.info(f"📄 Archivo: {uploaded_file.name}")
        temp_path = f"/tmp/{uploaded_file.name}"
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        if st.button("🚀 PROCESAR", type="primary"):
            with st.spinner("🔄 Procesando análisis de fraude..."):
                try:
                    # Ejecutar Script
                    cmd = ["uv", "run", "scripts/procesar_archivo_15min.py", temp_path, "--config", "config.yaml"]
                    result = subprocess.run(cmd, cwd="/dados/avc", capture_output=True, text=True, timeout=300)
                    
                    if result.returncode == 0:
                        st.success("✅ Procesamiento completado exitosamente")
                        
                        # --- LOGICA NUEVA: OBTENER ALERTAS RECIENTES PARA MAPA ---
                        
                        # Consultamos las alertas generadas en los últimos 5 minutos (las de este archivo)
                        # Y hacemos JOIN inmediato con cajeros para tener lat/lon
                        query_recientes = """
                            SELECT 
                                a.cod_cajero, 
                                a.severidad, 
                                a.score_anomalia, 
                                a.monto_dispensado, 
                                a.descripcion,
                                c.latitud, 
                                c.longitud, 
                                c.municipio_dane
                            FROM alertas_dispensacion a
                            LEFT JOIN cajeros c ON a.cod_cajero = c.codigo::VARCHAR
                            WHERE a.fecha_deteccion >= NOW() - INTERVAL '5 minutes'
                            ORDER BY a.score_anomalia DESC
                        """
                        
                        df_nuevas = pd.read_sql(query_recientes, engine)
                        
                        if not df_nuevas.empty:
                            st.markdown("---")
                            st.subheader(f"📍 Mapa de Hallazgos en este Archivo ({len(df_nuevas)} alertas)")
                            
                            # Dividir pantalla: Mapa a la izquierda, Tabla a la derecha
                            col_map, col_tab = st.columns([1, 1])
                            
                            with col_map:
                                # Mapa solo de lo que se acaba de cargar
                                if df_nuevas['latitud'].notnull().any():
                                    fig = px.scatter_mapbox(
                                        df_nuevas,
                                        lat='latitud',
                                        lon='longitud',
                                        color='severidad',
                                        size='score_anomalia',
                                        color_discrete_map={'Crítico': '#f44336', 'Advertencia': '#ff9800', 'Sospechoso': '#8bc34a'},
                                        zoom=4,
                                        height=400,
                                        hover_data=['cod_cajero', 'municipio_dane', 'monto_dispensado']
                                    )
                                    fig.update_layout(mapbox_style="open-street-map", margin={"r":0,"t":0,"l":0,"b":0})
                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    st.warning("Las alertas detectadas no tienen coordenadas asociadas en la base de datos.")

                            with col_tab:
                                st.dataframe(
                                    df_nuevas[['cod_cajero', 'severidad', 'monto_dispensado', 'descripcion']], 
                                    use_container_width=True,
                                    height=400
                                )
                                
                            st.cache_data.clear() # Limpiar caché para que se actualicen los históricos
                        else:
                            st.success("✅ El archivo fue procesado y NO se encontraron anomalías.")
                            
                        with st.expander("Ver log técnico"):
                            st.code(result.stdout)
                    else:
                        st.error("❌ Error en el procesamiento")
                        st.code(result.stderr)
                except Exception as e:
                    st.error(f"Excepción crítica: {e}")
                finally:
                    if os.path.exists(temp_path): os.remove(temp_path)

# ============================================================================
# PÁGINA 4: MAPA
# ============================================================================

def page_mapa():
    st.title("🗺️ Mapa de Riesgo Histórico")
    st.markdown("Visualización de la **reputación** de los cajeros basada en su historial de anomalías.")
    
    # Filtros
    col1, col2 = st.columns([1, 3])
    with col1:
        dias = st.slider("📅 Ventana de tiempo (días):", 30, 365, 90)
    
    # Consulta Agrupada para calcular "Score de Riesgo"
    # Crítico pesa 5, Advertencia pesa 1. Sospechoso pesa 0.2
    query = f"""
        SELECT 
            a.cod_cajero,
            c.latitud, 
            c.longitud, 
            c.municipio_dane, 
            c.departamento,
            COUNT(*) as total_eventos,
            SUM(CASE 
                WHEN a.severidad = 'Crítico' THEN 5 
                WHEN a.severidad = 'Advertencia' THEN 1 
                ELSE 0.2 
            END) as risk_score,
            MAX(a.fecha_hora) as ultima_anomalia
        FROM alertas_dispensacion a
        JOIN cajeros c ON a.cod_cajero = c.codigo::VARCHAR
        WHERE a.fecha_hora >= NOW() - INTERVAL '{dias} days'
        AND c.latitud IS NOT NULL
        GROUP BY a.cod_cajero, c.latitud, c.longitud, c.municipio_dane, c.departamento
        ORDER BY risk_score DESC
    """
    
    try:
        df = pd.read_sql(query, engine)
        
        if df.empty:
            st.warning("No hay datos suficientes para generar el mapa de riesgo.")
            return
        
        # Métricas rápidas
        cajeros_rojos = len(df[df['risk_score'] > 20])
        st.info(f"📍 Analizando **{len(df)} cajeros** con incidentes. Hay **{cajeros_rojos} cajeros de Alto Riesgo** en este periodo.")

        # CREACIÓN DEL MAPA DE CALOR (PUNTOS)
        # Usamos una escala de color personalizada: Verde (Bajo riesgo) -> Amarillo -> Rojo (Alto riesgo)
        fig = px.scatter_mapbox(
            df,
            lat='latitud',
            lon='longitud',
            color='risk_score',  # El color depende del puntaje acumulado
            size='total_eventos', # El tamaño depende de la cantidad de alertas
            color_continuous_scale="RdYlGn_r", # Rojo-Amarillo-Verde (Invertido para que Rojo sea alto valor)
            range_color=[0, df['risk_score'].quantile(0.95)], # Ajuste dinámico del rango para que no se sature
            zoom=5,
            height=650,
            hover_data={
                'municipio_dane': True,
                'total_eventos': True,
                'risk_score': ':.1f',
                'ultima_anomalia': True,
                'latitud': False,
                'longitud': False
            },
            title="Mapa de Calor de Riesgo (Rojo = Mayor Historial de Fraude)"
        )
        
        fig.update_layout(mapbox_style="open-street-map")
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla de los peores cajeros (Los puntos más rojos)
        st.subheader("🚨 Top 10 Cajeros de Mayor Riesgo Histórico")
        st.dataframe(
            df.head(10)[['cod_cajero', 'municipio_dane', 'departamento', 'total_eventos', 'risk_score']],
            use_container_width=True
        )
        
    except Exception as e:
        st.error(f"Error generando mapa: {e}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    with st.sidebar:
        st.title("🏧 Navegación")
        page = st.radio("Ir a:", ["🏠 Home", "📈 Histórico", "🚨 Alertas", "⚡ Procesar", "🗺️ Mapa"])
        if st.button("🔄 Refrescar"):
            st.cache_data.clear()
            st.rerun()
            
    if page == "🏠 Home": page_home()
    elif page == "📈 Histórico": page_analisis_historico()
    elif page == "🚨 Alertas": page_alertas_detectadas()
    elif page == "⚡ Procesar": page_procesar_archivo()
    elif page == "🗺️ Mapa": page_mapa()

if __name__ == "__main__":
    main()