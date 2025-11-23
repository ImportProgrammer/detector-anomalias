"""
🏠 Home - Vista General del Sistema
"""

import streamlit as st
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Agregar path del dashboard
dashboard_path = Path(__file__).parent.parent
sys.path.append(str(dashboard_path))

from utils.db import execute_query, test_connection
from utils import queries
from components.kpis import mostrar_kpis, mostrar_comparacion_periodos
from components.mapa import crear_mapa_alertas
from components.graficos import crear_grafico_tendencia_temporal, crear_heatmap_horario, crear_grafico_top_cajeros

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Home - Detección de Fraudes",
    page_icon="🏠",
    layout="wide"
)

st.title("🏠 Vista General del Sistema")

# ============================================================================
# VERIFICAR CONEXIÓN
# ============================================================================

if not test_connection():
    st.error("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
    st.stop()

# ============================================================================
# FILTROS TEMPORALES
# ============================================================================

st.markdown("### 🗓️ Filtros Temporales")

col1, col2, col3 = st.columns([2, 2, 1])

with col1:
    fecha_inicio = st.date_input(
        "Fecha inicio",
        value=datetime.now() - timedelta(days=30),
        max_value=datetime.now()
    )

with col2:
    fecha_fin = st.date_input(
        "Fecha fin",
        value=datetime.now(),
        max_value=datetime.now()
    )

with col3:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Actualizar", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# ============================================================================
# KPIs PRINCIPALES
# ============================================================================

st.markdown("### 📊 Indicadores Principales")

# Cargar KPIs
df_kpis = execute_query(queries.QUERY_KPIS_GENERALES)

if not df_kpis.empty:
    mostrar_kpis(df_kpis)
else:
    st.warning("⚠️ No hay datos de alertas disponibles. Asegúrate de haber ejecutado el proceso de detección.")

st.markdown("---")

# ============================================================================
# COMPARACIÓN DE PERÍODOS
# ============================================================================

st.markdown("### 📈 Comparación de Períodos")

df_comparacion = execute_query(queries.QUERY_COMPARACION_PERIODOS)
mostrar_comparacion_periodos(df_comparacion)

st.markdown("---")

# ============================================================================
# LAYOUT: MAPA + TENDENCIA
# ============================================================================

col_mapa, col_tendencia = st.columns([3, 2])

with col_mapa:
    st.markdown("### 🗺️ Mapa de Alertas Geográficas")
    
    # Cargar alertas con ubicación
    fecha_desde = datetime.combine(fecha_inicio, datetime.min.time())
    df_alertas_mapa = execute_query(
        queries.QUERY_ALERTAS_CON_UBICACION,
        params=(fecha_desde, 500)
    )
    
    if not df_alertas_mapa.empty:
        fig_mapa = crear_mapa_alertas(df_alertas_mapa)
        if fig_mapa:
            st.plotly_chart(fig_mapa, use_container_width=True)
    else:
        st.info("No hay alertas con ubicación geográfica en el período seleccionado")

with col_tendencia:
    st.markdown("### 📊 Tendencia Temporal")
    
    # Cargar tendencia por día
    df_tendencia = execute_query(queries.QUERY_ALERTAS_POR_DIA)
    
    if not df_tendencia.empty:
        fig_tendencia = crear_grafico_tendencia_temporal(df_tendencia)
        if fig_tendencia:
            st.plotly_chart(fig_tendencia, use_container_width=True)
    else:
        st.info("No hay datos de tendencia disponibles")

st.markdown("---")

# ============================================================================
# PATRONES HORARIOS
# ============================================================================

st.markdown("### 🕐 Patrones Horarios")

df_heatmap = execute_query(queries.QUERY_HEATMAP_HORARIO)

if not df_heatmap.empty:
    fig_heatmap = crear_heatmap_horario(df_heatmap)
    if fig_heatmap:
        st.plotly_chart(fig_heatmap, use_container_width=True)
else:
    st.info("No hay datos de patrones horarios disponibles")

st.markdown("---")

# ============================================================================
# TOP CAJEROS PROBLEMÁTICOS
# ============================================================================

st.markdown("### 🏆 Top Cajeros con Más Alertas")

df_top_cajeros = execute_query(
    queries.QUERY_TOP_CAJEROS_PROBLEMATICOS,
    params=(20,)
)

if not df_top_cajeros.empty:
    # Gráfico
    fig_top = crear_grafico_top_cajeros(df_top_cajeros)
    if fig_top:
        st.plotly_chart(fig_top, use_container_width=True)
    
    # Tabla detallada
    with st.expander("📋 Ver tabla detallada"):
        st.dataframe(
            df_top_cajeros,
            use_container_width=True,
            column_config={
                'cod_cajero': 'Cajero',
                'num_alertas': st.column_config.NumberColumn('Total Alertas', format="%d"),
                'alertas_criticas': st.column_config.NumberColumn('Críticas', format="%d"),
                'ultima_alerta': st.column_config.DatetimeColumn('Última Alerta'),
                'score_promedio': st.column_config.NumberColumn('Score Prom.', format="%.2f"),
                'municipio_dane': 'Municipio',
                'departamento': 'Departamento'
            },
            hide_index=True
        )
else:
    st.info("No hay datos de cajeros disponibles")

st.markdown("---")

# ============================================================================
# ALERTAS RECIENTES
# ============================================================================

st.markdown("### 🚨 Alertas Recientes")

limite_alertas = st.slider("Número de alertas a mostrar", 10, 100, 20, 10)

df_alertas_recientes = execute_query(
    queries.QUERY_ALERTAS_RECIENTES,
    params=(limite_alertas,)
)

if not df_alertas_recientes.empty:
    # Configurar colores por severidad
    def resaltar_severidad(row):
        if row['severidad'] == 'critico':
            return ['background-color: #ffebee'] * len(row)
        elif row['severidad'] == 'alto':
            return ['background-color: #fff3e0'] * len(row)
        elif row['severidad'] == 'medio':
            return ['background-color: #e8f5e9'] * len(row)
        return [''] * len(row)
    
    # Mostrar tabla
    st.dataframe(
        df_alertas_recientes.style.apply(resaltar_severidad, axis=1),
        use_container_width=True,
        column_config={
            'id': None,  # Ocultar ID
            'cod_cajero': 'Cajero',
            'fecha_hora': st.column_config.DatetimeColumn('Fecha/Hora'),
            'severidad': 'Severidad',
            'score_anomalia': st.column_config.NumberColumn('Score', format="%.1f"),
            'monto_dispensado': st.column_config.NumberColumn('Monto', format="$%,.0f"),
            'monto_esperado': st.column_config.NumberColumn('Esperado', format="$%,.0f"),
            'descripcion': st.column_config.TextColumn('Descripción', width='large'),
            'razones': st.column_config.TextColumn('Razones', width='large')
        },
        hide_index=True,
        height=400
    )
    
    # Botón para exportar
    if st.button("📥 Exportar alertas a CSV"):
        csv = df_alertas_recientes.to_csv(index=False)
        st.download_button(
            label="⬇️ Descargar CSV",
            data=csv,
            file_name=f"alertas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
else:
    st.info("No hay alertas recientes disponibles")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>Última actualización: {}</p>
    <p>Dashboard v1.0 | Sistema de Detección de Fraudes ATM</p>
</div>
""".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), unsafe_allow_html=True)