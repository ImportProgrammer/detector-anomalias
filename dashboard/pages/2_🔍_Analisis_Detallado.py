"""
🔍 Análisis Detallado - Drill-down por Cajero Específico
"""

import streamlit as st
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Agregar path del dashboard
dashboard_path = Path(__file__).parent.parent
sys.path.append(str(dashboard_path))

from utils.db import execute_query, test_connection
# from components.kpis import mostrar_kpis_cajero  # TODO: Implementar esta función
from components.graficos import crear_grafico_tendencia_temporal
from components.mapa import crear_mapa_alertas

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Análisis Detallado - Detección de Fraudes",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Análisis Detallado por Cajero")

# ============================================================================
# VERIFICAR CONEXIÓN
# ============================================================================

if not test_connection():
    st.error("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
    st.stop()

# ============================================================================
# BÚSQUEDA DE CAJERO
# ============================================================================

st.markdown("### 🔎 Buscar Cajero")

col1, col2 = st.columns([3, 1])

with col1:
    # Obtener lista de cajeros con alertas
    query_cajeros = """
    SELECT DISTINCT cod_cajero 
    FROM alertas_dispensacion 
    ORDER BY cod_cajero
    """
    df_cajeros_disponibles = execute_query(query_cajeros)
    
    if not df_cajeros_disponibles.empty:
        cajeros_list = df_cajeros_disponibles['cod_cajero'].tolist()
        
        # Búsqueda con selectbox
        cod_cajero = st.selectbox(
            "Seleccione un cajero",
            options=cajeros_list,
            help="Lista de cajeros con alertas detectadas"
        )
    else:
        st.warning("No hay cajeros con alertas en el sistema")
        st.stop()

with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    buscar = st.button("🔍 Analizar", width='stretch', type="primary")

if not buscar and 'ultimo_cajero' not in st.session_state:
    st.info("👆 Seleccione un cajero y presione 'Analizar' para ver el análisis detallado")
    st.stop()

# Guardar último cajero buscado
if buscar:
    st.session_state['ultimo_cajero'] = cod_cajero
else:
    cod_cajero = st.session_state.get('ultimo_cajero')

st.markdown("---")

# ============================================================================
# INFORMACIÓN BÁSICA DEL CAJERO
# ============================================================================

st.markdown(f"## 🏧 Cajero: {cod_cajero}")

# Obtener información del cajero
query_info_cajero = """
SELECT 
    f.cod_cajero,
    f.dispensacion_promedio,
    f.dispensacion_std,
    f.dispensacion_max,
    f.coef_variacion,
    f.pct_anomalias_3std,
    f.latitud,
    f.longitud,
    f.municipio_dane,
    f.departamento,
    f.num_periodos_15min,
    f.transacciones_totales
FROM features_ml f
WHERE f.cod_cajero = %s
"""

df_info = execute_query(query_info_cajero, params=(cod_cajero,))

if df_info.empty:
    st.error(f"No se encontró información para el cajero {cod_cajero}")
    st.stop()

info_cajero = df_info.iloc[0].to_dict()

# Obtener alertas del cajero
query_alertas_cajero = """
SELECT COUNT(*) as total_alertas
FROM alertas_dispensacion
WHERE cod_cajero = %s
"""

df_count = execute_query(query_alertas_cajero, params=(cod_cajero,))
num_alertas = int(df_count.iloc[0]['total_alertas']) if not df_count.empty else 0

# Mostrar KPIs del cajero (inline temporal hasta crear componente)
st.markdown("### 📊 Indicadores del Cajero")

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

with col_kpi1:
    st.metric(
        "🚨 Total Alertas",
        f"{num_alertas:,}",
        help="Número total de alertas generadas"
    )

with col_kpi2:
    dispensacion_prom = info_cajero.get('dispensacion_promedio', 0)
    st.metric(
        "💰 Dispensación Promedio",
        f"${dispensacion_prom:,.0f}" if dispensacion_prom else "N/A",
        help="Monto promedio dispensado"
    )

with col_kpi3:
    coef_var = info_cajero.get('coef_variacion', 0)
    st.metric(
        "📊 Coef. Variación",
        f"{coef_var:.2f}" if coef_var else "N/A",
        help="Coeficiente de variación (std/mean)"
    )

with col_kpi4:
    pct_anomalias = info_cajero.get('pct_anomalias_3std', 0)
    st.metric(
        "⚠️ % Anomalías",
        f"{pct_anomalias:.1f}%" if pct_anomalias else "N/A",
        help="Porcentaje de dispensaciones anómalas (>3σ)"
    )

st.markdown("---")

# # ============================================================================
# # INFORMACIÓN GEOGRÁFICA
# # ============================================================================

# st.markdown("### 📍 Información de Ubicación")

# col_a, col_b, col_c, col_d = st.columns(4)

# with col_a:
#     st.metric("🗺️ Departamento", info_cajero.get('departamento', 'N/A'))

# with col_b:
#     st.metric("📍 Municipio", info_cajero.get('municipio_dane', 'N/A'))

# with col_c:
#     lat = info_cajero.get('latitud')
#     st.metric("🌐 Latitud", f"{lat:.6f}" if lat else "N/A")

# with col_d:
#     lon = info_cajero.get('longitud')
#     st.metric("🌐 Longitud", f"{lon:.6f}" if lon else "N/A")

# st.markdown("---")

# ============================================================================
# TIMELINE DE ALERTAS
# ============================================================================

st.markdown("### 📈 Timeline de Alertas")

# Filtros de fecha
col_f1, col_f2 = st.columns(2)

with col_f1:
    fecha_desde = st.date_input(
        "Desde",
        value=datetime.now() - timedelta(days=90),
        key="fecha_desde_detalle"
    )

with col_f2:
    fecha_hasta = st.date_input(
        "Hasta",
        value=datetime.now(),
        key="fecha_hasta_detalle"
    )

# Query timeline
query_timeline = """
SELECT 
    DATE(fecha_hora) as fecha,
    COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
    COUNT(*) FILTER (WHERE severidad = 'alto') as altas,
    COUNT(*) FILTER (WHERE severidad = 'medio') as medias
FROM alertas_dispensacion
WHERE cod_cajero = %s
  AND fecha_hora >= %s
  AND fecha_hora <= %s
GROUP BY DATE(fecha_hora)
ORDER BY fecha ASC
"""

df_timeline = execute_query(
    query_timeline,
    params=(
        cod_cajero,
        datetime.combine(fecha_desde, datetime.min.time()),
        datetime.combine(fecha_hasta, datetime.max.time())
    )
)

if not df_timeline.empty:
    fig_timeline = crear_grafico_tendencia_temporal(df_timeline)
    if fig_timeline:
        st.plotly_chart(fig_timeline, config={'displayModeBar': True})
else:
    st.info("No hay alertas en el período seleccionado")

st.markdown("---")

# ============================================================================
# COMPARACIÓN CON CAJEROS SIMILARES
# ============================================================================

st.markdown("### 📊 Comparación con Cajeros Similares")

# Obtener cajeros del mismo municipio
query_comparacion = """
SELECT 
    a.cod_cajero,
    COUNT(*) as num_alertas,
    ROUND(AVG(a.score_anomalia), 2) as score_promedio
FROM alertas_dispensacion a
INNER JOIN features_ml f ON a.cod_cajero = f.cod_cajero
WHERE f.municipio_dane = %s
  AND a.cod_cajero != %s
GROUP BY a.cod_cajero
ORDER BY num_alertas DESC
LIMIT 10
"""

df_comparacion = execute_query(
    query_comparacion,
    params=(info_cajero.get('municipio_dane', 'N/A'), cod_cajero)
)

if not df_comparacion.empty:
    # Agregar el cajero actual para comparación
    cajero_actual = pd.DataFrame([{
        'cod_cajero': cod_cajero,
        'num_alertas': num_alertas,
        'score_promedio': df_info.iloc[0].get('pct_anomalias_3std', 0)
    }])
    
    df_comparacion_full = pd.concat([cajero_actual, df_comparacion], ignore_index=True)
    
    # MEJORA 1: Ordenar por num_alertas para mejor visualización
    df_comparacion_full = df_comparacion_full.sort_values('num_alertas', ascending=True)
    
    # MEJORA 2: Crear etiquetas categóricas explícitas
    df_comparacion_full['cajero_label'] = 'Cajero ' + df_comparacion_full['cod_cajero'].astype(str)
    
    # MEJORA 3: Identificar el cajero actual
    df_comparacion_full['es_actual'] = df_comparacion_full['cod_cajero'] == cod_cajero
    
    # Gráfico de comparación mejorado
    import plotly.graph_objects as go
    
    # Crear colores personalizados
    colors = ['#ef4444' if es_actual else '#60a5fa' 
              for es_actual in df_comparacion_full['es_actual']]
    
    fig_comp = go.Figure()
    
    fig_comp.add_trace(go.Bar(
        x=df_comparacion_full['num_alertas'],
        y=df_comparacion_full['cajero_label'],
        orientation='h',  # Horizontal es mejor para muchos cajeros
        marker_color=colors,
        text=df_comparacion_full['num_alertas'],
        textposition='outside',
        textfont=dict(size=12),
        hovertemplate='<b>%{y}</b><br>' +
                      'Alertas: %{x}<br>' +
                      'Score Promedio: %{customdata:.1f}<br>' +
                      '<extra></extra>',
        customdata=df_comparacion_full['score_promedio']
    ))
    
    fig_comp.update_layout(
        title=dict(
            text=f'Comparación con Cajeros en {info_cajero.get("municipio_dane", "la misma zona")}',
            x=0.5,
            xanchor='center'
        ),
        xaxis_title='Número de Alertas',
        yaxis_title='',
        height=max(400, len(df_comparacion_full) * 40),  # Altura dinámica
        showlegend=False,
        margin=dict(l=120, r=50, t=60, b=50),
        plot_bgcolor='white',
        xaxis=dict(
            gridcolor='lightgray',
            showgrid=True
        ),
        yaxis=dict(
            categoryorder='total ascending'  # Orden por valor
        )
    )
    
    st.plotly_chart(fig_comp, use_container_width=True, config={'displayModeBar': True})
    
    st.caption(f"🔴 Rojo = Cajero {cod_cajero} (actual) | 🔵 Azul = Otros cajeros de la zona")
    
    # EXTRA: Mostrar estadísticas de comparación
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    
    with col_stat1:
        posicion = len(df_comparacion_full) - list(df_comparacion_full['cod_cajero']).index(cod_cajero)
        st.metric(
            "Posición en la Zona", 
            f"{posicion}° de {len(df_comparacion_full)}",
            help="Ranking por número de alertas"
        )
    
    with col_stat2:
        promedio_zona = df_comparacion_full[~df_comparacion_full['es_actual']]['num_alertas'].mean()
        diff = num_alertas - promedio_zona
        st.metric(
            "vs Promedio Zona",
            f"{diff:+.0f} alertas",
            delta=f"{(diff/promedio_zona*100):+.1f}%" if promedio_zona > 0 else "N/A"
        )
    
    with col_stat3:
        max_zona = df_comparacion_full[~df_comparacion_full['es_actual']]['num_alertas'].max()
        if num_alertas >= max_zona:
            st.metric("Estado", "⚠️ Máximo de la zona", delta_color="inverse")
        else:
            st.metric("Estado", "✅ Bajo el máximo", delta_color="normal")
    
    # MAPA: Mostrar todos los cajeros de la comparación
    st.markdown("---")
    st.markdown("### 🗺️ Ubicación de Cajeros en la Zona")
    
    # Obtener coordenadas de todos los cajeros en la comparación
    query_coords_comparacion = """
    SELECT 
        f.cod_cajero,
        f.latitud,
        f.longitud,
        f.municipio_dane,
        f.departamento
    FROM features_ml f
    WHERE f.cod_cajero = ANY(%s)
      AND f.latitud IS NOT NULL
      AND f.longitud IS NOT NULL
    """
    
    cajeros_ids = df_comparacion_full['cod_cajero'].tolist()
    df_coords_zona = execute_query(query_coords_comparacion, params=(cajeros_ids,))
    
    if not df_coords_zona.empty:
        # Agregar información de alertas
        df_mapa_zona = df_coords_zona.merge(
            df_comparacion_full[['cod_cajero', 'num_alertas', 'score_promedio', 'es_actual']],
            on='cod_cajero',
            how='left'
        )
        
        # Asignar severidad según si es el cajero actual o no
        df_mapa_zona['severidad'] = df_mapa_zona['es_actual'].apply(
            lambda x: 'critico' if x else 'alto'
        )
        df_mapa_zona['score_anomalia'] = df_mapa_zona['score_promedio']
        df_mapa_zona['monto_dispensado'] = df_mapa_zona['num_alertas']  # Usar alertas como "monto"
        df_mapa_zona['descripcion'] = df_mapa_zona.apply(
            lambda row: f"{'⭐ ' if row['es_actual'] else ''}Cajero {row['cod_cajero']} - {row['num_alertas']} alertas",
            axis=1
        )
        
        # Crear mapa
        fig_mapa_zona = crear_mapa_alertas(df_mapa_zona)
        
        if fig_mapa_zona:
            st.plotly_chart(fig_mapa_zona, use_container_width=True, config={'displayModeBar': True})
            st.caption(f"🔴 Rojo = Cajero {cod_cajero} (actual) | 🟠 Naranja = Otros cajeros de {info_cajero.get('municipio_dane', 'la zona')}")
        else:
            st.warning("No se pudo crear el mapa de la zona")
    else:
        st.info("No hay coordenadas disponibles para los cajeros de la zona")

else:
    st.info(f"No hay otros cajeros en {info_cajero.get('municipio_dane', 'esta zona')} para comparar")

st.markdown("---")

# ============================================================================
# TABLA DETALLADA DE ALERTAS
# ============================================================================

st.markdown("### 🚨 Historial de Alertas Detallado")

# Filtro por severidad
severidades = st.multiselect(
    "Filtrar por severidad",
    options=['critico', 'alto', 'medio'],
    default=['critico', 'alto', 'medio']
)

# Query alertas detalladas
query_alertas_detalle = """
SELECT 
    id,
    fecha_hora,
    severidad,
    score_anomalia,
    monto_dispensado,
    monto_esperado,
    desviacion_std,
    descripcion,
    razones
FROM alertas_dispensacion
WHERE cod_cajero = %s
  AND severidad = ANY(%s)
ORDER BY fecha_hora DESC
LIMIT 500
"""

df_alertas_detalle = execute_query(
    query_alertas_detalle,
    params=(cod_cajero, severidades)
)

if not df_alertas_detalle.empty:
    # Configurar colores por severidad
    def resaltar_severidad(row):
        if row['severidad'] == 'critico':
            return ['background-color: #ffebee'] * len(row)
        elif row['severidad'] == 'alto':
            return ['background-color: #fff3e0'] * len(row)
        elif row['severidad'] == 'medio':
            return ['background-color: #e8f5e9'] * len(row)
        return [''] * len(row)
    
    st.dataframe(
        df_alertas_detalle.style.apply(resaltar_severidad, axis=1),
        width='stretch',
        column_config={
            'id': None,
            'fecha_hora': st.column_config.DatetimeColumn('Fecha/Hora', format='DD/MM/YYYY HH:mm'),
            'severidad': 'Severidad',
            'score_anomalia': st.column_config.NumberColumn('Score', format='%.1f'),
            'monto_dispensado': st.column_config.NumberColumn('Monto', format='$%.0f'),
            'monto_esperado': st.column_config.NumberColumn('Esperado', format='$%.0f'),
            'desviacion_std': st.column_config.NumberColumn('Desv. Std', format='%.2f'),
            'descripcion': st.column_config.TextColumn('Descripción', width='large'),
            'razones': st.column_config.TextColumn('Razones', width='large')
        },
        hide_index=True,
        height=500
    )
    
    # Botón de exportación
    col_exp1, col_exp2 = st.columns([1, 4])
    with col_exp1:
        if st.button("📥 Exportar a CSV"):
            csv = df_alertas_detalle.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar",
                data=csv,
                file_name=f"alertas_cajero_{cod_cajero}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
else:
    st.info("No hay alertas con los filtros seleccionados")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: #666;'>
    <p>Análisis generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
</div>
""", unsafe_allow_html=True)