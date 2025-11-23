"""
📊 Estadísticas - Análisis Avanzados y Reportes
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
from components.graficos import (
    crear_grafico_distribucion_scores,
    crear_grafico_alertas_por_municipio
)
import plotly.graph_objects as go
import plotly.express as px

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Estadísticas - Detección de Fraudes",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Estadísticas y Análisis Avanzados")

# ============================================================================
# VERIFICAR CONEXIÓN
# ============================================================================

if not test_connection():
    st.error("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
    st.stop()

# ============================================================================
# FILTROS GLOBALES
# ============================================================================

st.markdown("### 🗓️ Período de Análisis")

col1, col2 = st.columns(2)

with col1:
    fecha_inicio = st.date_input(
        "Fecha inicio",
        value=datetime.now() - timedelta(days=90),
        key="fecha_inicio_stats"
    )

with col2:
    fecha_fin = st.date_input(
        "Fecha fin",
        value=datetime.now(),
        key="fecha_fin_stats"
    )

fecha_inicio_dt = datetime.combine(fecha_inicio, datetime.min.time())
fecha_fin_dt = datetime.combine(fecha_fin, datetime.max.time())

st.markdown("---")

# ============================================================================
# RESUMEN EJECUTIVO
# ============================================================================

st.markdown("## 📋 Resumen Ejecutivo")

# Query para resumen
query_resumen = """
SELECT 
    COUNT(*) as total_alertas,
    COUNT(DISTINCT cod_cajero) as cajeros_afectados,
    ROUND(AVG(score_anomalia), 2) as score_promedio,
    ROUND(AVG(monto_dispensado), 0) as monto_promedio,
    COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
    COUNT(*) FILTER (WHERE severidad = 'alto') as altas,
    COUNT(*) FILTER (WHERE severidad = 'medio') as medias
FROM alertas_dispensacion
WHERE fecha_hora >= %s AND fecha_hora <= %s
"""

df_resumen = execute_query(query_resumen, params=(fecha_inicio_dt, fecha_fin_dt))

if not df_resumen.empty:
    row = df_resumen.iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🚨 Total Alertas", f"{int(row['total_alertas']):,}")
    
    with col2:
        st.metric("🏧 Cajeros Afectados", f"{int(row['cajeros_afectados']):,}")
    
    with col3:
        st.metric("📊 Score Promedio", f"{row['score_promedio']:.2f}")
    
    with col4:
        st.metric("💰 Monto Promedio", f"${row['monto_promedio']:,.0f}")
    
    # Distribución por severidad
    st.markdown("#### Distribución por Severidad")
    
    col_a, col_b, col_c = st.columns(3)
    
    with col_a:
        pct_criticas = (row['criticas'] / row['total_alertas'] * 100) if row['total_alertas'] > 0 else 0
        st.metric(
            "🔴 Críticas",
            f"{int(row['criticas']):,}",
            delta=f"{pct_criticas:.1f}%"
        )
    
    with col_b:
        pct_altas = (row['altas'] / row['total_alertas'] * 100) if row['total_alertas'] > 0 else 0
        st.metric(
            "🟡 Altas",
            f"{int(row['altas']):,}",
            delta=f"{pct_altas:.1f}%"
        )
    
    with col_c:
        pct_medias = (row['medias'] / row['total_alertas'] * 100) if row['total_alertas'] > 0 else 0
        st.metric(
            "🟢 Medias",
            f"{int(row['medias']):,}",
            delta=f"{pct_medias:.1f}%"
        )

st.markdown("---")

# ============================================================================
# DISTRIBUCIÓN DE SCORES
# ============================================================================

st.markdown("## 📈 Distribución de Scores de Anomalía")

query_scores = """
SELECT 
    CASE 
        WHEN score_anomalia >= 90 THEN '90-100'
        WHEN score_anomalia >= 80 THEN '80-89'
        WHEN score_anomalia >= 70 THEN '70-79'
        WHEN score_anomalia >= 60 THEN '60-69'
        WHEN score_anomalia >= 50 THEN '50-59'
        ELSE '0-49'
    END as rango_score,
    COUNT(*) as cantidad
FROM alertas_dispensacion
WHERE fecha_hora >= %s AND fecha_hora <= %s
GROUP BY 
    CASE 
        WHEN score_anomalia >= 90 THEN '90-100'
        WHEN score_anomalia >= 80 THEN '80-89'
        WHEN score_anomalia >= 70 THEN '70-79'
        WHEN score_anomalia >= 60 THEN '60-69'
        WHEN score_anomalia >= 50 THEN '50-59'
        ELSE '0-49'
    END
ORDER BY rango_score DESC
"""

df_scores = execute_query(query_scores, params=(fecha_inicio_dt, fecha_fin_dt))

if not df_scores.empty:
    fig_scores = crear_grafico_distribucion_scores(df_scores)
    if fig_scores:
        st.plotly_chart(fig_scores, use_container_width=True, config={'displayModeBar': True})

st.markdown("---")

# ============================================================================
# ANÁLISIS GEOGRÁFICO
# ============================================================================

st.markdown("## 🗺️ Análisis Geográfico")

col_geo1, col_geo2 = st.columns(2)

with col_geo1:
    st.markdown("### Por Departamento")
    
    query_depto = """
    SELECT 
        f.departamento,
        COUNT(*) as num_alertas,
        COUNT(DISTINCT a.cod_cajero) as cajeros_afectados,
        ROUND(AVG(a.score_anomalia), 2) as score_promedio
    FROM alertas_dispensacion a
    INNER JOIN features_ml f ON a.cod_cajero = f.cod_cajero
    WHERE a.fecha_hora >= %s AND a.fecha_hora <= %s
      AND f.departamento IS NOT NULL
    GROUP BY f.departamento
    ORDER BY num_alertas DESC
    LIMIT 10
    """
    
    df_depto = execute_query(query_depto, params=(fecha_inicio_dt, fecha_fin_dt))
    
    if not df_depto.empty:
        fig_depto = px.bar(
            df_depto,
            x='num_alertas',
            y='departamento',
            orientation='h',
            title='Top 10 Departamentos',
            color='num_alertas',
            color_continuous_scale='Reds',
            labels={'num_alertas': 'Alertas', 'departamento': 'Departamento'}
        )
        fig_depto.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_depto, use_container_width=True, config={'displayModeBar': False})

with col_geo2:
    st.markdown("### Por Municipio")
    
    query_municipio = """
    SELECT 
        f.municipio_dane,
        f.departamento,
        COUNT(*) as num_alertas,
        COUNT(DISTINCT a.cod_cajero) as cajeros_afectados
    FROM alertas_dispensacion a
    INNER JOIN features_ml f ON a.cod_cajero = f.cod_cajero
    WHERE a.fecha_hora >= %s AND a.fecha_hora <= %s
      AND f.municipio_dane IS NOT NULL
    GROUP BY f.municipio_dane, f.departamento
    ORDER BY num_alertas DESC
    LIMIT 10
    """
    
    df_municipio = execute_query(query_municipio, params=(fecha_inicio_dt, fecha_fin_dt))
    
    if not df_municipio.empty:
        fig_municipio = crear_grafico_alertas_por_municipio(df_municipio)
        if fig_municipio:
            st.plotly_chart(fig_municipio, use_container_width=True, config={'displayModeBar': False})

st.markdown("---")

# ============================================================================
# ANÁLISIS TEMPORAL DETALLADO
# ============================================================================

st.markdown("## ⏰ Análisis Temporal Detallado")

# Por hora del día
col_temp1, col_temp2 = st.columns(2)

with col_temp1:
    st.markdown("### Alertas por Hora del Día")
    
    query_hora = """
    SELECT 
        EXTRACT(HOUR FROM fecha_hora) as hora,
        COUNT(*) as num_alertas
    FROM alertas_dispensacion
    WHERE fecha_hora >= %s AND fecha_hora <= %s
    GROUP BY EXTRACT(HOUR FROM fecha_hora)
    ORDER BY hora
    """
    
    df_hora = execute_query(query_hora, params=(fecha_inicio_dt, fecha_fin_dt))
    
    if not df_hora.empty:
        fig_hora = px.line(
            df_hora,
            x='hora',
            y='num_alertas',
            title='Distribución por Hora',
            markers=True,
            labels={'hora': 'Hora del Día', 'num_alertas': 'Número de Alertas'}
        )
        fig_hora.update_layout(height=350)
        st.plotly_chart(fig_hora, use_container_width=True, config={'displayModeBar': False})

with col_temp2:
    st.markdown("### Alertas por Día de la Semana")
    
    query_dia = """
    SELECT 
        EXTRACT(DOW FROM fecha_hora) as dia_semana,
        COUNT(*) as num_alertas
    FROM alertas_dispensacion
    WHERE fecha_hora >= %s AND fecha_hora <= %s
    GROUP BY EXTRACT(DOW FROM fecha_hora)
    ORDER BY dia_semana
    """
    
    df_dia = execute_query(query_dia, params=(fecha_inicio_dt, fecha_fin_dt))
    
    if not df_dia.empty:
        dias_nombres = ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
        df_dia['dia_nombre'] = df_dia['dia_semana'].apply(lambda x: dias_nombres[int(x)])
        
        fig_dia = px.bar(
            df_dia,
            x='dia_nombre',
            y='num_alertas',
            title='Distribución por Día de la Semana',
            color='num_alertas',
            color_continuous_scale='Blues',
            labels={'dia_nombre': 'Día', 'num_alertas': 'Alertas'}
        )
        fig_dia.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig_dia, use_container_width=True, config={'displayModeBar': False})

st.markdown("---")

# ============================================================================
# TOP RANKINGS
# ============================================================================

st.markdown("## 🏆 Rankings y Comparativas")

tab1, tab2, tab3 = st.tabs(["🔝 Top Cajeros", "📍 Top Municipios", "📊 Comparativa Mensual"])

with tab1:
    query_top = """
    SELECT 
        a.cod_cajero,
        COUNT(*) as num_alertas,
        COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
        ROUND(AVG(score_anomalia), 2) as score_promedio,
        f.municipio_dane,
        f.departamento
    FROM alertas_dispensacion a
    LEFT JOIN features_ml f ON a.cod_cajero = f.cod_cajero
    WHERE a.fecha_hora >= %s AND a.fecha_hora <= %s
    GROUP BY a.cod_cajero, f.municipio_dane, f.departamento
    ORDER BY num_alertas DESC
    LIMIT 30
    """
    
    df_top = execute_query(query_top, params=(fecha_inicio_dt, fecha_fin_dt))
    
    if not df_top.empty:
        st.dataframe(
            df_top,
            use_container_width=True,
            column_config={
                'cod_cajero': 'Cajero',
                'num_alertas': st.column_config.ProgressColumn(
                    'Total Alertas',
                    format='%d',
                    min_value=0,
                    max_value=df_top['num_alertas'].max()
                ),
                'criticas': st.column_config.NumberColumn('🔴 Críticas', format='%d'),
                'score_promedio': st.column_config.NumberColumn('Score Prom.', format='%.2f'),
                'municipio_dane': 'Municipio',
                'departamento': 'Departamento'
            },
            hide_index=True,
            height=500
        )

with tab2:
    if not df_municipio.empty:
        st.dataframe(
            df_municipio,
            use_container_width=True,
            column_config={
                'municipio_dane': 'Municipio',
                'departamento': 'Departamento',
                'num_alertas': st.column_config.NumberColumn('Alertas', format='%d'),
                'cajeros_afectados': st.column_config.NumberColumn('Cajeros', format='%d')
            },
            hide_index=True
        )

with tab3:
    query_mensual = """
    SELECT 
        DATE_TRUNC('month', fecha_hora) as mes,
        COUNT(*) as num_alertas,
        COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
        COUNT(*) FILTER (WHERE severidad = 'alto') as altas
    FROM alertas_dispensacion
    WHERE fecha_hora >= %s AND fecha_hora <= %s
    GROUP BY DATE_TRUNC('month', fecha_hora)
    ORDER BY mes
    """
    
    df_mensual = execute_query(query_mensual, params=(fecha_inicio_dt, fecha_fin_dt))
    
    if not df_mensual.empty:
        fig_mensual = go.Figure()
        
        fig_mensual.add_trace(go.Bar(
            x=df_mensual['mes'],
            y=df_mensual['criticas'],
            name='Críticas',
            marker_color='red'
        ))
        
        fig_mensual.add_trace(go.Bar(
            x=df_mensual['mes'],
            y=df_mensual['altas'],
            name='Altas',
            marker_color='orange'
        ))
        
        fig_mensual.update_layout(
            title='Evolución Mensual de Alertas',
            xaxis_title='Mes',
            yaxis_title='Número de Alertas',
            barmode='stack',
            height=400
        )
        
        st.plotly_chart(fig_mensual, use_container_width=True, config={'displayModeBar': True})

st.markdown("---")

# ============================================================================
# EXPORTAR REPORTES
# ============================================================================

st.markdown("## 📥 Exportar Reportes")

col_exp1, col_exp2, col_exp3 = st.columns(3)

with col_exp1:
    if st.button("📊 Exportar Resumen Ejecutivo", use_container_width=True):
        if not df_resumen.empty:
            csv = df_resumen.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"resumen_ejecutivo_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

with col_exp2:
    if st.button("🏆 Exportar Top Cajeros", use_container_width=True):
        if not df_top.empty:
            csv = df_top.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"top_cajeros_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

with col_exp3:
    if st.button("🗺️ Exportar Análisis Geográfico", use_container_width=True):
        if not df_depto.empty:
            csv = df_depto.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"analisis_geografico_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: #666;'>
    <p>Reporte generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p>Período analizado: {fecha_inicio.strftime('%Y-%m-%d')} a {fecha_fin.strftime('%Y-%m-%d')}</p>
</div>
""", unsafe_allow_html=True)