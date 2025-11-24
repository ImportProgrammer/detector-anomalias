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
st.markdown("Vista agregada de alertas históricas en la base de datos")

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
        value=datetime.now() - timedelta(days=30),
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

if not df_resumen.empty and df_resumen.iloc[0]['total_alertas'] > 0:
    row = df_resumen.iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🚨 Total Alertas", f"{int(row['total_alertas']):,}")
    
    with col2:
        st.metric("🏧 Cajeros Afectados", f"{int(row['cajeros_afectados']):,}")
    
    with col3:
        score_val = float(row['score_promedio']) if row['score_promedio'] else 0
        st.metric("📊 Score Promedio", f"{score_val:.2f}")
    
    with col4:
        monto_val = float(row['monto_promedio']) if row['monto_promedio'] else 0
        st.metric("💰 Monto Promedio", f"${monto_val:,.0f}")
    
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
            "🟠 Altas",
            f"{int(row['altas']):,}",
            delta=f"{pct_altas:.1f}%"
        )
    
    with col_c:
        pct_medias = (row['medias'] / row['total_alertas'] * 100) if row['total_alertas'] > 0 else 0
        st.metric(
            "🟡 Medias",
            f"{int(row['medias']):,}",
            delta=f"{pct_medias:.1f}%"
        )
else:
    st.info("ℹ️ No hay alertas registradas en el período seleccionado")
    st.caption("💡 Las alertas se registran cuando se ejecuta el pipeline completo de detección")
    st.stop()

st.markdown("---")

# ============================================================================
# ANÁLISIS GEOGRÁFICO
# ============================================================================

st.markdown("## 🗺️ Análisis Geográfico")

col_geo1, col_geo2 = st.columns(2)

with col_geo1:
    st.markdown("### Top 10 Departamentos")
    
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
            color='num_alertas',
            color_continuous_scale='Reds',
            labels={'num_alertas': 'Alertas', 'departamento': 'Departamento'}
        )
        fig_depto.update_layout(
            height=400,
            showlegend=False,
            plot_bgcolor='white',
            xaxis=dict(gridcolor='lightgray'),
            yaxis=dict(gridcolor='lightgray')
        )
        st.plotly_chart(fig_depto, config={'displayModeBar': False})
    else:
        st.info("No hay datos geográficos disponibles")

with col_geo2:
    st.markdown("### Top 10 Municipios")
    
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
        fig_municipio = px.bar(
            df_municipio,
            x='num_alertas',
            y='municipio_dane',
            orientation='h',
            color='cajeros_afectados',
            color_continuous_scale='Oranges',
            labels={
                'num_alertas': 'Alertas',
                'municipio_dane': 'Municipio',
                'cajeros_afectados': 'Cajeros'
            },
            hover_data=['departamento']
        )
        fig_municipio.update_layout(
            height=400,
            plot_bgcolor='white',
            xaxis=dict(gridcolor='lightgray'),
            yaxis=dict(gridcolor='lightgray')
        )
        st.plotly_chart(fig_municipio, config={'displayModeBar': False})
    else:
        st.info("No hay datos de municipios disponibles")

st.markdown("---")

# ============================================================================
# ANÁLISIS TEMPORAL
# ============================================================================

st.markdown("## ⏰ Patrón Horario de Alertas")

query_hora = """
SELECT 
    EXTRACT(HOUR FROM fecha_hora) as hora,
    COUNT(*) as num_alertas,
    COUNT(*) FILTER (WHERE severidad = 'critico') as criticas
FROM alertas_dispensacion
WHERE fecha_hora >= %s AND fecha_hora <= %s
GROUP BY EXTRACT(HOUR FROM fecha_hora)
ORDER BY hora
"""

df_hora = execute_query(query_hora, params=(fecha_inicio_dt, fecha_fin_dt))

if not df_hora.empty:
    col_temp1, col_temp2 = st.columns([2, 1])
    
    with col_temp1:
        fig_hora = px.line(
            df_hora,
            x='hora',
            y='num_alertas',
            markers=True,
            labels={'hora': 'Hora del Día', 'num_alertas': 'Número de Alertas'}
        )
        
        # Agregar línea de críticas
        fig_hora.add_scatter(
            x=df_hora['hora'],
            y=df_hora['criticas'],
            mode='lines+markers',
            name='Críticas',
            line=dict(color='red', dash='dot')
        )
        
        fig_hora.update_layout(
            height=400,
            hovermode='x unified',
            plot_bgcolor='white',
            xaxis=dict(gridcolor='lightgray', dtick=2),
            yaxis=dict(gridcolor='lightgray'),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        st.plotly_chart(fig_hora, config={'displayModeBar': False})
    
    with col_temp2:
        st.markdown("#### 🔍 Insights")
        
        # Hora con más alertas
        hora_max = df_hora.loc[df_hora['num_alertas'].idxmax()]
        st.metric(
            "⏰ Hora pico",
            f"{int(hora_max['hora']):02d}:00",
            f"{int(hora_max['num_alertas'])} alertas"
        )
        
        # Horario nocturno (22-06)
        df_nocturno = df_hora[((df_hora['hora'] >= 22) | (df_hora['hora'] <= 6))]
        alertas_nocturnas = df_nocturno['num_alertas'].sum()
        pct_nocturno = (alertas_nocturnas / df_hora['num_alertas'].sum() * 100) if df_hora['num_alertas'].sum() > 0 else 0
        
        st.metric(
            "🌙 Alertas nocturnas",
            f"{int(alertas_nocturnas):,}",
            f"{pct_nocturno:.1f}%"
        )
        
        # Total críticas
        st.metric(
            "🔴 Total críticas",
            f"{int(df_hora['criticas'].sum()):,}"
        )

st.markdown("---")

# ============================================================================
# TOP CAJEROS
# ============================================================================

st.markdown("## 🏆 Top 30 Cajeros con Más Alertas")

query_top = """
SELECT 
    a.cod_cajero,
    COUNT(*) as num_alertas,
    COUNT(*) FILTER (WHERE severidad = 'critico') as criticas,
    COUNT(*) FILTER (WHERE severidad = 'alto') as altas,
    COUNT(*) FILTER (WHERE severidad = 'medio') as medias,
    ROUND(AVG(score_anomalia), 2) as score_promedio,
    ROUND(AVG(monto_dispensado), 0) as monto_promedio,
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
        width='stretch',
        column_config={
            'cod_cajero': st.column_config.TextColumn('Cajero', width='small'),
            'num_alertas': st.column_config.ProgressColumn(
                'Total Alertas',
                format='%d',
                min_value=0,
                max_value=int(df_top['num_alertas'].max()),
                width='medium'
            ),
            'criticas': st.column_config.NumberColumn('🔴', format='%d', width='small'),
            'altas': st.column_config.NumberColumn('🟠', format='%d', width='small'),
            'medias': st.column_config.NumberColumn('🟡', format='%d', width='small'),
            'score_promedio': st.column_config.NumberColumn('Score', format='%.1f', width='small'),
            'monto_promedio': st.column_config.NumberColumn('Monto Prom.', format='$%.0f', width='medium'),
            'municipio_dane': st.column_config.TextColumn('Municipio', width='medium'),
            'departamento': st.column_config.TextColumn('Departamento', width='medium')
        },
        hide_index=True,
        height=500
    )
    
    # Métricas adicionales
    st.markdown("---")
    col_met1, col_met2, col_met3, col_met4 = st.columns(4)
    
    with col_met1:
        cajero_max = df_top.iloc[0]
        st.metric(
            "🥇 Cajero #1",
            cajero_max['cod_cajero'],
            f"{int(cajero_max['num_alertas'])} alertas"
        )
    
    with col_met2:
        total_criticas_top = df_top['criticas'].sum()
        st.metric(
            "🔴 Críticas (Top 30)",
            f"{int(total_criticas_top):,}"
        )
    
    with col_met3:
        score_max = df_top['score_promedio'].max()
        st.metric(
            "📊 Score máximo",
            f"{float(score_max):.1f}"
        )
    
    with col_met4:
        monto_max = df_top['monto_promedio'].max()
        st.metric(
            "💰 Monto máximo prom.",
            f"${float(monto_max):,.0f}"
        )

st.markdown("---")

# ============================================================================
# EXPORTAR REPORTES
# ============================================================================

st.markdown("## 📥 Exportar Datos")

col_exp1, col_exp2, col_exp3 = st.columns(3)

with col_exp1:
    if st.button("📊 Resumen Ejecutivo", width='stretch'):
        if not df_resumen.empty:
            csv = df_resumen.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"resumen_ejecutivo_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                key="download_resumen"
            )

with col_exp2:
    if st.button("🏆 Top 30 Cajeros", width='stretch'):
        if not df_top.empty:
            csv = df_top.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"top_cajeros_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                key="download_top"
            )

with col_exp3:
    if st.button("🗺️ Análisis Geográfico", width='stretch'):
        if not df_depto.empty:
            # Combinar departamentos y municipios
            df_geo_combined = pd.DataFrame({
                'tipo': ['Departamento'] * len(df_depto) + ['Municipio'] * len(df_municipio),
                'ubicacion': list(df_depto['departamento']) + list(df_municipio['municipio_dane']),
                'num_alertas': list(df_depto['num_alertas']) + list(df_municipio['num_alertas']),
                'cajeros_afectados': list(df_depto['cajeros_afectados']) + list(df_municipio['cajeros_afectados'])
            })
            
            csv = df_geo_combined.to_csv(index=False)
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv,
                file_name=f"analisis_geografico_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                key="download_geo"
            )

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: #666;'>
    <p><strong>Sistema de Detección de Fraudes ATM</strong></p>
    <p>Reporte generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p>Período analizado: {fecha_inicio.strftime('%Y-%m-%d')} a {fecha_fin.strftime('%Y-%m-%d')}</p>
</div>
""", unsafe_allow_html=True)