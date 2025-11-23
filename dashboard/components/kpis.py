"""
Componente de KPIs para el dashboard
"""

import streamlit as st
import pandas as pd
from config import DASHBOARD_CONFIG

def mostrar_kpis(df_kpis):
    """
    Muestra KPIs principales en formato de métricas
    
    Args:
        df_kpis (pd.DataFrame): DataFrame con los KPIs
    """
    if df_kpis.empty:
        st.warning("No hay datos de KPIs disponibles")
        return
    
    row = df_kpis.iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🔴 Alertas Críticas",
            value=f"{int(row.get('alertas_criticas', 0)):,}",
            delta=None,
            help="Anomalías de severidad crítica que requieren atención inmediata"
        )
    
    with col2:
        st.metric(
            label="🟡 Alertas Altas",
            value=f"{int(row.get('alertas_altas', 0)):,}",
            delta=None,
            help="Anomalías de severidad alta que requieren revisión"
        )
    
    with col3:
        st.metric(
            label="🟢 Alertas Medias",
            value=f"{int(row.get('alertas_medias', 0)):,}",
            delta=None,
            help="Anomalías de severidad media para monitoreo"
        )
    
    with col4:
        st.metric(
            label="📊 Total Alertas",
            value=f"{int(row.get('total_alertas', 0)):,}",
            delta=None,
            help="Total de alertas en el período seleccionado"
        )

def mostrar_kpis_cajero(info_cajero, num_alertas):
    """
    Muestra KPIs específicos de un cajero
    
    Args:
        info_cajero (dict): Información del cajero
        num_alertas (int): Número de alertas del cajero
    """
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🚨 Alertas Totales",
            value=f"{num_alertas:,}",
            help="Número total de alertas de este cajero"
        )
    
    with col2:
        st.metric(
            label="💰 Dispensación Promedio",
            value=f"${info_cajero.get('dispensacion_promedio', 0):,.0f}",
            help="Monto promedio dispensado en ventanas de 15 minutos"
        )
    
    with col3:
        st.metric(
            label="📈 Coef. Variación",
            value=f"{info_cajero.get('coef_variacion', 0):.2f}",
            help="Coeficiente de variación (std/mean)"
        )
    
    with col4:
        st.metric(
            label="⚠️ % Anomalías Históricas",
            value=f"{info_cajero.get('pct_anomalias_3std', 0):.2f}%",
            help="Porcentaje de veces fuera de 3σ del promedio"
        )

def mostrar_comparacion_periodos(df_comparacion):
    """
    Muestra comparación entre períodos
    
    Args:
        df_comparacion (pd.DataFrame): DataFrame con comparación
    """
    if df_comparacion.empty:
        st.warning("No hay datos suficientes para comparar períodos")
        return
    
    row = df_comparacion.iloc[0]
    
    # Extraer valores
    alertas_actuales = int(row.get('alertas_actuales', 0))
    alertas_anteriores = int(row.get('alertas_anteriores', 0))
    score_actual = row.get('score_actual')
    score_anterior = row.get('score_anterior')
    cambio_porcentual = row.get('cambio_porcentual')
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Determinar color del delta
        if cambio_porcentual is not None and pd.notna(cambio_porcentual):
            # Rojo si aumentó (malo), verde si disminuyó (bueno)
            delta_color = "inverse" if cambio_porcentual > 0 else "normal"
            delta_text = f"{cambio_porcentual:+.1f}%"
        else:
            delta_color = "off"
            delta_text = "Sin datos"
        
        st.metric(
            label="📊 Alertas Período Actual",
            value=f"{alertas_actuales:,}",
            delta=delta_text,
            delta_color=delta_color,
            help="Número de alertas en el período seleccionado comparado con el período anterior"
        )
    
    with col2:
        st.metric(
            label="📉 Alertas Período Anterior",
            value=f"{alertas_anteriores:,}",
            help="Número de alertas en el período anterior (mismo número de días)"
        )
    
    with col3:
        # Calcular cambio en score
        if score_actual is not None and score_anterior is not None:
            cambio_score = score_actual - score_anterior
            delta_score = f"{cambio_score:+.2f}"
            delta_color_score = "inverse" if cambio_score > 0 else "normal"
        else:
            delta_score = None
            delta_color_score = "off"
        
        st.metric(
            label="📈 Score Promedio Actual",
            value=f"{score_actual:.2f}" if score_actual is not None else "N/A",
            delta=delta_score,
            delta_color=delta_color_score,
            help="Score promedio de severidad de anomalías (mayor = más severo)"
        )
    
    with col4:
        st.metric(
            label="📊 Score Promedio Anterior",
            value=f"{score_anterior:.2f}" if score_anterior is not None else "N/A",
            help="Score promedio del período anterior"
        )
    
    # # Mensaje interpretativo
    # if cambio_porcentual is not None and pd.notna(cambio_porcentual):
    #     if cambio_porcentual > 50:
    #         st.error(f"⚠️ **ALERTA:** Las alertas aumentaron significativamente ({cambio_porcentual:+.1f}%). Se recomienda investigar.")
    #     elif cambio_porcentual > 20:
    #         st.warning(f"⚡ **Atención:** Aumento moderado de alertas ({cambio_porcentual:+.1f}%).")
    #     elif cambio_porcentual < -20:
    #         st.success(f"✅ **Positivo:** Las alertas disminuyeron ({cambio_porcentual:.1f}%).")
    #     else:
    #         st.info(f"ℹ️ Cambio normal en alertas ({cambio_porcentual:+.1f}%).")
# def mostrar_comparacion_periodos(df_comparacion):
#     """
#     Muestra comparación entre períodos
    
#     Args:
#         df_comparacion (pd.DataFrame): DataFrame con comparación
#     """
#     if df_comparacion.empty:
#         return
    
#     row = df_comparacion.iloc[0]
    
#     col1, col2, col3 = st.columns(3)
    
#     with col1:
#         cambio = row.get('cambio_porcentual')
#         if cambio is not None and pd.notna(cambio):
#             delta_color = "inverse" if cambio > 0 else "normal"
#             delta_text = f"{cambio:+.1f}% vs período anterior"
#         else:
#             delta_color = "off"
#             delta_text = "Sin datos previos"
        
#         st.metric(
#             label="📊 Alertas (7 días)",
#             value=f"{int(row.get('alertas_actuales', 0)):,}",
#             delta=delta_text,
#             delta_color=delta_color,
#             help="Comparación con los 7 días anteriores"
#         )
    
#     with col2:
#         score_actual = row.get('score_actual')
#         st.metric(
#             label="📈 Score Actual",
#             value=f"{score_actual:.1f}" if score_actual is not None else "N/A",
#             help="Score promedio de anomalías en los últimos 7 días"
#         )
    
#     with col3:
#         score_anterior = row.get('score_anterior')
#         st.metric(
#             label="📉 Score Anterior",
#             value=f"{score_anterior:.1f}" if score_anterior is not None else "N/A",
#             help="Score promedio del período anterior"
#         )

def tarjeta_alerta(alerta, mostrar_detalles=True):
    """
    Muestra una alerta en formato de tarjeta
    
    Args:
        alerta (dict): Diccionario con información de la alerta
        mostrar_detalles (bool): Si mostrar detalles completos
    """
    colors = DASHBOARD_CONFIG['colors']
    severidad = alerta.get('severidad', 'medio')
    color = colors.get(severidad, colors['normal'])
    
    # Determinar emoji por severidad
    emoji_map = {
        'critico': '🔴',
        'alto': '🟡',
        'medio': '🟢'
    }
    emoji = emoji_map.get(severidad, '⚪')
    
    with st.container():
        st.markdown(f"""
        <div class='alerta-{severidad}'>
            <h4>{emoji} {alerta.get('cod_cajero', 'N/A')} - {severidad.upper()}</h4>
            <p><strong>Fecha:</strong> {alerta.get('fecha_hora', 'N/A')}</p>
            <p><strong>Score:</strong> {alerta.get('score_anomalia', 0):.1f}/100</p>
            <p><strong>Monto:</strong> ${alerta.get('monto_dispensado', 0):,.0f} 
               (Esperado: ${alerta.get('monto_esperado', 0):,.0f})</p>
            {f"<p><strong>Descripción:</strong> {alerta.get('descripcion', 'N/A')}</p>" if mostrar_detalles else ""}
            {f"<p><strong>Razones:</strong> {alerta.get('razones', 'N/A')}</p>" if mostrar_detalles else ""}
        </div>
        """, unsafe_allow_html=True)