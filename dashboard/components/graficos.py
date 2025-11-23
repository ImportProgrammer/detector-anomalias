"""
Componente de gráficos para el dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import DASHBOARD_CONFIG

# def crear_grafico_tendencia_temporal(df_tendencia):
#     """
#     Crea gráfico de tendencia temporal de alertas
    
#     Args:
#         df_tendencia (pd.DataFrame): DataFrame con tendencia por fecha
    
#     Returns:
#         plotly.graph_objects.Figure: Figura del gráfico
#     """
#     if df_tendencia.empty:
#         return None
    
#     colors = DASHBOARD_CONFIG['colors']
    
#     fig = go.Figure()
    
#     # Líneas por severidad
#     if 'criticas' in df_tendencia.columns:
#         fig.add_trace(go.Scatter(
#             x=df_tendencia['fecha'],
#             y=df_tendencia['criticas'],
#             mode='lines+markers',
#             name='Críticas',
#             line=dict(color=colors['critico'], width=2),
#             marker=dict(size=6)
#         ))
    
#     if 'altas' in df_tendencia.columns:
#         fig.add_trace(go.Scatter(
#             x=df_tendencia['fecha'],
#             y=df_tendencia['altas'],
#             mode='lines+markers',
#             name='Altas',
#             line=dict(color=colors['alto'], width=2),
#             marker=dict(size=6)
#         ))
    
#     if 'medias' in df_tendencia.columns:
#         fig.add_trace(go.Scatter(
#             x=df_tendencia['fecha'],
#             y=df_tendencia['medias'],
#             mode='lines+markers',
#             name='Medias',
#             line=dict(color=colors['medio'], width=2),
#             marker=dict(size=6)
#         ))
    
#     fig.update_layout(
#         title='Tendencia de Alertas por Día',
#         xaxis_title='Fecha',
#         yaxis_title='Número de Alertas',
#         height=400,
#         hovermode='x unified',
#         legend=dict(
#             orientation="h",
#             yanchor="bottom",
#             y=1.02,
#             xanchor="right",
#             x=1
#         )
#     )
    
#     return fig

def crear_grafico_tendencia_temporal(df_tendencia):
    """
    Crea gráfico de tendencia temporal de alertas
    
    Args:
        df_tendencia (pd.DataFrame): DataFrame con tendencia por fecha
    
    Returns:
        plotly.graph_objects.Figure: Figura del gráfico
    """
    if df_tendencia.empty:
        return None
    
    colors = DASHBOARD_CONFIG['colors']
    
    fig = go.Figure()
    
    # Líneas por severidad
    if 'criticas' in df_tendencia.columns:
        fig.add_trace(go.Scatter(
            x=df_tendencia['fecha'],
            y=df_tendencia['criticas'],
            mode='lines+markers',
            name='Críticas',
            line=dict(color=colors['critico'], width=3),
            marker=dict(size=8),
            fill='tonexty',
            fillcolor='rgba(244, 67, 54, 0.1)'
        ))
    
    if 'altas' in df_tendencia.columns:
        fig.add_trace(go.Scatter(
            x=df_tendencia['fecha'],
            y=df_tendencia['altas'],
            mode='lines+markers',
            name='Altas',
            line=dict(color=colors['alto'], width=3),
            marker=dict(size=8),
            fill='tonexty',
            fillcolor='rgba(255, 152, 0, 0.1)'
        ))
    
    if 'medias' in df_tendencia.columns:
        fig.add_trace(go.Scatter(
            x=df_tendencia['fecha'],
            y=df_tendencia['medias'],
            mode='lines+markers',
            name='Medias',
            line=dict(color=colors['medio'], width=3),
            marker=dict(size=8),
            fill='tozeroy',
            fillcolor='rgba(76, 175, 80, 0.1)'
        ))
    
    fig.update_layout(
        title='Tendencia de Alertas por Día',
        xaxis_title='Fecha',
        yaxis_title='Número de Alertas',
        height=500,  # Más alto
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=50, r=50, t=80, b=50)  # Más margen
    )
    
    # Mejorar ejes
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(128, 128, 128, 0.2)',
        tickformat='%Y-%m-%d'
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(128, 128, 128, 0.2)'
    )
    
    return fig

def crear_heatmap_horario(df_heatmap):
    """
    Crea heatmap de patrones horarios
    
    Args:
        df_heatmap (pd.DataFrame): DataFrame con hora y día de semana
    
    Returns:
        plotly.graph_objects.Figure: Figura del heatmap
    """
    if df_heatmap.empty:
        return None
    
    # Pivot para crear matriz
    pivot = df_heatmap.pivot_table(
        values='num_alertas',
        index='hora',
        columns='dia_semana',
        fill_value=0
    )
    
    # Nombres de días
    dias = ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=[dias[int(col)] if col < len(dias) else f'Día {col}' for col in pivot.columns],
        y=[f"{int(h):02d}:00" for h in pivot.index],
        colorscale='Reds',
        hoverongaps=False,
        hovertemplate='Día: %{x}<br>Hora: %{y}<br>Alertas: %{z}<extra></extra>'
    ))
    
    fig.update_layout(
        title='Patrón de Alertas por Hora y Día de la Semana',
        xaxis_title='Día de la Semana',
        yaxis_title='Hora del Día',
        height=500
    )
    
    return fig

def crear_grafico_distribucion_scores(df_scores):
    """
    Crea gráfico de distribución de scores
    
    Args:
        df_scores (pd.DataFrame): DataFrame con distribución de scores
    
    Returns:
        plotly.graph_objects.Figure: Figura del gráfico
    """
    if df_scores.empty:
        return None
    
    fig = px.bar(
        df_scores,
        x='rango_score',
        y='cantidad',
        title='Distribución de Scores de Anomalías',
        labels={'rango_score': 'Rango de Score', 'cantidad': 'Cantidad de Alertas'},
        color='cantidad',
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        height=400,
        showlegend=False
    )
    
    return fig

def crear_grafico_top_cajeros(df_top):
    """
    Crea gráfico de top cajeros problemáticos
    
    Args:
        df_top (pd.DataFrame): DataFrame con top cajeros
    
    Returns:
        plotly.graph_objects.Figure: Figura del gráfico
    """
    if df_top.empty:
        return None
    
    # Limitar a top 20 y ordenar
    df_plot = df_top.head(20).copy()
    df_plot = df_plot.sort_values('num_alertas', ascending=True)  # Menor arriba, mayor abajo
    
    # Convertir cod_cajero a string para mejor visualización
    df_plot['cod_cajero_str'] = 'Cajero ' + df_plot['cod_cajero'].astype(str)
    
    fig = go.Figure()
    
    # Crear barras horizontales
    fig.add_trace(go.Bar(
        y=df_plot['cod_cajero_str'],
        x=df_plot['num_alertas'],
        orientation='h',
        marker=dict(
            color=df_plot['alertas_criticas'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(
                title='Alertas<br>Críticas',
                len=0.7,
                thickness=15
            ),
            line=dict(color='rgba(0,0,0,0.3)', width=1)
        ),
        text=df_plot['num_alertas'],
        textposition='outside',
        textfont=dict(size=11, color='black'),
        hovertemplate=(
            '<b>%{y}</b><br>' +
            'Total Alertas: %{x:,}<br>' +
            'Críticas: %{marker.color:,}<br>' +
            'Ubicación: %{customdata[0]}, %{customdata[1]}<br>' +
            '<extra></extra>'
        ),
        customdata=df_plot[['municipio_dane', 'departamento']].fillna('N/A').values
    ))
    
    fig.update_layout(
        title={
            'text': 'Top 20',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18}
        },
        xaxis_title='Número Total de Alertas',
        yaxis_title='',
        height=700,
        showlegend=False,
        margin=dict(l=120, r=80, t=80, b=50),
        plot_bgcolor='rgba(240, 240, 240, 0.5)',
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128, 128, 128, 0.2)'
        ),
        yaxis=dict(
            showgrid=False,
            tickfont=dict(size=11)
        ),
        font=dict(family="Arial, sans-serif")
    )
    
    return fig

def crear_grafico_comparacion_montos(df_alerta):
    """
    Crea gráfico comparando monto dispensado vs esperado
    
    Args:
        df_alerta (pd.DataFrame): DataFrame con alertas
    
    Returns:
        plotly.graph_objects.Figure: Figura del gráfico
    """
    if df_alerta.empty:
        return None
    
    # Tomar últimas 50 alertas
    df_plot = df_alerta.head(50).copy()
    df_plot = df_plot.sort_values('fecha_hora')
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_plot['fecha_hora'],
        y=df_plot['monto_dispensado'],
        mode='lines+markers',
        name='Monto Dispensado',
        line=dict(color='red', width=2),
        marker=dict(size=8)
    ))
    
    fig.add_trace(go.Scatter(
        x=df_plot['fecha_hora'],
        y=df_plot['monto_esperado'],
        mode='lines',
        name='Monto Esperado',
        line=dict(color='blue', width=2, dash='dash')
    ))
    
    fig.update_layout(
        title='Comparación: Monto Dispensado vs Esperado',
        xaxis_title='Fecha/Hora',
        yaxis_title='Monto ($)',
        height=400,
        hovermode='x unified'
    )
    
    return fig

def crear_grafico_alertas_por_municipio(df_municipio):
    """
    Crea gráfico de alertas por municipio
    
    Args:
        df_municipio (pd.DataFrame): DataFrame con alertas por municipio
    
    Returns:
        plotly.graph_objects.Figure: Figura del gráfico
    """
    if df_municipio.empty:
        return None
    
    # Top 20 municipios
    df_plot = df_municipio.head(20)
    
    fig = px.bar(
        df_plot,
        x='num_alertas',
        y='municipio_dane',
        orientation='h',
        title='Top 20 Municipios con Más Alertas',
        labels={'num_alertas': 'Número de Alertas', 'municipio_dane': 'Municipio'},
        color='num_alertas',
        color_continuous_scale='Reds',
        hover_data=['departamento', 'cajeros_afectados']
    )
    
    fig.update_layout(
        height=600,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig

def crear_grafico_tendencia_con_bandas(df_tendencia):
    """
    Crea gráfico de tendencia con bandas de confianza
    
    Args:
        df_tendencia (pd.DataFrame): DataFrame con tendencia por fecha
    
    Returns:
        plotly.graph_objects.Figure: Figura del gráfico
    """
    if df_tendencia.empty:
        return None
    
    import pandas as pd
    import numpy as np
    
    colors = DASHBOARD_CONFIG['colors']
    
    # Calcular bandas para alertas críticas
    df_tendencia = df_tendencia.sort_values('fecha')
    
    # Calcular promedio móvil de 7 días
    df_tendencia['criticas_promedio'] = df_tendencia['criticas'].rolling(window=7, center=True).mean()
    df_tendencia['criticas_std'] = df_tendencia['criticas'].rolling(window=7, center=True).std()
    
    # Calcular bandas (promedio ± 2 desviaciones estándar)
    df_tendencia['banda_superior'] = df_tendencia['criticas_promedio'] + (2 * df_tendencia['criticas_std'])
    df_tendencia['banda_inferior'] = df_tendencia['criticas_promedio'] - (2 * df_tendencia['criticas_std'])
    
    # Asegurar que banda inferior no sea negativa
    df_tendencia['banda_inferior'] = df_tendencia['banda_inferior'].clip(lower=0)
    
    fig = go.Figure()
    
    # Banda de confianza (área sombreada)
    fig.add_trace(go.Scatter(
        x=df_tendencia['fecha'],
        y=df_tendencia['banda_superior'],
        mode='lines',
        name='Límite Superior',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    fig.add_trace(go.Scatter(
        x=df_tendencia['fecha'],
        y=df_tendencia['banda_inferior'],
        mode='lines',
        name='Rango Normal',
        line=dict(width=0),
        fillcolor='rgba(244, 67, 54, 0.15)',
        fill='tonexty',
        showlegend=True,
        hoverinfo='skip'
    ))
    
    # Línea de promedio móvil
    fig.add_trace(go.Scatter(
        x=df_tendencia['fecha'],
        y=df_tendencia['criticas_promedio'],
        mode='lines',
        name='Tendencia',
        line=dict(color=colors['critico'], width=2, dash='dash'),
        showlegend=True
    ))
    
    # Valores reales (con marcadores más grandes donde hay anomalías)
    # Detectar puntos fuera de la banda
    df_tendencia['es_anomalia'] = (
        (df_tendencia['criticas'] > df_tendencia['banda_superior']) |
        (df_tendencia['criticas'] < df_tendencia['banda_inferior'])
    )
    
    # Puntos normales
    df_normal = df_tendencia[~df_tendencia['es_anomalia']]
    fig.add_trace(go.Scatter(
        x=df_normal['fecha'],
        y=df_normal['criticas'],
        mode='lines+markers',
        name='Alertas Críticas',
        line=dict(color=colors['critico'], width=3),
        marker=dict(size=6, color=colors['critico']),
        showlegend=True
    ))
    
    # Puntos anómalos (fuera de la banda)
    df_anomalo = df_tendencia[df_tendencia['es_anomalia']]
    if not df_anomalo.empty:
        fig.add_trace(go.Scatter(
            x=df_anomalo['fecha'],
            y=df_anomalo['criticas'],
            mode='markers',
            name='⚠️ Fuera de Rango',
            marker=dict(
                size=12,
                color='red',
                symbol='diamond',
                line=dict(color='darkred', width=2)
            ),
            showlegend=True,
            hovertemplate='<b>ANOMALÍA</b><br>Fecha: %{x}<br>Alertas: %{y}<extra></extra>'
        ))
    
    fig.update_layout(
        title='Tendencia de Alertas Críticas con Banda de Confianza',
        xaxis_title='Fecha',
        yaxis_title='Número de Alertas Críticas',
        height=500,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=50, r=50, t=80, b=50)
    )
    
    # Mejorar ejes
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(128, 128, 128, 0.2)'
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(128, 128, 128, 0.2)'
    )
    
    return fig