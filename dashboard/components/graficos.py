"""
Componente de gráficos para el dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import DASHBOARD_CONFIG

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
            line=dict(color=colors['critico'], width=2),
            marker=dict(size=6)
        ))
    
    if 'altas' in df_tendencia.columns:
        fig.add_trace(go.Scatter(
            x=df_tendencia['fecha'],
            y=df_tendencia['altas'],
            mode='lines+markers',
            name='Altas',
            line=dict(color=colors['alto'], width=2),
            marker=dict(size=6)
        ))
    
    if 'medias' in df_tendencia.columns:
        fig.add_trace(go.Scatter(
            x=df_tendencia['fecha'],
            y=df_tendencia['medias'],
            mode='lines+markers',
            name='Medias',
            line=dict(color=colors['medio'], width=2),
            marker=dict(size=6)
        ))
    
    fig.update_layout(
        title='Tendencia de Alertas por Día',
        xaxis_title='Fecha',
        yaxis_title='Número de Alertas',
        height=400,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
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
    
    # Limitar a top 20
    df_plot = df_top.head(20).copy()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_plot['num_alertas'],
        y=df_plot['cod_cajero'],
        orientation='h',
        marker=dict(
            color=df_plot['alertas_criticas'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(title='Alertas<br>Críticas')
        ),
        text=df_plot['num_alertas'],
        textposition='auto',
        hovertemplate='<b>Cajero %{y}</b><br>' +
                      'Total alertas: %{x}<br>' +
                      'Críticas: %{marker.color}<br>' +
                      '<extra></extra>'
    ))
    
    fig.update_layout(
        title='Top 20 Cajeros con Más Alertas',
        xaxis_title='Número de Alertas',
        yaxis_title='Cajero',
        height=600,
        yaxis={'categoryorder': 'total ascending'}
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