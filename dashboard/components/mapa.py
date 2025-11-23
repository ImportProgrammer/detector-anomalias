"""
Componente de mapas para visualización geográfica
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import DASHBOARD_CONFIG

def crear_mapa_alertas(df_alertas):
    """
    Crea mapa interactivo con alertas geográficas
    
    Args:
        df_alertas (pd.DataFrame): DataFrame con alertas y coordenadas
    
    Returns:
        plotly.graph_objects.Figure: Figura del mapa
    """
    if df_alertas.empty or 'latitud' not in df_alertas.columns:
        st.warning("No hay datos de ubicación disponibles para el mapa")
        return None
    
    # Filtrar registros con coordenadas válidas
    df_map = df_alertas[
        (df_alertas['latitud'].notna()) & 
        (df_alertas['longitud'].notna())
    ].copy()
    
    if df_map.empty:
        st.warning("No hay alertas con coordenadas válidas")
        return None
    
    # Mapear severidad a colores
    colors = DASHBOARD_CONFIG['colors']
    color_map = {
        'critico': colors['critico'],
        'alto': colors['alto'],
        'medio': colors['medio']
    }
    
    df_map['color'] = df_map['severidad'].map(color_map)
    
    # Crear mapa con Plotly
    fig = go.Figure()
    
    # Agregar puntos por severidad
    for severidad in ['critico', 'alto', 'medio']:
        df_sev = df_map[df_map['severidad'] == severidad]
        
        if not df_sev.empty:
            fig.add_trace(go.Scattermapbox(
                lat=df_sev['latitud'],
                lon=df_sev['longitud'],
                mode='markers',
                marker=dict(
                    size=10 if severidad == 'critico' else 8,
                    color=color_map[severidad],
                    opacity=0.8
                ),
                text=df_sev.apply(lambda row: 
                    f"Cajero: {row['cod_cajero']}<br>"
                    f"Severidad: {row['severidad']}<br>"
                    f"Score: {row['score_anomalia']:.1f}<br>"
                    f"Monto: ${row['monto_dispensado']:,.0f}<br>"
                    f"Ubicación: {row.get('municipio_dane', 'N/A')}, {row.get('departamento', 'N/A')}"
                , axis=1),
                hoverinfo='text',
                name=severidad.upper(),
                showlegend=True
            ))
    
    # Configurar layout del mapa
    fig.update_layout(
        mapbox=dict(
            style='open-street-map',
            center=dict(
                lat=df_map['latitud'].mean(),
                lon=df_map['longitud'].mean()
            ),
            zoom=5
        ),
        height=600,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255, 255, 255, 0.8)"
        )
    )
    
    return fig

def crear_mapa_calor_departamentos(df_departamentos):
    """
    Crea mapa de calor por departamentos
    
    Args:
        df_departamentos (pd.DataFrame): DataFrame con alertas por departamento
    
    Returns:
        plotly.graph_objects.Figure: Figura del mapa
    """
    if df_departamentos.empty:
        return None
    
    # Crear gráfico de barras horizontal como alternativa
    fig = px.bar(
        df_departamentos,
        x='num_alertas',
        y='departamento',
        orientation='h',
        title='Alertas por Departamento',
        labels={'num_alertas': 'Número de Alertas', 'departamento': 'Departamento'},
        color='num_alertas',
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        height=400,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig

def crear_mapa_clusters(df_alertas, zoom_level=5):
    """
    Crea mapa con clustering de alertas
    
    Args:
        df_alertas (pd.DataFrame): DataFrame con alertas
        zoom_level (int): Nivel de zoom inicial
    
    Returns:
        plotly.graph_objects.Figure: Figura del mapa
    """
    if df_alertas.empty:
        return None
    
    # Filtrar coordenadas válidas
    df_map = df_alertas[
        (df_alertas['latitud'].notna()) & 
        (df_alertas['longitud'].notna())
    ].copy()
    
    if df_map.empty:
        return None
    
    # Crear mapa con densidad
    fig = px.density_mapbox(
        df_map,
        lat='latitud',
        lon='longitud',
        z='score_anomalia',
        radius=15,
        center=dict(
            lat=df_map['latitud'].mean(),
            lon=df_map['longitud'].mean()
        ),
        zoom=zoom_level,
        mapbox_style='open-street-map',
        title='Densidad de Anomalías',
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=30, b=0)
    )
    
    return fig