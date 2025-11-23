"""
Componentes reutilizables del dashboard
"""

from .kpis import mostrar_kpis, mostrar_kpis_cajero, mostrar_comparacion_periodos, tarjeta_alerta
from .mapa import crear_mapa_alertas, crear_mapa_calor_departamentos, crear_mapa_clusters
from .graficos import (
    crear_grafico_tendencia_temporal,
    crear_heatmap_horario,
    crear_grafico_distribucion_scores,
    crear_grafico_top_cajeros,
    crear_grafico_comparacion_montos,
    crear_grafico_alertas_por_municipio
)

__all__ = [
    'mostrar_kpis',
    'mostrar_kpis_cajero',
    'mostrar_comparacion_periodos',
    'tarjeta_alerta',
    'crear_mapa_alertas',
    'crear_mapa_calor_departamentos',
    'crear_mapa_clusters',
    'crear_grafico_tendencia_temporal',
    'crear_heatmap_horario',
    'crear_grafico_distribucion_scores',
    'crear_grafico_top_cajeros',
    'crear_grafico_comparacion_montos',
    'crear_grafico_alertas_por_municipio'
]