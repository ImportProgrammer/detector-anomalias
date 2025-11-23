"""
Configuración del Dashboard
"""

import yaml
from pathlib import Path

# Cargar configuración desde el archivo principal del proyecto
def load_config():
    """Carga configuración desde config.yaml"""
    config_path = Path(__file__).parent.parent / 'config.yaml'
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        # Configuración por defecto si no se encuentra el archivo
        return {
            'postgres': {
                'host': 'localhost',
                'port': 5432,
                'database': 'fraud_detection',
                'user': 'fraud_user',
                'password': 'avcSeguro123!'
            }
        }

# Configuración del dashboard
DASHBOARD_CONFIG = {
    'title': 'Sistema de Detección de Fraudes ATM',
    'version': '1.0',
    'page_icon': '🏦',
    
    # Colores por severidad
    'colors': {
        'critico': '#f44336',
        'alto': '#ff9800',
        'medio': '#4caf50',
        'normal': '#2196f3'
    },
    
    # Límites para visualización
    'limits': {
        'top_cajeros': 20,
        'alertas_recientes': 50,
        'mapa_max_markers': 1000
    },
    
    # Refresh automático
    'auto_refresh': {
        'enabled': False,
        'interval_seconds': 300  # 5 minutos
    }
}

# Cargar config de PostgreSQL
PROJECT_CONFIG = load_config()