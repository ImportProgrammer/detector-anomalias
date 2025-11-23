"""
🏦 SISTEMA DE DETECCIÓN DE FRAUDES EN CAJEROS ATM
==================================================

Dashboard principal - Punto de entrada de la aplicación

Ejecutar:
    streamlit run app.py

Autor: Sistema de Detección de Fraudes
Versión: 1.0
"""

import streamlit as st
import sys
from pathlib import Path

# Agregar rutas al path
dashboard_path = Path(__file__).parent
sys.path.append(str(dashboard_path))

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Detección de Fraudes ATM",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "Sistema de Detección de Fraudes en Cajeros ATM usando Machine Learning"
    }
)

# ============================================================================
# ESTILOS PERSONALIZADOS
# ============================================================================

st.markdown("""
<style>
    /* Mejorar apariencia general */
    .main {
        padding: 2rem;
    }
    
    /* Títulos */
    h1 {
        color: #1f77b4;
        padding-bottom: 1rem;
        border-bottom: 2px solid #e0e0e0;
    }
    
    /* Cards de métricas */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: bold;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    
    /* Botones */
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        font-weight: 600;
    }
    
    /* Alertas */
    .alerta-critica {
        background-color: #ffebee;
        padding: 1rem;
        border-left: 4px solid #f44336;
        margin: 1rem 0;
    }
    
    .alerta-alta {
        background-color: #fff3e0;
        padding: 1rem;
        border-left: 4px solid #ff9800;
        margin: 1rem 0;
    }
    
    .alerta-media {
        background-color: #e8f5e9;
        padding: 1rem;
        border-left: 4px solid #4caf50;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# PÁGINA PRINCIPAL
# ============================================================================

# Header
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.image("https://img.icons8.com/fluency/96/bank-building.png", width=96)

st.title("🏦 Sistema de Detección de Fraudes en Cajeros ATM")
st.markdown("### Detección Inteligente de Anomalías usando Machine Learning")

st.markdown("---")

# Información principal
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("""
    ## Bienvenido al Sistema de Detección de Fraudes
    
    Este sistema utiliza **Inteligencia Artificial** y **Machine Learning** para detectar 
    anomalías y posibles fraudes en la dispensación de efectivo en cajeros automáticos.
    
    ### 🚀 Capacidades:
    
    - **Detección automática** de patrones anómalos en tiempo real
    - **Análisis histórico** de más de 37 millones de transacciones
    - **Clasificación inteligente** por nivel de severidad (Crítico, Alto, Medio)
    - **Visualización geográfica** de cajeros con comportamiento sospechoso
    - **Análisis temporal** de patrones por hora, día, mes
    - **Sistema de alertas** con razones detalladas
    
    ### 📊 Tecnologías:
    
    - **Isolation Forest**: Modelo de ML no supervisado para detección de anomalías
    - **PostgreSQL + TimescaleDB**: Base de datos optimizada para series temporales
    - **Streamlit**: Dashboard interactivo en tiempo real
    """)

with col2:
    st.info("""
    ### 📍 Navegación
    
    Usa el menú lateral para acceder a:
    
    🏠 **Home**  
    Vista general y KPIs principales
    
    🔍 **Análisis Detallado**  
    Drill-down por cajero específico
    
    📤 **Procesar Datos**  
    Cargar archivos nuevos
    
    📊 **Estadísticas**  
    Análisis avanzados y reportes
    """)
    
    st.success("""
    ### ✅ Estado del Sistema
    
    - Base de datos: **Activa**
    - Modelo ML: **Entrenado**
    - Alertas: **Activas**
    """)

st.markdown("---")

# Quick stats preview
st.markdown("## 📈 Vista Rápida del Sistema")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="🔴 Alertas Críticas",
        value="--",
        delta="Cargando...",
        help="Anomalías de severidad crítica que requieren atención inmediata"
    )

with col2:
    st.metric(
        label="🟡 Alertas Altas",
        value="--",
        delta="Cargando...",
        help="Anomalías de severidad alta que requieren revisión"
    )

with col3:
    st.metric(
        label="🟢 Alertas Medias",
        value="--",
        delta="Cargando...",
        help="Anomalías de severidad media para monitoreo"
    )

with col4:
    st.metric(
        label="🏧 Cajeros Monitoreados",
        value="2,903",
        delta="100%",
        help="Total de cajeros en el sistema"
    )

st.info("👈 **Selecciona una página en el menú lateral para comenzar**")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem;'>
    <p><strong>Sistema de Detección de Fraudes ATM v1.0</strong></p>
    <p>Powered by Machine Learning | Isolation Forest + TimescaleDB</p>
    <p>📧 Soporte técnico disponible</p>
</div>
""", unsafe_allow_html=True)