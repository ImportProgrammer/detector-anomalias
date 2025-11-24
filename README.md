# 🏦 Dashboard de Detección de Fraudes ATM

Dashboard interactivo construido con Streamlit para visualizar y analizar anomalías detectadas por el sistema de Machine Learning.

---

## 📦 Estructura del Proyecto

```
dashboard/
├── app.py                          # Punto de entrada principal
├── config.py                       # Configuración del dashboard
│
├── pages/                          # Páginas del dashboard (Streamlit multipage)
│   ├── 1_🏠_Home.py                # Vista general ✅
│   ├── 2_🔍_Analisis_Detallado.py  # Análisis por cajero (por implementar)
│   ├── 3_📤_Procesar_Datos.py      # Carga de archivos (por implementar)
│   └── 4_📊_Estadisticas.py        # Estadísticas avanzadas (por implementar)
│
├── components/                     # Componentes reutilizables
│   ├── __init__.py
│   ├── kpis.py                     # KPIs y métricas ✅
│   ├── mapa.py                     # Mapas interactivos ✅
│   └── graficos.py                 # Gráficos Plotly ✅
│
└── utils/                          # Utilidades
    ├── __init__.py
    ├── db.py                       # Conexión a PostgreSQL ✅
    └── queries.py                  # Queries SQL ✅
```

---

## 🚀 Instalación

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
# O con uv:
uv pip install -r requirements.txt
```

### 2. Verificar configuración

Asegúrate de que `../config.yaml` existe y tiene las credenciales correctas de PostgreSQL:

```yaml
postgres:
  host: 'localhost'
  port: 5432
  database: 'fraud_detection'
  user: 'fraud_user'
  password: 'contraseña!'
```

---

## 🎯 Ejecución

```bash
cd /dados/avc/dashboard
streamlit run app.py
```

El dashboard se abrirá en: `http://localhost:8501`

---

## 📊 Páginas Disponibles

### 🏠 **Home** (✅ Implementada)

Vista general del sistema con:
- KPIs principales (alertas críticas, altas, medias)
- Comparación de períodos
- Mapa geográfico de alertas
- Tendencia temporal
- Patrones horarios (heatmap)
- Top cajeros problemáticos
- Alertas recientes

**Funcionalidades:**
- Filtros por rango de fechas
- Actualización manual de datos
- Exportación de alertas a CSV
- Drill-down en tablas

---

### 🔍 **Análisis Detallado** (🔄 Por implementar)

Análisis profundo por cajero específico:
- Búsqueda por código de cajero
- Perfil completo del cajero
- Timeline de alertas
- Comparación con cajeros similares
- Distribución de montos
- Historial completo

---

### 📤 **Procesar Datos** (🔄 Por implementar)

Sistema de carga de archivos nuevos:
- Upload de archivo (Excel/Parquet/CSV)
- Vista previa de datos
- Validación automática
- Procesamiento y detección de anomalías
- Actualización del dashboard

---

### 📊 **Estadísticas** (🔄 Por implementar)

Análisis avanzados:
- Distribución de scores
- Análisis geográfico detallado
- Tendencias mensuales
- Análisis por tipo de operación (cuando esté disponible)
- Reportes exportables

---

## 🎨 Componentes Reutilizables

### **KPIs** (`components/kpis.py`)

```python
from components.kpis import mostrar_kpis, mostrar_kpis_cajero

# Mostrar KPIs generales
mostrar_kpis(df_kpis)

# Mostrar KPIs de un cajero específico
mostrar_kpis_cajero(info_cajero, num_alertas)
```

### **Mapas** (`components/mapa.py`)

```python
from components.mapa import crear_mapa_alertas

# Crear mapa con alertas
fig = crear_mapa_alertas(df_alertas)
st.plotly_chart(fig)
```

### **Gráficos** (`components/graficos.py`)

```python
from components.graficos import crear_grafico_tendencia_temporal

# Crear gráfico de tendencia
fig = crear_grafico_tendencia_temporal(df_tendencia)
st.plotly_chart(fig)
```

---

## 🗄️ Conexión a Base de Datos

El dashboard usa connection pooling y caché para optimizar el rendimiento:

```python
from utils.db import execute_query

# Ejecutar query con caché de 5 minutos
df = execute_query("SELECT * FROM alertas_dispensacion LIMIT 10")
```

**Caché configurado:**
- KPIs y estadísticas: 5 minutos
- Verificación de conexión: 1 minuto

---

## 🎨 Personalización

### Colores por Severidad

Definidos en `config.py`:

```python
'colors': {
    'critico': '#f44336',  # Rojo
    'alto': '#ff9800',     # Naranja
    'medio': '#4caf50',    # Verde
    'normal': '#2196f3'    # Azul
}
```

### Límites de Visualización

```python
'limits': {
    'top_cajeros': 20,
    'alertas_recientes': 50,
    'mapa_max_markers': 1000
}
```

---

## 🔧 Desarrollo

### Agregar Nueva Página

1. Crear archivo en `pages/` con formato: `N_📌_Nombre.py`
2. El número determina el orden en el sidebar
3. El emoji aparece en el menú

```python
# pages/5_📈_Nueva_Pagina.py
import streamlit as st

st.title("📈 Nueva Página")
# Tu código aquí
```

### Agregar Nuevo Componente

```python
# components/nuevo_componente.py
def crear_nuevo_grafico(df):
    # Tu lógica aquí
    return fig

# Agregar a components/__init__.py
from .nuevo_componente import crear_nuevo_grafico
```

### Agregar Nuevo Query

```python
# utils/queries.py
QUERY_NUEVO = """
SELECT ...
FROM ...
WHERE ...
"""
```

---

## ⚡ Optimización

### Caché de Datos

```python
@st.cache_data(ttl=300)  # 5 minutos
def funcion_costosa(parametro):
    # Código costoso
    return resultado
```

### Caché de Recursos

```python
@st.cache_resource
def get_connection():
    # Recurso persistente
    return connection
```

---

## 🐛 Troubleshooting

### Error: No se conecta a la base de datos

```bash
# Verificar que PostgreSQL está corriendo
systemctl status postgresql

# Verificar credenciales en config.yaml
cat ../config.yaml
```

### Error: Módulo no encontrado

```bash
# Reinstalar dependencias
pip install -r requirements.txt
```

### Dashboard no actualiza datos

```bash
# Limpiar caché
# En el dashboard: Presiona 'C' para limpiar caché
# O reinicia el dashboard: Ctrl+C y vuelve a ejecutar
```

---

## 📈 Próximas Mejoras

- [ ] Implementar página de Análisis Detallado
- [ ] Implementar página de Procesamiento de Datos
- [ ] Implementar página de Estadísticas
- [ ] Agregar sistema de alertas por email
- [ ] Agregar exportación de reportes en PDF
- [ ] Agregar autenticación de usuarios
- [ ] Agregar modo oscuro
- [ ] Agregar refresh automático configurable

---

## 📞 Soporte

Para dudas o problemas:
1. Verificar logs del dashboard
2. Revisar configuración en `config.py`
3. Consultar queries en `utils/queries.py`

---

**Versión:** 1.0  
**Última actualización:** Noviembre 2024  
**Estado:** En desarrollo (Home completado, otras páginas pendientes)