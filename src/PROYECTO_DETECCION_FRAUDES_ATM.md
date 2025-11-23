# 🎯 PROYECTO: DETECCIÓN DE FRAUDES EN CAJEROS ATM CON IA/ML

## 📋 DOCUMENTO MAESTRO PARA FUTURAS CONVERSACIONES

**Fecha creación:** Noviembre 2024  
**Estado:** En desarrollo - MVP  
**Cliente:** Sistema bancario en Colombia  
**Tecnologías:** Python, PostgreSQL, TimescaleDB, Isolation Forest, Streamlit

---

## 🎯 OBJETIVO DEL PROYECTO

Crear un sistema de **detección inteligente de fraudes y anomalías** en cajeros automáticos usando **Machine Learning (Isolation Forest)** que:

1. **Detecte anomalías en tiempo real** en la dispensación de efectivo
2. **Identifique patrones de fraude** que se replican en múltiples cajeros/zonas
3. **Aprenda de patrones temporales** (estacionalidad, días de la semana, quincenas)
4. **Sea escalable** para detectar nuevos tipos de fraude
5. **Proporcione explicaciones claras** de por qué algo es anómalo

---

## 🎬 FASES DEL PROYECTO

### **FASE 1: MVP (Actual)**
**Objetivo:** Demostrar que IA/ML puede detectar fraudes mejor que reglas simples

**Entregables:**
1. ✅ Base de datos histórica con 37.7M registros procesados
2. 🔄 Modelo ML (Isolation Forest) entrenado con features correctas
3. 🔄 Dashboard interactivo con:
   - Vista histórica de anomalías
   - Mapa geográfico de cajeros problemáticos
   - Análisis temporal (patrones por hora/día/mes)
   - Sistema de carga para archivos nuevos de 15 minutos
4. 🔄 Sistema de alertas clasificadas por severidad

**Demostración clave:**
- "Este cajero dispensó 300% más que su promedio el viernes a las 6pm"
- "Este patrón de retiros se detectó en 5 cajeros de la misma zona"
- "Diciembre siempre sube, pero este cajero subió más que toda su zona"

### **FASE 2: Producción (Futuro)**
- Integración con sistemas bancarios en tiempo real
- Modelo supervisado con feedback de analistas
- Red neuronal para detección avanzada
- Alertas automáticas vía email/SMS
- API REST para consultas externas

---

## 🏗️ ARQUITECTURA TÉCNICA

### **Stack Tecnológico**
```
├── Base de Datos
│   ├── PostgreSQL 14+
│   ├── TimescaleDB (extensión para series temporales)
│   └── 64GB RAM, almacenamiento para ~100M registros
│
├── Procesamiento
│   ├── Python 3.10+
│   ├── pandas, numpy (manipulación de datos)
│   ├── scikit-learn (Isolation Forest)
│   ├── uv (gestor de dependencias)
│   └── PyYAML (configuración)
│
├── Visualización
│   ├── Streamlit (dashboard web)
│   ├── Plotly (gráficos interactivos)
│   └── Folium/PyDeck (mapas geográficos)
│
└── Gestión
    ├── config.yaml (configuración centralizada)
    ├── Git (control de versiones)
    └── screen (procesos largos persistentes)
```

### **Estructura de Directorios**
```
/dados/avc/
├── config.yaml                          # Configuración central
├── data/                                # CSVs originales por mes
├── parquet/                             # Datos consolidados
├── models/                              # Modelos ML entrenados
├── logs/                                # Logs de ejecución
├── outputs/                             # Reportes y exports
│
├── fraud_detection_historical/          # Scripts de detección
│   ├── crear_features_temporales.py     # Genera features ML
│   ├── entrenar_modelo.py               # Entrena Isolation Forest
│   ├── detectar_anomalias.py            # Aplica modelo a datos
│   └── verify_alertas.py                # Verifica resultados
│
└── dashboard/                           # Dashboard Streamlit
    └── dashboard_dispensacion.py        # App principal
```

---

## 🗄️ ESTRUCTURA DE BASE DE DATOS

### **Tablas Principales**

#### 1. `mv_dispensacion_por_cajero_15min` (Vista materializada)
**Descripción:** Agregación de transacciones en ventanas de 15 minutos por cajero

```sql
Columnas principales:
├── bucket_15min (timestamp)           # Ventana temporal
├── cod_terminal (varchar)             # ID del cajero
├── monto_total_dispensado (numeric)   # Total dispensado en la ventana
├── num_transacciones (int)            # Cantidad de transacciones
└── [agregaciones adicionales]

Registros: 37,788,972
Índices: bucket_15min, cod_terminal
```

#### 2. `features_ml` (Tabla de características de cajeros)
**Descripción:** Features estadísticas calculadas por cajero (histórico completo)

```sql
Columnas principales:
├── cod_cajero (varchar PK)
├── dispensacion_promedio (numeric)    # Promedio histórico
├── dispensacion_std (numeric)         # Desviación estándar
├── dispensacion_max (numeric)
├── coef_variacion (numeric)
├── std_por_hora (numeric)
├── volatilidad_promedio (numeric)
├── pct_anomalias_3std (numeric)       # % de veces fuera de 3σ
├── max_z_score_historico (numeric)
├── latitud, longitud (numeric)        # Ubicación geográfica
├── municipio_dane, departamento (varchar)
└── [45 columnas en total]

Registros: 2,903 cajeros
```

#### 3. `alertas_dispensacion` (Tabla de alertas generadas)
**Descripción:** Anomalías detectadas por el modelo ML

```sql
Columnas principales:
├── id (serial PK)
├── cod_cajero (varchar)
├── fecha_hora (timestamp)             # Momento de la anomalía
├── tipo_anomalia (varchar)            # 'isolation_forest', 'regla', etc.
├── severidad (varchar)                # 'critico', 'alto', 'medio'
├── score_anomalia (numeric)           # Score 0-100
├── monto_dispensado (numeric)
├── monto_esperado (numeric)           # Lo que se esperaba
├── desviacion_std (numeric)           # Cuántas σ de desviación
├── descripcion (text)                 # Descripción legible
├── razones (text)                     # Motivos específicos
├── modelo_usado (varchar)             # Versión del modelo
├── fecha_deteccion (timestamp)
├── validado (boolean)                 # Feedback del analista
├── validado_por (varchar)
└── fecha_validacion (timestamp)

Estado actual: Vacía (0 registros) - pendiente de poblar
Constraint: UNIQUE(cod_cajero, fecha_hora)
```

#### 4. `features_temporales` (Nueva - a crear)
**Descripción:** Features con contexto temporal para cada ventana de 15 min

```sql
Columnas a crear:
├── id (serial PK)
├── bucket_15min (timestamp)
├── cod_terminal (varchar)
│
├── -- Features básicas --
├── monto_total_dispensado (numeric)
├── num_transacciones (int)
│
├── -- Features temporales --
├── hora_del_dia (int)                 # 0-23
├── dia_semana (int)                   # 1=lunes, 7=domingo
├── mes (int)                          # 1-12
├── es_fin_de_semana (boolean)
├── es_fin_de_mes (boolean)            # Días 28-31
├── es_quincena (boolean)              # Días 14-16, 29-1
│
├── -- Features de desviación --
├── z_score_vs_cajero (numeric)        # vs promedio del cajero
├── z_score_vs_hora (numeric)          # vs misma hora histórica
├── z_score_vs_dia_semana (numeric)    # vs mismo día de semana
├── percentil_vs_mes (numeric)         # percentil en el mes
│
├── -- Features de tendencia --
├── cambio_vs_anterior (numeric)       # % cambio vs ventana anterior
├── cambio_vs_ayer (numeric)           # % cambio vs mismo momento ayer
├── tendencia_24h (numeric)            # Slope de últimas 24h
├── volatilidad_reciente (numeric)     # Std de últimas 24h
│
└── fecha_calculo (timestamp)

Constraint: UNIQUE(bucket_15min, cod_terminal)
Índices: bucket_15min, cod_terminal, hora_del_dia, dia_semana
```

---

## 🤖 ENFOQUE DE MACHINE LEARNING

### **¿Por qué Isolation Forest?**

**Ventajas:**
1. ✅ **No supervisado:** No necesita datos etiquetados de fraudes
2. ✅ **Detecta outliers complejos:** Patrones que reglas simples no ven
3. ✅ **Escalable:** Entrena rápido con millones de registros
4. ✅ **Explicable:** Podemos ver qué features causaron la anomalía
5. ✅ **Robusto:** No afectado por desbalance de clases

**Cómo funciona:**
```
1. Crea múltiples árboles de decisión aleatorios
2. Puntos normales requieren muchas divisiones para aislarse
3. Puntos anómalos se aíslan rápido (pocas divisiones)
4. Score = promedio de divisiones necesarias
   → Score bajo = anomalía
```

### **Features Engineering - LA CLAVE DEL ÉXITO**

El modelo es tan bueno como sus features. Necesitamos **contexto multidimensional:**

```python
# ❌ MAL: Solo características estáticas del cajero
features = ['dispensacion_promedio', 'dispensacion_std']
# Resultado: "Este cajero es raro en general" (no útil)

# ✅ BIEN: Características temporales + contextuales
features = [
    # Qué pasó
    'monto_dispensado', 'num_transacciones',
    
    # Cuándo pasó (contexto temporal)
    'hora', 'dia_semana', 'mes', 'es_fin_de_semana', 'es_quincena',
    
    # Qué tan raro es (múltiples perspectivas)
    'z_score_vs_promedio_cajero',      # vs histórico del cajero
    'z_score_vs_misma_hora',           # vs misma hora otros días
    'z_score_vs_mismo_dia_semana',     # vs mismo día de semana
    'percentil_vs_mes',                # vs todo el mes
    
    # Cambios y tendencias
    'cambio_vs_ventana_anterior',     # cambio brusco
    'tendencia_ultimas_24h',          # dirección del movimiento
    'volatilidad_reciente'            # estabilidad reciente
]
# Resultado: "Este cajero dispensó 300% más que SU promedio 
#             para un viernes a las 6pm, y es 5σ mayor que 
#             otros cajeros en su zona ese día"
```

### **Parámetros del Modelo**

```python
IsolationForest(
    contamination=0.01,      # Esperamos 1% de anomalías
    n_estimators=200,        # 200 árboles para robustez
    max_samples='auto',      # Usa subsamples automáticos
    max_features=0.8,        # Usa 80% de features por árbol
    random_state=42,         # Reproducibilidad
    n_jobs=-1               # Usa todos los cores
)
```

### **Proceso de Entrenamiento**

```
1. Preparar datos
   ├── Cargar ventanas de 15 min históricas (37.7M)
   ├── Calcular features temporales y contextuales
   └── Normalizar con StandardScaler
   
2. Entrenar modelo
   ├── Fit en ~1-2M ventanas representativas
   ├── Validar con diferentes períodos temporales
   └── Guardar modelo + scaler + feature_names
   
3. Aplicar a histórico completo
   ├── Procesar en chunks de 100k
   ├── Score_samples() para cada ventana
   ├── Filtrar anomalías (score < umbral)
   └── Clasificar por severidad
   
4. Guardar alertas
   ├── Insertar en alertas_dispensacion
   ├── Incluir razones detalladas
   └── Marcar para validación humana
```

---

## 📊 TIPOS DE ANOMALÍAS A DETECTAR

### **1. Anomalías Temporales**
```
Ejemplo: "Diciembre siempre sube, pero este cajero subió 400% vs su zona"
Features clave: mes, percentil_vs_mes, z_score_vs_zona
```

### **2. Anomalías de Día/Hora**
```
Ejemplo: "Los viernes a las 6pm suben 50%, pero este subió 300%"
Features clave: dia_semana, hora_del_dia, z_score_vs_misma_hora
```

### **3. Cambios Bruscos**
```
Ejemplo: "Este cajero pasó de $500K a $5M en 15 minutos"
Features clave: cambio_vs_anterior, volatilidad_reciente
```

### **4. Patrones Replicados**
```
Ejemplo: "5 cajeros en Bogotá norte muestran el mismo patrón anómalo"
Features clave: ubicación geográfica + score similar + mismo horario
```

### **5. Tendencias Sostenidas**
```
Ejemplo: "Este cajero ha subido 20% cada día durante una semana"
Features clave: tendencia_24h, cambio_vs_ayer
```

---

## 🎨 DASHBOARD - REQUISITOS

### **Página 1: Home (Vista General)**

```
┌─────────────────────────────────────────────┐
│ 📊 KPIs Globales                            │
│ ┌──────────┬──────────┬──────────┬────────┐│
│ │🔴 Crítico│🟡 Alto   │🟢 Medio  │📈Total ││
│ │  X,XXX   │  XX,XXX  │ XXX,XXX  │37.7M  ││
│ └──────────┴──────────┴──────────┴────────┘│
│                                             │
│ 🗓️ Filtros: [Fecha inicio] [Fecha fin]     │
│           [Intervalo: 15/30/45/60 min]     │
│                                             │
│ 🗺️ Mapa Geográfico Interactivo            │
│ ┌─────────────────────────────────────────┐│
│ │ 🔴 = Crítica  🟡 = Alta  🟢 = Media    ││
│ │                                         ││
│ │ [Mapa con clustering de alertas]       ││
│ │ Click → Detalle de la alerta           ││
│ └─────────────────────────────────────────┘│
│                                             │
│ 📈 Patrones Horarios (Heatmap)             │
│ ┌─────────────────────────────────────────┐│
│ │        0  3  6  9  12 15 18 21         ││
│ │ Lun   ██░░░░██████████░░               ││
│ │ Mar   ██░░░░████████████                ││
│ │ ...                                     ││
│ └─────────────────────────────────────────┘│
│                                             │
│ 📋 Alertas Recientes (Top 20)              │
│ [Tabla interactiva con detalles]          │
└─────────────────────────────────────────────┘
```

### **Página 2: Análisis Detallado**

```
- Búsqueda por cajero específico
- Timeline de alertas del cajero
- Comparación con cajeros similares
- Distribución de montos
- Historial completo
```

### **Página 3: Procesar Nuevos Datos**

```
- Upload de archivo (Excel/Parquet/CSV)
- Vista previa de datos
- Procesamiento automático
- Detección de anomalías en tiempo real
- Actualización del dashboard
```

### **Página 4: Estadísticas Globales**

```
- Top 20 cajeros problemáticos
- Distribución geográfica
- Tendencias temporales
- Análisis de montos
- Comparativos período actual vs anterior
```

---

## 🔄 FLUJO DE TRABAJO

### **A. Procesamiento Histórico (Una vez)**

```
1. Consolidar datos CSV → Parquet
   ├── Script: consolidar_datos.py
   └── Output: transacciones_consolidadas.parquet
   
2. Cargar a PostgreSQL + TimescaleDB
   ├── Script: cargar_postgres.py
   └── Crear mv_dispensacion_por_cajero_15min
   
3. Calcular features de cajeros
   ├── Script: calcular_features_dispensacion.py
   └── Output: tabla features_ml (2,903 cajeros)
   
4. Generar features temporales
   ├── Script: crear_features_temporales.py (NUEVO)
   └── Output: tabla features_temporales (37.7M registros)
   
5. Entrenar modelo
   ├── Script: entrenar_modelo.py (NUEVO)
   └── Output: isolation_forest_v2.pkl
   
6. Detectar anomalías históricas
   ├── Script: detectar_anomalias.py (NUEVO)
   └── Output: alertas_dispensacion poblada
   
7. Lanzar dashboard
   ├── Script: dashboard_dispensacion.py
   └── URL: http://localhost:8501
```

### **B. Procesamiento Incremental (Periódico)**

```
1. Recibir archivo nuevo (15 min de datos)
   └── Via upload en dashboard
   
2. Validar y cargar
   ├── Insertar en mv_dispensacion_por_cajero_15min
   └── Calcular features temporales
   
3. Aplicar modelo
   ├── Cargar modelo entrenado
   ├── Score nuevas ventanas
   └── Detectar anomalías
   
4. Generar alertas
   ├── Insertar en alertas_dispensacion
   └── Clasificar por severidad
   
5. Actualizar dashboard
   └── Refresh automático
```

---

## 🎯 CRITERIOS DE ÉXITO DEL MVP

### **Técnicos**
- ✅ Modelo entrenado con >1M ventanas históricas
- ✅ Detección de anomalías en <5 minutos para archivo nuevo
- ✅ Dashboard carga en <3 segundos
- ✅ Alertas clasificadas correctamente (critico/alto/medio)
- ✅ Explicaciones claras de cada anomalía

### **Negocio**
- ✅ Detectar al menos 3 tipos de patrones anómalos diferentes
- ✅ Identificar cajeros con comportamiento sospechoso
- ✅ Demostrar que ML supera reglas simples
- ✅ Sistema escalable para producción

### **Demostración**
```
"Mira, este cajero (2532) tuvo una anomalía crítica:
 
 📍 Ubicación: Bogotá, Calle 72
 📅 Fecha: 2024-10-15 18:15
 💰 Dispensó: $5,200,000
 📊 Esperado: $450,000 (promedio para viernes 6pm)
 ⚠️  Severidad: CRÍTICO (score: 95/100)
 
 Razones:
 • Z-score: 8.2σ vs su promedio histórico
 • 400% más que otros viernes a esa hora
 • Cambio brusco: +600% vs ventana anterior
 • Único cajero en su zona con este patrón
 
 Modelo detectó esto automáticamente y alertó en tiempo real.
 
 Además, detectamos 4 cajeros más en zonas similares con 
 patrones parecidos en las últimas 48 horas → posible fraude organizado"
```

---

## 📝 CONVENCIONES Y ESTÁNDARES

### **Código**
- Python 3.10+
- PEP 8 para estilo
- Type hints donde sea posible
- Docstrings en funciones principales
- Logging en lugar de prints

### **Commits Git**
```
feat: Nueva funcionalidad
fix: Corrección de bug
refactor: Refactorización de código
docs: Actualización de documentación
perf: Mejora de rendimiento
```

### **Archivos de Config**
- `config.yaml` para configuración centralizada
- NO hardcodear credenciales
- Variables de entorno para producción

### **Naming**
```python
# Tablas: snake_case
alertas_dispensacion
features_temporales

# Columnas: snake_case
cod_terminal
monto_total_dispensado

# Variables Python: snake_case
df_alertas
modelo_isolation_forest

# Funciones: snake_case + verbo
calcular_features_temporales()
detectar_anomalias()

# Clases: PascalCase
DetectorAnomalias
ModeloIsolationForest
```

---

## 🚨 DECISIONES CLAVE TOMADAS

### **1. Isolation Forest sobre otras opciones**
```
❌ Rechazadas:
- Reglas de negocio simples: No escalan, muchos falsos positivos
- LSTM/Redes neuronales: Requieren mucho más datos etiquetados
- K-Means: No detecta outliers, solo agrupa

✅ Isolation Forest porque:
- No supervisado (no necesita etiquetas)
- Rápido de entrenar y aplicar
- Explicable (vemos qué features contribuyen)
- Probado en detección de fraudes
- Base para modelo supervisado futuro
```

### **2. Features temporales son críticas**
```
Sin contexto temporal:
→ "Este cajero dispensó $5M" (¿es raro?)

Con contexto temporal:
→ "Este cajero dispensó $5M el viernes a las 6pm,
   cuando su promedio para viernes 6pm es $450K,
   y otros cajeros en su zona dispensaron $500K"
   (DEFINITIVAMENTE RARO)
```

### **3. TimescaleDB para series temporales**
```
- Optimizado para queries temporales
- Compresión automática de datos antiguos
- Agregaciones rápidas (time_bucket)
- Retención de datos configurable
```

### **4. Ventanas de 15 minutos**
```
- Balance entre granularidad y volumen de datos
- Suficiente para detectar ataques rápidos
- No genera demasiados registros
- Configurable a 30/45/60 min si es necesario
```

---

## 📚 RECURSOS Y REFERENCIAS

### **Documentación Técnica**
- Isolation Forest: https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html
- TimescaleDB: https://docs.timescale.com/
- Streamlit: https://docs.streamlit.io/

### **Papers Relevantes**
- "Isolation Forest" (Liu et al., 2008)
- "Anomaly Detection in Time Series" (Chandola et al., 2009)

### **Archivos del Proyecto**
- `Roadmap.md`: Roadmap original del proyecto
- `config.yaml`: Configuración completa del sistema
- `CAMBIOS_FINALES.md`: Últimos cambios realizados

---

## 🔮 ROADMAP FUTURO (Post-MVP)

### **Corto Plazo (1-3 meses)**
- [ ] Feedback loop: Analistas validan alertas
- [ ] Modelo supervisado con datos etiquetados
- [ ] Alertas automáticas vía email/SMS
- [ ] Integración con Power Automate

### **Medio Plazo (3-6 meses)**
- [ ] Red neuronal (LSTM) para series temporales
- [ ] Detección de patrones de fraude organizados
- [ ] Clustering de cajeros por comportamiento
- [ ] Predicción proactiva de fraudes

### **Largo Plazo (6-12 meses)**
- [ ] Sistema multi-banco
- [ ] IA generativa para reportes automáticos
- [ ] Integración con sistemas core bancarios
- [ ] App móvil para analistas

---

## 📞 CONTACTO Y SOPORTE

**Desarrollador:** Import  
**Stack:** Python, PostgreSQL, TimescaleDB, ML  
**Herramienta de gestión:** uv, Git  
**Servidor:** /dados/avc/ (64GB RAM)

---

## 🎯 USO DE ESTE DOCUMENTO

**Para futuras conversaciones con Claude:**

```
"Hola Claude, estoy trabajando en el proyecto de detección 
de fraudes en ATM. Lee el documento 
PROYECTO_DETECCION_FRAUDES_ATM.md para contexto completo.

Necesito ayuda con [tu pregunta específica]"
```

Esto evita re-explicar objetivos, arquitectura y decisiones ya tomadas.

---

**Última actualización:** Noviembre 21, 2024  
**Versión:** 1.0  
**Estado:** ✅ Listo para iniciar desarrollo del MVP con enfoque ML correcto