# 🚀 SISTEMA DE DETECCIÓN DE FRAUDES CON ML - MVP

## ✅ NUEVO ENFOQUE CORRECTO

Este sistema usa **Isolation Forest con features temporales** para detectar anomalías inteligentemente.

---

## 📦 ARCHIVOS INCLUIDOS

```
fraud_detection_historical/
├── 1_crear_features_temporales.py     ⭐ Genera features con contexto temporal
├── 2_entrenar_modelo.py                ⭐ Entrena Isolation Forest
├── 3_detectar_anomalias.py             ⭐ Detecta anomalías y genera alertas
├── ejecutar_pipeline.sh                 Ejecuta todo el pipeline
└── README_NUEVO_ENFOQUE.md              Este archivo
```

**IMPORTANTE:** También descarga el documento maestro:
- `PROYECTO_DETECCION_FRAUDES_ATM.md` - Para futuras conversaciones con Claude

---

## 🎯 DIFERENCIA CLAVE vs Anterior

### ❌ **Lo que estaba mal:**
```python
# Entrenaba con features ESTÁTICAS de cajeros
features = ['dispensacion_promedio', 'dispensacion_std', ...]
# Resultado: "Este cajero es raro en general" (poco útil)
```

### ✅ **Lo que hace ahora:**
```python
# Entrena con features TEMPORALES + CONTEXTUALES por ventana
features = [
    'monto_dispensado',           # Lo que pasó
    'hora', 'dia_semana', 'mes',  # Cuándo pasó (contexto temporal)
    'z_score_vs_cajero',          # Qué tan raro vs SU promedio
    'z_score_vs_hora',            # Qué tan raro vs ESTA hora
    'z_score_vs_dia_semana',      # Qué tan raro vs ESTE día
    'cambio_vs_anterior',         # Cambios bruscos
    'tendencia_24h',              # Dirección del movimiento
    ...
]
# Resultado: "Este cajero dispensó 300% más que SU promedio 
#             para un viernes a las 6pm en diciembre"
```

---

## 📊 DETECTA PATRONES COMO:

1. **Estacionalidad:** "Diciembre siempre sube, pero este cajero subió 400% vs su zona"
2. **Día/Hora:** "Los viernes 6pm suben 50%, pero este subió 300%"
3. **Cambios bruscos:** "Pasó de $500K a $5M en 15 minutos"
4. **Patrones replicados:** "5 cajeros en Bogotá norte con el mismo patrón anómalo"
5. **Tendencias sostenidas:** "Sube 20% cada día durante una semana"

---

## 🚀 EJECUCIÓN RÁPIDA

### **Opción A: Pipeline Completo (Automático)**
```bash
cd /dados/avc/src
chmod +x ejecutar_pipeline.sh

# Ejecutar todo de una vez (2-3 horas)
screen -S fraud_pipeline
./ejecutar_pipeline.sh
# Ctrl+A, D para detach
```

### **Opción B: Paso a Paso (Manual)**
```bash
cd /dados/avc/src

# Paso 1: Crear features temporales (~30-60 min)
uv run 1_crear_features_temporales.py --config ../config.yaml

# Paso 2: Entrenar modelo (~10-20 min)
uv run 2_entrenar_modelo.py --config ../config.yaml --contamination 0.01

# Paso 3: Detectar anomalías (~20-40 min)
uv run 3_detectar_anomalias.py --config ../config.yaml
```

---

## ⏱️ TIEMPOS ESTIMADOS

| Paso | Tiempo | Descripción |
|------|--------|-------------|
| 1. Features temporales | 30-60 min | Procesa 37.7M registros |
| 2. Entrenar modelo | 10-20 min | Entrena con 2M muestras |
| 3. Detectar anomalías | 20-40 min | Analiza todos los registros |
| **TOTAL** | **60-120 min** | Pipeline completo |

---

## 📋 QUÉ HACE CADA SCRIPT

### **1️⃣ crear_features_temporales.py**

**Entrada:** `mv_dispensacion_por_cajero_15min` (37.7M registros)

**Proceso:**
- Crea tabla `features_temporales`
- Calcula features temporales (hora, día, mes, etc.)
- Calcula z-scores vs múltiples baselines
- Calcula cambios y tendencias

**Salida:** Tabla `features_temporales` poblada (37.7M registros con 18 features)

---

### **2️⃣ entrenar_modelo.py**

**Entrada:** `features_temporales` (usa muestra de 2M para entrenar)

**Proceso:**
- Carga features temporales
- Normaliza con StandardScaler
- Entrena Isolation Forest (200 árboles, contamination=0.01)
- Valida modelo

**Salida:** `isolation_forest_dispensacion_v2.pkl` (~50MB)

---

### **3️⃣ detectar_anomalias.py**

**Entrada:** 
- Modelo entrenado
- `features_temporales` (todos los registros)

**Proceso:**
- Carga modelo
- Aplica a todos los registros en chunks
- Calcula scores de anomalía
- Clasifica por severidad (crítico/alto/medio)
- Genera razones detalladas

**Salida:** Tabla `alertas_dispensacion` poblada

---

## 🗄️ ESTRUCTURA DE DATOS

### **Nueva Tabla: features_temporales**
```sql
CREATE TABLE features_temporales (
    bucket_15min TIMESTAMP,
    cod_terminal VARCHAR,
    
    -- Features básicas
    monto_total_dispensado NUMERIC,
    num_transacciones INT,
    
    -- Features temporales (CLAVE)
    hora_del_dia INT,
    dia_semana INT,
    mes INT,
    es_fin_de_semana BOOLEAN,
    es_fin_de_mes BOOLEAN,
    es_quincena BOOLEAN,
    
    -- Features de desviación (CLAVE)
    z_score_vs_cajero NUMERIC,
    z_score_vs_hora NUMERIC,
    z_score_vs_dia_semana NUMERIC,
    percentil_vs_mes NUMERIC,
    
    -- Features de tendencia (CLAVE)
    cambio_vs_anterior NUMERIC,
    cambio_vs_ayer NUMERIC,
    tendencia_24h NUMERIC,
    volatilidad_reciente NUMERIC
);
```

### **Tabla Actualizada: alertas_dispensacion**
```sql
-- Ya existe, solo se poblará con nuevos datos
SELECT * FROM alertas_dispensacion LIMIT 5;
```

---

## ✅ VERIFICACIÓN

### **Después del Paso 1:**
```sql
-- Verificar que se crearon las features
SELECT COUNT(*) FROM features_temporales;
-- Esperado: 37,788,972

-- Ver muestra
SELECT 
    cod_terminal,
    bucket_15min,
    monto_total_dispensado,
    hora_del_dia,
    dia_semana,
    z_score_vs_cajero
FROM features_temporales
ORDER BY z_score_vs_cajero DESC
LIMIT 5;
```

### **Después del Paso 2:**
```bash
# Verificar que se creó el modelo
ls -lh ../models/isolation_forest_dispensacion_v2.pkl
# Esperado: ~50 MB
```

### **Después del Paso 3:**
```sql
-- Verificar alertas generadas
SELECT COUNT(*) FROM alertas_dispensacion;
-- Esperado: ~180,000+ (1% de 37.7M con contamination=0.01)

-- Por severidad
SELECT severidad, COUNT(*) 
FROM alertas_dispensacion 
GROUP BY severidad;

-- Top cajeros problemáticos
SELECT 
    cod_cajero,
    COUNT(*) as alertas
FROM alertas_dispensacion
GROUP BY cod_cajero
ORDER BY alertas DESC
LIMIT 10;
```

---

## 🎨 SIGUIENTE PASO: DASHBOARD

Una vez que `alertas_dispensacion` esté poblada:

```bash
streamlit run dashboard/dashboard_dispensacion.py
```

El dashboard mostrará:
- ✅ Mapa interactivo con alertas
- ✅ KPIs por severidad
- ✅ Patrones temporales (heatmap)
- ✅ Análisis por cajero
- ✅ Top cajeros problemáticos
- ✅ Sistema de carga para archivos nuevos

---

## 🔧 SOLUCIÓN DE PROBLEMAS

### **Error: Tabla features_temporales no existe**
```
Solución: Ejecutar paso 1 primero
→ uv run 1_crear_features_temporales.py --config ../config.yaml
```

### **Error: Modelo no encontrado**
```
Solución: Ejecutar paso 2 primero
→ uv run 2_entrenar_modelo.py --config ../config.yaml
```

### **Error: No genera alertas (0 alertas)**
```
Posibles causas:
1. contamination muy bajo → Aumentar a 0.02
2. Features con muchos NaN → Revisar log del paso 1
3. Modelo no entrenado correctamente → Re-ejecutar paso 2
```

### **Error de memoria**
```
Solución: Reducir batch-size o chunk-size
→ uv run 1_crear_features_temporales.py --batch-size 25000
→ uv run 3_detectar_anomalias.py --chunk-size 50000
```

---

## 📚 DOCUMENTACIÓN COMPLETA

Lee el documento maestro para contexto completo:
- `PROYECTO_DETECCION_FRAUDES_ATM.md`

Para futuras conversaciones con Claude:
```
"Hola Claude, estoy trabajando en el proyecto de detección 
de fraudes en ATM. Lee PROYECTO_DETECCION_FRAUDES_ATM.md 
para contexto.

Necesito ayuda con [tu pregunta]"
```

---

## 🎯 EXPECTATIVAS DEL MVP

Al finalizar tendrás:
- ✅ ~180,000 alertas detectadas automáticamente
- ✅ Clasificadas por severidad (crítico/alto/medio)
- ✅ Con razones detalladas de cada anomalía
- ✅ Dashboard interactivo funcional
- ✅ Sistema de ML que aprende patrones complejos

**Demostración clave:**
```
"Mira, este cajero (2532) tuvo una anomalía crítica:
 
 📍 Ubicación: Bogotá, Calle 72
 📅 Fecha: 2024-10-15 18:15
 💰 Dispensó: $5,200,000
 📊 Esperado: $450,000 (viernes 6pm)
 ⚠️  Severidad: CRÍTICO (score: 95/100)
 
 Razones:
 • Z-score: 8.2σ vs su promedio
 • 400% más que otros viernes a esa hora
 • Cambio brusco: +600% vs ventana anterior
 
 El modelo detectó esto automáticamente."
```

---

**¡Éxito con el MVP!** 🚀