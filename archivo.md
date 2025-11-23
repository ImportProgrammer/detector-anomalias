# 📋 RESUMEN COMPLETO DEL PROYECTO - Estado Actual y Próximos Pasos

## ✅ LO QUE YA ESTÁ COMPLETADO

### **1. Infraestructura Base**
```
✅ Servidor: 64GB RAM, Ubuntu 22.04
✅ PostgreSQL 15 instalado
✅ TimescaleDB 2.23.0 configurado
✅ Python 3.10 + uv como gestor de paquetes
✅ Dependencias instaladas: pandas, pyarrow, psycopg2, sqlalchemy, pyyaml, tqdm
```

### **2. Arquitectura Híbrida Implementada**

```
PARQUET (Almacenamiento histórico)
├─ Ubicación: /dados/avc/parquet/
├─ Archivo principal: transacciones_consolidadas.parquet
├─ Registros: 227,233,140 transacciones (2 años completos)
├─ Período: 2024-2025
├─ Meses: 14 meses consolidados
├─ Tamaño: ~5 GB
└─ Uso: Entrenamiento de modelos ML, análisis históricos

POSTGRESQL + TIMESCALEDB (Base operacional)
├─ Base de datos: fraud_detection
├─ Usuario: fraud_user
├─ Registros: ~13-14M transacciones (últimos 6 meses)
├─ Filtros aplicados:
│   ├─ Solo últimos 6 meses
│   ├─ Tipo Operación: Cambio De Pin, Avance, Retiro, Depositos, Transferencias
│   ├─ Autorizador requerido (no NULL)
│   └─ Sin duplicados
├─ Compresión: Columnstore habilitado (ahorro 50-70% espacio)
└─ Uso: Dashboard en tiempo real, queries rápidas (<1 seg)
```

### **3. Estructura de Base de Datos**

```sql
fraud_detection
├── cajeros (metadata de ATMs)
│   ├── 25 columnas (codigo, longitud, latitud, municipio, etc.)
│   ├── Índices: ubicación, tipo, estado
│   └── Estado: ⚠️ VACÍA (archivo Excel pendiente de cargar)
│
├── transacciones (hypertable con TimescaleDB)
│   ├── 20 columnas + fecha_transaccion_15min (granularidad 15 min)
│   ├── ~13-14M registros
│   ├── Particionado por semanas (chunk_interval: 1 week)
│   ├── Compresión automática después de 30 días
│   └── Índices optimizados
│
└── Tablas preparadas (vacías, para próximas fases):
    ├── features (para features calculadas)
    ├── scores (para anomalías detectadas)
    ├── razones_anomalias (explicaciones detalladas)
    ├── feedback (validación humana - Fase 2)
    └── modelos (versionamiento de modelos)
```

### **4. Scripts de Producción Creados**

```
/dados/avc/
├── config.yaml (configuración centralizada)
├── scripts/
│   ├── consolidar_a_parquet.py ✅ (CSV → Parquet)
│   └── cargar_a_postgres.py ✅ (Parquet → PostgreSQL)
└── logs/
    ├── consolidacion.log
    └── postgres.log
```

### **5. Modelos Desarrollados (Fase 1 previa)**

```
✅ Modelo 1: Reglas de Negocio (6 reglas detectando patrones conocidos)
✅ Modelo 2: Isolation Forest (detección no supervisada)
```

---

## 🎯 ESTADO ACTUAL - DÓNDE ESTAMOS

```
┌─────────────────────────────────────────────────────────────┐
│                    ROADMAP COMPLETO                         │
├─────────────────────────────────────────────────────────────┤
│ FASE 1: Preparación y EDA                          ✅ 100% │
│ FASE 2: Feature Engineering                        ⏳ 0%   │
│ FASE 3: Modelado ML                                ⏳ 40%  │
│ FASE 4: Dashboard Interactivo                      ⏳ 0%   │
│ FASE 5: Integración y Presentación                 ⏳ 0%   │
└─────────────────────────────────────────────────────────────┘
```

**Estamos entre FASE 1 y FASE 2:**
- ✅ Datos consolidados y cargados
- ✅ Infraestructura lista
- ⏳ Falta: Calcular features y aplicar modelos sobre PostgreSQL

---

## 📊 DATOS DISPONIBLES

### **Transacciones en PostgreSQL:**
```
- Total registros: ~13-14M
- Período: Últimos 6 meses
- Tipos de operación: 5 tipos relevantes
- Granularidad temporal: 15 minutos (fecha_transaccion_15min)
- Sin duplicados
- Todos con autorizador válido
```

### **Columnas principales en transacciones:**
```
- id_tlf (identificador transacción)
- fecha_transaccion (timestamp original)
- fecha_transaccion_15min (redondeado a 15 min)
- cod_terminal (código del cajero)
- tipo_operacion (Avance, Retiro, etc.)
- valor_transaccion (monto)
- cod_estado_transaccion (1=exitosa, 2=rechazada)
- autorizador (banco)
- adquiriente
- archivo_origen, mes_origen
```

---

## 🚀 PRÓXIMOS PASOS - LO QUE FALTA HACER

### **PASO 1: Cargar Metadata de Cajeros** ⚠️ PENDIENTE

**Problema actual:** Tabla `cajeros` está vacía

**Solución:**

1. **Limpiar archivo Excel:**
   ```bash
   # Crear script para limpiar Excel
   cd /dados/avc
   nano scripts/limpiar_excel_cajeros.py
   ```

   ```python
   import pandas as pd
   
   # Leer desde línea 3 (donde están los headers)
   df = pd.read_excel(
       "data/Inventario General Disp ATM_Centro de Efectivo_36_8260511562052405324.xlsx", 
       header=3
   )
   
   # Eliminar primera columna vacía
   df = df.iloc[:, 1:]
   
   # Guardar limpio
   df.to_excel("Inventario_General_Disp_ATM_Centro_de_Efectivo.xlsx", index=False)
   print(f"✅ {len(df):,} cajeros exportados")
   ```

2. **Ejecutar limpieza:**
   ```bash
   uv run scripts/limpiar_excel_cajeros.py
   ```

3. **Cargar a PostgreSQL:**
   ```bash
   # Opción A: Re-ejecutar cargar_a_postgres.py completo
   uv run scripts/cargar_a_postgres.py --config config.yaml
   
   # Opción B: Script específico para solo cajeros (crear)
   ```

**Tiempo estimado:** 5-10 minutos

---

### **PASO 2: Feature Engineering (CRÍTICO)** 🔥

**Objetivo:** Calcular 25+ features para ML desde tabla `transacciones`

**Script a crear:** `/dados/avc/scripts/calcular_features.py`

**Features a calcular (del Roadmap):**

#### **A. Features Temporales:**
```python
- hora (0-23)
- dia_semana (0-6)
- es_fin_de_semana (bool)
- es_horario_nocturno (bool: 22:00-06:00)
- es_madrugada (bool: 00:00-06:00)
```

#### **B. Features de Monto:**
```python
- diferencia_valor (Valor Transacción - Valor Original)
- es_retiro_maximo (bool: >=2,000,000)
- monto_normalizado_por_cajero (z-score)
```

#### **C. Features de Velocidad:**
```python
- tiempo_desde_anterior_seg (tiempo entre tx en mismo cajero)
- es_transaccion_rapida (bool: <10 seg)
- velocidad_tx_por_minuto (tx/min por cajero)
```

#### **D. Features de Operación:**
```python
- es_cambio_pin (bool)
- tipo_operacion_encoded (numérico)
- transaccion_exitosa (estado=1)
- transaccion_rechazada (estado=2)
```

#### **E. Features Agregadas por Cajero:**
```python
- tx_por_hora_cajero (rolling window 1 hora)
- monto_promedio_cajero (histórico)
- tasa_rechazo_cajero (rechazadas/total)
- desviacion_monto_cajero (std dev)
- velocidad_promedio_cajero
```

#### **F. Features de Cajero (desde metadata):**
```python
- cajero_adyacente_encoded (bool→int)
- cierre_nocturno_encoded (bool→int)
- es_ubicacion_aislada
- tipo_funcion_encoded
```

**Implementación:**

```python
# Pseudocódigo de calcular_features.py

import pandas as pd
from sqlalchemy import create_engine
import yaml

# 1. Conectar a PostgreSQL
# 2. Leer transacciones
# 3. Calcular features temporales (pandas)
# 4. Calcular features de ventana móvil (groupby + rolling)
# 5. JOIN con cajeros para features de ubicación
# 6. Guardar en tabla features
# 7. Log de progreso
```

**Tiempo estimado:** 2-3 horas de desarrollo + 10-20 min de ejecución

---

### **PASO 3: Aplicar Modelos de Detección** 🤖

**Scripts a adaptar:**
- `MODELO_1_REGLAS_NEGOCIO.py` (ya existe, adaptar a PostgreSQL)
- `MODELO_2_ISOLATION_FOREST.py` (ya existe, adaptar a PostgreSQL)

**Proceso:**

1. **Leer features desde PostgreSQL**
2. **Aplicar Modelo 1 (Reglas):**
   ```python
   # 6 reglas hardcodeadas:
   - Más de 5 cambios PIN en 1 hora → CRÍTICO
   - Retiros > $2M en 10 minutos → CRÍTICO
   - 10+ rechazos consecutivos → ADVERTENCIA
   - Transacciones fuera de horario → ADVERTENCIA
   - Velocidad > 10 tx/minuto → CRÍTICO
   - Patrón PIN→Retiro < 5 min → CRÍTICO
   ```

3. **Aplicar Modelo 2 (Isolation Forest):**
   ```python
   # Cargar modelo entrenado (.pkl)
   # Predecir anomalías
   # Score de 0-1
   ```

4. **Combinar scores:**
   ```python
   score_final = 0.5 * score_reglas + 0.5 * score_isolation_forest
   
   if score_final > 0.8: nivel = 'CRÍTICO'
   elif score_final > 0.5: nivel = 'SOSPECHOSO'
   else: nivel = 'NORMAL'
   ```

5. **Guardar en tabla `scores`:**
   ```sql
   INSERT INTO scores (
       id_transaccion,
       score_reglas,
       score_isolation_forest,
       score_final,
       nivel_anomalia,
       fecha_scoring
   ) VALUES (...)
   ```

**Tiempo estimado:** 1-2 horas de adaptación + 5-10 min de ejecución

---

### **PASO 4: Generar Razones Detalladas** 📝

**Script a crear:** `generar_razones_anomalias.py`

**Objetivo:** Explicar POR QUÉ cada transacción es anómala

**Ejemplo de razones:**
```python
Transacción ID: 123456789
Score Final: 0.92 (CRÍTICO)

Razones:
1. Monto anómalo: $2,500,000 (3.5σ sobre promedio del cajero)
2. Velocidad alta: 12 tx/minuto (normal: 2 tx/min)
3. Horario sospechoso: 03:45 AM (cajero cierra a 22:00)
4. Patrón Cambio PIN → Retiro en 3 minutos
5. Ubicación de riesgo: Cajero aislado sin adyacencia
```

**Implementación:**
```python
# Para cada anomalía en scores:
for transaccion in anomalias:
    razones = []
    
    # Analizar cada feature
    if monto > umbral_monto:
        razones.append(("Monto", "Alto", f"{monto:,.0f} > {umbral:,.0f}"))
    
    if velocidad > umbral_velocidad:
        razones.append(("Velocidad", "Alta", f"{velocidad} tx/min"))
    
    # ... más checks
    
    # Guardar en razones_anomalias
    INSERT INTO razones_anomalias (id_transaccion, tipo_razon, detalle, severidad)
```

**Tiempo estimado:** 1 hora de desarrollo + 5 min de ejecución

---

### **PASO 5: Dashboard Básico (MVP)** 📊

**Herramienta:** Streamlit (Python) o conectar a Power BI

**Componentes mínimos:**

```python
# dashboard.py con Streamlit

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.title("🚨 Sistema de Detección de Fraudes ATM")

# KPIs principales
col1, col2, col3 = st.columns(3)
col1.metric("Alertas Críticas", num_criticas, delta="+5")
col2.metric("Alertas Sospechosas", num_sospechosas)
col3.metric("Cajeros Monitoreados", num_cajeros)

# Mapa de cajeros con alertas
fig_mapa = px.scatter_mapbox(
    df_alertas,
    lat="latitud",
    lon="longitud",
    color="nivel_anomalia",
    hover_data=["cod_terminal", "score_final"],
    mapbox_style="open-street-map"
)
st.plotly_chart(fig_mapa)

# Timeline de alertas
fig_timeline = px.bar(
    df_timeline,
    x="fecha_transaccion_15min",
    y="num_alertas",
    color="nivel_anomalia"
)
st.plotly_chart(fig_timeline)

# Tabla de alertas detalladas
st.dataframe(df_alertas_detalle)
```

**Ejecutar:**
```bash
uv pip install streamlit plotly
streamlit run dashboard.py
```

**Tiempo estimado:** 2-3 horas de desarrollo

---

### **PASO 6: Reportes Automáticos por Email** 📧

**Script a crear:** `reportes_email.py`

**Objetivo:** Enviar reporte cada 15 minutos si hay alertas

**Componentes:**
```python
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template

# Template HTML del reporte
template = """
<html>
<body>
    <h2>🚨 Reporte de Alertas - {{ fecha }}</h2>
    
    <h3>Resumen:</h3>
    <ul>
        <li>Alertas Críticas: {{ num_criticas }}</li>
        <li>Alertas Sospechosas: {{ num_sospechosas }}</li>
    </ul>
    
    <h3>Top 5 Cajeros con Alertas:</h3>
    <table>
        {% for cajero in top_cajeros %}
        <tr>
            <td>{{ cajero.codigo }}</td>
            <td>{{ cajero.ubicacion }}</td>
            <td>{{ cajero.num_alertas }} alertas</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
"""

# Generar y enviar
html = template.render(fecha=now, num_criticas=..., ...)
enviar_email(destinatarios, "Reporte Fraudes ATM", html)
```

**Configurar cron:**
```bash
# Ejecutar cada 15 minutos
*/15 * * * * cd /dados/avc && /dados/avc/.venv/bin/python scripts/reportes_email.py
```

**Tiempo estimado:** 1-2 horas de desarrollo

---

## 📅 CRONOGRAMA SUGERIDO

### **Semana 1: Feature Engineering**
```
Lunes:    Cargar metadata cajeros
Martes:   Desarrollar script calcular_features.py (features temporales)
Miércoles: Continuar features (agregadas por cajero)
Jueves:   Continuar features (joins con cajeros)
Viernes:  Ejecutar y validar features completas
```

### **Semana 2: Modelos y Detección**
```
Lunes:    Adaptar Modelo 1 (Reglas) a PostgreSQL
Martes:   Adaptar Modelo 2 (Isolation Forest) a PostgreSQL
Miércoles: Aplicar modelos y generar scores
Jueves:   Desarrollar script razones_anomalias.py
Viernes:  Validar detecciones con casos reales
```

### **Semana 3: Dashboard y Reportes**
```
Lunes-Martes:    Desarrollar dashboard Streamlit
Miércoles:       Desarrollar reportes email
Jueves:          Testing end-to-end
Viernes:         Presentación al cliente
```

---

## 🔧 COMANDOS ÚTILES PARA CONTINUAR

### **Conectar a PostgreSQL:**
```bash
psql -U fraud_user -d fraud_detection
```

### **Queries de verificación:**
```sql
-- Ver registros en transacciones
SELECT COUNT(*) FROM transacciones;

-- Ver tipos de operación
SELECT tipo_operacion, COUNT(*) 
FROM transacciones 
GROUP BY tipo_operacion;

-- Ver rango de fechas
SELECT MIN(fecha_transaccion), MAX(fecha_transaccion) 
FROM transacciones;

-- Ver cajeros (debería estar vacía por ahora)
SELECT COUNT(*) FROM cajeros;

-- Ver estructura de features (vacía)
SELECT * FROM features LIMIT 1;
```

### **Activar entorno Python:**
```bash
cd /dados/avc
source .venv/bin/activate  # o usar: uv run
```

### **Ver logs:**
```bash
tail -f /dados/avc/logs/postgres.log
tail -f /dados/avc/logs/consolidacion.log
```

---

## 📂 ESTRUCTURA DE ARCHIVOS ACTUALIZADA

```
/dados/avc/
├── config.yaml
├── data/
│   ├── 2024/ (CSVs originales)
│   ├── 2025/ (CSVs originales)
│   └── Inventario General... .xlsx (metadata cajeros)
│
├── parquet/
│   ├── enero_2024.parquet ... junio_2025.parquet (14 archivos)
│   └── transacciones_consolidadas.parquet (227M registros)
│
├── scripts/
│   ├── consolidar_a_parquet.py ✅
│   ├── cargar_a_postgres.py ✅
│   ├── limpiar_excel_cajeros.py (por crear)
│   ├── calcular_features.py (por crear)
│   ├── aplicar_modelos.py (por crear)
│   ├── generar_razones.py (por crear)
│   ├── dashboard.py (por crear)
│   └── reportes_email.py (por crear)
│
├── models/ (modelos entrenados .pkl - ya tienes algunos)
├── logs/ (consolidacion.log, postgres.log)
└── outputs/ (reportes, mapas generados)
```

---

## ⚠️ PUNTOS CRÍTICOS A RECORDAR

1. **Tabla cajeros vacía** - Cargar primero antes de calcular features que la usen
2. **Granularidad 15 minutos** - Usar `fecha_transaccion_15min` para agregaciones
3. **Filtros aplicados** - PostgreSQL tiene solo datos filtrados (no todo el histórico)
4. **Compresión activa** - Chunks de +30 días se comprimen automáticamente
5. **Parquet intacto** - Para reentrenar modelos usa Parquet, no PostgreSQL

---

## 🎯 OBJETIVO FINAL

```
Sistema en producción con:
✅ 227M transacciones históricas (Parquet)
✅ 14M transacciones operacionales (PostgreSQL últimos 6 meses)
✅ Features calculadas para todas las transacciones
✅ 2 modelos detectando anomalías
✅ Scores y razones detalladas
✅ Dashboard en tiempo real
✅ Reportes automáticos cada 15 minutos
✅ Metadata de cajeros integrada
⏳ (Fase 2 futura: Supervised Learning con feedback)
```

---

## 📞 INFORMACIÓN DE CONTACTO DEL SISTEMA

```
Servidor: soacolpoc01
Usuario: jmcardenas1
Directorio: /dados/avc/
Base de datos: fraud_detection
Usuario DB: fraud_user
Puerto: 5432
```

---
