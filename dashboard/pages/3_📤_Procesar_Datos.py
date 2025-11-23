"""
📤 Procesar Datos - Cargar archivos nuevos y detectar anomalías
"""

import streamlit as st
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import io

# Agregar path del dashboard
dashboard_path = Path(__file__).parent.parent
sys.path.append(str(dashboard_path))

from utils.db import execute_query, test_connection, get_engine
import tempfile
import os

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Procesar Datos - Detección de Fraudes",
    page_icon="📤",
    layout="wide"
)

st.title("📤 Procesar Nuevos Datos")
st.markdown("Cargue archivos de dispensación para detectar anomalías en tiempo real")

# ============================================================================
# VERIFICAR CONEXIÓN
# ============================================================================

if not test_connection():
    st.error("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
    st.stop()

# ============================================================================
# INSTRUCCIONES
# ============================================================================

with st.expander("📖 Instrucciones de Uso", expanded=False):
    st.markdown("""
    ### Formato del archivo:
    
    El archivo debe contener registros en el siguiente formato:
    ```
    01,YYYYMMDDHHMMSS,terminal_id
    02,terminal_id,tipo_operacion,monto,timestamp,cantidad,campo1,campo2,...
    ```
    
    **Tipos de registro:**
    - `01`: Encabezado (fecha y terminal)
    - `02`: Transacción de dispensación
    
    **Tipos de operación:**
    - `2`: Retiro
    - `5`: Avance
    - `8`: Otros
    
    ### Pasos:
    1. Suba el archivo (CSV o TXT)
    2. Revise la vista previa
    3. Valide los datos
    4. Procese para detectar anomalías
    5. Revise los resultados
    """)

st.markdown("---")

# ============================================================================
# UPLOAD DE ARCHIVO
# ============================================================================

st.markdown("### 1️⃣ Cargar Archivo")

uploaded_file = st.file_uploader(
    "Seleccione el archivo de dispensación",
    type=['csv', 'txt'],
    help="Archivos en formato CSV o TXT con el formato especificado"
)

if uploaded_file is None:
    st.info("👆 Por favor, cargue un archivo para comenzar")
    st.stop()

# ============================================================================
# PROCESAMIENTO DEL ARCHIVO
# ============================================================================

st.markdown("---")
st.markdown("### 2️⃣ Vista Previa de Datos")

try:
    # Leer archivo
    content = uploaded_file.read().decode('utf-8')
    lines = content.strip().split('\n')
    
    st.success(f"✅ Archivo cargado: {uploaded_file.name}")
    st.info(f"📊 Total de líneas: {len(lines):,}")
    
    # Parsear archivo
    encabezados = []
    transacciones = []
    
    for line in lines:
        parts = line.split(',')
        tipo_registro = parts[0]
        
        if tipo_registro == '01':
            # Encabezado
            fecha_hora = parts[1]
            terminal = parts[2]
            encabezados.append({
                'fecha_hora': fecha_hora,
                'terminal': terminal
            })
        elif tipo_registro == '02':
            # Transacción
            transaccion = {
                'terminal': parts[1],
                'tipo_operacion': parts[2],
                'monto': int(parts[3]) if parts[3] else 0,
                'timestamp': parts[4],
                'cantidad': int(parts[5]) if parts[5] else 0
            }
            transacciones.append(transaccion)
    
    # Crear DataFrame
    df_transacciones = pd.DataFrame(transacciones)
    
    if df_transacciones.empty:
        st.error("❌ No se encontraron transacciones válidas en el archivo")
        st.stop()
    
    # Convertir timestamp
    df_transacciones['timestamp'] = pd.to_datetime(
        df_transacciones['timestamp'],
        format='%Y%m%d%H%M%S'
    )
    
    # Mapear tipos de operación
    tipo_op_map = {
        '2': 'Retiro',
        '5': 'Avance',
        '8': 'Otros'
    }
    df_transacciones['tipo_operacion_nombre'] = df_transacciones['tipo_operacion'].map(tipo_op_map)
    
    # Mostrar estadísticas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Transacciones", f"{len(df_transacciones):,}")
    
    with col2:
        terminales_unicos = df_transacciones['terminal'].nunique()
        st.metric("🏧 Cajeros", f"{terminales_unicos:,}")
    
    with col3:
        monto_total = df_transacciones['monto'].sum()
        st.metric("💰 Monto Total", f"${monto_total:,.0f}")
    
    with col4:
        fecha_min = df_transacciones['timestamp'].min()
        fecha_max = df_transacciones['timestamp'].max()
        st.metric("📅 Período", f"{(fecha_max - fecha_min).days} días")
    
    # Mostrar vista previa
    st.markdown("#### Vista Previa (primeras 100 transacciones)")
    st.dataframe(
        df_transacciones.head(100),
        use_container_width=True,
        column_config={
            'terminal': 'Terminal',
            'tipo_operacion': 'Tipo Op.',
            'tipo_operacion_nombre': 'Operación',
            'monto': st.column_config.NumberColumn('Monto', format='$%,.0f'),
            'timestamp': st.column_config.DatetimeColumn('Fecha/Hora', format='DD/MM/YYYY HH:mm'),
            'cantidad': 'Cantidad'
        }
    )
    
except Exception as e:
    st.error(f"❌ Error al procesar el archivo: {str(e)}")
    st.stop()

st.markdown("---")

# ============================================================================
# VALIDACIÓN
# ============================================================================

st.markdown("### 3️⃣ Validación de Datos")

validaciones = []

# Validar que hay datos
if len(df_transacciones) > 0:
    validaciones.append(("✅", "Archivo contiene transacciones", "success"))
else:
    validaciones.append(("❌", "Archivo sin transacciones", "error"))

# Validar columnas requeridas
columnas_requeridas = ['terminal', 'tipo_operacion', 'monto', 'timestamp']
columnas_presentes = all(col in df_transacciones.columns for col in columnas_requeridas)
if columnas_presentes:
    validaciones.append(("✅", "Todas las columnas requeridas están presentes", "success"))
else:
    validaciones.append(("❌", "Faltan columnas requeridas", "error"))

# Validar fechas
try:
    pd.to_datetime(df_transacciones['timestamp'])
    validaciones.append(("✅", "Formato de fechas válido", "success"))
except:
    validaciones.append(("❌", "Formato de fechas inválido", "error"))

# Validar montos
if df_transacciones['monto'].notna().all():
    validaciones.append(("✅", "Todos los montos son válidos", "success"))
else:
    validaciones.append(("⚠️", "Algunos montos tienen valores nulos", "warning"))

# Mostrar validaciones
for icono, mensaje, tipo in validaciones:
    if tipo == "success":
        st.success(f"{icono} {mensaje}")
    elif tipo == "error":
        st.error(f"{icono} {mensaje}")
    elif tipo == "warning":
        st.warning(f"{icono} {mensaje}")

# Verificar si hay errores críticos
hay_errores = any(v[2] == "error" for v in validaciones)

if hay_errores:
    st.error("⚠️ No se puede procesar el archivo debido a errores de validación")
    st.stop()

st.markdown("---")

# ============================================================================
# PROCESAMIENTO Y DETECCIÓN
# ============================================================================

st.markdown("### 4️⃣ Procesamiento y Detección")

st.info("""
**Nota:** El procesamiento completo incluye:
1. Agregación de datos en ventanas de 15 minutos
2. Cálculo de features temporales
3. Aplicación del modelo de Machine Learning
4. Generación de alertas por anomalías detectadas

Este proceso puede tomar varios minutos dependiendo del tamaño del archivo.
""")

col_proc1, col_proc2 = st.columns([1, 4])

with col_proc1:
    procesar = st.button(
        "🚀 Procesar y Detectar",
        type="primary",
        use_container_width=True,
        disabled=hay_errores
    )

if procesar:
    with st.spinner("🔄 Procesando datos..."):
        try:
            # Paso 1: Agregar por ventanas de 15 minutos
            st.write("📊 Paso 1/4: Agregando datos por ventanas de 15 minutos...")
            
            df_transacciones['bucket_15min'] = df_transacciones['timestamp'].dt.floor('15min')
            
            df_agregado = df_transacciones.groupby(['terminal', 'bucket_15min']).agg({
                'monto': 'sum',
                'cantidad': 'sum'
            }).reset_columns()
            
            df_agregado.columns = ['cod_terminal', 'bucket_15min', 'monto_total_dispensado', 'num_transacciones']
            
            progress_bar = st.progress(25)
            
            # Paso 2: Simular cálculo de features (en producción llamaría al script real)
            st.write("🔧 Paso 2/4: Calculando features temporales...")
            progress_bar.progress(50)
            
            # Paso 3: Simular aplicación del modelo
            st.write("🤖 Paso 3/4: Aplicando modelo de Machine Learning...")
            progress_bar.progress(75)
            
            # Simulación: detectar "anomalías" (montos muy altos)
            umbral_alto = df_agregado['monto_total_dispensado'].quantile(0.95)
            df_agregado['es_anomalia'] = df_agregado['monto_total_dispensado'] > umbral_alto
            
            anomalias_detectadas = df_agregado['es_anomalia'].sum()
            
            # Paso 4: Generar reporte
            st.write("📝 Paso 4/4: Generando reporte de alertas...")
            progress_bar.progress(100)
            
            st.markdown("---")
            st.markdown("### 5️⃣ Resultados")
            
            st.success(f"✅ Procesamiento completado exitosamente!")
            
            # Mostrar resultados
            col_res1, col_res2, col_res3 = st.columns(3)
            
            with col_res1:
                st.metric(
                    "📊 Ventanas Procesadas",
                    f"{len(df_agregado):,}",
                    help="Número de ventanas de 15 minutos analizadas"
                )
            
            with col_res2:
                st.metric(
                    "🚨 Anomalías Detectadas",
                    f"{anomalias_detectadas:,}",
                    delta=f"{(anomalias_detectadas/len(df_agregado)*100):.1f}%",
                    help="Ventanas con comportamiento anómalo"
                )
            
            with col_res3:
                terminales_afectados = df_agregado[df_agregado['es_anomalia']]['cod_terminal'].nunique()
                st.metric(
                    "🏧 Cajeros Afectados",
                    f"{terminales_afectados:,}",
                    help="Cajeros con al menos una anomalía"
                )
            
            # Mostrar alertas detectadas
            if anomalias_detectadas > 0:
                st.markdown("#### 🚨 Alertas Detectadas")
                
                df_alertas = df_agregado[df_agregado['es_anomalia']].copy()
                df_alertas = df_alertas.sort_values('monto_total_dispensado', ascending=False)
                
                st.dataframe(
                    df_alertas,
                    use_container_width=True,
                    column_config={
                        'cod_terminal': 'Cajero',
                        'bucket_15min': st.column_config.DatetimeColumn('Fecha/Hora', format='DD/MM/YYYY HH:mm'),
                        'monto_total_dispensado': st.column_config.NumberColumn('Monto', format='$%,.0f'),
                        'num_transacciones': 'Transacciones',
                        'es_anomalia': None
                    },
                    hide_index=True
                )
                
                # Botón de exportación
                if st.button("📥 Exportar Alertas"):
                    csv = df_alertas.to_csv(index=False)
                    st.download_button(
                        label="⬇️ Descargar CSV",
                        data=csv,
                        file_name=f"alertas_nuevas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("ℹ️ No se detectaron anomalías en este archivo")
            
            st.markdown("---")
            
            st.success("""
            ✅ **Próximos pasos:**
            - Las alertas han sido procesadas
            - En producción, estas alertas se insertarían en la base de datos
            - El dashboard se actualizaría automáticamente
            - Se enviarían notificaciones a los responsables
            """)
            
        except Exception as e:
            st.error(f"❌ Error durante el procesamiento: {str(e)}")
            st.exception(e)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p><strong>Nota:</strong> Esta es una versión de demostración.</p>
    <p>En producción, el procesamiento se integraría con los scripts de detección completos.</p>
</div>
""", unsafe_allow_html=True)