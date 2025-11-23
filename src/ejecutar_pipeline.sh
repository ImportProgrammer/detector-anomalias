#!/bin/bash
# ============================================================================
# PIPELINE COMPLETO: DETECCIÓN DE ANOMALÍAS CON ML
# ============================================================================
# 
# Este script ejecuta todo el pipeline desde cero:
# 1. Crear features temporales
# 2. Entrenar modelo Isolation Forest
# 3. Detectar anomalías y generar alertas
#
# Uso:
#   chmod +x ejecutar_pipeline.sh
#   ./ejecutar_pipeline.sh
#
# O en screen para persistencia:
#   screen -S fraud_pipeline
#   ./ejecutar_pipeline.sh
#   # Ctrl+A, D para detach
# ============================================================================

set -e  # Salir si hay errores

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     🚀 PIPELINE DE DETECCIÓN DE ANOMALÍAS CON ML              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Fecha inicio: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "../config.yaml" ]; then
    echo "❌ ERROR: No se encontró config.yaml en el directorio padre"
    echo "   Asegúrate de ejecutar desde /dados/avc/src/"
    exit 1
fi

# ============================================================================
# PASO 1: CREAR FEATURES TEMPORALES
# ============================================================================

# echo "════════════════════════════════════════════════════════════════"
# echo "📊 PASO 1: CREANDO FEATURES TEMPORALES"
# echo "════════════════════════════════════════════════════════════════"
# echo ""

# START_TIME=$(date +%s)

# uv run 1_crear_features_temporales.py --config ../config.yaml --batch-size 50000

# END_TIME=$(date +%s)
# DURATION=$((END_TIME - START_TIME))
# echo ""
# echo "✅ Paso 1 completado en $(($DURATION / 60)) minutos"
# echo ""

# ============================================================================
# PASO 2: ENTRENAR MODELO
# ============================================================================

echo "════════════════════════════════════════════════════════════════"
echo "🤖 PASO 2: ENTRENANDO MODELO ISOLATION FOREST"
echo "════════════════════════════════════════════════════════════════"
echo ""

START_TIME=$(date +%s)

uv run 2_entrenar_modelo.py --config ../config.yaml --contamination 0.01 --sample-size 2000000

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo ""
echo "✅ Paso 2 completado en $(($DURATION / 60)) minutos"
echo ""

# ============================================================================
# PASO 3: DETECTAR ANOMALÍAS
# ============================================================================

echo "════════════════════════════════════════════════════════════════"
echo "🔍 PASO 3: DETECTANDO ANOMALÍAS"
echo "════════════════════════════════════════════════════════════════"
echo ""

START_TIME=$(date +%s)

uv run 3_detectar_anomalias.py --config ../config.yaml --chunk-size 100000

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo ""
echo "✅ Paso 3 completado en $(($DURATION / 60)) minutos"
echo ""

# ============================================================================
# RESUMEN FINAL
# ============================================================================

echo "════════════════════════════════════════════════════════════════"
echo "🎉 PIPELINE COMPLETADO"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Fecha fin: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo "✅ Features temporales creadas"
echo "✅ Modelo entrenado y guardado"
echo "✅ Anomalías detectadas y guardadas en alertas_dispensacion"
echo ""
echo "📊 Próximo paso: Lanzar dashboard"
echo "   → streamlit run dashboard/dashboard_dispensacion.py"
echo ""
echo "════════════════════════════════════════════════════════════════"