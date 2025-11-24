#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funciones de Reglas de Negocio - Sistema de Detección de Fraudes
Generado automáticamente por entrenar_modelo_dispensacion.py
"""

def aplicar_reglas_negocio(
    dispensacion_actual,
    features_historicos,
    hora_actual,
    es_madrugada=False,
    dispensacion_reciente_promedio=None
):
    """
    Aplica reglas de negocio a una dispensación nueva.
    
    Args:
        dispensacion_actual: Monto dispensado en el período actual
        features_historicos: Dict con features del cajero
        hora_actual: Hora del día (0-23)
        es_madrugada: Bool indicando si es madrugada
        dispensacion_reciente_promedio: Promedio de últimos períodos
    
    Returns:
        score_reglas, razones, reglas_activadas
    """
    
    score = 0.0
    razones = []
    reglas_activadas = {}
    
    promedio = features_historicos.get('dispensacion_promedio', 0)
    std = features_historicos.get('dispensacion_std', 1)
    disp_madrugada_hist = features_historicos.get('disp_madrugada', 0)
    ratio_vs_zona = features_historicos.get('ratio_vs_zona', 1)
    pct_anomalias_hist = features_historicos.get('pct_anomalias_3std', 0)
    
    # REGLA 1: Dispensación extrema (peso: 0.30)
    if std > 0:
        z_score = abs((dispensacion_actual - promedio) / std)
        if z_score > 3:
            score_regla1 = min(z_score / 10, 1.0)
            score += 0.30 * score_regla1
            reglas_activadas['regla_1_dispensacion_extrema'] = {
                'activada': True, 'z_score': z_score,
                'score_parcial': 0.30 * score_regla1
            }
            razones.append(
                f"Dispensación extrema: ${dispensacion_actual:,.0f} "
                f"({z_score:.1f}σ del promedio ${promedio:,.0f})"
            )
    
    # REGLA 2: Horario sospechoso (peso: 0.25)
    if es_madrugada or (0 <= hora_actual <= 5):
        ratio_madrugada = disp_madrugada_hist / promedio if promedio > 0 else 0
        if ratio_madrugada < 0.1:
            score_regla2 = 1.0
            score += 0.25 * score_regla2
            reglas_activadas['regla_2_horario_sospechoso'] = {
                'activada': True, 'hora': hora_actual,
                'score_parcial': 0.25 * score_regla2
            }
            razones.append(
                f"Dispensación en madrugada ({hora_actual}:00h) "
                f"cuando normalmente no opera"
            )
    
    # REGLA 3: Cambio drástico (peso: 0.20)
    if dispensacion_reciente_promedio and dispensacion_reciente_promedio > 0:
        cambio_pct = ((dispensacion_actual - dispensacion_reciente_promedio) / 
                      dispensacion_reciente_promedio) * 100
        if abs(cambio_pct) > 200:
            score_regla3 = min(abs(cambio_pct) / 500, 1.0)
            score += 0.20 * score_regla3
            reglas_activadas['regla_3_cambio_drastico'] = {
                'activada': True, 'cambio_pct': cambio_pct,
                'score_parcial': 0.20 * score_regla3
            }
            direccion = "aumento" if cambio_pct > 0 else "disminución"
            razones.append(f"Cambio drástico: {direccion} de {abs(cambio_pct):.0f}%")
    
    # REGLA 4: Historial de anomalías (peso: 0.15)
    if pct_anomalias_hist > 5:
        score_regla4 = min(pct_anomalias_hist / 20, 1.0)
        score += 0.15 * score_regla4
        reglas_activadas['regla_4_historial_anomalias'] = {
            'activada': True, 'pct_anomalias': pct_anomalias_hist,
            'score_parcial': 0.15 * score_regla4
        }
        razones.append(
            f"Historial problemático: {pct_anomalias_hist:.1f}% anomalías"
        )
    
    # REGLA 5: Patrón geográfico (peso: 0.10)
    if ratio_vs_zona > 3 or ratio_vs_zona < 0.3:
        score_regla5 = 1.0 if ratio_vs_zona > 3 else 0.7
        score += 0.10 * score_regla5
        reglas_activadas['regla_5_patron_geografico'] = {
            'activada': True, 'ratio_vs_zona': ratio_vs_zona,
            'score_parcial': 0.10 * score_regla5
        }
        tipo = "mucho mayor" if ratio_vs_zona > 3 else "mucho menor"
        razones.append(f"Dispensación {tipo} que cajeros cercanos")
    
    score = min(score, 1.0)
    return score, razones, reglas_activadas
