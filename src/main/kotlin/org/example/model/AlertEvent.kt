package org.example.model

import java.time.LocalDateTime

/**
 * Representa um evento de alerta meteorológico derivado.
 * Esta estrutura é publicada no tópico `weather-alerts` e consumida pelo WhatsAppNotifier.
 */
data class AlertEvent(
    val timestamp: LocalDateTime,
    val alertType: AlertType,
    val message: String,
    val severity: Severity,
    val metadata: Map<String, String> = emptyMap()
)

/**
 * Tipos de alertas suportados pelo sistema.
 */
enum class AlertType {
    DAILY_UMBRELLA,      // Guarda-chuva diário (6h00)
    DAILY_UV_PROTECTION, // Proteção UV (6h00)
    DAILY_THERMAL,       // Conforto térmico (6h00)
    IMMINENT_RAIN,       // Chuva iminente (tempo real)
    HEATWAVE             // Onda de calor (janela temporal)
}

/**
 * Níveis de severidade dos alertas.
 */
enum class Severity {
    INFO,    // Informativo
    WARNING, // Atenção
    ALERT    // Crítico
}
