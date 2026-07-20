package org.example.tools

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.example.config.WeatherWatcherConfig
import org.example.model.AlertEvent
import org.example.model.AlertType
import org.example.model.Severity
import org.example.serdes.AlertSerializer
import java.time.LocalDateTime
import java.util.Properties

/**
 * Publica alertas manualmente no tópico weather-alerts para fins de demonstração.
 * Útil para exibir alertas difíceis de disparar naturalmente (ex: TEMPERATURE_DROP, HEAT_BEFORE_RAIN).
 *
 * Uso — publicar todos os tipos:
 *   mvn compile exec:java -Dexec.mainClass="org.example.tools.ManualAlertPublisherKt"
 *
 * Uso — publicar um tipo específico:
 *   mvn compile exec:java -Dexec.mainClass="org.example.tools.ManualAlertPublisherKt" -Dexec.args="TEMPERATURE_DROP"
 *
 * Tipos disponíveis: TEMPERATURE_DROP, HEAT_BEFORE_RAIN, HEATWAVE, HUMID_HEAT,
 *                    IMMINENT_RAIN, DAILY_UMBRELLA, DAILY_UV_PROTECTION, DAILY_THERMAL, ALL
 */
fun main(args: Array<String>) {
    val producer = KafkaProducer<String, AlertEvent>(Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, WeatherWatcherConfig.kafkaBootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AlertSerializer::class.java.name)
        put(ProducerConfig.ACKS_CONFIG, "all")
    })

    val location = "${WeatherWatcherConfig.locationLatitude},${WeatherWatcherConfig.locationLongitude}"
    val target = args.firstOrNull()?.uppercase() ?: "ALL"

    val alerts = buildAlerts(location).filter { target == "ALL" || it.alertType.name == target }

    if (alerts.isEmpty()) {
        println("Tipo desconhecido: $target. Use ALL ou um dos: ${AlertType.entries.joinToString()}")
        producer.close()
        return
    }

    alerts.forEach { alert ->
        val record = ProducerRecord("weather-alerts", alert.alertType.name, alert)
        producer.send(record) { _, ex ->
            if (ex != null) println("Erro ao publicar ${alert.alertType}: ${ex.message}")
            else println("Publicado: ${alert.alertType} [${alert.severity}]")
        }
    }

    producer.flush()
    producer.close()
}

private fun buildAlerts(location: String): List<AlertEvent> = listOf(
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.TEMPERATURE_DROP,
        message = "🌬️ Queda brusca de temperatura detectada! De 31.0°C para 24.5°C (queda de 6.5°C). Possível frente fria chegando.",
        severity = Severity.WARNING,
        metadata = mapOf(
            "previous_avg" to "31.0°C",
            "current_avg" to "24.5°C",
            "drop" to "6.5°C",
            "drop_threshold" to "5.0°C",
            "location" to location
        )
    ),
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.HEAT_BEFORE_RAIN,
        message = "⛈️ Padrão de tempestade de verão detectado! Período de calor (pico 34.2°C) seguido de chuva (75% de probabilidade). Prepare-se para a chuva.",
        severity = Severity.WARNING,
        metadata = mapOf(
            "heat_peak" to "34.2°C",
            "rain_peak_probability" to "75%",
            "allen_relation" to "BEFORE",
            "location" to location
        )
    ),
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.HEATWAVE,
        message = "🔥 Onda de calor detectada! Temperatura acima de 35.0°C por aproximadamente 45 minutos consecutivos. Beba água e evite exposição ao sol.",
        severity = Severity.ALERT,
        metadata = mapOf(
            "critical_threshold" to "35.0°C",
            "duration_minutes" to "45",
            "location" to location
        )
    ),
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.HUMID_HEAT,
        message = "🌧️🌡️ Calor úmido detectado! Temperatura de 29.5°C com 65% de chance de chuva na próxima hora. Hidrate-se e prefira ambientes ventilados.",
        severity = Severity.WARNING,
        metadata = mapOf(
            "current_temperature" to "29.5°C",
            "rain_probability" to "65%",
            "location" to location
        )
    ),
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.IMMINENT_RAIN,
        message = "🌧️ Chuva iminente! Probabilidade de 82% de chuva na próxima hora. Leve guarda-chuva.",
        severity = Severity.WARNING,
        metadata = mapOf(
            "rain_probability" to "82%",
            "threshold" to "70%",
            "location" to location
        )
    ),
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.DAILY_UMBRELLA,
        message = "☂️ Leve guarda-chuva hoje! Probabilidade de chuva de 78% prevista para o dia.",
        severity = Severity.INFO,
        metadata = mapOf(
            "daily_precipitation_probability" to "78%",
            "location" to location
        )
    ),
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.DAILY_UV_PROTECTION,
        message = "🌞 Use protetor solar hoje! Índice UV máximo previsto: 8.0. Evite exposição entre 10h e 16h.",
        severity = Severity.INFO,
        metadata = mapOf(
            "uv_index" to "8.0",
            "location" to location
        )
    ),
    AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.DAILY_THERMAL,
        message = "🌡️ Dia quente pela frente! Temperatura máxima prevista de 33.0°C. Vista roupas leves e mantenha-se hidratado.",
        severity = Severity.INFO,
        metadata = mapOf(
            "max_temperature" to "33.0°C",
            "location" to location
        )
    )
)
