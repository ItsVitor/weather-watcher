package org.example.serdes

import org.example.model.AlertEvent
import org.example.model.AlertType
import org.example.model.Severity
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class AlertSerdesTest {

    private val serializer = AlertSerializer()
    private val deserializer = AlertDeserializer()

    @Test
    fun `should serialize and deserialize AlertEvent successfully`() {
        val original = AlertEvent(
            timestamp = LocalDateTime.of(2024, 5, 6, 6, 0),
            alertType = AlertType.DAILY_UMBRELLA,
            message = "☔ Leve seu guarda-chuva! Previsão de 80% de chuva hoje.",
            severity = Severity.INFO,
            metadata = mapOf("precipitation" to "80%", "location" to "Vitória-ES")
        )

        val bytes = serializer.serialize("weather-alerts", original)
        assertNotNull(bytes)
        assertTrue(bytes!!.isNotEmpty())

        val deserialized = deserializer.deserialize("weather-alerts", bytes)
        assertNotNull(deserialized)
        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle all AlertType enums correctly`() {
        AlertType.entries.forEach { type ->
            val alert = AlertEvent(
                timestamp = LocalDateTime.now(),
                alertType = type,
                message = "Test message for $type",
                severity = Severity.INFO
            )

            val bytes = serializer.serialize("weather-alerts", alert)
            val deserialized = deserializer.deserialize("weather-alerts", bytes)

            assertEquals(type, deserialized!!.alertType)
        }
    }

    @Test
    fun `should handle all Severity levels correctly`() {
        Severity.entries.forEach { severity ->
            val alert = AlertEvent(
                timestamp = LocalDateTime.now(),
                alertType = AlertType.HEATWAVE,
                message = "Test message",
                severity = severity
            )

            val bytes = serializer.serialize("weather-alerts", alert)
            val deserialized = deserializer.deserialize("weather-alerts", bytes)

            assertEquals(severity, deserialized!!.severity)
        }
    }

    @Test
    fun `should handle empty metadata`() {
        val original = AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.IMMINENT_RAIN,
            message = "Chuva em 1 hora!",
            severity = Severity.WARNING,
            metadata = emptyMap()
        )

        val bytes = serializer.serialize("weather-alerts", original)
        val deserialized = deserializer.deserialize("weather-alerts", bytes)

        assertTrue(deserialized!!.metadata.isEmpty())
    }

    @Test
    fun `should handle null data gracefully`() {
        val bytes = serializer.serialize("weather-alerts", null)
        assertNull(bytes)

        val deserialized = deserializer.deserialize("weather-alerts", null)
        assertNull(deserialized)
    }
}
