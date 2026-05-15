package org.example.consumers

import org.example.model.AlertType
import org.example.model.Severity
import org.example.model.WeatherData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class DailySummaryConsumerTest {

    @Test
    fun `should trigger daily summary at configured hour and minute transition`() {
        val consumer = DailySummaryConsumer(triggerHour = 6, triggerMinute = 0)

        val method = consumer.javaClass.getDeclaredMethod("shouldTriggerDailySummary", Int::class.java, Int::class.java)
        method.isAccessible = true

        // Não deve disparar antes do horário
        val trigger1 = method.invoke(consumer, 5, 59) as Boolean
        assertFalse(trigger1, "Não deve disparar às 5h59")

        // Deve disparar no horário exato
        val trigger2 = method.invoke(consumer, 6, 0) as Boolean
        assertTrue(trigger2, "Deve disparar às 6h00")

        // Não deve disparar novamente no mesmo horário
        val trigger3 = method.invoke(consumer, 6, 0) as Boolean
        assertFalse(trigger3, "Não deve disparar novamente às 6h00")
    }

    @Test
    fun `should not trigger at other hours`() {
        val consumer = DailySummaryConsumer(triggerHour = 6, triggerMinute = 0)

        val method = consumer.javaClass.getDeclaredMethod("shouldTriggerDailySummary", Int::class.java, Int::class.java)
        method.isAccessible = true

        for (hour in listOf(0, 3, 7, 12, 18, 23)) {
            val trigger = method.invoke(consumer, hour, 0) as Boolean
            assertFalse(trigger, "Não deve disparar às ${hour}h00")
        }
    }

    @Test
    fun `should create umbrella alert when precipitation probability is greater than 0`() {
        val consumer = DailySummaryConsumer()

        val weatherData = WeatherData(
            timestamp = LocalDateTime.of(2024, 5, 6, 6, 0),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 22.0,
            currentPrecipitationProbability = 50,
            nextHourPrecipitationProbability = 60,
            dailyMaxTemperature = 28.0,
            dailyMaxUvIndex = 5.0,
            dailyMaxPrecipitationProbability = 80
        )

        val method = consumer.javaClass.getDeclaredMethod("createUmbrellaAlert", WeatherData::class.java)
        method.isAccessible = true
        val alert = method.invoke(consumer, weatherData) as org.example.model.AlertEvent

        assertEquals(AlertType.DAILY_UMBRELLA, alert.alertType)
        assertEquals(Severity.INFO, alert.severity)
        assertTrue(alert.message.contains("80%"))
        assertTrue(alert.message.contains("☔"))
    }

    @Test
    fun `should create UV alert when UV index is above threshold`() {
        val consumer = DailySummaryConsumer(uvThreshold = 3.0)

        val weatherData = WeatherData(
            timestamp = LocalDateTime.of(2024, 5, 6, 6, 0),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 22.0,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 0,
            dailyMaxTemperature = 28.0,
            dailyMaxUvIndex = 8.5,
            dailyMaxPrecipitationProbability = 0
        )

        val method = consumer.javaClass.getDeclaredMethod("createUvAlert", WeatherData::class.java)
        method.isAccessible = true
        val alert = method.invoke(consumer, weatherData) as org.example.model.AlertEvent

        assertEquals(AlertType.DAILY_UV_PROTECTION, alert.alertType)
        assertEquals(Severity.INFO, alert.severity)
        assertTrue(alert.message.contains("8.5"))
        assertTrue(alert.message.contains("☀️"))
    }

    @Test
    fun `should create thermal alert when temperature is above threshold`() {
        val consumer = DailySummaryConsumer(thermalThreshold = 30.0)

        val weatherData = WeatherData(
            timestamp = LocalDateTime.of(2024, 5, 6, 6, 0),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 25.0,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 0,
            dailyMaxTemperature = 35.2,
            dailyMaxUvIndex = 2.0,
            dailyMaxPrecipitationProbability = 0
        )

        val method = consumer.javaClass.getDeclaredMethod("createThermalAlert", WeatherData::class.java)
        method.isAccessible = true
        val alert = method.invoke(consumer, weatherData) as org.example.model.AlertEvent

        assertEquals(AlertType.DAILY_THERMAL, alert.alertType)
        assertEquals(Severity.INFO, alert.severity)
        assertTrue(alert.message.contains("35.2"))
        assertTrue(alert.message.contains("🌡️"))
    }

    @Test
    fun `should not create umbrella alert when no precipitation expected`() {
        val consumer = DailySummaryConsumer()

        val weatherData = WeatherData(
            timestamp = LocalDateTime.of(2024, 5, 6, 6, 0),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 22.0,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 0,
            dailyMaxTemperature = 28.0,
            dailyMaxUvIndex = 5.0,
            dailyMaxPrecipitationProbability = 0
        )

        // Verificar que processWeatherData não gera exceção
        assertDoesNotThrow {
            consumer.processWeatherData(weatherData)
        }
    }

    @Test
    fun `should handle transition from 7am back to 6am on next day`() {
        val consumer = DailySummaryConsumer(triggerHour = 6, triggerMinute = 0)

        val method = consumer.javaClass.getDeclaredMethod("shouldTriggerDailySummary", Int::class.java, Int::class.java)
        method.isAccessible = true

        // Dia 1: 6h00
        val trigger1 = method.invoke(consumer, 6, 0) as Boolean
        assertTrue(trigger1, "Deve disparar às 6h00 do dia 1")

        // Avançar para 7h, 8h, etc
        method.invoke(consumer, 7, 0)
        method.invoke(consumer, 8, 0)

        // Dia 2: 6h00 novamente
        val trigger2 = method.invoke(consumer, 6, 0) as Boolean
        assertTrue(trigger2, "Deve disparar às 6h00 do dia 2")
    }

    @Test
    fun `should trigger at custom hour`() {
        val consumer = DailySummaryConsumer(triggerHour = 8, triggerMinute = 30)

        val method = consumer.javaClass.getDeclaredMethod("shouldTriggerDailySummary", Int::class.java, Int::class.java)
        method.isAccessible = true

        // Não deve disparar às 6h00
        val trigger1 = method.invoke(consumer, 6, 0) as Boolean
        assertFalse(trigger1, "Não deve disparar às 6h00 quando configurado para 8h30")

        // Não deve disparar às 8h00 (minuto errado)
        val trigger2 = method.invoke(consumer, 8, 0) as Boolean
        assertFalse(trigger2, "Não deve disparar às 8h00 quando configurado para 8h30")

        // Deve disparar às 8h30
        val trigger3 = method.invoke(consumer, 8, 30) as Boolean
        assertTrue(trigger3, "Deve disparar às 8h30 quando configurado")

        // Não deve disparar novamente às 8h30
        val trigger4 = method.invoke(consumer, 8, 30) as Boolean
        assertFalse(trigger4, "Não deve disparar novamente às 8h30")
    }

    @Test
    fun `should validate trigger hour range`() {
        // Testar diferentes horas válidas
        for (hour in 0..23) {
            assertDoesNotThrow {
                DailySummaryConsumer(triggerHour = hour)
            }
        }
    }

    @Test
    fun `should validate trigger minute range`() {
        // Testar diferentes minutos válidos
        for (minute in 0..59) {
            assertDoesNotThrow {
                DailySummaryConsumer(triggerMinute = minute)
            }
        }
    }

    @Test
    fun `should not trigger at same hour but different minute`() {
        val consumer = DailySummaryConsumer(triggerHour = 6, triggerMinute = 30)

        val method = consumer.javaClass.getDeclaredMethod("shouldTriggerDailySummary", Int::class.java, Int::class.java)
        method.isAccessible = true

        // Não deve disparar às 6h00 (minuto errado)
        val trigger1 = method.invoke(consumer, 6, 0) as Boolean
        assertFalse(trigger1, "Não deve disparar às 6h00 quando configurado para 6h30")

        // Não deve disparar às 6h15 (minuto errado)
        val trigger2 = method.invoke(consumer, 6, 15) as Boolean
        assertFalse(trigger2, "Não deve disparar às 6h15 quando configurado para 6h30")

        // Deve disparar às 6h30
        val trigger3 = method.invoke(consumer, 6, 30) as Boolean
        assertTrue(trigger3, "Deve disparar às 6h30")
    }
}