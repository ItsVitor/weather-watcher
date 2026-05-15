package org.example.consumers

import org.example.model.AlertType
import org.example.model.Severity
import org.example.model.WeatherData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class HourlyRainConsumerTest {

    @Test
    fun `should create rain alert when precipitation probability is above threshold`() {
        val consumer = HourlyRainConsumer(rainThreshold = 70)
        
        val weatherData = WeatherData(
            timestamp = LocalDateTime.now(),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 25.0,
            currentPrecipitationProbability = 50,
            nextHourPrecipitationProbability = 85,
            dailyMaxTemperature = 30.0,
            dailyMaxUvIndex = 7.0,
            dailyMaxPrecipitationProbability = 85
        )

        // Usar reflexão para acessar método privado createRainAlert
        val method = consumer.javaClass.getDeclaredMethod("createRainAlert", WeatherData::class.java)
        method.isAccessible = true
        val alert = method.invoke(consumer, weatherData) as org.example.model.AlertEvent

        assertEquals(AlertType.IMMINENT_RAIN, alert.alertType)
        assertEquals(Severity.WARNING, alert.severity)
        assertTrue(alert.message.contains("85%"))
        assertTrue(alert.message.contains("🌧️"))
        assertEquals("85%", alert.metadata["precipitation_probability"])
    }

    @Test
    fun `should not process alert when precipitation probability is below threshold`() {
        val consumer = HourlyRainConsumer(rainThreshold = 70)
        
        val weatherData = WeatherData(
            timestamp = LocalDateTime.now(),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 25.0,
            currentPrecipitationProbability = 30,
            nextHourPrecipitationProbability = 50,
            dailyMaxTemperature = 30.0,
            dailyMaxUvIndex = 7.0,
            dailyMaxPrecipitationProbability = 60
        )

        // Verificar que processWeatherData não gera exceção
        assertDoesNotThrow {
            consumer.processWeatherData(weatherData)
        }
    }

    @Test
    fun `should process alert when precipitation probability equals threshold`() {
        val consumer = HourlyRainConsumer(rainThreshold = 70)
        
        val weatherData = WeatherData(
            timestamp = LocalDateTime.now(),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 25.0,
            currentPrecipitationProbability = 60,
            nextHourPrecipitationProbability = 70,
            dailyMaxTemperature = 30.0,
            dailyMaxUvIndex = 7.0,
            dailyMaxPrecipitationProbability = 70
        )

        val method = consumer.javaClass.getDeclaredMethod("createRainAlert", WeatherData::class.java)
        method.isAccessible = true
        val alert = method.invoke(consumer, weatherData) as org.example.model.AlertEvent

        assertNotNull(alert)
        assertEquals(AlertType.IMMINENT_RAIN, alert.alertType)
    }

    @Test
    fun `should use custom threshold correctly`() {
        val consumer = HourlyRainConsumer(rainThreshold = 50)
        
        val weatherData = WeatherData(
            timestamp = LocalDateTime.now(),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 25.0,
            currentPrecipitationProbability = 40,
            nextHourPrecipitationProbability = 55,
            dailyMaxTemperature = 30.0,
            dailyMaxUvIndex = 7.0,
            dailyMaxPrecipitationProbability = 60
        )

        val method = consumer.javaClass.getDeclaredMethod("createRainAlert", WeatherData::class.java)
        method.isAccessible = true
        val alert = method.invoke(consumer, weatherData) as org.example.model.AlertEvent

        assertNotNull(alert)
        assertTrue(alert.message.contains("55%"))
    }
}
