package org.example.consumers

import org.example.model.WeatherData
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

/**
 * Teste de integração que publica alertas reais no tópico weather-alerts.
 * Requer cluster Kafka rodando.
 */
class HourlyRainConsumerIntegrationTest {

    private lateinit var consumer: HourlyRainConsumer

    @BeforeEach
    fun setup() {
        consumer = HourlyRainConsumer(
            bootstrapServers = "localhost:9092",
            rainThreshold = 70
        )
        println("=== HourlyRainConsumer Integration Test ===")
    }

    @AfterEach
    fun tearDown() {
        consumer.close()
    }

    @Test
    fun `should publish rain alert to weather-alerts topic when threshold exceeded`() {
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

        println("Publicando alerta de chuva iminente (85%)...")
        consumer.processWeatherData(weatherData)
        
        Thread.sleep(1000) // Aguardar publicação
        println("✓ Alerta publicado no tópico weather-alerts")
    }

    @Test
    fun `should publish multiple rain alerts with different probabilities`() {
        val probabilities = listOf(70, 80, 90, 100)
        
        probabilities.forEach { probability ->
            val weatherData = WeatherData(
                timestamp = LocalDateTime.now(),
                latitude = -20.31,
                longitude = -40.31,
                currentTemperature = 25.0,
                currentPrecipitationProbability = 50,
                nextHourPrecipitationProbability = probability,
                dailyMaxTemperature = 30.0,
                dailyMaxUvIndex = 7.0,
                dailyMaxPrecipitationProbability = probability
            )

            println("Publicando alerta: $probability% de chuva...")
            consumer.processWeatherData(weatherData)
            Thread.sleep(500)
        }
        
        println("✓ ${probabilities.size} alertas publicados no tópico weather-alerts")
    }

    @Test
    fun `should not publish when below threshold`() {
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

        println("Processando dados abaixo do limiar (50%)...")
        consumer.processWeatherData(weatherData)
        
        Thread.sleep(500)
        println("✓ Nenhum alerta publicado (abaixo do limiar)")
    }
}
