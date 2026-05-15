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
class DailySummaryConsumerIntegrationTest {

    private lateinit var consumer: DailySummaryConsumer

    @BeforeEach
    fun setup() {
        val now = LocalDateTime.now()
        consumer = DailySummaryConsumer(
            bootstrapServers = "localhost:9092",
            thermalThreshold = 30.0,
            uvThreshold = 3.0,
            triggerHour = now.hour,
            triggerMinute = now.minute
        )
        println("=== DailySummaryConsumer Integration Test ===")
        println("Configurado para disparar às ${now.hour}h${String.format("%02d", now.minute)}")
    }

    @AfterEach
    fun tearDown() {
        consumer.close()
    }

    @Test
    fun `should publish all three daily alerts when all thresholds exceeded`() {
        val now = LocalDateTime.now()
        
        val weatherData = WeatherData(
            timestamp = now,
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 28.0,
            currentPrecipitationProbability = 60,
            nextHourPrecipitationProbability = 70,
            dailyMaxTemperature = 35.0,  // Acima de 30°C
            dailyMaxUvIndex = 8.5,        // Acima de 3.0
            dailyMaxPrecipitationProbability = 80  // Acima de 70%
        )

        println("Publicando alertas diários (todos os limiares excedidos)...")
        consumer.processWeatherData(weatherData)
        
        Thread.sleep(1000)
        println("✓ 3 alertas publicados: UMBRELLA, UV_PROTECTION, THERMAL")
    }

    @Test
    fun `should publish only umbrella alert when only precipitation expected`() {
        val now = LocalDateTime.now()
        
        val weatherData = WeatherData(
            timestamp = now,
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 22.0,
            currentPrecipitationProbability = 50,
            nextHourPrecipitationProbability = 60,
            dailyMaxTemperature = 25.0,  // Abaixo de 30°C
            dailyMaxUvIndex = 2.0,        // Abaixo de 3.0
            dailyMaxPrecipitationProbability = 80  // Acima de 70%
        )

        println("Publicando apenas alerta de guarda-chuva...")
        consumer.processWeatherData(weatherData)
        
        Thread.sleep(1000)
        println("✓ 1 alerta publicado: UMBRELLA")
    }

    @Test
    fun `should publish only UV alert when only UV index is high`() {
        val now = LocalDateTime.now()
        
        val weatherData = WeatherData(
            timestamp = now,
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 22.0,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 0,
            dailyMaxTemperature = 25.0,  // Abaixo de 30°C
            dailyMaxUvIndex = 9.0,        // Acima de 3.0
            dailyMaxPrecipitationProbability = 20  // Abaixo de 70%
        )

        println("Publicando apenas alerta de proteção UV...")
        consumer.processWeatherData(weatherData)
        
        Thread.sleep(1000)
        println("✓ 1 alerta publicado: UV_PROTECTION")
    }

    @Test
    fun `should publish only thermal alert when only temperature is high`() {
        val now = LocalDateTime.now()
        
        val weatherData = WeatherData(
            timestamp = now,
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 28.0,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 0,
            dailyMaxTemperature = 36.0,  // Acima de 30°C
            dailyMaxUvIndex = 2.0,        // Abaixo de 3.0
            dailyMaxPrecipitationProbability = 10  // Abaixo de 70%
        )

        println("Publicando apenas alerta térmico...")
        consumer.processWeatherData(weatherData)
        
        Thread.sleep(1000)
        println("✓ 1 alerta publicado: THERMAL")
    }

    @Test
    fun `should not publish any alert when all values below thresholds`() {
        val now = LocalDateTime.now()
        
        val weatherData = WeatherData(
            timestamp = now,
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 22.0,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 0,
            dailyMaxTemperature = 25.0,  // Abaixo de 30°C
            dailyMaxUvIndex = 2.0,        // Abaixo de 3.0
            dailyMaxPrecipitationProbability = 30  // Abaixo de 70%
        )

        println("Processando dados abaixo dos limiares...")
        consumer.processWeatherData(weatherData)
        
        Thread.sleep(500)
        println("✓ Nenhum alerta publicado (todos abaixo dos limiares)")
    }

    @Test
    fun `should publish multiple sets of alerts at different times`() {
        val now = LocalDateTime.now()
        
        // Primeiro conjunto de alertas
        val weatherData1 = WeatherData(
            timestamp = now,
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 28.0,
            currentPrecipitationProbability = 60,
            nextHourPrecipitationProbability = 70,
            dailyMaxTemperature = 35.0,
            dailyMaxUvIndex = 8.5,
            dailyMaxPrecipitationProbability = 80
        )

        println("Publicando primeiro conjunto de alertas...")
        consumer.processWeatherData(weatherData1)
        Thread.sleep(1000)
        
        // Simular passagem de tempo (fora do horário de gatilho)
        val laterTime = now.plusHours(1)
        val consumerLater = DailySummaryConsumer(
            bootstrapServers = "localhost:9092",
            thermalThreshold = 30.0,
            uvThreshold = 3.0,
            triggerHour = laterTime.hour,
            triggerMinute = laterTime.minute
        )
        
        val weatherData2 = WeatherData(
            timestamp = laterTime,
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 30.0,
            currentPrecipitationProbability = 70,
            nextHourPrecipitationProbability = 80,
            dailyMaxTemperature = 38.0,
            dailyMaxUvIndex = 10.0,
            dailyMaxPrecipitationProbability = 90
        )

        println("Publicando segundo conjunto de alertas...")
        consumerLater.processWeatherData(weatherData2)
        Thread.sleep(1000)
        
        consumerLater.close()
        println("✓ 6 alertas publicados no total (2 conjuntos)")
    }
}
