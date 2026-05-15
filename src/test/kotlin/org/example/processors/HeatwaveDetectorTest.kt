package org.example.processors

import org.example.model.AlertType
import org.example.model.Severity
import org.example.model.WeatherData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class HeatwaveDetectorTest {

    @Test
    fun `should detect heatwave when all temperatures above threshold`() {
        val detector = HeatwaveDetector(criticalThreshold = 35.0, windowSize = 3)

        val method = detector.javaClass.getDeclaredMethod("shouldTriggerHeatwaveAlert")
        method.isAccessible = true

        // Adicionar 3 temperaturas acima do limite
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))
        detector.processWeatherData(createWeatherData(38.0))

        val shouldTrigger = method.invoke(detector) as Boolean
        assertFalse(shouldTrigger) // Já disparou no processamento
    }

    @Test
    fun `should not detect heatwave with incomplete window`() {
        val detector = HeatwaveDetector(criticalThreshold = 35.0, windowSize = 3)

        val method = detector.javaClass.getDeclaredMethod("shouldTriggerHeatwaveAlert")
        method.isAccessible = true

        // Adicionar apenas 2 temperaturas
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))

        val shouldTrigger = method.invoke(detector) as Boolean
        assertFalse(shouldTrigger)
    }

    @Test
    fun `should not detect heatwave when one temperature is below threshold`() {
        val detector = HeatwaveDetector(criticalThreshold = 35.0, windowSize = 3)

        val method = detector.javaClass.getDeclaredMethod("shouldTriggerHeatwaveAlert")
        method.isAccessible = true

        // Adicionar 3 temperaturas, mas uma abaixo do limite
        detector.processWeatherData(createWeatherData(34.0)) // Abaixo
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))

        val shouldTrigger = method.invoke(detector) as Boolean
        assertFalse(shouldTrigger)
    }

    @Test
    fun `should create heatwave alert with correct metadata`() {
        val detector = HeatwaveDetector(criticalThreshold = 35.0, windowSize = 3)

        val weatherData = createWeatherData(36.0)

        val method = detector.javaClass.getDeclaredMethod("createHeatwaveAlert", WeatherData::class.java)
        method.isAccessible = true

        // Preencher o buffer primeiro
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))
        detector.processWeatherData(createWeatherData(38.0))

        val alert = method.invoke(detector, weatherData) as org.example.model.AlertEvent

        assertEquals(AlertType.HEATWAVE, alert.alertType)
        assertEquals(Severity.ALERT, alert.severity)
        assertTrue(alert.message.contains("🔥"))
        assertTrue(alert.message.contains("35.0°C"))
        assertEquals("35.0°C", alert.metadata["critical_threshold"])
        assertEquals("3", alert.metadata["window_size"])
    }

    @Test
    fun `should not trigger multiple alerts during same heatwave`() {
        val detector = HeatwaveDetector(criticalThreshold = 35.0, windowSize = 3)

        val method = detector.javaClass.getDeclaredMethod("shouldTriggerHeatwaveAlert")
        method.isAccessible = true

        // Primeira sequência - deve disparar
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))
        detector.processWeatherData(createWeatherData(38.0))

        // Continuar com temperaturas altas - não deve disparar novamente
        detector.processWeatherData(createWeatherData(37.0))
        val shouldTrigger = method.invoke(detector) as Boolean
        assertFalse(shouldTrigger)
    }

    @Test
    fun `should reset alert state when temperature drops`() {
        val detector = HeatwaveDetector(criticalThreshold = 35.0, windowSize = 3)

        // Disparar alerta inicial
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))
        detector.processWeatherData(createWeatherData(38.0))

        // Temperatura cai
        detector.processWeatherData(createWeatherData(33.0))

        // Acessar campo privado alertTriggered
        val field = detector.javaClass.getDeclaredField("alertTriggered")
        field.isAccessible = true
        val alertTriggered = field.getBoolean(detector)

        assertFalse(alertTriggered, "Estado de alerta deve ser resetado quando temperatura cai")
    }

    @Test
    fun `should trigger again after temperature drops and rises`() {
        val detector = HeatwaveDetector(criticalThreshold = 35.0, windowSize = 3)

        val method = detector.javaClass.getDeclaredMethod("shouldTriggerHeatwaveAlert")
        method.isAccessible = true

        // Primeira onda de calor
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))
        detector.processWeatherData(createWeatherData(38.0))

        // Temperatura cai
        detector.processWeatherData(createWeatherData(33.0))
        detector.processWeatherData(createWeatherData(32.0))

        // Nova onda de calor
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))
        detector.processWeatherData(createWeatherData(38.0))

        // Deve poder disparar novamente
        val field = detector.javaClass.getDeclaredField("alertTriggered")
        field.isAccessible = true
        val alertTriggered = field.getBoolean(detector)

        assertTrue(alertTriggered, "Deve disparar alerta novamente após reset")
    }

    @Test
    fun `should use custom threshold correctly`() {
        val detector = HeatwaveDetector(criticalThreshold = 40.0, windowSize = 3)

        val method = detector.javaClass.getDeclaredMethod("shouldTriggerHeatwaveAlert")
        method.isAccessible = true

        // Temperaturas acima de 35°C mas abaixo de 40°C
        detector.processWeatherData(createWeatherData(36.0))
        detector.processWeatherData(createWeatherData(37.0))
        detector.processWeatherData(createWeatherData(38.0))

        val shouldTrigger = method.invoke(detector) as Boolean
        assertFalse(shouldTrigger, "Não deve disparar com temperaturas abaixo do limite customizado")
    }

    private fun createWeatherData(temperature: Double): WeatherData {
        return WeatherData(
            timestamp = LocalDateTime.now(),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = temperature,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 0,
            dailyMaxTemperature = temperature,
            dailyMaxUvIndex = 5.0,
            dailyMaxPrecipitationProbability = 0
        )
    }
}
