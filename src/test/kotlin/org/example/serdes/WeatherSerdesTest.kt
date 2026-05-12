package org.example.serdes

import org.example.model.WeatherData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class WeatherSerdesTest {

    private val serializer = WeatherSerializer()
    private val deserializer = WeatherDeserializer()

    @Test
    fun `should serialize and deserialize WeatherData successfully`() {
        val original = WeatherData(
            timestamp = LocalDateTime.of(2024, 5, 6, 14, 30),
            latitude = -20.31,
            longitude = -40.31,
            currentTemperature = 28.5,
            currentPrecipitationProbability = 15,
            nextHourPrecipitationProbability = 80,
            dailyMaxTemperature = 33.2,
            dailyMaxUvIndex = 8.5,
            dailyMaxPrecipitationProbability = 80
        )

        val bytes = serializer.serialize("weather-raw", original)
        assertNotNull(bytes)
        assertTrue(bytes!!.isNotEmpty())

        val deserialized = deserializer.deserialize("weather-raw", bytes)
        assertNotNull(deserialized)
        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle null data gracefully`() {
        val bytes = serializer.serialize("weather-raw", null)
        assertNull(bytes)

        val deserialized = deserializer.deserialize("weather-raw", null)
        assertNull(deserialized)
    }

    @Test
    fun `should preserve all numeric precision`() {
        val original = WeatherData(
            timestamp = LocalDateTime.now(),
            latitude = -20.314567,
            longitude = -40.312345,
            currentTemperature = 28.567,
            currentPrecipitationProbability = 0,
            nextHourPrecipitationProbability = 100,
            dailyMaxTemperature = 35.123,
            dailyMaxUvIndex = 9.876,
            dailyMaxPrecipitationProbability = 50
        )

        val bytes = serializer.serialize("weather-raw", original)
        val deserialized = deserializer.deserialize("weather-raw", bytes)

        assertEquals(original.latitude, deserialized!!.latitude)
        assertEquals(original.longitude, deserialized.longitude)
        assertEquals(original.currentTemperature, deserialized.currentTemperature)
        assertEquals(original.dailyMaxUvIndex, deserialized.dailyMaxUvIndex)
    }
}
