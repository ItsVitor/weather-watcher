package org.example.producers

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

class OpenMeteoClientTest {

    @Test
    fun `should build correct API URL`() {
        val client = OpenMeteoClient(
            latitude = -20.31,
            longitude = -40.31,
            timezone = "America/Sao_Paulo"
        )

        // Usando reflexão para acessar método privado (apenas para teste)
        val method = client.javaClass.getDeclaredMethod("buildUrl")
        method.isAccessible = true
        val url = method.invoke(client) as String

        assertTrue(url.contains("latitude=-20.31"))
        assertTrue(url.contains("longitude=-40.31"))
        assertTrue(url.contains("hourly=temperature_2m,precipitation_probability"))
        assertTrue(url.contains("daily=temperature_2m_max,uv_index_max,precipitation_probability_max"))
        assertTrue(url.contains("timezone=America/Sao_Paulo"))
        assertTrue(url.contains("forecast_days=2"))
    }

    @Test
    fun `should find current hour index correctly`() {
        val client = OpenMeteoClient()
        val now = LocalDateTime.of(2024, 5, 6, 14, 30)

        val times = listOf(
            "2024-05-06T12:00",
            "2024-05-06T13:00",
            "2024-05-06T14:00",
            "2024-05-06T15:00"
        )

        val method = client.javaClass.getDeclaredMethod(
            "findCurrentHourIndex",
            List::class.java,
            LocalDateTime::class.java
        )
        method.isAccessible = true
        val index = method.invoke(client, times, now) as Int

        assertEquals(2, index) // Índice da hora 14:00
    }

    @Test
    fun `should return 0 when current hour not found`() {
        val client = OpenMeteoClient()
        val now = LocalDateTime.of(2024, 5, 6, 10, 30)

        val times = listOf(
            "2024-05-06T12:00",
            "2024-05-06T13:00",
            "2024-05-06T14:00"
        )

        val method = client.javaClass.getDeclaredMethod(
            "findCurrentHourIndex",
            List::class.java,
            LocalDateTime::class.java
        )
        method.isAccessible = true
        val index = method.invoke(client, times, now) as Int

        assertEquals(0, index) // Fallback para primeiro índice
    }
}
