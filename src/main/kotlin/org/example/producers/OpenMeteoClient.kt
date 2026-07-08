package org.example.producers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import org.example.config.WeatherWatcherConfig
import org.example.model.WeatherData
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Cliente HTTP para a API Open-Meteo.
 * Responsável por construir requisições, fazer chamadas HTTP e parsear respostas JSON.
 */
class OpenMeteoClient(
    private val latitude: Double = WeatherWatcherConfig.locationLatitude,
    private val longitude: Double = WeatherWatcherConfig.locationLongitude,
    private val timezone: String = WeatherWatcherConfig.timezone
) {
    private val httpClient = OkHttpClient()
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())
    }

    /**
     * Busca dados meteorológicos da API Open-Meteo.
     * @return WeatherData com dados horários e diários
     */
    fun fetchWeatherData(): WeatherData {
        val url = buildUrl()
        val request = Request.Builder().url(url).build()

        httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw RuntimeException("Falha na requisição HTTP: ${response.code}")
            }

            val json = objectMapper.readTree(response.body?.string())
            return parseWeatherData(json)
        }
    }

    private fun buildUrl(): String {
        return "https://api.open-meteo.com/v1/forecast" +
                "?latitude=$latitude" +
                "&longitude=$longitude" +
                "&hourly=temperature_2m,precipitation_probability" +
                "&daily=temperature_2m_max,uv_index_max,precipitation_probability_max" +
                "&timezone=$timezone" +
                "&forecast_days=2"
    }

    private fun parseWeatherData(json: JsonNode): WeatherData {
        val hourly = json.get("hourly")
        val daily = json.get("daily")
        val now = LocalDateTime.now()
        val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

        data class HourlyReading(
            val time: LocalDateTime,
            val temperature: Double,
            val precipitationProb: Int,
            val index: Int
        )

        val hourlyReadings = hourly.get("time")
            .asSequence()  // Converter para sequence (equivalente a stream)
            .mapIndexed { index, timeNode ->
                HourlyReading(
                    time = LocalDateTime.parse(timeNode.asText(), formatter),
                    temperature = hourly.get("temperature_2m").get(index).asDouble(),
                    precipitationProb = hourly.get("precipitation_probability").get(index).asInt(),
                    index = index
                )
            }
            .toList()

        val currentReading = hourlyReadings
            .stream() 
            .filter { it.time.hour == now.hour && it.time.dayOfMonth == now.dayOfMonth }
            .findFirst()
            .orElse(hourlyReadings.first())

        val nextHourReading = hourlyReadings
            .getOrNull(currentReading.index + 1)

        // Extrair dados diários
        val dailyMaxTemp = daily.get("temperature_2m_max").get(0).asDouble()
        val dailyMaxUv = daily.get("uv_index_max").get(0).asDouble()
        val dailyMaxPrecip = daily.get("precipitation_probability_max").get(0).asInt()

        return WeatherData(
            timestamp = now,
            latitude = json.get("latitude").asDouble(),
            longitude = json.get("longitude").asDouble(),
            currentTemperature = currentReading.temperature,
            currentPrecipitationProbability = currentReading.precipitationProb,
            nextHourPrecipitationProbability = nextHourReading?.precipitationProb ?: 0,
            dailyMaxTemperature = dailyMaxTemp,
            dailyMaxUvIndex = dailyMaxUv,
            dailyMaxPrecipitationProbability = dailyMaxPrecip
        )
    }

    private fun findCurrentHourIndex(times: List<String>, now: LocalDateTime): Int {
        val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
        return times.indexOfFirst { timeStr ->
            val time = LocalDateTime.parse(timeStr, formatter)
            time.hour == now.hour && time.dayOfMonth == now.dayOfMonth
        }.takeIf { it >= 0 } ?: 0
    }
}
