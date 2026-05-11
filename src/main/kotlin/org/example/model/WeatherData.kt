package org.example.model

import java.time.LocalDateTime

/**
 * Representa os dados meteorológicos brutos coletados da API Open-Meteo.
 * Esta estrutura é publicada no tópico `weather-raw` a cada 15 minutos.
 */
data class WeatherData(
    val timestamp: LocalDateTime,
    val latitude: Double,
    val longitude: Double,
    
    // Dados horários (tempo quase real)
    val currentTemperature: Double,
    val currentPrecipitationProbability: Int,
    val nextHourPrecipitationProbability: Int,
    
    // Dados diários (agregações)
    val dailyMaxTemperature: Double,
    val dailyMaxUvIndex: Double,
    val dailyMaxPrecipitationProbability: Int
)
