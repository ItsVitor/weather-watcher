package org.example.producers

import org.example.config.WeatherWatcherConfig
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Aplicação principal do produtor meteorológico.
 * Realiza polling da API Open-Meteo e publica no Kafka.
 */
class WeatherProducerApp(
    private val pollingIntervalMinutes: Long = WeatherWatcherConfig.pollingIntervalMinutes
) {
    private val logger = LoggerFactory.getLogger(WeatherProducerApp::class.java)
    private val client = OpenMeteoClient()
    private val producer = WeatherAPIProducer()

    fun start() {
        logger.info("=== Weather-Watcher Producer iniciado ===")
        logger.info("Polling interval: $pollingIntervalMinutes minutos")
        logger.info("Localização: Vitória-ES (lat: ${WeatherWatcherConfig.locationLatitude}, lon: ${WeatherWatcherConfig.locationLongitude})")

        Runtime.getRuntime().addShutdownHook(Thread {
            logger.info("Encerrando produtor...")
            producer.close()
        })

        while (true) {
            try {
                logger.info("Coletando dados meteorológicos...")
                val weatherData = client.fetchWeatherData()

                logger.info(
                    "Dados coletados: temp=${weatherData.currentTemperature}°C, " +
                            "chuva_atual=${weatherData.currentPrecipitationProbability}%, " +
                            "chuva_proxima_hora=${weatherData.nextHourPrecipitationProbability}%"
                )

                producer.publish(weatherData)

                logger.info("Aguardando $pollingIntervalMinutes minutos até próxima coleta...")
                TimeUnit.MINUTES.sleep(pollingIntervalMinutes)

            } catch (e: Exception) {
                logger.error("Erro no ciclo de coleta", e)
                logger.info("Tentando novamente em 1 minuto...")
                TimeUnit.MINUTES.sleep(1)
            }
        }
    }
}

fun main() {
    WeatherProducerApp().start()
}
