package org.example.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.StreamJoined
import org.example.config.WeatherWatcherConfig
import org.example.model.AlertEvent
import org.example.model.AlertType
import org.example.model.Severity
import org.example.model.WeatherData
import org.example.serdes.AlertSerializer
import org.example.serdes.AlertDeserializer
import org.example.serdes.WeatherDeserializer
import org.example.serdes.WeatherSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.util.Properties

/**
 * Detecta calor úmido: chuva iminente + temperatura alta na mesma janela de tempo.
 *
 * Topologia:
 *   weather-raw
 *     → filter (precipitação >= 50%)     [stateless] → selectKey → streamChuva
 *     → filter (temperatura >= 28°C)     [stateless] → selectKey → streamCalor
 *   streamChuva.join(streamCalor, JoinWindows 30min) [stateful]
 *     → map (cria AlertEvent HUMID_HEAT)
 *     → weather-alerts
 */
class HumidHeatAlertStream(
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers,
    private val rainThreshold: Int = -1,
    private val temperatureThreshold: Double = 20.0,
    private val joinWindowMinutes: Long = 5
) {
    private val logger = LoggerFactory.getLogger(HumidHeatAlertStream::class.java)

    private val weatherSerde = Serdes.serdeFrom(WeatherSerializer(), WeatherDeserializer())
    private val alertSerde = Serdes.serdeFrom(AlertSerializer(), AlertDeserializer())

    fun buildTopology(builder: StreamsBuilder) {
        val source = builder.stream("weather-raw", Consumed.with(Serdes.String(), weatherSerde))

        val streamChuva = source
            .filter { _, data -> data.nextHourPrecipitationProbability >= rainThreshold }
            .selectKey { _, data -> "${data.latitude},${data.longitude}" }

        val streamCalor = source
            .filter { _, data -> data.currentTemperature >= temperatureThreshold }
            .selectKey { _, data -> "${data.latitude},${data.longitude}" }

        streamChuva
            .join(
                streamCalor,
                { chuva, calor -> createHumidHeatAlert(chuva, calor) },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(joinWindowMinutes)),
                StreamJoined.with(Serdes.String(), weatherSerde, weatherSerde)
            )
            .map { key, alert -> KeyValue(key, alert) }
            .to("weather-alerts", Produced.with(Serdes.String(), alertSerde))
    }

    fun start(): KafkaStreams {
        val builder = StreamsBuilder()
        buildTopology(builder)

        val streams = KafkaStreams(builder.build(), Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "humid-heat-streams")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
            put("consumer.auto.offset.reset", "latest")
        })

        streams.start()

        logger.info("=== HumidHeatAlertStream iniciado ===")
        logger.info("Limiar de precipitação: >= ${rainThreshold}%")
        logger.info("Limiar de temperatura: >= ${temperatureThreshold}°C")
        logger.info("Janela de join: $joinWindowMinutes minutos")
        logger.info("Consumindo de: weather-raw")
        logger.info("Publicando em: weather-alerts")

        return streams
    }

    private fun createHumidHeatAlert(chuva: WeatherData, calor: WeatherData): AlertEvent {
        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.HUMID_HEAT,
            message = "🌧️🌡️ Calor úmido detectado! Temperatura de ${calor.currentTemperature}°C com ${chuva.nextHourPrecipitationProbability}% de chance de chuva na próxima hora. Hidrate-se e prefira ambientes ventilados.",
            severity = Severity.WARNING,
            metadata = mapOf(
                "current_temperature" to "${calor.currentTemperature}°C",
                "rain_probability" to "${chuva.nextHourPrecipitationProbability}%",
                "rain_threshold" to "${rainThreshold}%",
                "temperature_threshold" to "${temperatureThreshold}°C",
                "location" to "${chuva.latitude},${chuva.longitude}"
            )
        )
    }
}
