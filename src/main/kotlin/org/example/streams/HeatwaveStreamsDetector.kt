package org.example.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.SlidingWindows
import org.apache.kafka.streams.kstream.Grouped
import org.example.config.WeatherWatcherConfig
import org.example.model.AlertEvent
import org.example.model.AlertType
import org.example.model.Severity
import org.example.model.WeatherData
import org.example.serdes.AlertSerializer
import org.example.serdes.WeatherDeserializer
import org.example.serdes.WeatherSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.util.Properties

/**
 * Detecta ondas de calor usando Kafka Streams DSL.
 * Migração do HeatwaveDetector (Projeto 1) para topologia declarativa.
 *
 * Topologia:
 *   weather-raw
 *     → filter (temp > limiar)           [stateless]
 *     → selectKey (localização)
 *     → groupByKey
 *     → windowedBy (SlidingWindows)      [stateful]
 *     → count()                          [stateful]
 *     → toStream
 *     → filter (count >= windowSize)     [stateless]
 *     → mapValues (cria AlertEvent)      [stateless]
 *     → weather-alerts
 */
class HeatwaveStreamsDetector(
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers,
    private val criticalThreshold: Double = 20.0,
    private val windowSize: Long = 5,
    private val pollingIntervalMinutes: Long = WeatherWatcherConfig.pollingIntervalMinutes
) {
    private val logger = LoggerFactory.getLogger(HeatwaveStreamsDetector::class.java)

    private val weatherSerde = Serdes.serdeFrom(WeatherSerializer(), WeatherDeserializer())
    private val alertSerde = Serdes.serdeFrom(AlertSerializer(), org.example.serdes.AlertDeserializer())

    fun buildTopology(builder: StreamsBuilder) {
        val windowDuration = Duration.ofMinutes(windowSize * pollingIntervalMinutes)

        builder
            .stream("weather-raw", Consumed.with(Serdes.String(), weatherSerde))
            .filter { _, data -> data.currentTemperature > criticalThreshold }
            .selectKey { _, data -> "${data.latitude},${data.longitude}" }
            .groupByKey(Grouped.with(Serdes.String(), weatherSerde))
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(windowDuration))
            .count()
            .toStream()
            .filter { _, count -> count >= windowSize }
            .map { windowedKey, _ ->
                org.apache.kafka.streams.KeyValue(
                    windowedKey.key(),
                    createHeatwaveAlert(windowedKey.window().startTime(), windowedKey.window().endTime())
                )
            }
            .to("weather-alerts", Produced.with(Serdes.String(), alertSerde))
    }

    fun start(): KafkaStreams {
        val builder = StreamsBuilder()
        buildTopology(builder)

        val streams = KafkaStreams(builder.build(), Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "heatwave-streams")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
            put("consumer.auto.offset.reset", "latest")
        })

        streams.start()

        logger.info("=== HeatwaveStreamsDetector iniciado ===")
        logger.info("Limiar crítico: ${criticalThreshold}°C")
        logger.info("Janela temporal: $windowSize leituras × $pollingIntervalMinutes min = ${windowSize * pollingIntervalMinutes} min")
        logger.info("Consumindo de: weather-raw")
        logger.info("Publicando em: weather-alerts")

        return streams
    }

    private fun createHeatwaveAlert(
        windowStart: java.time.Instant,
        windowEnd: java.time.Instant
    ): AlertEvent {
        val durationMinutes = windowSize * pollingIntervalMinutes
        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.HEATWAVE,
            message = "🔥 Onda de calor detectada! Temperatura acima de ${criticalThreshold}°C por aproximadamente ${durationMinutes} minutos consecutivos. Beba água e evite exposição ao sol.",
            severity = Severity.ALERT,
            metadata = mapOf(
                "critical_threshold" to "${criticalThreshold}°C",
                "window_size" to windowSize.toString(),
                "duration_minutes" to durationMinutes.toString(),
                "window_start" to windowStart.toString(),
                "window_end" to windowEnd.toString()
            )
        )
    }
}
