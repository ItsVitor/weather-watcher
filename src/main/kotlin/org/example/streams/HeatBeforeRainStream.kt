package org.example.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.SessionWindows
import org.apache.kafka.streams.kstream.StreamJoined
import org.apache.kafka.streams.kstream.Materialized
import org.example.config.WeatherWatcherConfig
import org.example.model.AlertEvent
import org.example.model.AlertType
import org.example.model.Severity
import org.example.serdes.AlertDeserializer
import org.example.serdes.AlertSerializer
import org.example.serdes.WeatherDeserializer
import org.example.serdes.WeatherSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.util.Properties

/**
 * Detecta padrão Allen BEFORE: período de calor precede período de chuva.
 * Típico de tarde de verão com tempestade.
 *
 * Topologia:
 *   weather-raw
 *     → filter (temp > 32°C)  → selectKey → SessionWindows → aggregate → toStream → streamSessoesCalor
 *     → filter (chuva > 60%)  → selectKey → SessionWindows → aggregate → toStream → streamSessoesChuva
 *   streamSessoesCalor.join(streamSessoesChuva, JoinWindows 2h) [stateful]
 *     → filter: Allen BEFORE — fimCalor < inicioChuva         [stateless]
 *     → map (cria AlertEvent HEAT_BEFORE_RAIN)
 *     → weather-alerts
 */
class HeatBeforeRainStream(
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers,
    private val heatThreshold: Double = 25.0,
    private val rainThreshold: Int = 60,
    private val sessionGapMinutes: Long = 30,
    private val joinWindowHours: Long = 2
) {
    private val logger = LoggerFactory.getLogger(HeatBeforeRainStream::class.java)

    private val weatherSerde = Serdes.serdeFrom(WeatherSerializer(), WeatherDeserializer())
    private val alertSerde = Serdes.serdeFrom(AlertSerializer(), AlertDeserializer())

    // Representa uma sessão com seus limites temporais
    data class SessionInfo(val start: Long, val end: Long, val peakValue: Double)

    fun buildTopology(builder: StreamsBuilder) {
        val sessionGap = Duration.ofMinutes(sessionGapMinutes)
        val source = builder.stream("weather-raw", Consumed.with(Serdes.String(), weatherSerde))

        // Sessões de calor: eventos consecutivos com temp > 32°C agrupados por inatividade
        val streamSessoesCalor = source
            .filter { _, data -> data.currentTemperature > heatThreshold }
            .selectKey { _, data -> "${data.latitude},${data.longitude}" }
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(sessionGap))
            .aggregate(
                { SessionInfo(Long.MAX_VALUE, Long.MIN_VALUE, 0.0) },
                // aggregator: atualiza limites da sessão e pico de temperatura
                { _, data, session ->
                    val ts = data.timestamp.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
                    SessionInfo(
                        start = minOf(session.start, ts),
                        end = maxOf(session.end, ts),
                        peakValue = maxOf(session.peakValue, data.currentTemperature)
                    )
                },
                // merger: une duas sessões quando janelas se fundem
                { _, a, b -> SessionInfo(minOf(a.start, b.start), maxOf(a.end, b.end), maxOf(a.peakValue, b.peakValue)) },
                Materialized.with(Serdes.String(), sessionInfoSerde())
            )
            .toStream()
            .map { windowedKey, session -> KeyValue(windowedKey.key(), session) }

        // Sessões de chuva: eventos consecutivos com precipitação > 60%
        val streamSessoesChuva = source
            .filter { _, data -> data.nextHourPrecipitationProbability > rainThreshold }
            .selectKey { _, data -> "${data.latitude},${data.longitude}" }
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(sessionGap))
            .aggregate(
                { SessionInfo(Long.MAX_VALUE, Long.MIN_VALUE, 0.0) },
                { _, data, session ->
                    val ts = data.timestamp.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
                    SessionInfo(
                        start = minOf(session.start, ts),
                        end = maxOf(session.end, ts),
                        peakValue = maxOf(session.peakValue, data.nextHourPrecipitationProbability.toDouble())
                    )
                },
                { _, a, b -> SessionInfo(minOf(a.start, b.start), maxOf(a.end, b.end), maxOf(a.peakValue, b.peakValue)) },
                Materialized.with(Serdes.String(), sessionInfoSerde())
            )
            .toStream()
            .map { windowedKey, session -> KeyValue(windowedKey.key(), session) }

        // Join: verifica se sessão de calor ocorreu dentro de 2h antes da sessão de chuva
        streamSessoesCalor
            .join(
                streamSessoesChuva,
                { calor, chuva -> Pair(calor, chuva) },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(joinWindowHours)),
                StreamJoined.with(Serdes.String(), sessionInfoSerde(), sessionInfoSerde())
            )
            // Allen BEFORE: fim do calor precede início da chuva
            .filter { _, (calor, chuva) -> calor.end < chuva.start }
            .map { key, (calor, chuva) -> KeyValue(key, createHeatBeforeRainAlert(key, calor, chuva)) }
            .to("weather-alerts", Produced.with(Serdes.String(), alertSerde))
    }

    fun start(): KafkaStreams {
        val builder = StreamsBuilder()
        buildTopology(builder)

        val streams = KafkaStreams(builder.build(), Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "heat-before-rain-streams")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
            put("consumer.auto.offset.reset", "latest")
        })

        streams.start()

        logger.info("=== HeatBeforeRainStream iniciado ===")
        logger.info("Limiar de calor: > ${heatThreshold}°C")
        logger.info("Limiar de chuva: > ${rainThreshold}%")
        logger.info("Gap de sessão: $sessionGapMinutes minutos de inatividade")
        logger.info("Janela de join: $joinWindowHours hora(s)")
        logger.info("Relação Allen: BEFORE (fim do calor < início da chuva)")
        logger.info("Consumindo de: weather-raw")
        logger.info("Publicando em: weather-alerts")

        return streams
    }

    private fun createHeatBeforeRainAlert(location: String, calor: SessionInfo, chuva: SessionInfo): AlertEvent {
        val fimCalor = Instant.ofEpochMilli(calor.end).toString()
        val inicioChuva = Instant.ofEpochMilli(chuva.start).toString()
        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.HEAT_BEFORE_RAIN,
            message = "⛈️ Padrão de tempestade de verão detectado! Período de calor (pico ${String.format("%.1f", calor.peakValue)}°C) seguido de chuva (${String.format("%.0f", chuva.peakValue)}% de probabilidade). Prepare-se para a chuva.",
            severity = Severity.WARNING,
            metadata = mapOf(
                "heat_session_end" to fimCalor,
                "rain_session_start" to inicioChuva,
                "heat_peak" to "${String.format("%.1f", calor.peakValue)}°C",
                "rain_peak_probability" to "${String.format("%.0f", chuva.peakValue)}%",
                "allen_relation" to "BEFORE",
                "location" to location
            )
        )
    }

    private fun sessionInfoSerde() = Serdes.serdeFrom(
        { _, s -> s?.let { "${it.start}:${it.end}:${it.peakValue}".toByteArray() } ?: ByteArray(0) },
        { _, b -> b?.let {
            val p = String(it).split(":")
            SessionInfo(p[0].toLong(), p[1].toLong(), p[2].toDouble())
        } ?: SessionInfo(Long.MAX_VALUE, Long.MIN_VALUE, 0.0) }
    )
}
