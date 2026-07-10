package org.example.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
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
import java.time.LocalDateTime
import java.util.Properties

/**
 * Detecta queda brusca de temperatura entre janelas consecutivas (frente fria).
 *
 * Topologia:
 *   weather-raw
 *     → selectKey (localização)
 *     → groupByKey
 *     → windowedBy (TumblingWindow 1h)   [stateful]
 *     → aggregate (soma + contagem)      [stateful]
 *     → toStream
 *     → map: calcula média, compara com janela anterior via state store
 *     → filter (queda >= 5°C)            [stateless]
 *     → weather-alerts
 */
class TemperatureDropStream(
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers,
    private val dropThreshold: Double = 0.5,
    private val windowHours: Long = 1
) {
    private val logger = LoggerFactory.getLogger(TemperatureDropStream::class.java)

    private val weatherSerde = Serdes.serdeFrom(WeatherSerializer(), WeatherDeserializer())
    private val alertSerde = Serdes.serdeFrom(AlertSerializer(), AlertDeserializer())

    // Acumulador para calcular média por janela: (soma, contagem)
    data class TempAccumulator(val sum: Double, val count: Long)

    fun buildTopology(builder: StreamsBuilder) {
        // State store para guardar a média da janela anterior por localização
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("previous-window-avg"),
                Serdes.String(),
                Serdes.Double()
            )
        )

        builder
            .stream("weather-raw", Consumed.with(Serdes.String(), weatherSerde))
            .selectKey { _, data -> "${data.latitude},${data.longitude}" }
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowHours)))
            .aggregate(
                { TempAccumulator(0.0, 0L) },
                { _, data, acc -> TempAccumulator(acc.sum + data.currentTemperature, acc.count + 1) },
                Materialized.with(Serdes.String(), accumulatorSerde())
            )
            .toStream()
            .map { windowedKey, acc ->
                val avg = if (acc.count > 0) acc.sum / acc.count else 0.0
                KeyValue(windowedKey.key(), Pair(avg, windowedKey.window().startTime().toEpochMilli()))
            }
            .processValues(
                FixedKeyProcessorSupplier { PreviousWindowProcessor(dropThreshold) },
                "previous-window-avg"
            )
            .filter { _, alert -> alert != null }
            .map { key, alert -> KeyValue(key, alert!!) }
            .to("weather-alerts", Produced.with(Serdes.String(), alertSerde))
    }

    fun start(): KafkaStreams {
        val builder = StreamsBuilder()
        buildTopology(builder)

        val streams = KafkaStreams(builder.build(), Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "temperature-drop-streams")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
            put("consumer.auto.offset.reset", "latest")
        })

        streams.start()

        logger.info("=== TemperatureDropStream iniciado ===")
        logger.info("Limiar de queda: >= ${dropThreshold}°C entre janelas consecutivas")
        logger.info("Tamanho da janela: $windowHours hora(s)")
        logger.info("Consumindo de: weather-raw")
        logger.info("Publicando em: weather-alerts")

        return streams
    }

    private fun accumulatorSerde() = Serdes.serdeFrom(
        { _, acc -> acc?.let {
            "${it.sum}:${it.count}".toByteArray()
        } ?: ByteArray(0) },
        { _, bytes -> bytes?.let {
            val parts = String(it).split(":")
            TempAccumulator(parts[0].toDouble(), parts[1].toLong())
        } ?: TempAccumulator(0.0, 0L) }
    )
}

/**
 * Processor que compara a média atual com a anterior armazenada no state store.
 * Emite AlertEvent apenas quando a queda supera o limiar configurado.
 */
class PreviousWindowProcessor(
    private val dropThreshold: Double
) : FixedKeyProcessor<String, Pair<Double, Long>, AlertEvent?> {

    private lateinit var context: FixedKeyProcessorContext<String, AlertEvent?>
    private lateinit var store: KeyValueStore<String, Double>

    override fun init(context: FixedKeyProcessorContext<String, AlertEvent?>) {
        this.context = context
        @Suppress("UNCHECKED_CAST")
        store = context.getStateStore("previous-window-avg") as KeyValueStore<String, Double>
    }

    override fun process(record: FixedKeyRecord<String, Pair<Double, Long>>) {
        val (currentAvg, _) = record.value()
        val key = record.key()
        val previousAvg = store.get(key)
        store.put(key, currentAvg)

        val alert = if (previousAvg != null && (previousAvg - currentAvg) >= dropThreshold) {
            AlertEvent(
                timestamp = LocalDateTime.now(),
                alertType = AlertType.TEMPERATURE_DROP,
                message = "🌬️ Queda brusca de temperatura detectada! De ${String.format("%.1f", previousAvg)}°C para ${String.format("%.1f", currentAvg)}°C (queda de ${String.format("%.1f", previousAvg - currentAvg)}°C). Possível frente fria chegando.",
                severity = Severity.WARNING,
                metadata = mapOf(
                    "previous_avg" to "${String.format("%.1f", previousAvg)}°C",
                    "current_avg" to "${String.format("%.1f", currentAvg)}°C",
                    "drop" to "${String.format("%.1f", previousAvg - currentAvg)}°C",
                    "drop_threshold" to "${dropThreshold}°C",
                    "location" to key
                )
            )
        } else null

        context.forward(record.withValue(alert))
    }

    override fun close() {}
}
