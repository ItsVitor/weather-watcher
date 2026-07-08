package org.example.processors

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.example.config.WeatherWatcherConfig
import org.example.model.AlertEvent
import org.example.model.AlertType
import org.example.model.Severity
import org.example.model.WeatherData
import org.example.serdes.AlertSerializer
import org.example.serdes.WeatherDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.util.*

/**
 * Processador stateful que detecta ondas de calor.
 * Mantém janela temporal de temperaturas e alerta quando todas excedem o limite crítico.
 */
class HeatwaveDetector(
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers,
    private val criticalThreshold: Double = 34.0,
    private val windowSize: Int = 3,
    private val pollingIntervalMinutes: Long = WeatherWatcherConfig.pollingIntervalMinutes
) : AutoCloseable {

    private val logger = LoggerFactory.getLogger(HeatwaveDetector::class.java)
    private val temperatureStore = TemperatureStore(windowSize)
    private var alertTriggered = false

    private val consumer: KafkaConsumer<String, WeatherData> = KafkaConsumer(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "heatwave-detector-group")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WeatherDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        }
    )

    private val producer: KafkaProducer<String, AlertEvent> = KafkaProducer(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AlertSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 3)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        }
    )

    init {
        consumer.subscribe(listOf("weather-raw"))
    }

    /**
     * Processa dados meteorológicos e detecta ondas de calor.
     */
    fun processWeatherData(data: WeatherData) {
        val currentTemp = data.currentTemperature

        // Adicionar temperatura ao store
        temperatureStore.add(currentTemp)

        logger.debug(
            "Temperatura adicionada: ${currentTemp}°C | " +
                    "Buffer: ${temperatureStore.getTemperatures()} | " +
                    "Tamanho: ${temperatureStore.size()}/${windowSize}"
        )

        // Verificar se temperatura caiu abaixo do limite (resetar estado)
        if (currentTemp <= criticalThreshold && alertTriggered) {
            logger.info("Temperatura caiu para ${currentTemp}°C. Resetando estado de alerta.")
            alertTriggered = false
        }

        // Detectar onda de calor
        if (shouldTriggerHeatwaveAlert()) {
            val alert = createHeatwaveAlert(data)
            publishAlert(alert)
            alertTriggered = true
            logger.info("Onda de calor detectada! Temperaturas: ${temperatureStore.getTemperatures()}")
        }
    }

    private fun shouldTriggerHeatwaveAlert(): Boolean {
        return temperatureStore.isFull() &&
                temperatureStore.allAboveThreshold(criticalThreshold) &&
                !alertTriggered
    }

    private fun createHeatwaveAlert(data: WeatherData): AlertEvent {
        val temps = temperatureStore.getTemperatures()
        val avgTemp = temps.average()
        val duration = windowSize * pollingIntervalMinutes

        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.HEATWAVE,
            message = "🔥 Onda de calor detectada! Temperatura acima de ${criticalThreshold}°C por aproximadamente ${duration} minutos consecutivos. Beba água e evite exposição ao sol.",
            severity = Severity.ALERT,
            metadata = mapOf(
                "critical_threshold" to "${criticalThreshold}°C",
                "average_temperature" to String.format("%.1f°C", avgTemp),
                "window_size" to windowSize.toString(),
                "polling_interval_minutes" to pollingIntervalMinutes.toString(),
                "duration_minutes" to duration.toString(),
                "temperatures" to temps.joinToString(", ") { String.format("%.1f°C", it) },
                "location" to "${data.latitude},${data.longitude}"
            )
        )
    }

    private fun publishAlert(alert: AlertEvent) {
        val record = ProducerRecord("weather-alerts", alert.alertType.name, alert)
        producer.send(record) { metadata, exception ->
            if (exception != null) {
                logger.error("Erro ao publicar alerta", exception)
            } else {
                logger.info("Alerta de onda de calor publicado: partition=${metadata.partition()}")
            }
        }
    }

    /**
     * Inicia o loop de consumo contínuo.
     */
    fun start() {
        logger.info("=== HeatwaveDetector iniciado ===")
        logger.info("Limiar crítico: ${criticalThreshold}°C")
        logger.info("Janela temporal: $windowSize leituras")
        logger.info("Consumindo de: weather-raw")
        logger.info("Publicando em: weather-alerts")

        try {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))
                records.forEach { record ->
                    processWeatherData(record.value())
                }
            }
        } catch (e: Exception) {
            logger.error("Erro no loop de consumo", e)
        }
    }

    override fun close() {
        consumer.close()
        producer.close()
        logger.info("HeatwaveDetector fechado")
    }
}
