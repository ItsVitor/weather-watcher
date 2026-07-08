package org.example.consumers

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
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
 * Consumidor que monitora probabilidade de chuva iminente (próxima hora).
 * Consome de weather-raw e publica alertas em weather-alerts.
 */
class HourlyRainConsumer(
    private val bootstrapServers: String = "localhost:9092",
    private val rainThreshold: Int = 70
) : AutoCloseable {

    private val logger = LoggerFactory.getLogger(HourlyRainConsumer::class.java)

    private val consumer: KafkaConsumer<String, WeatherData> = KafkaConsumer(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "hourly-rain-consumer-group")
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
     * Processa dados meteorológicos e gera alertas de chuva iminente.
     */
    fun processWeatherData(data: WeatherData) {
        if (data.nextHourPrecipitationProbability >= rainThreshold) {
            val alert = createRainAlert(data)
            publishAlert(alert)
            logger.info("Alerta de chuva iminente gerado: ${data.nextHourPrecipitationProbability}%")
        }
    }

    private fun createRainAlert(data: WeatherData): AlertEvent {
        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.IMMINENT_RAIN,
            message = "🌧️ Chuva iminente! ${data.nextHourPrecipitationProbability}% de chance na próxima hora. Pegue seu guarda-chuva!",
            severity = Severity.WARNING,
            metadata = mapOf(
                "precipitation_probability" to "${data.nextHourPrecipitationProbability}%",
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
                logger.info("Alerta publicado: topic=${metadata.topic()}, partition=${metadata.partition()}")
            }
        }
    }

    /**
     * Inicia o loop de consumo contínuo.
     */
    fun start() {
        logger.info("=== HourlyRainConsumer iniciado ===")
        logger.info("Limiar de chuva: $rainThreshold%")
        logger.info("Consumindo de: weather-raw")
        logger.info("Publicando em: weather-alerts")

        try {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))
                records.forEach { record ->
                    logger.debug("Mensagem recebida: offset=${record.offset()}, partition=${record.partition()}")
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
        logger.info("HourlyRainConsumer fechado")
    }
}
