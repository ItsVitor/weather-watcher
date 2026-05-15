package org.example.producers

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.example.config.WeatherWatcherConfig
import org.example.model.WeatherData
import org.example.serdes.WeatherSerializer
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Produtor Kafka responsável por publicar dados meteorológicos no tópico weather-raw.
 */
class WeatherAPIProducer(
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers,
    private val topic: String = "weather-raw"
) : AutoCloseable {

    private val logger = LoggerFactory.getLogger(WeatherAPIProducer::class.java)

    private val producer: KafkaProducer<String, WeatherData> = KafkaProducer(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WeatherSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 3)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        }
    )

    /**
     * Publica dados meteorológicos no Kafka.
     * @param data Dados meteorológicos a serem publicados
     */
    fun publish(data: WeatherData) {
        val key = "${data.latitude},${data.longitude}"
        val record = ProducerRecord(topic, key, data)

        producer.send(record) { metadata, exception ->
            if (exception != null) {
                logger.error("Erro ao publicar mensagem no Kafka", exception)
            } else {
                logger.info(
                    "Mensagem publicada com sucesso: topic=${metadata.topic()}, " +
                            "partition=${metadata.partition()}, offset=${metadata.offset()}"
                )
            }
        }
    }

    override fun close() {
        producer.close()
        logger.info("Produtor Kafka fechado")
    }
}
