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
 * Consumidor que detecta a transição de um horário específico e publica alertas diários.
 * Mantém estado local para evitar múltiplos disparos no mesmo horário.
 */
class DailySummaryConsumer(
    private val bootstrapServers: String = "localhost:9092",
    private val thermalThreshold: Double = 30.0,
    private val uvThreshold: Double = 3.0,
    private val triggerHour: Int = 23,
    private val triggerMinute: Int = 2
) : AutoCloseable {

    private val logger = LoggerFactory.getLogger(DailySummaryConsumer::class.java)
    private var lastProcessedTime: String? = null

    private val consumer: KafkaConsumer<String, WeatherData> = KafkaConsumer(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "daily-summary-consumer-group")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WeatherDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
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
     * Processa dados meteorológicos e gera alertas diários se for o horário configurado.
     */
    fun processWeatherData(data: WeatherData) {
        val currentHour = data.timestamp.hour
        val currentMinute = data.timestamp.minute

        if (shouldTriggerDailySummary(currentHour, currentMinute)) {
            logger.info("Gatilho de ${triggerHour}h${String.format("%02d", triggerMinute)} detectado! Gerando alertas diários...")
            generateDailyAlerts(data)
        }
    }

    private fun shouldTriggerDailySummary(currentHour: Int, currentMinute: Int): Boolean {
        val currentTime = "${currentHour}:${currentMinute}"
        
        val shouldTrigger = currentHour == triggerHour && 
                           currentMinute == triggerMinute && 
                           lastProcessedTime != currentTime
        
        // Resetar estado quando sair do horário de gatilho
        // Isso permite disparar novamente no dia seguinte
        if (currentHour != triggerHour || currentMinute != triggerMinute) {
            lastProcessedTime = null
        } else if (shouldTrigger) {
            lastProcessedTime = currentTime
        }
        
        return shouldTrigger
    }

    private fun generateDailyAlerts(data: WeatherData) {
        val alerts = mutableListOf<AlertEvent>()

        // 1. Alerta de Guarda-Chuva
        if (data.dailyMaxPrecipitationProbability > 0) {
            alerts.add(createUmbrellaAlert(data))
        }

        // 2. Alerta de Proteção UV
        if (data.dailyMaxUvIndex > uvThreshold) {
            alerts.add(createUvAlert(data))
        }

        // 3. Alerta de Conforto Térmico
        if (data.dailyMaxTemperature > thermalThreshold) {
            alerts.add(createThermalAlert(data))
        }

        alerts.forEach { alert ->
            publishAlert(alert)
        }

        logger.info("${alerts.size} alerta(s) diário(s) gerado(s)")
    }

    private fun createUmbrellaAlert(data: WeatherData): AlertEvent {
        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.DAILY_UMBRELLA,
            message = "☔ Leve seu guarda-chuva! Previsão de ${data.dailyMaxPrecipitationProbability}% de chuva hoje.",
            severity = Severity.INFO,
            metadata = mapOf(
                "precipitation_probability" to "${data.dailyMaxPrecipitationProbability}%",
                "location" to "${data.latitude},${data.longitude}"
            )
        )
    }

    private fun createUvAlert(data: WeatherData): AlertEvent {
        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.DAILY_UV_PROTECTION,
            message = "☀️ Use protetor solar! Índice UV de ${data.dailyMaxUvIndex} previsto para hoje.",
            severity = Severity.INFO,
            metadata = mapOf(
                "uv_index" to data.dailyMaxUvIndex.toString(),
                "location" to "${data.latitude},${data.longitude}"
            )
        )
    }

    private fun createThermalAlert(data: WeatherData): AlertEvent {
        return AlertEvent(
            timestamp = LocalDateTime.now(),
            alertType = AlertType.DAILY_THERMAL,
            message = "🌡️ Dia quente! Temperatura máxima de ${data.dailyMaxTemperature}°C. Vista roupas leves e hidrate-se.",
            severity = Severity.INFO,
            metadata = mapOf(
                "max_temperature" to "${data.dailyMaxTemperature}°C",
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
                logger.info("Alerta publicado: ${alert.alertType}")
            }
        }
    }

    /**
     * Inicia o loop de consumo contínuo.
     */
    fun start() {
        logger.info("=== DailySummaryConsumer iniciado ===")
        logger.info("Gatilho: ${triggerHour}h${String.format("%02d", triggerMinute)}")
        logger.info("Limiar térmico: $thermalThreshold°C")
        logger.info("Limiar UV: $uvThreshold")
        logger.info("Consumindo de: weather-raw")
        logger.info("Publicando em: weather-alerts")

        try {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))
                records.forEach { record ->
                    logger.debug("Mensagem recebida: hora=${record.value().timestamp.hour}:${record.value().timestamp.minute}")
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
        logger.info("DailySummaryConsumer fechado")
    }
}
