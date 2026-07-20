package org.example.dashboard

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.example.config.WeatherWatcherConfig
import org.example.serdes.AlertDeserializer
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.time.Duration
import java.util.Properties

/**
 * Servidor WebSocket que consome weather-alerts e faz broadcast para clientes conectados.
 * Combina WebSocketServer (Java-WebSocket) com KafkaConsumer em thread dedicada.
 */
class DashboardServer(
    port: Int = 8887,
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers
) : WebSocketServer(InetSocketAddress(port)) {

    private val logger = LoggerFactory.getLogger(DashboardServer::class.java)
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())
    }

    private val consumer = KafkaConsumer<String, org.example.model.AlertEvent>(Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ConsumerConfig.GROUP_ID_CONFIG, "dashboard-consumer")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AlertDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    })

    @Volatile private var running = false

    override fun onOpen(conn: WebSocket, handshake: ClientHandshake) {
        logger.info("Cliente conectado: ${conn.remoteSocketAddress}")
    }

    override fun onClose(conn: WebSocket, code: Int, reason: String, remote: Boolean) {
        logger.info("Cliente desconectado: ${conn.remoteSocketAddress}")
    }

    override fun onMessage(conn: WebSocket, message: String) {}

    override fun onError(conn: WebSocket?, ex: Exception) {
        logger.error("Erro WebSocket", ex)
    }

    override fun onStart() {
        logger.info("=== DashboardServer iniciado na porta ${port} ===")
        logger.info("Abra dashboard.html no navegador para visualizar os alertas")
        startKafkaLoop()
    }

    private fun startKafkaLoop() {
        running = true
        consumer.subscribe(listOf("weather-alerts"))

        Thread({
            logger.info("Consumindo de: weather-alerts")
            while (running) {
                val records = consumer.poll(Duration.ofMillis(500))
                for (record in records) {
                    val json = objectMapper.writeValueAsString(record.value())
                    broadcast(json)
                    logger.debug("Alerta transmitido: ${record.value().alertType}")
                }
            }
            consumer.close()
        }, "kafka-dashboard-thread").start()
    }

    fun shutdown() {
        running = false
        stop()
    }
}
