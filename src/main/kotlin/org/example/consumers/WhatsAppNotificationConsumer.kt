package org.example.consumers

import com.twilio.Twilio
import com.twilio.rest.api.v2010.account.Message
import com.twilio.type.PhoneNumber
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.example.config.WeatherWatcherConfig
import org.example.model.AlertEvent
import org.example.serdes.AlertDeserializer
import java.time.Duration
import java.time.format.DateTimeFormatter

/**
 * Consumer que atua como Sink Connector.
 * Consome exclusivamente do tópico weather-alerts e envia notificações via WhatsApp usando Twilio.
 */
class WhatsAppNotificationConsumer(
    private val bootstrapServers: String = WeatherWatcherConfig.kafkaBootstrapServers,
    private val accountSid: String = WeatherWatcherConfig.twilioAccountSid,
    private val authToken: String = WeatherWatcherConfig.twilioAuthToken,
    private val fromNumber: String = WeatherWatcherConfig.twilioWhatsAppFrom,
    private val toNumber: String = WeatherWatcherConfig.whatsAppTo
) {
    private val consumer: KafkaConsumer<String, AlertEvent>
    private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm")
    
    init {
        validateConfiguration()
        Twilio.init(accountSid, authToken)
        
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "whatsapp-notification-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to AlertDeserializer::class.java.name,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
        )
        
        consumer = KafkaConsumer(props)
        consumer.subscribe(listOf("weather-alerts"))
    }
    
    private fun validateConfiguration() {
        require(accountSid.isNotBlank()) { "TWILIO_ACCOUNT_SID não configurado" }
        require(authToken.isNotBlank()) { "TWILIO_AUTH_TOKEN não configurado" }
        require(fromNumber.isNotBlank()) { "TWILIO_WHATSAPP_FROM não configurado" }
        require(toNumber.isNotBlank()) { "WHATSAPP_TO não configurado" }
    }
    
    fun start() {
        println("WhatsAppNotificationConsumer iniciado. Aguardando alertas...")
        
        while (true) {
            val records = consumer.poll(Duration.ofMillis(1000))
            
            for (record in records) {
                val alert = record.value()
                try {
                    sendWhatsAppMessage(alert)
                    println("✓ Notificação enviada: ${alert.alertType} [${alert.severity}]")
                } catch (e: Exception) {
                    println("✗ Erro ao enviar notificação: ${e.message}")
                }
            }
        }
    }
    
    private fun sendWhatsAppMessage(alert: AlertEvent) {
        val formattedMessage = formatMessage(alert)
        
        Message.creator(
            PhoneNumber(toNumber),
            PhoneNumber(fromNumber),
            formattedMessage
        ).create()
    }
    
    private fun formatMessage(alert: AlertEvent): String {
        val emoji = when (alert.severity) {
            org.example.model.Severity.INFO -> "ℹ️"
            org.example.model.Severity.WARNING -> "⚠️"
            org.example.model.Severity.ALERT -> "🚨"
        }
        
        val time = alert.timestamp.format(timeFormatter)
        
        return """
            $emoji *Weather-Watcher*
            
            ${alert.message}
            
            _${alert.alertType} • ${time}_
        """.trimIndent()
    }
    
    fun close() {
        consumer.close()
    }
}
