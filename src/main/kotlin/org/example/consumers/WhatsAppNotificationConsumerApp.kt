package org.example.consumers

/**
 * Aplicação principal do WhatsAppNotificationConsumer.
 * Consome alertas do tópico weather-alerts e envia notificações via WhatsApp.
 */
fun main() {
    val consumer = WhatsAppNotificationConsumer()
    
    Runtime.getRuntime().addShutdownHook(Thread {
        println("\nEncerrando WhatsAppNotificationConsumer...")
        consumer.close()
    })
    
    consumer.start()
}
