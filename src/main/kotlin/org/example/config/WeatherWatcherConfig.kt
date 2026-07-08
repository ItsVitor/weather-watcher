package org.example.config

/**
 * Configurações centralizadas do sistema Weather-Watcher.
 * Permite configuração via variáveis de ambiente ou valores padrão.
 */
object WeatherWatcherConfig {
    
    /**
     * Intervalo de polling da API Open-Meteo em minutos.
     * Padrão: 15 minutos
     * Variável de ambiente: POLLING_INTERVAL_MINUTES
     */
    val pollingIntervalMinutes: Long = System.getenv("POLLING_INTERVAL_MINUTES")?.toLongOrNull() ?: 1
    
    /**
     * Servidores Kafka bootstrap.
     * Padrão: localhost:9092
     * Variável de ambiente: KAFKA_BOOTSTRAP_SERVERS
     */
    val kafkaBootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
    
    /**
     * Latitude da localização monitorada.
     * Padrão: -20.31 (Vitória-ES)
     * Variável de ambiente: LOCATION_LATITUDE
     */
    val locationLatitude: Double = System.getenv("LOCATION_LATITUDE")?.toDoubleOrNull() ?: -20.31
    
    /**
     * Longitude da localização monitorada.
     * Padrão: -40.31 (Vitória-ES)
     * Variável de ambiente: LOCATION_LONGITUDE
     */
    val locationLongitude: Double = System.getenv("LOCATION_LONGITUDE")?.toDoubleOrNull() ?: -40.31
    
    /**
     * Timezone para ajuste de horários.
     * Padrão: America/Sao_Paulo
     * Variável de ambiente: TIMEZONE
     */
    val timezone: String = System.getenv("TIMEZONE") ?: "America/Sao_Paulo"
    
    /**
     * Twilio Account SID.
     * Variável de ambiente: TWILIO_ACCOUNT_SID (obrigatória)
     */
    val twilioAccountSid: String = System.getenv("TWILIO_ACCOUNT_SID") ?: ""
    
    /**
     * Twilio Auth Token.
     * Variável de ambiente: TWILIO_AUTH_TOKEN (obrigatória)
     */
    val twilioAuthToken: String = System.getenv("TWILIO_AUTH_TOKEN") ?: ""
    
    /**
     * Número WhatsApp do Twilio (remetente).
     * Formato: whatsapp:+14155238886
     * Variável de ambiente: TWILIO_WHATSAPP_FROM (obrigatória)
     */
    val twilioWhatsAppFrom: String = System.getenv("TWILIO_WHATSAPP_FROM") ?: ""
    
    /**
     * Número WhatsApp do destinatário.
     * Formato: whatsapp:+5527999999999
     * Variável de ambiente: WHATSAPP_TO (obrigatória)
     */
    val whatsAppTo: String = System.getenv("WHATSAPP_TO") ?: ""
}
