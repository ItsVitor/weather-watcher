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
}
