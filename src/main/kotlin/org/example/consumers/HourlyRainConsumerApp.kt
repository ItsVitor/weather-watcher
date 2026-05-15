package org.example.consumers

import org.slf4j.LoggerFactory

/**
 * Aplicação principal do consumidor de chuva iminente.
 */
fun main() {
    val logger = LoggerFactory.getLogger("HourlyRainConsumerApp")
    val consumer = HourlyRainConsumer()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando consumidor...")
        consumer.close()
    })

    consumer.start()
}
