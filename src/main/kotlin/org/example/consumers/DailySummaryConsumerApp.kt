package org.example.consumers

import org.slf4j.LoggerFactory

/**
 * Aplicação principal do consumidor de alertas diários.
 */
fun main() {
    val logger = LoggerFactory.getLogger("DailySummaryConsumerApp")
    val consumer = DailySummaryConsumer()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando consumidor...")
        consumer.close()
    })

    consumer.start()
}
