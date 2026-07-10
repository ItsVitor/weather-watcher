package org.example.streams

import org.slf4j.LoggerFactory

fun main() {
    val logger = LoggerFactory.getLogger("HumidHeatAlertStreamApp")
    val stream = HumidHeatAlertStream()
    val kafkaStreams = stream.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando HumidHeatAlertStream...")
        kafkaStreams.close()
    })
}
