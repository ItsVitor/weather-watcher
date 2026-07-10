package org.example.streams

import org.slf4j.LoggerFactory

fun main() {
    val logger = LoggerFactory.getLogger("TemperatureDropStreamApp")
    val stream = TemperatureDropStream()
    val kafkaStreams = stream.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando TemperatureDropStream...")
        kafkaStreams.close()
    })
}
