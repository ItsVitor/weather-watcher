package org.example.streams

import org.slf4j.LoggerFactory

fun main() {
    val logger = LoggerFactory.getLogger("HeatBeforeRainStreamApp")
    val stream = HeatBeforeRainStream()
    val kafkaStreams = stream.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando HeatBeforeRainStream...")
        kafkaStreams.close()
    })
}
