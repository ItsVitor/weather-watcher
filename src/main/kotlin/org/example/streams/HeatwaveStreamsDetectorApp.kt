package org.example.streams

import org.slf4j.LoggerFactory

/**
 * Aplicação principal do detector de ondas de calor (Kafka Streams).
 */
fun main() {
    val logger = LoggerFactory.getLogger("HeatwaveStreamsDetectorApp")
    val detector = HeatwaveStreamsDetector()
    val streams = detector.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando HeatwaveStreamsDetector...")
        streams.close()
    })
}
