package org.example.processors

import org.slf4j.LoggerFactory

/**
 * Aplicação principal do detector de ondas de calor.
 */
fun main() {
    val logger = LoggerFactory.getLogger("HeatwaveDetectorApp")
    val detector = HeatwaveDetector()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando detector...")
        detector.close()
    })

    detector.start()
}
