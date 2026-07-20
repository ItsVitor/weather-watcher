package org.example.dashboard

import org.slf4j.LoggerFactory

fun main() {
    val logger = LoggerFactory.getLogger("DashboardServerApp")
    val server = DashboardServer()
    server.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Encerrando DashboardServer...")
        server.shutdown()
    })
}
