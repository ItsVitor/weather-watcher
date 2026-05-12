package org.example.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Serializer
import org.example.model.AlertEvent

class AlertSerializer : Serializer<AlertEvent> {
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())
    }

    override fun serialize(topic: String?, data: AlertEvent?): ByteArray? {
        return data?.let { objectMapper.writeValueAsBytes(it) }
    }
}
