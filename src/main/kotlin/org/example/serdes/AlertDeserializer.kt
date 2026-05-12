package org.example.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.example.model.AlertEvent

class AlertDeserializer : Deserializer<AlertEvent> {
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())
    }

    override fun deserialize(topic: String?, data: ByteArray?): AlertEvent? {
        return data?.let { objectMapper.readValue(it, AlertEvent::class.java) }
    }
}
