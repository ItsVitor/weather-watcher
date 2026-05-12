package org.example.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Serializer
import org.example.model.WeatherData

class WeatherSerializer : Serializer<WeatherData> {
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())
    }

    override fun serialize(topic: String?, data: WeatherData?): ByteArray? {
        return data?.let { objectMapper.writeValueAsBytes(it) }
    }
}
