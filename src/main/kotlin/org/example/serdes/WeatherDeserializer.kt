package org.example.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.example.model.WeatherData

class WeatherDeserializer : Deserializer<WeatherData> {
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())
    }

    override fun deserialize(topic: String?, data: ByteArray?): WeatherData? {
        return data?.let { objectMapper.readValue(it, WeatherData::class.java) }
    }
}
