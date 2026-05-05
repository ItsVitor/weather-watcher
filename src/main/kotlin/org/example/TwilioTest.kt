package org.example

import com.twilio.Twilio
import com.twilio.rest.api.v2010.account.Message
import com.twilio.type.PhoneNumber

fun main() {
    // Encontre no Console do Twilio
    val accountSid = "AC19391606048a75d6958f58a4d3586f9f"
    val authToken = "9294df676b3cf1a4009b8260a0c51308"

    Twilio.init(accountSid, authToken)

    try {
        val message = Message.creator(
            PhoneNumber("whatsapp:+5527999063482"), // Seu número (com DDD)
            PhoneNumber("whatsapp:+14155238886"), // Número do Sandbox do Twilio
            "☀️ Alerta do Kafka: Onda de calor detectada! Beba água e evite exposição ao sol."
        ).create()

        println("Mensagem enviada com sucesso! SID: ${message.sid}")
        
    } catch (e: Exception) {
        println("Erro ao enviar mensagem:")
        e.printStackTrace()
    }
}