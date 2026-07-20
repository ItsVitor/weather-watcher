# Weather-Watcher: Development Guidelines

## Code Structure Conventions

### Package Organization
- Each logical layer has its own package: `config`, `model`, `serdes`, `producers`, `consumers`, `processors`
- Every runnable component has a dedicated `*App.kt` file containing only `fun main()` that instantiates and calls `.start()`
- Business logic lives in the main class; `*App.kt` is a thin launcher

### Class Design Patterns
- **Consumers/Processors** implement `AutoCloseable` and override `close()` to shut down both consumer and producer
- **Configuration** uses a Kotlin `object` singleton (`WeatherWatcherConfig`) — never instantiated, accessed directly
- **Models** are Kotlin `data class` — immutable, no logic
- **State stores** are plain classes with explicit API (add, isFull, clear, getTemperatures)

## Kafka Configuration Patterns

### Producer setup (used in WeatherAPIProducer, HourlyRainConsumer, DailySummaryConsumer, HeatwaveDetector):
```kotlin
KafkaProducer(Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WeatherSerializer::class.java.name)
    put(ProducerConfig.ACKS_CONFIG, "all")
    put(ProducerConfig.RETRIES_CONFIG, 3)
    put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)  // Always enabled
})
```

### Consumer setup:
```kotlin
KafkaConsumer(Properties().apply {
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    put(ConsumerConfig.GROUP_ID_CONFIG, "component-specific-group")
    put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WeatherDeserializer::class.java.name)
    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
})
```

### Publish pattern (all alert publishers):
```kotlin
val record = ProducerRecord("weather-alerts", alert.alertType.name, alert)
producer.send(record) { metadata, exception ->
    if (exception != null) logger.error("...", exception)
    else logger.info("...")
}
```

## Jackson / Serdes Pattern
All serializers/deserializers follow the same structure:
```kotlin
class WeatherSerializer : Serializer<WeatherData> {
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        registerModule(JavaTimeModule())  // Required for LocalDateTime
    }
    override fun serialize(topic: String?, data: WeatherData?): ByteArray? =
        data?.let { objectMapper.writeValueAsBytes(it) }
}
```
- Always register both `KotlinModule` and `JavaTimeModule`
- Use null-safe `?.let` for serialize/deserialize return

## Logging Conventions
- Use `LoggerFactory.getLogger(ClassName::class.java)` — one logger per class
- `logger.info("=== ComponentName iniciado ===")` at start of `start()`
- Log all configurable thresholds at startup
- `logger.debug(...)` for per-message processing details
- `logger.error("message", exception)` for all caught exceptions
- `WhatsAppNotificationConsumer` uses `println` instead of SLF4J (inconsistency — prefer SLF4J)

## Alert Creation Pattern
All alert factories follow this structure:
```kotlin
private fun createXxxAlert(data: WeatherData): AlertEvent {
    return AlertEvent(
        timestamp = LocalDateTime.now(),
        alertType = AlertType.XXX,
        message = "emoji Description with ${data.field} value.",
        severity = Severity.INFO / WARNING / ALERT,
        metadata = mapOf(
            "field_name" to "value",
            "location" to "${data.latitude},${data.longitude}"  // Always include location
        )
    )
}
```
- Messages always include an emoji prefix
- metadata always includes `"location"` key
- Severity mapping: daily informational → INFO, imminent rain → WARNING, heatwave → ALERT

## Stateful Processing Patterns

### Idempotency flag pattern (HeatwaveDetector):
```kotlin
private var alertTriggered = false

// Reset when condition clears
if (currentTemp <= criticalThreshold && alertTriggered) {
    alertTriggered = false
}

// Guard in trigger check
private fun shouldTriggerHeatwaveAlert(): Boolean =
    temperatureStore.isFull() && temperatureStore.allAboveThreshold(criticalThreshold) && !alertTriggered
```

### Time-based deduplication (DailySummaryConsumer):
```kotlin
private var lastProcessedTime: String? = null

private fun shouldTriggerDailySummary(hour: Int, minute: Int): Boolean {
    val currentTime = "$hour:$minute"
    val shouldTrigger = hour == triggerHour && minute == triggerMinute && lastProcessedTime != currentTime
    if (hour != triggerHour || minute != triggerMinute) lastProcessedTime = null
    else if (shouldTrigger) lastProcessedTime = currentTime
    return shouldTrigger
}
```

## Testing Conventions

### Unit tests use reflection to access private members:
```kotlin
val method = detector.javaClass.getDeclaredMethod("privateMethodName")
method.isAccessible = true
val result = method.invoke(detector) as ReturnType

val field = detector.javaClass.getDeclaredField("privateField")
field.isAccessible = true
val value = field.getBoolean(detector)
```

### Test naming: backtick descriptive strings in English:
```kotlin
@Test
fun `should detect heatwave when all temperatures above threshold`() { ... }
```

### Test helper: always define a `createWeatherData(temperature)` factory in processor/consumer tests:
```kotlin
private fun createWeatherData(temperature: Double) = WeatherData(
    timestamp = LocalDateTime.now(),
    latitude = -20.31, longitude = -40.31,
    currentTemperature = temperature,
    currentPrecipitationProbability = 0,
    nextHourPrecipitationProbability = 0,
    dailyMaxTemperature = temperature,
    dailyMaxUvIndex = 5.0,
    dailyMaxPrecipitationProbability = 0
)
```

### Integration tests connect to a real Kafka cluster — named `*IntegrationTest`, run separately from unit tests.

## Functional Kotlin Idioms Used
- `Properties().apply { put(...) }` for Kafka config blocks
- `listOf(...).mapNotNull { it() }` for conditional alert generation (DailySummaryConsumer)
- `alerts.forEach(::publishAlert)` — method references for iteration
- `asSequence()` for lazy processing of hourly readings (OpenMeteoClient)
- `?.let { }` for null-safe serialization
- `?.toLongOrNull() ?: default` for safe env var parsing in WeatherWatcherConfig

## Shutdown Hook Pattern
```kotlin
Runtime.getRuntime().addShutdownHook(Thread {
    logger.info("Encerrando...")
    producer.close()
})
```
Used in WeatherProducerApp; consumers rely on AutoCloseable instead.
