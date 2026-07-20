# Weather-Watcher: Project Structure

## Directory Layout
```
weather-watcher/
├── src/main/kotlin/org/example/
│   ├── config/
│   │   └── WeatherWatcherConfig.kt       # Singleton object; all env var config
│   ├── model/
│   │   ├── WeatherData.kt                # Raw API payload (data class)
│   │   └── AlertEvent.kt                 # Alert output + AlertType/Severity enums
│   ├── serdes/
│   │   ├── WeatherSerializer.kt / WeatherDeserializer.kt
│   │   └── AlertSerializer.kt / AlertDeserializer.kt
│   ├── producers/
│   │   ├── OpenMeteoClient.kt            # HTTP client + JSON parsing
│   │   ├── WeatherAPIProducer.kt         # Kafka producer for weather-raw
│   │   └── WeatherProducerApp.kt         # Polling orchestrator + main()
│   ├── consumers/
│   │   ├── DailySummaryConsumer.kt       # Stateful daily alert processor
│   │   ├── HourlyRainConsumer.kt         # Imminent rain detector
│   │   ├── WhatsAppNotificationConsumer.kt # Twilio sink connector
│   │   └── *App.kt (x3)                  # main() entry points
│   └── processors/
│       ├── HeatwaveDetector.kt           # Stateful consumer+producer
│       ├── TemperatureStore.kt           # Circular buffer state store
│       └── HeatwaveDetectorApp.kt        # main() entry point
│   └── streams/
│       ├── HeatwaveStreamsDetector.kt    # Projeto 2: HeatwaveDetector migrado para Streams DSL
│       ├── HeatwaveStreamsDetectorApp.kt # main() entry point
│       ├── HumidHeatAlertStream.kt       # Situação A: KStream-KStream join (Sliding 30min)
│       ├── HumidHeatAlertStreamApp.kt    # main() entry point
│       ├── TemperatureDropStream.kt      # Situação B: TumblingWindow + FixedKeyProcessor
│       ├── TemperatureDropStreamApp.kt   # main() entry point
│       ├── HeatBeforeRainStream.kt       # Situação C: Allen BEFORE (SessionWindows + join)
│       └── HeatBeforeRainStreamApp.kt    # main() entry point
│   └── dashboard/
│       ├── DashboardServer.kt            # WebSocketServer + KafkaConsumer de weather-alerts
│       └── DashboardServerApp.kt         # main() entry point
├── src/main/resources/
│   └── dashboard.html                    # Página do dashboard (abrir no navegador)
├── src/test/kotlin/org/example/
│   ├── consumers/                        # Unit + integration tests
│   ├── processors/                       # Unit tests with reflection
│   ├── producers/                        # Unit tests
│   └── serdes/                           # Serialization round-trip tests
├── infrastructure/kraft/
│   ├── server-{1,2,3}.properties         # 3-broker KRaft config
│   ├── start-cluster.sh / stop-cluster.sh
│   └── create-topics.sh
├── pom.xml
├── CONFIG.md
└── README.md
```

## Core Components and Relationships

### Kafka Topics
- `weather-raw`: 3 partitions, replication 3, 24h retention — raw WeatherData events
- `weather-alerts`: 3 partitions, replication 3, 7-day retention — derived AlertEvent events

### Component Roles
| Component | Consumes | Produces | State |
|---|---|---|---|
| WeatherAPIProducer | Open-Meteo HTTP | weather-raw | Stateless |
| DailySummaryConsumer | weather-raw | weather-alerts | Stateful (lastProcessedTime) |
| HourlyRainConsumer | weather-raw | weather-alerts | Stateless |
| HeatwaveDetector | weather-raw | weather-alerts | Stateful (TemperatureStore + alertTriggered flag) |
| HeatwaveStreamsDetector | weather-raw | weather-alerts | Stateful (Streams SlidingWindows + count) |
| HumidHeatAlertStream | weather-raw | weather-alerts | Stateful (KStream-KStream join, SlidingWindow 30min) |
| TemperatureDropStream | weather-raw | weather-alerts | Stateful (TumblingWindow + aggregate + FixedKeyProcessor) |
| HeatBeforeRainStream | weather-raw | weather-alerts | Stateful (SessionWindows + KStream-KStream join) |
| DashboardServer | weather-alerts | WebSocket broadcast | Stateless |

### Architectural Patterns
- **Publisher/Subscriber** via Kafka topics
- **Sink Connector** pattern: WhatsAppNotificationConsumer
- **Stateful Stream Processing**: HeatwaveDetector uses circular buffer (TemperatureStore); HeatwaveStreamsDetector uses Kafka Streams SlidingWindows
- **Kafka Streams DSL**: Projeto 2 topologies in `streams/` package use declarative topology (StreamsBuilder)
- **Session Windows**: HeatBeforeRainStream uses inactivity-based sessions to model heat/rain periods
- **Allen's Interval Algebra (BEFORE)**: HeatBeforeRainStream verifies `heatSession.end < rainSession.start` after join
- **FixedKeyProcessor**: TemperatureDropStream uses Processor API for cross-window state comparison (replaces deprecated Transformer)
- **EXACTLY_ONCE_V2**: all Streams topologies use transactional processing guarantee
- **WebSocket Server**: DashboardServer usa Java-WebSocket para broadcast em tempo real para o browser
- **Separation of concerns**: each component has a dedicated `*App.kt` with `main()`
- **Configuration via environment variables**: centralized in WeatherWatcherConfig singleton
