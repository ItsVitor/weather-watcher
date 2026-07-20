# Weather-Watcher: Technology Stack

## Languages & Runtime
- Kotlin 1.9.23 (JVM target: Java 17)
- Build: Maven (pom.xml)

## Core Dependencies
| Library | Version | Purpose |
|---|---|---|
| kafka-clients | 4.3.0 | Kafka producer/consumer API |
| kafka-streams | 4.3.0 | Kafka Streams DSL (Projeto 2) |
| kafka-streams-test-utils | 4.3.0 | TopologyTestDriver para testes unitários (test scope) |
| jackson-module-kotlin | 2.15.3 | JSON serialization with Kotlin support |
| jackson-datatype-jsr310 | 2.15.3 | Java 8 time (LocalDateTime) support |
| jackson-annotations | 2.15.3 | Fixado explicitamente para evitar conflito transitivo com kafka-streams |
| jackson-databind | 2.15.3 | Fixado explicitamente para evitar conflito transitivo com kafka-streams |
| jackson-core | 2.15.3 | Fixado explicitamente para evitar conflito transitivo com kafka-streams |
| okhttp | 4.12.0 | HTTP client for Open-Meteo API |
| twilio | 10.1.0 | WhatsApp notification delivery |
| slf4j-simple | 1.7.36 | Logging |
| Java-WebSocket | 1.5.4 | Servidor WebSocket embutido para o dashboard |
| junit-jupiter | 5.10.2 | Testing (test scope) |

## Infrastructure
- Apache Kafka 4.3.0 in KRaft mode (no Zookeeper), 3 combined brokers
- Broker configs: `infrastructure/kraft/server-{1,2,3}.properties`

## Build & Run Commands
```bash
# Compile
mvn compile

# Run all unit tests
mvn test

# Run specific integration test (requires Kafka running)
mvn test -Dtest=HeatwaveDetectorTest

# Run a component
source .env && mvn compile exec:java -Dexec.mainClass="org.example.producers.WeatherProducerAppKt"

# Run HeatwaveStreamsDetector (Projeto 2)
source .env && mvn compile exec:java -Dexec.mainClass="org.example.streams.HeatwaveStreamsDetectorAppKt"

# Run HumidHeatAlertStream (Projeto 2 - Situação A)
source .env && mvn compile exec:java -Dexec.mainClass="org.example.streams.HumidHeatAlertStreamAppKt"

# Run TemperatureDropStream (Projeto 2 - Situação B)
source .env && mvn compile exec:java -Dexec.mainClass="org.example.streams.TemperatureDropStreamAppKt"

# Run HeatBeforeRainStream (Projeto 2 - Situação C)
source .env && mvn compile exec:java -Dexec.mainClass="org.example.streams.HeatBeforeRainStreamAppKt"

# Run Dashboard (WebSocket server)
source .env && mvn compile exec:java -Dexec.mainClass="org.example.dashboard.DashboardServerAppKt"
# Abrir no navegador: src/main/resources/dashboard.html

# Start Kafka cluster
cd infrastructure/kraft && ./start-cluster.sh && ./create-topics.sh
```

## Key Configuration (WeatherWatcherConfig)
All config via environment variables with safe defaults:
- `POLLING_INTERVAL_MINUTES` (default: 1 in code, 15 recommended)
- `KAFKA_BOOTSTRAP_SERVERS` (default: localhost:9092)
- `LOCATION_LATITUDE` / `LOCATION_LONGITUDE` (default: Vitória-ES: -20.31, -40.31)
- `TIMEZONE` (default: America/Sao_Paulo)
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_FROM`, `WHATSAPP_TO` (required for WhatsApp)

## External APIs
- **Open-Meteo**: `https://api.open-meteo.com/v1/forecast` — free, no auth required
- **Twilio**: REST API for WhatsApp messaging — requires account credentials
