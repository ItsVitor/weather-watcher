# Configuração do Weather-Watcher

O sistema pode ser configurado através de variáveis de ambiente ou usar valores padrão.

## Variáveis de Ambiente

### POLLING_INTERVAL_MINUTES
Intervalo de polling da API Open-Meteo em minutos.
- **Padrão**: 15
- **Exemplo**: `export POLLING_INTERVAL_MINUTES=1`

### KAFKA_BOOTSTRAP_SERVERS
Endereço dos servidores Kafka bootstrap.
- **Padrão**: localhost:9092
- **Exemplo**: `export KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9192`

### LOCATION_LATITUDE
Latitude da localização monitorada.
- **Padrão**: -20.31 (Vitória-ES)
- **Exemplo**: `export LOCATION_LATITUDE=-23.55`

### LOCATION_LONGITUDE
Longitude da localização monitorada.
- **Padrão**: -40.31 (Vitória-ES)
- **Exemplo**: `export LOCATION_LONGITUDE=-46.63`

### TIMEZONE
Timezone para ajuste de horários.
- **Padrão**: America/Sao_Paulo
- **Exemplo**: `export TIMEZONE=America/Sao_Paulo`

## Exemplo de Uso

```bash
# Configurar para polling de 1 minuto (teste)
export POLLING_INTERVAL_MINUTES=1

# Executar produtor
mvn compile exec:java -Dexec.mainClass="org.example.producers.WeatherProducerAppKt"

# Executar detector de ondas de calor
mvn compile exec:java -Dexec.mainClass="org.example.processors.HeatwaveDetectorAppKt"
```

## Valores Padrão

Se nenhuma variável de ambiente for definida, o sistema usa:
- Polling: 15 minutos
- Kafka: localhost:9092
- Localização: Vitória-ES (-20.31, -40.31)
- Timezone: America/Sao_Paulo
