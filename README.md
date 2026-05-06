# Weather-Watcher: Sistema de Monitoramento Meteorológico Orientado a Eventos

## Visão Geral

O **Weather-Watcher** é um sistema distribuído de monitoramento meteorológico que utiliza arquitetura orientada a eventos. O sistema ingere dados climáticos continuamente, processa regras de negócio em tempo real (como previsões diárias, probabilidade de chuva iminente e detecção de anomalias climáticas) e orquestra notificações aos usuários via WhatsApp.

## Arquitetura de Infraestrutura (Apache Kafka)

O sistema opera sobre um cluster Kafka local (3 Brokers para garantia de alta disponibilidade e tolerância a falhas) e implementa o padrão *Publisher/Subscriber*.

### 1. Tópicos

* `weather-raw` (Retenção curta): Tópico de alta frequência que recebe os "eventos primitivos" brutos coletados da API meteorológica.
* `weather-alerts` (Retenção estendida): Tópico que recebe os "eventos derivados" (anomalias calculadas por consumidores com estado) e gatilhos de notificação.

### 2. Produtores (Producers)

* **`WeatherAPIProducer`**: Atua como um *Source Connector*. Realiza *polling* a cada 15 minutos na API Open-Meteo, extrai as métricas de temperatura, chuva e índice UV, empacota os dados via SerDes customizado em formato JSON e publica o evento primitivo no tópico `weather-raw`.

### 3. Consumidores (Consumers & Processors)

* **`DailySummaryConsumer`**: Consome de `weather-raw`. Mantém estado local para identificar a transição das 6h00 da manhã. Ao atingir o gatilho, processa a previsão diária e publica comandos de alerta (Chuva, UV, Temperatura) em `weather-alerts`.
* **`HourlyRainConsumer`**: Consome de `weather-raw`. Monitora o campo de probabilidade de precipitação para a janela de T+1 (próxima hora). Caso ultrapasse o limiar definido, publica um evento de chuva iminente em `weather-alerts`.
* **`HeatwaveDetector` (Stateful Processor)**: Consumidor complexo que atua com janela de tempo. Consome de `weather-raw` e armazena o histórico recente de temperaturas em um *State Store*. Se registrar *N* leituras consecutivas acima do limite crítico de temperatura, assume o papel de Produtor e publica o evento derivado `Onda-de-Calor` em `weather-alerts`.
* **`WhatsAppNotificationConsumer`**: Atua como um *Sink Connector*. Consome exclusivamente do tópico `weather-alerts` e realiza integrações HTTP com a API do Twilio para rotear a mensagem final ao usuário.

---

## Estrutura de Classes (Kotlin)

O projeto adota o padrão de desenvolvimento em pacotes lógicos, isolando modelos, infraestrutura e processamento.

```text
src/main/kotlin/org/example/
├── model/
│   ├── WeatherData.kt           # Data class representando o payload da API
│   └── AlertEvent.kt            # Data class padronizando a mensagem de saída
├── serdes/
│   ├── WeatherSerializer.kt     # Serialização de WeatherData via Jackson ObjectMapper
│   └── WeatherDeserializer.kt   # Deserialização de WeatherData via Jackson ObjectMapper
├── producers/
│   └── WeatherAPIProducer.kt    # Lógica de HTTP Client e envio para o broker
├── consumers/
│   ├── DailySummaryConsumer.kt  # Processamento dos alertas das 6h00
│   ├── HourlyRainConsumer.kt    # Avaliação de chuva iminente
│   └── WhatsAppNotifier.kt      # Integração Twilio
└── processors/
    ├── HeatwaveDetector.kt      # Processamento stateful (Produtor + Consumidor)
    └── TemperatureStore.kt      # Classe auxiliar para manter o estado (fila circular de temps)
```

---

## Regras de Negócio e Situações de Interesse

O sistema monitora 5 situações fundamentais baseadas no contexto temporal e limiares de segurança:

1. **Guarda-Chuva Diário (Evento Simples):** Disparado às 6h00 caso a precipitação acumulada prevista para o dia seja maior que 0mm.
2. **Proteção UV (Evento Simples):** Disparado às 6h00 orientando o uso de protetor solar caso o índice UV máximo do dia seja > 3.
3. **Conforto Térmico (Evento Simples):** Disparado às 6h00 com recomendação de vestuário e hidratação se a temperatura máxima prevista superar o limite de conforto pré-determinado.
4. **Chuva Iminente (Evento Simples):** Disparado em tempo quase real caso a leitura atual indique chuva para a hora imediatamente subsequente.
5. **Onda de Calor (Evento Complexo):** Disparado dinamicamente quando a temperatura ultrapassa um limite crítico (ex: 35ºC) de forma sustentada por uma janela de tempo definida (ex: 3 horas consecutivas), mitigando falsos positivos de picos isolados.
