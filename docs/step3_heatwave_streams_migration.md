# Passo 3 — Migração do HeatwaveDetector para Kafka Streams

## Objetivo

Reimplementar a detecção de onda de calor usando a DSL do Kafka Streams, substituindo o gerenciamento manual de estado (`TemperatureStore`, `alertTriggered`) e o loop de polling (`while(true)`) pela infraestrutura declarativa do Streams.

---

## O que SAI e por quê

### `processWeatherData()` — o orquestrador imperativo

```kotlin
// PROJETO 1 — sai
fun processWeatherData(data: WeatherData) {
    temperatureStore.add(data.currentTemperature)   // 1. atualiza estado
    if (currentTemp <= criticalThreshold && alertTriggered) {
        alertTriggered = false                       // 2. reseta flag
    }
    if (shouldTriggerHeatwaveAlert()) {
        publishAlert(createHeatwaveAlert(data))      // 3. publica
        alertTriggered = true
    }
}
```

**Por quê sai:** Esse método existe porque alguém precisa coordenar as etapas — e no modelo imperativo esse alguém é o desenvolvedor. No Projeto 2, cada etapa vira um operador da topologia, e a coordenação passa a ser responsabilidade do runtime do Streams:

| Responsabilidade | Projeto 1 | Projeto 2 |
|---|---|---|
| Atualizar estado | `temperatureStore.add(currentTemp)` | `groupByKey` + `windowedBy` + `count()` |
| Verificar condição | `shouldTriggerHeatwaveAlert()` | `filter { count >= windowSize }` |
| Publicar resultado | `publishAlert(createHeatwaveAlert())` | `mapValues { createHeatwaveAlert() }` + `.to(...)` |

O `processWeatherData()` não é migrado — ele é *dissolvido* na topologia declarativa.

---


### `KafkaConsumer` e `KafkaProducer` manuais

```kotlin
// PROJETO 1 — sai
private val consumer: KafkaConsumer<String, WeatherData> = KafkaConsumer(...)
private val producer: KafkaProducer<String, AlertEvent> = KafkaProducer(...)
```

**Por quê sai:** Na API de baixo nível, o desenvolvedor é responsável por criar, configurar, subscrever, fazer poll e fechar o consumer/producer manualmente. O Kafka Streams encapsula tudo isso internamente — a topologia declara *o que* processar, e o runtime do Streams cuida do *como* (threads, poll, commit, rebalance, etc.).

---

### O loop de consumo `while(true)`

```kotlin
// PROJETO 1 — sai
fun start() {
    while (true) {
        val records = consumer.poll(Duration.ofMillis(1000))
        records.forEach { record -> processWeatherData(record.value()) }
    }
}
```

**Por quê sai:** O loop de polling é a forma imperativa de consumir mensagens. Com Streams, a topologia é contínua por definição — ao chamar `kafkaStreams.start()`, o runtime inicia threads internas que fazem o polling automaticamente. O desenvolvedor não escreve mais loops.

---

### `TemperatureStore` (fila circular manual)

```kotlin
// PROJETO 1 — sai
private val temperatureStore = TemperatureStore(windowSize)
```

```kotlin
// TemperatureStore: fila circular em memória local da JVM
fun add(temperature: Double) {
    if (temperatures.size >= capacity) temperatures.removeAt(0)
    temperatures.add(temperature)
}
```

**Por quê sai:** O `TemperatureStore` é um estado mantido na memória da JVM, invisível ao Kafka. Isso traz dois problemas sérios:

1. **Sem tolerância a falhas:** se o processo reiniciar, o histórico de temperaturas é perdido e a janela recomeça do zero.
2. **Sem escalabilidade:** se a aplicação rodar em múltiplas instâncias (para processar partições diferentes), cada instância tem seu próprio store isolado — não há coordenação.

O Kafka Streams substitui isso por **State Stores gerenciados**, que são persistidos localmente e replicados via changelog topic no Kafka. Ao reiniciar, o estado é restaurado automaticamente.

---

### A flag de idempotência `alertTriggered`

```kotlin
// PROJETO 1 — sai
private var alertTriggered = false

if (currentTemp <= criticalThreshold && alertTriggered) {
    alertTriggered = false
}
if (shouldTriggerHeatwaveAlert()) {
    alertTriggered = true
}
```

**Por quê sai:** Essa flag existe para evitar que o alerta seja disparado repetidamente enquanto a temperatura permanece alta. Ela é necessária no Projeto 1 porque o loop processa cada mensagem individualmente, sem noção de janela temporal.

Com Streams e `SlidingWindows`, a semântica muda: a topologia conta quantas leituras acima do limiar ocorreram dentro de uma janela de tempo deslizante. O alerta é emitido apenas quando o `count` da janela atinge o limiar `N`. Quando a temperatura cai, a janela naturalmente deixa de ter `N` leituras acima do limiar — a idempotência passa a ser responsabilidade da janela, não de uma flag manual.

---

### `AutoCloseable` e `close()`

```kotlin
// PROJETO 1 — sai
override fun close() {
    consumer.close()
    producer.close()
}
```

**Por quê sai:** Com Streams, o fechamento do consumer e producer é gerenciado pelo próprio `KafkaStreams`. O shutdown hook chama `kafkaStreams.close()`, que encerra tudo internamente.

---

## O que FICA e por quê

### Os limiares de configuração

```kotlin
// FICA
private val criticalThreshold: Double = 35.0
private val windowSize: Int = 3
```

**Por quê fica:** São parâmetros de negócio — o limiar de temperatura e o número de leituras consecutivas necessárias. Esses valores não têm relação com a infraestrutura de processamento e permanecem configuráveis da mesma forma.

---

### A lógica de criação do `AlertEvent`

```kotlin
// FICA (adaptada)
private fun createHeatwaveAlert(...): AlertEvent {
    return AlertEvent(
        alertType = AlertType.HEATWAVE,
        message = "🔥 Onda de calor detectada! ...",
        severity = Severity.ALERT,
        metadata = mapOf(...)
    )
}
```

**Por quê fica:** A regra de negócio — o conteúdo, a severidade e os metadados do alerta — não muda. O que muda é *quando* essa função é chamada (pelo `mapValues` da DSL em vez de pelo `processWeatherData` manual).

---

### Os Serdes existentes (`WeatherDeserializer`, `AlertSerializer`)

**Por quê ficam:** Como discutido no Passo 3 removido do plano, os `Serializer`/`Deserializer` existentes implementam as interfaces padrão do Kafka e são diretamente compatíveis com a API Streams via `Serdes.serdeFrom(WeatherSerializer(), WeatherDeserializer())`. Não há necessidade de reescrevê-los.

---

## O que MUDA e por quê

### A janela temporal: de contagem por eventos para janela deslizante por tempo

**Projeto 1:** a janela é definida por **número de eventos** — as últimas `N` leituras, independentemente de quando ocorreram. O `TemperatureStore` com capacidade `N` implementa isso.

**Projeto 2:** a janela é definida por **intervalo de tempo** — usando `SlidingWindows`, que agrupa registros cujos timestamps estão dentro de uma diferença máxima configurada. O `count` dentro dessa janela representa quantas leituras acima do limiar ocorreram no período.

**Por que muda:** Janelas baseadas em tempo são mais corretas semanticamente para dados meteorológicos. "3 leituras consecutivas" no Projeto 1 pode representar 45 minutos ou 3 horas dependendo do intervalo de polling. Com `SlidingWindows`, o critério é explícito: "N leituras acima do limiar nas últimas X horas".

A configuração padrão será: janela de `windowSize * pollingIntervalMinutes` minutos, mantendo equivalência com o comportamento do Projeto 1.

---

### A estrutura da classe: de Consumer/Producer para topologia declarativa

**Projeto 1:**
```
HeatwaveDetector
  ├── KafkaConsumer (subscreve weather-raw)
  ├── KafkaProducer (publica em weather-alerts)
  ├── TemperatureStore (estado manual)
  ├── alertTriggered (flag manual)
  ├── processWeatherData() (lógica imperativa)
  └── start() → while(true) { poll → process → publish }
```

**Projeto 2:**
```
HeatwaveStreamsDetector
  ├── buildTopology(builder) → retorna a topologia declarativa
  │     ├── stream("weather-raw")
  │     ├── filter { temp > threshold }        ← stateless
  │     ├── selectKey { location }
  │     ├── groupByKey
  │     ├── windowedBy(SlidingWindows)          ← stateful
  │     ├── count()                             ← stateful
  │     ├── toStream
  │     ├── filter { count >= windowSize }      ← stateless
  │     ├── mapValues { createHeatwaveAlert() } ← stateless
  │     └── to("weather-alerts")
  └── (KafkaStreams gerencia consumer, producer, estado e threads)
```

**Por que muda:** A DSL do Streams é declarativa — descreve *transformações* sobre o stream, não *como* executá-las. Isso torna o código mais legível, mais testável (via `TopologyTestDriver`) e delega ao runtime as responsabilidades operacionais (rebalanceamento, fault tolerance, paralelismo).

---

## Resumo visual

| Elemento | Projeto 1 | Projeto 2 | Motivo |
|---|---|---|---|
| `processWeatherData()` | Orquestrador imperativo das 3 etapas | Dissolvido nos operadores da topologia | A DSL *é* o processamento |
| `KafkaConsumer` | Manual | Gerenciado pelo Streams | Infraestrutura delegada ao runtime |
| `KafkaProducer` | Manual | Gerenciado pelo Streams | Infraestrutura delegada ao runtime |
| Loop `while(true)` | Explícito | Implícito no `KafkaStreams.start()` | Modelo declarativo |
| `TemperatureStore` | Fila circular em memória JVM | State Store do Streams (persistido + replicado) | Tolerância a falhas e escalabilidade |
| Flag `alertTriggered` | Variável de instância | Semântica da janela deslizante | Idempotência gerenciada pela janela |
| Janela temporal | Por contagem de eventos | Por intervalo de tempo (`SlidingWindows`) | Semântica temporal explícita |
| `close()` | Fecha consumer e producer manualmente | `kafkaStreams.close()` | Gerenciamento unificado pelo runtime |
| Limiares de configuração | Parâmetros do construtor | Parâmetros do construtor | Sem mudança — são regras de negócio |
| Criação do `AlertEvent` | `createHeatwaveAlert()` | `createHeatwaveAlert()` (via `mapValues`) | Sem mudança — é regra de negócio |
| Serdes | `WeatherDeserializer`, `AlertSerializer` | Os mesmos, via `Serdes.serdeFrom()` | Compatibilidade direta com a API Streams |
