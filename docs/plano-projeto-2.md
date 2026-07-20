# Plano de Implementação — Projeto 2: Kafka Streams

## Contexto

O Projeto 2 migra e expande o Weather-Watcher para usar obrigatoriamente a **API Kafka Streams DSL**. O código existente (Projeto 1) usa a API de baixo nível `kafka-clients` (KafkaConsumer/KafkaProducer). A nova versão deve coexistir no mesmo repositório, organizada em novos pacotes.

---

## Requisitos do Projeto 2

1. **HeatwaveDetector** migrado para Kafka Streams (substitui a implementação manual com `TemperatureStore`)
2. **3 novas topologias** Kafka Streams detectando situações de interesse
   - Uma delas deve usar **Álgebra de Allen** (relações temporais entre intervalos)
3. Uso obrigatório de operações **stateless** (filter, map) **e stateful** (agregações, janelas, joins)

---

## Novas Situações de Interesse Propostas

### Situação A — Alerta de Conforto Composto (Join KStream-KTable)

**Descrição:** Detecta quando há simultaneamente chuva iminente **e** temperatura alta (>28°C) na mesma janela de tempo — condição de "calor úmido" desconfortável.

**Operações DSL utilizadas:**

- `filter` (stateless) — filtra eventos com chuva >= 50% e temperatura >= 28°C separadamente
- `KStream-KStream join` com janela de tempo (stateful) — une os dois streams filtrados dentro de uma janela de 30 minutos
- `mapValues` (stateless) — transforma o par joined em um `AlertEvent`

**Relação com a DSL:** Demonstra join windowed entre dois KStreams derivados do mesmo tópico fonte.

---

### Situação B — Tendência de Queda de Temperatura (Agregação com Janela Tumbling)

**Descrição:** Detecta quando a temperatura média em uma janela de 1 hora é significativamente menor que a janela anterior (queda >= 5°C) — indicativo de frente fria chegando.

**Operações DSL utilizadas:**

- `mapValues` (stateless) — extrai a temperatura como Double
- `groupByKey` + `windowedBy(TumblingWindow)` + `aggregate` (stateful) — calcula média por janela de 1 hora
- `toStream` + `filter` (stateless) — filtra janelas onde a queda foi detectada
- Comparação entre janelas consecutivas via state store auxiliar

**Relação com a DSL:** Demonstra agregação windowed (tumbling) e processamento de resultados de janelas.

---

### Situação C — Período de Calor Prolongado com Álgebra de Allen (Allen's Interval Algebra)

**Descrição:** Detecta quando um **período de alta temperatura** (>32°C) **precede** (relação `BEFORE` de Allen) um **período de alta probabilidade de chuva** (>60%) — padrão típico de tarde de verão com tempestade.

**Álgebra de Allen aplicada:** Relação `BEFORE` — o intervalo de calor termina antes do intervalo de chuva começar, com gap de até 2 horas entre eles.

**Operações DSL utilizadas:**

- `filter` (stateless) — separa eventos de "calor" e eventos de "chuva iminente"
- `groupByKey` + `windowedBy(SessionWindow)` (stateful) — agrupa eventos contíguos em sessões de calor e sessões de chuva
- `KStream-KStream join` com `JoinWindows` (stateful) — verifica se a sessão de calor ocorreu antes da sessão de chuva dentro de uma janela de 2 horas
- `mapValues` (stateless) — constrói o `AlertEvent` com metadados dos dois intervalos

**Relação com a DSL:** Demonstra session windows + join windowed para modelar relações temporais entre intervalos (Allen's `BEFORE`).

---

## Estrutura de Pacotes Nova

```
src/main/kotlin/org/example/
└── streams/
    ├── HeatwaveStreamsDetector.kt         # Migração do HeatwaveDetector para Streams DSL
    ├── HeatwaveStreamsDetectorApp.kt      # main()
    ├── HumidHeatAlertStream.kt            # Situação A: Join chuva + calor
    ├── HumidHeatAlertStreamApp.kt         # main()
    ├── TemperatureDropStream.kt           # Situação B: Queda de temperatura (tumbling window)
    ├── TemperatureDropStreamApp.kt        # main()
    ├── HeatBeforeRainStream.kt            # Situação C: Allen BEFORE (session + join)
    └── HeatBeforeRainStreamApp.kt         # main()
```

Novos `AlertType` adicionados ao `AlertEvent.kt`:

- `HUMID_HEAT` — calor úmido (Situação A)
- `TEMPERATURE_DROP` — queda de temperatura (Situação B)
- `HEAT_BEFORE_RAIN` — calor precede chuva (Situação C)

---

## Passos de Implementação

### Passo 1 — Adicionar dependência `kafka-streams` ao `pom.xml`

A dependência `kafka-streams` é separada de `kafka-clients`. Adicionar ao `pom.xml`.

### Passo 2 — Adicionar novos `AlertType` ao modelo

Adicionar `HUMID_HEAT`, `TEMPERATURE_DROP` e `HEAT_BEFORE_RAIN` ao enum `AlertType` em `AlertEvent.kt`.

### Passo 3 — Implementar `HeatwaveStreamsDetector`

Migrar a lógica do `HeatwaveDetector` (Projeto 1) para Kafka Streams DSL:

- `filter` para temperaturas acima do limiar
- `groupByKey` + `windowedBy(SlidingWindow)` + `count` para contar leituras consecutivas acima do limiar
- `filter` no resultado para acionar quando count >= N
- `mapValues` para criar o `AlertEvent`
- `.to("weather-alerts")`

### Passo 4 — Implementar `HumidHeatAlertStream` (Situação A)

- Criar dois KStreams filtrados a partir de `weather-raw`
- Realizar `KStream-KStream join` com janela de 30 minutos
- Publicar alerta `HUMID_HEAT` em `weather-alerts`

### Passo 5 — Implementar `TemperatureDropStream` (Situação B)

- Agregar temperaturas em janelas tumbling de 1 hora
- Comparar médias de janelas consecutivas via state store
- Publicar alerta `TEMPERATURE_DROP` quando queda >= 5°C

### Passo 6 — Implementar `HeatBeforeRainStream` (Situação C — Allen BEFORE)

- Criar sessões de calor e sessões de chuva com `SessionWindows`
- Realizar join entre os dois streams de sessões com `JoinWindows` de 2 horas
- Verificar a relação temporal `BEFORE` (fim do calor < início da chuva)
- Publicar alerta `HEAT_BEFORE_RAIN`

### Passo 7 — Criar `*App.kt` para cada topologia

Cada app instancia `StreamsBuilder`, constrói a topologia, cria `KafkaStreams` e chama `.start()` com shutdown hook.

### Passo 8 — Testes unitários com `TopologyTestDriver`

Kafka Streams oferece `TopologyTestDriver` para testes sem cluster real. Criar testes unitários para cada topologia.

---

## Mapeamento de Operações DSL por Topologia

| Topologia | Stateless | Stateful |
|---|---|---|
| HeatwaveStreamsDetector | `filter`, `mapValues` | `groupByKey`, `windowedBy` (sliding), `count` |
| HumidHeatAlertStream | `filter`, `mapValues` | `KStream-KStream join` (windowed) |
| TemperatureDropStream | `mapValues`, `filter` | `groupByKey`, `windowedBy` (tumbling), `aggregate` |
| HeatBeforeRainStream | `filter`, `mapValues` | `SessionWindows`, `KStream-KStream join` (windowed) |

---

## Notas Técnicas

- Todas as topologias consomem de `weather-raw` e publicam em `weather-alerts`
- O `application.id` de cada topologia deve ser único (ex: `heatwave-streams`, `humid-heat-streams`, etc.)
- Os consumidores do Projeto 1 continuam funcionando em paralelo — não há remoção de código existente
- A chave dos registros em `weather-raw` pode ser `null` (o produtor atual não define chave); para joins e groupBy, será necessário usar `selectKey` para derivar uma chave (ex: coordenadas da localização)
- Para a Álgebra de Allen (Situação C), a relação `BEFORE` é implementada verificando que `endTime(calor) < startTime(chuva)` dentro da janela de join — os metadados do `AlertEvent` devem registrar os dois intervalos detectados
