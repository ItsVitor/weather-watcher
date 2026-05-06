# Infraestrutura Kafka - Weather Watcher

## Arquitetura KRaft

Cluster com **3 brokers em modo combinado** (controller + broker).

### Configuração dos Brokers

| Broker | Node ID | Porta Cliente | Porta Controller | Log Dir |
|--------|---------|---------------|------------------|---------|
| Broker 1 | 1 | 9092 | 9093 | /tmp/kraft-logs-1 |
| Broker 2 | 2 | 9192 | 9094 | /tmp/kraft-logs-2 |
| Broker 3 | 3 | 9292 | 9095 | /tmp/kraft-logs-3 |

### Tópicos

#### `weather-raw`

- **Partições**: 3
- **Replicação**: 3
- **Retenção**: 24 horas (86400000 ms)
- **Compressão**: LZ4
- **Uso**: Eventos primitivos da API Open-Meteo (alta frequência)

#### `weather-alerts`

- **Partições**: 3
- **Replicação**: 3
- **Retenção**: 7 dias (604800000 ms)
- **Compressão**: LZ4
- **Uso**: Eventos derivados e gatilhos de notificação

---

## Comandos

### Iniciar o Cluster

```bash
./start-cluster.sh
```

### Criar os Tópicos

```bash
./create-topics.sh
```

### Parar o Cluster

```bash
./stop-cluster.sh
```

### Limpar Dados (Reset Completo)

```bash
rm -rf /tmp/kraft-logs-*
rm infrastructure/kraft/cluster.id
```

---

## Verificação

Adapte os caminhos de acordo com a estrutura de arquivos do seu kafka.

### Listar Tópicos

```bash
/home/vitor-ubuntu/kafka_2.13-4.2.0/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```

### Descrever Tópico

```bash
/home/vitor-ubuntu/kafka_2.13-4.2.0/bin/kafka-topics.sh \
  --describe --topic weather-raw --bootstrap-server localhost:9092
```

### Consumir Mensagens (Debug)

```bash
/home/vitor-ubuntu/kafka_2.13-4.2.0/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic weather-raw \
  --from-beginning
```

### Produzir Mensagens (Debug)

```bash
/home/vitor-ubuntu/kafka_2.13-4.2.0/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic weather-raw
```

---

## Logs

Os logs dos brokers ficam em:

- `/tmp/kafka-broker-1.log`
- `/tmp/kafka-broker-2.log`
- `/tmp/kafka-broker-3.log`
