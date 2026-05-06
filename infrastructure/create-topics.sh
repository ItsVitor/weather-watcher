#!/bin/bash

KAFKA_HOME="/home/vitor-ubuntu/kafka_2.13-4.2.0"
BOOTSTRAP_SERVERS="localhost:9092"

echo "=== Criando Tópicos do Weather-Watcher ==="
echo ""

# Tópico weather-raw: alta frequência, retenção curta (24h)
echo "Criando tópico: weather-raw"
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic weather-raw \
    --partitions 3 \
    --replication-factor 3 \
    --config retention.ms=86400000 \
    --config segment.ms=3600000 \
    --config compression.type=lz4

echo ""

# Tópico weather-alerts: eventos derivados, retenção estendida (7 dias)
echo "Criando tópico: weather-alerts"
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic weather-alerts \
    --partitions 3 \
    --replication-factor 3 \
    --config retention.ms=604800000 \
    --config segment.ms=86400000 \
    --config compression.type=lz4

echo ""
echo "✓ Tópicos criados com sucesso!"
echo ""
echo "Para listar os tópicos:"
echo "  $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVERS"
echo ""
echo "Para descrever um tópico:"
echo "  $KAFKA_HOME/bin/kafka-topics.sh --describe --topic weather-raw --bootstrap-server $BOOTSTRAP_SERVERS"
