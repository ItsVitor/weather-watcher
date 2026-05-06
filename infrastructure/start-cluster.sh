#!/bin/bash

KAFKA_HOME="/home/vitor-ubuntu/kafka_2.13-4.2.0"
CONFIG_DIR="$(pwd)/infrastructure/kraft"

echo "=== Weather-Watcher Kafka KRaft Cluster ==="
echo ""

# Gerar UUID do cluster (apenas na primeira execução)
if [ ! -f "$CONFIG_DIR/cluster.id" ]; then
    echo "Gerando Cluster UUID..."
    CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
    echo $CLUSTER_ID > "$CONFIG_DIR/cluster.id"
    echo "Cluster ID: $CLUSTER_ID"
    echo ""
    
    # Formatar storage para cada broker
    echo "Formatando storage dos brokers..."
    $KAFKA_HOME/bin/kafka-storage.sh format -t $CLUSTER_ID -c $CONFIG_DIR/server-1.properties
    $KAFKA_HOME/bin/kafka-storage.sh format -t $CLUSTER_ID -c $CONFIG_DIR/server-2.properties
    $KAFKA_HOME/bin/kafka-storage.sh format -t $CLUSTER_ID -c $CONFIG_DIR/server-3.properties
    echo ""
else
    CLUSTER_ID=$(cat "$CONFIG_DIR/cluster.id")
    echo "Usando Cluster ID existente: $CLUSTER_ID"
    echo ""
fi

# Iniciar os 3 brokers em background
echo "Iniciando Broker 1 (porta 9092)..."
$KAFKA_HOME/bin/kafka-server-start.sh $CONFIG_DIR/server-1.properties > /tmp/kafka-broker-1.log 2>&1 &
echo $! > /tmp/kafka-broker-1.pid

sleep 3

echo "Iniciando Broker 2 (porta 9192)..."
$KAFKA_HOME/bin/kafka-server-start.sh $CONFIG_DIR/server-2.properties > /tmp/kafka-broker-2.log 2>&1 &
echo $! > /tmp/kafka-broker-2.pid

sleep 3

echo "Iniciando Broker 3 (porta 9292)..."
$KAFKA_HOME/bin/kafka-server-start.sh $CONFIG_DIR/server-3.properties > /tmp/kafka-broker-3.log 2>&1 &
echo $! > /tmp/kafka-broker-3.pid

echo ""
echo "✓ Cluster iniciado com sucesso!"
echo ""
echo "Brokers disponíveis:"
echo "  - Broker 1: localhost:9092"
echo "  - Broker 2: localhost:9192"
echo "  - Broker 3: localhost:9292"
echo ""
echo "Logs disponíveis em /tmp/kafka-broker-*.log"
echo "Para criar os tópicos, execute: ./create-topics.sh"
echo "Para parar o cluster, execute: ./stop-cluster.sh"
