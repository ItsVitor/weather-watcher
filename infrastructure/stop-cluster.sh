#!/bin/bash

echo "=== Parando Kafka KRaft Cluster ==="
echo ""

# Parar os brokers usando os PIDs salvos
if [ -f /tmp/kafka-broker-1.pid ]; then
    echo "Parando Broker 1..."
    kill $(cat /tmp/kafka-broker-1.pid) 2>/dev/null
    rm /tmp/kafka-broker-1.pid
fi

if [ -f /tmp/kafka-broker-2.pid ]; then
    echo "Parando Broker 2..."
    kill $(cat /tmp/kafka-broker-2.pid) 2>/dev/null
    rm /tmp/kafka-broker-2.pid
fi

if [ -f /tmp/kafka-broker-3.pid ]; then
    echo "Parando Broker 3..."
    kill $(cat /tmp/kafka-broker-3.pid) 2>/dev/null
    rm /tmp/kafka-broker-3.pid
fi

echo ""
echo "✓ Cluster parado com sucesso!"
echo ""
echo "Para limpar completamente os dados (CUIDADO!):"
echo "  rm -rf /tmp/kraft-logs-* infrastructure/kraft/cluster.id"
