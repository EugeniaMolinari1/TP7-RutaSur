
#!/bin/bash
set -e

KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
KAFKA_CONFIGS="/opt/kafka/bin/kafka-configs.sh"
BROKER="kafka:9092"

echo "Creando topics de RutaSur..."

crear_topic() {
  TOPIC="$1"
  RETENCION="$2"

  if ! "$KAFKA_TOPICS" --bootstrap-server "$BROKER" \
    --describe --topic "$TOPIC" >/dev/null 2>&1; then

    "$KAFKA_TOPICS" --bootstrap-server "$BROKER" \
      --create \
      --topic "$TOPIC" \
      --partitions 3 \
      --replication-factor 1 \
      --config "$RETENCION"
  fi
}

crear_topic "envios.estados" "retention.ms=2592000000"
crear_topic "envios.promesas" "cleanup.policy=compact"
crear_topic "envios.reprogramaciones" "retention.ms=2592000000"
crear_topic "alertas.sla" "retention.ms=604800000"
crear_topic "notificaciones.enviadas" "retention.ms=604800000"
crear_topic "envios.estados.dlq" "retention.ms=2592000000"

echo "Topics de RutaSur listos."
