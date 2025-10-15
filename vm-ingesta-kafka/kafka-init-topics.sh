echo "Esperando a que Kafka esté disponible..."
sleep 10  # pequeño delay para asegurar que Kafka ya inició

echo "Creando tópicos si no existen..."
docker exec kafka bash -c "
  kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic crypto_stream --partitions 1 --replication-factor 1
  kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic connect-configs --partitions 1 --replication-factor 1
  kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic connect-offsets --partitions 1 --replication-factor 1
  kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic connect-status --partitions 1 --replication-factor 1

  kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name connect-offsets --add-config cleanup.policy=compact
  kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name connect-status --add-config cleanup.policy=compact
  kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name connect-configs --add-config cleanup.policy=compact
"
echo "✅ Tópicos listos."