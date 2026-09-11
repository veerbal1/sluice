KAFKA = docker exec sluice-kafka-1 /opt/kafka/bin

reset:
	$(KAFKA)/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic events || true
	$(KAFKA)/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions 3 --replication-factor 1

offsets:
	$(KAFKA)/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events

consume:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events --from-beginning --property print.key=true --property print.partition=true

load:
	k6 run --summary-mode=full temp.js