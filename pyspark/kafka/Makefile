doc:
	open https://github.com/apache/kafka/blob/trunk/config/kraft/README.md
	open https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

init:
	$(eval uuid=$(shell cd ../../kafka-3.2.0-src && ./bin/kafka-storage.sh random-uuid))
	cd ../../kafka-3.2.0-src && ./bin/kafka-storage.sh format -t ${uuid} -c ./config/kraft/server.properties

start:
	cd ../../kafka-3.2.0-src && ./bin/kafka-server-start.sh ./config/kraft/server.properties

create-topic:
	cd ../../kafka-3.2.0-src && \
	./bin/kafka-topics.sh --create --topic foo --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

show-topic:
	cd ../../kafka-3.2.0-src && \
	./bin/kafka-topics.sh --describe --topic foo --bootstrap-server localhost:9092

producer:
	cd ../../kafka-3.2.0-src && \
	bin/kafka-console-producer.sh --topic foo --bootstrap-server localhost:9092

consumer:
	cd ../../kafka-3.2.0-src && \
	./bin/kafka-console-consumer.sh --topic foo --from-beginning --bootstrap-server localhost:9092

clean:
	rm -rf /tmp/kraft-combined-logs

kafka-dump-log:
	cd ../../kafka-3.2.0-src && \
	./bin/kafka-dump-log.sh  --cluster-metadata-decoder --skip-record-metadata --files /tmp/kraft-combined-logs/__cluster_metadata-0/*.log

kafka-metadata-shell:
	cd ../../kafka-3.2.0-src && \
	./bin/kafka-metadata-shell.sh  --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
