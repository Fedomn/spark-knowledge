compose-file:
	open https://hudi.apache.org/docs/docker_demo
	wget https://raw.githubusercontent.com/apache/hudi/master/docker/compose/docker-compose_hadoop284_hive233_spark244.yml
	wget https://raw.githubusercontent.com/apache/hudi/master/docker/compose/hadoop.env

pull:
	HUDI_WS=./ docker-compose -f docker-compose_hadoop284_hive233_spark244.yml pull

up:
	HUDI_WS=./ docker-compose -f docker-compose_hadoop284_hive233_spark244.yml up -d

down:
	HUDI_WS=./ docker-compose -f docker-compose_hadoop284_hive233_spark244.yml down

setup-host:
	echo "127.0.0.1 adhoc-1 adhoc-2 namenode datanode1 hiveserver hivemetastore kafkabroker sparkmaster zookeeper"