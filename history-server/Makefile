init:
	mkdir /tmp/spark-events | true
	cp ./spark-defaults.conf ../spark-3.2.0-bin-hadoop3.2/conf/

start: stop
	cd ../spark-3.2.0-bin-hadoop3.2/sbin && ./start-history-server.sh
	open http://localhost:18080

stop:
	cd ../spark-3.2.0-bin-hadoop3.2/sbin && ./stop-history-server.sh