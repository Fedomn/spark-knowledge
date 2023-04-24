DIR := $(CURDIR)

hadoop-bin:
	# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html#Hadoop_Rack_Awareness
	wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
	tar -zxvf ./hadoop-3.3.4.tar.gz

HADOOP_HOME=${DIR}/hadoop-3.3.4
hdfs-init: hdfs-clean
	mkdir -p /tmp/hdfs/data
	mkdir -p /tmp/hdfs/name
	cp ./core-site.xml ./hadoop-3.3.4/etc/hadoop/core-site.xml
	cp ./hdfs-site.xml ./hadoop-3.3.4/etc/hadoop/hdfs-site.xml
	${HADOOP_HOME}/bin/hdfs namenode -format

hdfs-clean:
	rm -rf /tmp/hdfs/data
	rm -rf /tmp/hdfs/name

hdfs-start: hdfs-stop
	${HADOOP_HOME}/bin/hdfs --daemon start namenode
	${HADOOP_HOME}/bin/hdfs --daemon start datanode

hdfs-stop:
	${HADOOP_HOME}/bin/hdfs --daemon stop namenode
	${HADOOP_HOME}/bin/hdfs --daemon stop datanode

hdfs-open:
	open http://localhost:9870

# ------------------------------------------- hive -------------------------------------------
hive-bin:
	wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
	tar -zxvf apache-hive-3.1.3-bin.tar.gz

hms-start:
	open https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+3.0+Administration
	export HADOOP_HOME=/Users/frankma/dev/db/fedomn/spark-knowledge/hadoop/hadoop-3.3.4
	export HADOOP_CLASSPATH=$(/Users/frankma/dev/db/fedomn/spark-knowledge/hadoop/hadoop-3.3.4/bin/hadoop classpath)
	./bin/schematool -dbType derby -initSchema
	./bin/hive --service metastore

