DIR := $(CURDIR)

hadoop-bin:
	# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html#Hadoop_Rack_Awareness
	wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
	tar -zxvf ./hadoop-3.3.4.tar.gz

HADOOP_HOME=${DIR}/hadoop-3.3.4
hdfs-init: hdfs-clean
	mkdir -p Users/frankma/dev/db/fedomn/spark-knowledge/hadoop/hdfs/data
	mkdir -p Users/frankma/dev/db/fedomn/spark-knowledge/hadoop/hdfs/name
	cp ./core-site.xml ./hadoop-3.3.4/etc/hadoop/core-site.xml
	cp ./hdfs-site.xml ./hadoop-3.3.4/etc/hadoop/hdfs-site.xml
	${HADOOP_HOME}/bin/hdfs namenode -format

hdfs-clean:
	rm -rf Users/frankma/dev/db/fedomn/spark-knowledge/hadoop/hdfs/data
	rm -rf Users/frankma/dev/db/fedomn/spark-knowledge/hadoop/hdfs/name

hdfs-start: hdfs-stop
	${HADOOP_HOME}/bin/hdfs --daemon start namenode
	${HADOOP_HOME}/bin/hdfs --daemon start datanode

hdfs-stop:
	${HADOOP_HOME}/bin/hdfs --daemon stop namenode
	${HADOOP_HOME}/bin/hdfs --daemon stop datanode

hdfs-shell:
	${HADOOP_HOME}/bin/hdfs dfs -ls -R /iceberg-warehouse | \
	awk '{print $8}' | \
	sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

hdfs-open:
	open http://localhost:9870

# ------------------------------------------- hive -------------------------------------------
# on macos download https://dbngin.com/ for hive external rdbms
# start postgres and create hms database
hive-bin:
	# https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+3.0+Administration
	wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
	tar -zxvf apache-hive-3.1.3-bin.tar.gz

HIVE_HOME=${DIR}/apache-hive-3.1.3-bin
hive-init:
	cp ./hive-site.xml ${HIVE_HOME}/conf/hive-site.xml

hms-initSchema:
	export HADOOP_HOME=${HADOOP_HOME} && \
	export HADOOP_CLASSPATH=$$(${HADOOP_HOME}/bin/hadoop classpath) && \
	${HIVE_HOME}/bin/schematool -dbType postgres -initSchema

hms-start:
	export HADOOP_HOME=${HADOOP_HOME} && \
	export HADOOP_CLASSPATH=$$(${HADOOP_HOME}/bin/hadoop classpath) && \
	${HIVE_HOME}/bin/hive --service metastore

# in external shell: use java8 to start: sdk use java 8
# https://sparkbyexamples.com/apache-hive/hive-start-hiveserver2-and-beeline/
hiveserver2:
	export HADOOP_HOME=${HADOOP_HOME} && \
	export HADOOP_CLASSPATH=$$(${HADOOP_HOME}/bin/hadoop classpath) && \
	${HIVE_HOME}/bin/hiveserver2

beeline:
	export HADOOP_HOME=${HADOOP_HOME} && \
	export HADOOP_CLASSPATH=$$(${HADOOP_HOME}/bin/hadoop classpath) && \
	${HIVE_HOME}/bin/beeline -u jdbc:hive2://127.0.0.1:10000 scott tiger

hiveserver2-web:
	open http://localhost:10002/
