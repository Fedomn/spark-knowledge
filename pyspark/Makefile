clean:
	rm -rf ./hive-warehouse
	rm -rf ./iceberg-warehouse
	rm -rf ./metastore_db
	rm -f ./derby.log
	rm -rf ./checkpoint

# show iceberg medata
avro-tool:
	wget https://dlcdn.apache.org/avro/avro-1.11.1/java/avro-tools-1.11.1.jar
	#java -jar avro-tools-1.11.1.jar tojson xx-m0.avro > manifest.json
	#java -jar avro-tools-1.11.1.jar tojson snap-xx.avro > snap-manifest-list.json

show-parquet:
	pip install parquet-tools
	#parquet-tools show xxx.parquet

submit-yarn:
	spark-submit --master yarn --deploy-mode cluster --name "xxx" --driver-memory 10g --executor-memory 10g --executor-cores 3 xxx.py

enable-hive-catalog:
	cp ./hive-site.xml ~/.pyenv/versions/3.8.10/lib/python3.8/site-packages/pyspark/conf/

remove-hive-catalog:
	rm ~/.pyenv/versions/3.8.10/lib/python3.8/site-packages/pyspark/conf/hive-site.xml