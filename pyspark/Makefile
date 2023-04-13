clean:
	rm -rf ./hive-warehouse
	rm -rf ./iceberg-warehouse
	rm -rf ./metastore_db

# --conf spark.sql.catalog.spark_catalog.uri=thrift://127.0.0.1:9083

iceberg-shell:
	spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.1 \
        --conf spark.sql.warehouse.dir=./hive-warehouse \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hive \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hadoop \
        --conf spark.sql.catalog.iceberg.warehouse=./iceberg-warehouse \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
