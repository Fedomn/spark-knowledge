from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("iceberg") \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type', 'hive') \
        .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.local.type', 'hadoop') \
        .config('spark.sql.catalog.local.warehouse', './iceberg-warehouse') \
        .getOrCreate()

    spark.sql("CREATE TABLE local.db.test2 (id bigint, data string, type string) USING iceberg partitioned by (type);")
    spark.sql("INSERT INTO local.db.test2 VALUES (1, 'a', 't1'), (2, 'b', 't1'), (3, 'c', 't2');")
    spark.sql("SELECT count(1) as count, type FROM local.db.test2 GROUP BY type;").show()
