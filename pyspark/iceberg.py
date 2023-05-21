from pyspark.sql import SparkSession


def local_run():
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


# https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
# execute `make enable-hive-catalog` first
def hive_catalog_run():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("iceberg") \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type', 'hive') \
        .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.local.type', 'hive') \
        .config('spark.sql.catalog.local.warehouse', 'hdfs:///iceberg-warehouse') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("SET hive.exec.dynamic.partition.mode=non-strict;")
    spark.sql("drop table if exists test_hive;")
    spark.sql("CREATE TABLE test_hive (id bigint, data string, type string) USING hive partitioned by (type);")
    spark.sql("INSERT INTO test_hive VALUES (1, 'a', 't1'), (2, 'b', 't1'), (3, 'c', 't2');")
    spark.sql("SELECT count(1) as count, type FROM test_hive GROUP BY type;").show()

    spark.sql("drop table if exists local.db.test_iceberg;")
    spark.sql("create database if not exists local.db;")
    spark.sql(
        "CREATE TABLE local.db.test_iceberg (id bigint, data string, type string) USING iceberg partitioned by (type);")
    spark.sql("INSERT INTO local.db.test_iceberg VALUES (1, 'a', 't1'), (2, 'b', 't1'), (3, 'c', 't2');")
    spark.sql("SELECT count(1) as count, type FROM local.db.test_iceberg GROUP BY type;").show()


def hive_catalog_insert():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("iceberg") \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type', 'hive') \
        .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.local.type', 'hive') \
        .config('spark.sql.catalog.local.warehouse', 'hdfs:///iceberg-warehouse') \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql("INSERT INTO local.db.test_iceberg VALUES (4, 'd', 't1');")


if __name__ == '__main__':
    # local_run()
    # hive_catalog_run()
    hive_catalog_insert()
