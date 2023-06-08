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
def _build_hive_catalog_spark():
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
    return spark


def hive_catalog_run():
    spark = _build_hive_catalog_spark()

    spark.sql("SET hive.exec.dynamic.partition.mode=non-strict;")
    spark.sql("drop table if exists test_hive;")
    spark.sql("CREATE TABLE test_hive (id bigint, data string, type string) USING hive partitioned by (type);")
    spark.sql("INSERT INTO test_hive VALUES (1, 'a', 't1'), (2, 'b', 't1'), (3, 'c', 't2');")
    spark.sql("SELECT count(1) as count, type FROM test_hive GROUP BY type;").show()

    spark.sql("drop table if exists local.db.test_iceberg PURGE;")
    spark.sql("create database if not exists local.db;")
    spark.sql(
        "CREATE TABLE local.db.test_iceberg (id bigint, data string, type string) USING iceberg partitioned by (type);")
    spark.sql("INSERT INTO local.db.test_iceberg VALUES (1, 'a', 't1'), (2, 'b', 't1'), (3, 'c', 't2');")
    spark.sql("SELECT count(1) as count, type FROM local.db.test_iceberg GROUP BY type;").show()


def hive_catalog_insert():
    spark = _build_hive_catalog_spark()
    spark.sql("INSERT INTO local.db.test_iceberg VALUES (4, 'd', 't1');")


def hive_catalog_delete():
    spark = _build_hive_catalog_spark()
    spark.sql("DELETE FROM local.db.test_iceberg WHERE id = 2;")


def hive_catalog_select():
    spark = _build_hive_catalog_spark()
    spark.sql("SELECT * FROM local.db.test_iceberg;").show()


def hidden_partition_test():
    """
    https://iceberg.apache.org/docs/latest/spark-ddl/#partitioned-by
    """
    spark = _build_hive_catalog_spark()
    spark.sql("drop table if exists local.db.hidden_iceberg PURGE;")
    spark.sql(
        "CREATE TABLE local.db.hidden_iceberg (id bigint, event_timestamp timestamp) "
        "USING iceberg partitioned by (months(event_timestamp));")
    spark.sql(
        "INSERT INTO local.db.hidden_iceberg VALUES "
        "(1, cast('2023-05-03 15:12:41' as timestamp)),"
        "(2, cast('2023-06-03 15:13:19' as timestamp)),"
        "(3, cast('2023-05-01 15:13:24' as timestamp));")
    spark.sql("SELECT count(1) as count from local.db.hidden_iceberg").show()


def partition_evolution():
    """
    https://iceberg.apache.org/docs/1.2.1/spark-ddl/#alter-table--add-partition-field
    """
    spark = _build_hive_catalog_spark()
    spark.sql(
        "ALTER TABLE local.db.hidden_iceberg REPLACE PARTITION FIELD event_timestamp_month with days(event_timestamp)")
    spark.sql("INSERT INTO local.db.hidden_iceberg VALUES (4, cast('2023-09-03 15:13:19' as timestamp))")
    spark.sql("INSERT INTO local.db.hidden_iceberg VALUES (5, cast('2023-05-01 15:13:19' as timestamp))")
    spark.sql("SELECT count(1) as count from local.db.hidden_iceberg").show()


def schema_evolution():
    spark = _build_hive_catalog_spark()
    spark.sql("ALTER TABLE local.db.hidden_iceberg ADD COLUMN data string")
    spark.sql("INSERT INTO local.db.hidden_iceberg VALUES (6, cast('2023-09-01 15:13:19' as timestamp), 'a')")
    spark.sql("SELECT * from local.db.hidden_iceberg").show()


if __name__ == '__main__':
    # write local file
    # local_run()

    # metadata
    # hive_catalog_run()
    # hive_catalog_insert()
    # hive_catalog_delete()
    # hive_catalog_select()

    # evolution
    # hidden_partition_test()
    # partition_evolution()
    schema_evolution()
