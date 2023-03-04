from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("hudi") \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
        .getOrCreate()

    tableName = "hudi_trips_cow"
    basePath = "file:///tmp/hudi_trips_cow"
    # https://github.com/apache/hudi/blob/master/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java
    dataGen = spark._jvm.org.apache.hudi.QuickstartUtils.DataGenerator()

    inserts = spark._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
    print(inserts)
    df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.printSchema()

    hudi_options = {
        'hoodie.table.name': tableName,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.datasource.write.table.name': tableName,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    # write_options: https://hudi.apache.org/docs/writing_data/

    # hudi的各种FAQ，对于设计原则理解很有帮助
    # https://cwiki.apache.org/confluence/display/HUDI/Design+And+Architecture#DesignAndArchitecture-DesignPrinciples
    # https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113709185#FAQ-HowdoImodelthedatastoredinHudi
    df.write.format("hudi"). \
        options(**hudi_options). \
        mode("overwrite"). \
        save(basePath)

    tripsSnapshotDF = spark.read.format("hudi").load(basePath)
    tripsSnapshotDF.printSchema()
