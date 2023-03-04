[FAQ](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113709185#FAQ-HowdoImodelthedatastoredinHudi)

## How do I model the data stored in Hudi

When writing data into Hudi, you model the records like how you would on a key-value store - specify a key field (unique for a single partition/across dataset),
a partition field (denotes partition to place key into) and preCombine/combine logic that specifies how to handle duplicates in a batch of records written

## How does Hudi actually store data inside a dataset

At a high level, Hudi is based on MVCC design that writes data to versioned parquet/base files and log files that contain changes to the base file.
All the files are stored under a partitioning scheme for the dataset, which closely resembles how Apache Hive tables are laid out on DFS. Please refer here for more details.
https://hudi.apache.org/docs/concepts/
