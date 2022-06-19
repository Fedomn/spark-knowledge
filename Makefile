scala-doc:
	open https://docs.scala-lang.org/getting-started/index.html
	open https://docs.scala-lang.org/tour/tour-of-scala.html
	open https://docs.scala-lang.org/cheatsheets/index.html

spark-doc:
	open https://spark.apache.org/docs/latest/sql-programming-guide.html
	open https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
	open https://spark.apache.org/docs/latest/configuration.html

link-data:
	git clone https://github.com/databricks/Spark-The-Definitive-Guide.git
	ln -s "$(PWD)/Spark-The-Definitive-Guide/data" "$(PWD)/src/main/resources/data"
	ln -s "$(PWD)/Spark-The-Definitive-Guide/data" "$(PWD)/pyspark/data"

spark-bin:
	wget https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
	tar -zxvf ./spark-3.2.0-bin-hadoop3.2.tgz

kafka-bin:
	wget https://dlcdn.apache.org/kafka/3.2.0/kafka-3.2.0-src.tgz
	tar -zxvf ./kafka-3.2.0-src.tgz