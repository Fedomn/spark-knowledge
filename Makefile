scala-doc:
	open https://docs.scala-lang.org/getting-started/index.html
	open https://docs.scala-lang.org/tour/tour-of-scala.html
	open https://docs.scala-lang.org/cheatsheets/index.html

spark-doc:
	open https://spark.apache.org/docs/latest/sql-programming-guide.html
	open https://spark.apache.org/docs/latest/submitting-applications.html#master-urls

link-data:
	git clone https://github.com/databricks/Spark-The-Definitive-Guide.git
	ln -s "$(PWD)/Spark-The-Definitive-Guide/data" "$(PWD)/src/main/resources/data"