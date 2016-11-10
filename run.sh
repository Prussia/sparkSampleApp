export SPARK_HOME=~/spark-2.0.1-bin-hadoop2.7
~/spark-2.0.1-bin-hadoop2.7/bin/spark-submit 		\
		--master local 				\
		--conf spark.driver.memory=2g 		\
		--class org.madhu.App              	\
		--driver-library-path=/usr/lib 		\
		~/sparkSampleApp/target/parquet-0.99.jar write snappy 2048 file:///home/madhu/cust-snappy
