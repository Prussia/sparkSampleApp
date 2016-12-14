export SPARK_HOME=~/spark-2.0.1-bin-hadoop2.7
~/spark-2.0.1-bin-hadoop2.7/bin/spark-submit 		\
		--master local 				\
		--conf spark.driver.memory=10g 		\
        --conf spark.sql.warehouse.dir=file:///tmp \
		--class org.madhu.parquetRW              	\
		--driver-library-path=/usr/lib 		\
        ~/sparkSampleApp/target/parquet-0.99.jar read file:///home/madhu/sparkSampleApp/snappy


    #~/sparkSampleApp/target/parquet-0.99.jar write snappy 2048 file:///home/madhu/sparkSampleApp/snappy
	#/home/madhu/sparkSampleApp/target/parquet-0.99.jar read file:///home/madhu/sparkSampleApp/snappy
