/usr/local/Cellar/apache-spark/2.2.0/bin/spark-submit 		\
		--master local 				\
		--conf spark.driver.memory=2g 		\
        --conf spark.sql.warehouse.dir=file:///tmp \
		--class org.madhu.parquetRW              	\
		--driver-library-path=/usr/lib 		\
        ./target/parquet-0.99.jar read file:////Users/prussia/workspaces/data/prussia_order.parquet


    #~/sparkSampleApp/target/parquet-0.99.jar write snappy 2048 file:///home/madhu/sparkSampleApp/snappy
	#/home/madhu/sparkSampleApp/target/parquet-0.99.jar read file:///home/madhu/sparkSampleApp/snappy
