export SPARK_HOME=/usr/local/Cellar/apache-spark/2.2.0
rm -rf oprofile_data
operf -s -e CYCLES:1000000 &
OPPID=$!

$SPARK_HOME/bin/spark-submit 			\
	--master local 				\
	--conf spark.driver.memory=10g 		\
        --conf spark.sql.warehouse.dir=file:///tmp \
        --conf spark.io.compression.codec=snappy \
	--class org.madhu.App              	\
	--driver-library-path=/usr/lib 		\
	--driver-java-options="-agentpath:/home/madhu/oprofile_install/lib/oprofile/libjvmti_oprofile.so" \
	./target/parquet-0.99.jar read file:///home/madhu/sparkSampleApp/snappy

	#~/sparkSampleApp/target/parquet-0.99.jar write snappy 2048 file:///home/madhu/sparkSampleApp/snappy
/bin/kill -SIGINT $OPPID


opreport > out-report
opreport --symbols > out-report--symbols
#opannotate -a >out-annotate--assembly
