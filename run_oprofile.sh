export SPARK_HOME=~/spark-2.0.1-bin-hadoop2.7
rm -rf oprofile_data
operf -s -e CYCLES:1000000 &
OPPID=$!

$SPARK_HOME/bin/spark-submit 			\
	--master local 				\
	--conf spark.driver.memory=2g 		\
	--class org.madhu.App              	\
	--driver-library-path=/usr/lib 		\
	--driver-java-options="-agentpath:/home/madhu/oprofile_install/lib/oprofile/libjvmti_oprofile.so" \
	~/sparkSampleApp/target/parquet-0.99.jar write snappy 2048 file:///home/madhu/sparkSampleApp/snappy
	#~/sparkSampleApp/target/parquet-0.99.jar read file:///home/madhu/sparkSampleApp/snappy

/bin/kill -SIGINT $OPPID


opreport > out-report
opreport --symbols > out-report--symbols
opannotate -a >out-annotate--assembly
