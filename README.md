[![buildstatus](https://travis-ci.org/holdenk/learning-spark-examples.svg?branch=master)](https://travis-ci.org/holdenk/learning-spark-examples)
Examples for Learning Spark
===============
Examples for the Learning Spark book. These examples require a number of libraries and as such have long build files. We have also added a stand alone example with minimal dependencies and a small build file
in the mini-complete-example directory.


These examples have been updated to run against Spark 1.3 so they may
be slightly different than the versions in your copy of "Learning Spark".

Requirements
==
* JDK 1.7 or higher
* Scala 2.10.3
- scala-lang.org
* Spark 1.3
* Protobuf compiler
- On debian you can install with sudo apt-get install protobuf-compiler
* R & the CRAN package Imap are required for the ChapterSixExample
* The Python examples require urllib3

Python examples
===

From spark just run ./bin/pyspark ./src/python/[example]

Spark Submit
===

You can also create an assembly jar with all of the dependencies for running either the java or scala
versions of the code and run the job with the spark-submit script

./sbt/sbt assembly OR mvn package
cd $SPARK_HOME; ./bin/spark-submit   --class com.oreilly.learningsparkexamples.[lang].[example] ../learning-spark-examples/target/scala-2.10/learning-spark-examples-assembly-0.0.1.jar

[![Learning Spark](http://akamaicovers.oreilly.com/images/0636920028512/cat.gif)](http://www.jdoqocy.com/click-7645222-11260198?url=http%3A%2F%2Fshop.oreilly.com%2Fproduct%2F0636920028512.do%3Fcmp%3Daf-strata-books-videos-product_cj_9781449358600_%2525zp&cjsku=0636920028512)



spark-submit --class com.oreilly.learningsparkexamples.mini.java.BasicMap --master spark://nn1.hadoop.com:7077 /home/hadoop/learning-spark-0.0.2-jar-with-dependencies.jar


spark-submit --class com.oreilly.learningsparkexamples.java.LoadHive --master local --executor-memory 1G --total-executor-cores 1  /var/lib/hadoop-hdfs/learning-spark-0.0.2-jar-with-dependencies.jar local tld 1
--total-executor-cores 400 --driver-memory 256MB
 --executor-memory 1GB --num-executors 1
spark-submit --class com.oreilly.learningsparkexamples.java.LoadHive --master spark://172.31.54.156:7077 --executor-memory 5G --total-executor-cores 1 /home/hadoop/learning-spark-0.0.2-jar-with-dependencies.jar spark://172.31.54.156:7077 tld 1
spark-submit --class org.apache.spark.mllib.rank.SVMRankRunner --master spark://172.31.54.156:7077 --executor-memory 10G --total-executor-cores 6 /home/hadoop/muc/test/spark-rank.jar hdfs://yoho/data/yoho/recom_yohobuy/svm_rank/all_tr.txt 86