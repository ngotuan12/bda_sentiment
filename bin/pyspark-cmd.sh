JAVA_HOME=/home/hdfs/jdk/jdk-17.0.14/ pyspark --master yarn --jars /home/hdfs/jar/spark-sql-kafka-0-10_2.12-3.5.5.jar,/home/hdfs/kafka/kafka_2.13-4.0.0/libs/kafka-clients-4.0.0.jar
pyspark --master yarn \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.5