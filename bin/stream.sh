JAVA_HOME=/home/hdfs/jdk/jdk-17.0.14/  spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.5 \
  src/stream.py