from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
import argparse
import os

# check java home
# require java 11 or higher
print(os.environ.get('JAVA_HOME'))
# initialization Spark Session with YARN
spark = SparkSession.builder \
    .appName("Sentiment Analysis Realtime Prediction") \
    .master("yarn") \
    .getOrCreate()
# load model
model_name = "03042025_131813"
parser = argparse.ArgumentParser()
parser.add_argument('--model-name', type=str, required=False, help='Name of model to load')
args = parser.parse_args()
if args.model_name is not None and args.model_name != "":
    model_name = args.model_name
print(f"Model name: {model_name}")
loaded_model = PipelineModel.load(f"/study/sentiment/model/{model_name}/")
# load data
kafka_server = "10.10.101.13:9092"
# Ream stream from Kafka with topic sentiment_data
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "sentiment_data") \
    .option("startingOffsets", "latest") \
    .load()
# Data frame
df = lines.selectExpr("CAST(value AS STRING) as text")
# test stream
# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
# predict stream data
predictions = loaded_model.transform(df)
# select columns to send to Kafka
predictions_to_kafka = predictions.selectExpr(
    "CAST(text AS STRING) as key",
    "CAST(prediction AS STRING) as value"
)
# # test predictions
# predictions_to_kafka.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
# Start running the query that prints the running counts to the console
query = predictions_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.10.101.13:9092") \
    .option("topic", "sentiment_predictions") \
    .option("checkpointLocation", "/home/hdfs/kafka/kafka_checkpoint/") \
    .outputMode("append") \
    .start()
# wait for the termination of the query
query.awaitTermination()
# close spark session
spark.stop()
