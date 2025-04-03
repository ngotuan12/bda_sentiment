from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from datetime import datetime

# Khởi tạo Spark Session với YARN
spark = SparkSession.builder \
    .appName("IMDb Sentiment Analysis") \
    .master("yarn") \
    .getOrCreate()

# Đọc dữ liệu từ HDFS theo đường dẫn trong ảnh
train_data = spark.read.parquet("hdfs:///study/sentiment/data/sentiment_train_data.parquet")
test_data = spark.read.parquet("hdfs:///study/sentiment/data/sentiment_test_data.parquet")

# Kiểm tra schema để xác định tên cột chính xác
print("Train data schema:")
train_data.printSchema()

# Chuẩn bị dữ liệu - đảm bảo tên cột phù hợp
# Giả định rằng cột văn bản là "text" và cột nhãn là "label"
# Có thể cần điều chỉnh tùy thuộc vào schema thực tế

# Tiền xử lý và xây dựng pipeline
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)

pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

# Huấn luyện model
print("Bắt đầu huấn luyện model...")
start_time = datetime.now()
model = pipeline.fit(train_data)
training_time = (datetime.now() - start_time).total_seconds()
print(f"Thời gian huấn luyện: {training_time:.2f} giây")

# Biến đổi dữ liệu kiểm thử
transformed_test_data = model.transform(test_data)

# Tính AUC-ROC
evaluator_auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName='areaUnderROC')
auc_roc = evaluator_auc.evaluate(transformed_test_data)

# Tính accuracy
evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName='accuracy')
accuracy = evaluator_acc.evaluate(transformed_test_data)

# Tính Precision
evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName='weightedPrecision')
precision = evaluator_precision.evaluate(transformed_test_data)

# Tính F1-score
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName='f1')
f1_score = evaluator_f1.evaluate(transformed_test_data)

# In các chỉ số ra màn hình
print(f"AUC-ROC: {auc_roc:.4f}")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"F1-score: {f1_score:.4f}")

# Tạo timestamp với định dạng ddMMyyyy_hh24miss
current_time = datetime.now().strftime("%d%m%Y_%H%M%S")
model_save_path = f"/study/sentiment/model/{current_time}/"

# Lưu model với đường dẫn mới
model.write().overwrite().save(model_save_path)
print(f"Model đã được lưu tại: {model_save_path}")

# Lưu metrics vào HDFS
metrics_data = [
    (current_time, "AUC-ROC", auc_roc),
    (current_time, "Accuracy", accuracy),
    (current_time, "Precision", precision),
    (current_time, "F1-score", f1_score),
    (current_time, "Training Time (s)", training_time)
]

metrics_df = spark.createDataFrame(metrics_data, ["timestamp", "metric", "value"])
metrics_df.write.mode("append").parquet(f"{model_save_path}/metrics.parquet")
print("Các metrics đã được lưu thành công.")