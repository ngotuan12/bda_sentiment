#!/bin/bash

# Chạy ứng dụng Spark
spark-submit --master yarn --deploy-mode client /home/hdfs/study/bda_sentiment/src/app.py

# Lấy thư mục model mới nhất (dựa vào timestamp)
LATEST_MODEL=$(hdfs dfs -ls /study/sentiment/model | grep -v Found | sort -k6,7 -r | head -n 1 | awk '{print $8}')
echo "Thư mục model mới nhất: $LATEST_MODEL"

# Gộp các file metrics
hdfs dfs -getmerge ${LATEST_MODEL}/metrics/* /tmp/merged_metrics.csv

# Upload lại file đã gộp
hdfs dfs -put -f /tmp/merged_metrics.csv ${LATEST_MODEL}/metrics_combined.csv

echo "Đã gộp metrics thành công: ${LATEST_MODEL}/metrics_combined.csv"