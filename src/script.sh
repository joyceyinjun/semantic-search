#!/bin/bash

N_CENTROIDS=300
KMEANS_PKL=s3a://bucket_name/kmeans_${N_CENTROIDS}.pkl

echo $KMEANS_PKL

# spark-submit $SPARK_HOST:$PORT process_ingest.py
# python3 train_kmeans.py $N_CENTROIDS $KMEANS_PKL
