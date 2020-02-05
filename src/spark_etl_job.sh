#!/bin/bash
/usr/local/spark/bin/spark-submit --master spark://10.0.0.6:7077 --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2 /home/ubuntu/perf-ops/src/spark_etl.py
