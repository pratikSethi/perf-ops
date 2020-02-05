import os
import configparser

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

import schema


# TODO: All the configs can be placed in config.yaml file
# AWS Config params
AWS_CREDENTIALS_PATH = '~/.aws/credentials'
AWS_PROFILE = 'default'
AWS_ACCESS_KEY_ID = 'aws_access_key_id'
AWS_SECRET_ACCESS_KEY = 'aws_secret_access_key'

# Spark Config params
SPARK_EXECUTOR_MEMORY_KEY = 'spark.executor.memory'
SPARK_EXECUTOR_MEMORY_VALUE = '5g'
SPARK_DRIVER_MEMORY_KEY = 'spark.driver.memory'
SPARK_DRIVER_MEMORY_VALUE = '5g'

# Hadoop FS Config params
FS_S3_IMPL = 'fs.s3n.impl'
FS_S3_IMPL_CLASSNAME = 'org.apache.hadoop.fs.s3native.NativeS3FileSystem'
FS_S3_AWS_ACCESS_KEY_ID_KEY = 'fs.s3n.awsAccessKeyId'
FS_S3_AWS_SECRET_ACCESS_KEY_KEY = 'fs.s3n.awsSecretAccessKey'

# Sample Unprocessed datasets
SMALL_SAMPLE_UNPROCESSED_DATASET_ROOT_URL = 's3a://sample-unprocessed/tpch/block/1'
# S3 Locations 10 GB TPCH Dataset
MEDIUM_SAMPLE_UNPROCESSED_DATASET_ROOT_URL = 's3a://sample-unprocessed/tpch/block/10'
# S3 Locations 100 GB TPCH Dataset

# Actual Datasets
# S3 Locations 10 GB TPCH Dataset
SMALL_SAMPLE_UNPROCESSED_DATASET_ROOT_URL = 's3a: // optmark-unprocessed/tpch/block/1/*'
# S3 Locations 10 GB TPCH Dataset
MEDIUM_SAMPLE_UNPROCESSED_DATASET_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/10/*'
# S3 Location 100 GB TPCH Dataset
LARGE_DATASET_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/*'
# S3 Locations 1000 GB TPCH Dataset
XTRA_LARGE_DATASET_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/1000/*'

SAMLPE_UNPROCESSED_BORDER_DATA_URL = 's3a://optmark-sample-data/border-crossing.csv'
SAMPLE_PROCESSED_BORDER_DATA_URL = 's3a://sample-processed/'

PIPE_DELIMITER = '|'

# App Config
APP_NAME = 'spark-etl'


def main():
    # get aws credentials for accessing S3
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(AWS_CREDENTIALS_PATH))
    access_id = config.get(AWS_PROFILE, AWS_ACCESS_KEY_ID)
    access_key = config.get(AWS_PROFILE, AWS_SECRET_ACCESS_KEY)

    # initialize spark session
    spark = SparkSession.builder.appName(APP_NAME) \
        .config(SPARK_EXECUTOR_MEMORY_KEY, SPARK_EXECUTOR_MEMORY_VALUE) \
        .config(SPARK_DRIVER_MEMORY_KEY, SPARK_DRIVER_MEMORY_VALUE) \
        .getOrCreate()
    sc = spark.sparkContext

    # hadoop configs for accessing S3
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set(FS_S3_IMPL, FS_S3_IMPL_CLASSNAME)
    hadoop_conf.set(FS_S3_AWS_ACCESS_KEY_ID_KEY, AWS_ACCESS_KEY_ID)
    hadoop_conf.set(FS_S3_AWS_SECRET_ACCESS_KEY_KEY, AWS_SECRET_ACCESS_KEY)

    sqlContext = SQLContext(sc)

    customerSchema = schema.TPCHSchema().getCustomerSchema()
    lineitemSchema = schema.TPCHSchema().getLineitemSchema()
    nationSchema = schema.TPCHSchema().getNationSchema()
    orderSchema = schema.TPCHSchema().getOrderSchema()
    partSchema = schema.TPCHSchema().getPartSchema()
    partsuppSchema = schema.TPCHSchema().getPartsuppSchema()
    regionSchema = schema.TPCHSchema().getRegionSchema()
    supplierSchema = schema.TPCHSchema().getSupplierSchema()

    sampleBorderSchema = schema.TPCHSchema().getSampleBorderSchema()

    df = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=',') \
        .load(SAMLPE_UNPROCESSED_BORDER_DATA_URL, schema=sampleBorderSchema)

    df.write.parquet(SAMPLE_PROCESSED_BORDER_DATA_URL)


if __name__ == '__main__':
    main()
