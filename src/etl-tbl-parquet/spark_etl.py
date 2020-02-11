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
SPARK_EXECUTOR_MEMORY_VALUE = '6g'
SPARK_DRIVER_MEMORY_KEY = 'spark.driver.memory'
SPARK_DRIVER_MEMORY_VALUE = '6g'

# Hadoop FS Config params
FS_S3_IMPL = 'fs.s3n.impl'
FS_S3_IMPL_CLASSNAME = 'org.apache.hadoop.fs.s3native.NativeS3FileSystem'
FS_S3_AWS_ACCESS_KEY_ID_KEY = 'fs.s3n.awsAccessKeyId'
FS_S3_AWS_SECRET_ACCESS_KEY_KEY = 'fs.s3n.awsSecretAccessKey'

# TODO: Maybe create a constant map SMALL_SAMPLE_UNPROCESSED_DATASET = {'Customer' : <s3-url>}
# So that we can automate the loading or maybe use some wildcards trick to automate the stuff
# TODO: generic processes tpch url

# ================ MEDIUM UNPROCESSED TABLES ================ #
MEDIUM_SAMPLE_UNPROCESSED_DATASET_CUSTOMER_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/customer.tbl'
MEDIUM_SAMPLE_UNPROCESSED_DATASET_LINEITEM_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/lineitem.tbl'
MEDIUM_SAMPLE_UNPROCESSED_DATASET_NATION_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/nation.tbl'
MEDIUM_SAMPLE_UNPROCESSED_DATASET_ORDER_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/orders.tbl'
MEDIUM_SAMPLE_UNPROCESSED_DATASET_PART_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/part.tbl'
MEDIUM_SAMPLE_UNPROCESSED_DATASET_PARTSUPP_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/partsupp.tbl'
MEDIUM_SAMPLE_UNPROCESSED_DATASET_REGION_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/region.tbl'
MEDIUM_SAMPLE_UNPROCESSED_DATASET_SUPPLIER_ROOT_URL = 's3a://optmark-unprocessed/tpch/block/100/supplier.tbl'
# ================ MEDIUM UNPROCESSED TABLES ================ #

# ================ MEDIUM PROCESSED TABLES ================ #
MEDIUM_SAMPLE_PROCESSED_DATASET_CUSTOMER_ROOT_URL = 's3a://optmark-processed/tpch/block/100/customer/'
MEDIUM_SAMPLE_PROCESSED_DATASET_LINEITEM_ROOT_URL = 's3a://optmark-processed/tpch/block/100/lineitem/'
MEDIUM_SAMPLE_PROCESSED_DATASET_NATION_ROOT_URL = 's3a://optmark-processed/tpch/block/100/nation/'
MEDIUM_SAMPLE_PROCESSED_DATASET_ORDER_ROOT_URL = 's3a://optmark-processed/tpch/block/100/orders/'
MEDIUM_SAMPLE_PROCESSED_DATASET_PART_ROOT_URL = 's3a://optmark-processed/tpch/block/100/part/'
MEDIUM_SAMPLE_PROCESSED_DATASET_PARTSUPP_ROOT_URL = 's3a://optmark-processed/tpch/block/100/partsupp/'
MEDIUM_SAMPLE_PROCESSED_DATASET_REGION_ROOT_URL = 's3a://optmark-processed/tpch/block/100/region/'
MEDIUM_SAMPLE_PROCESSED_DATASET_SUPPLIER_ROOT_URL = 's3a://optmark-processed/tpch/block/100/supplier/'
# ================ MEDIUM PROCESSED TABLES ================ #


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

    # sampleBorderSchema = schema.TPCHSchema().getSampleBorderSchema()

    # TODO: Automate this maybe using a for loop or some other profile config param
    # 10 GB Datasets Trasnformations Start =====================================================

    # Customer
    dfm_customer = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_CUSTOMER_ROOT_URL, schema=customerSchema)

    dfm_customer.write.parquet(
        MEDIUM_SAMPLE_PROCESSED_DATASET_CUSTOMER_ROOT_URL)

    # Lineitem
    dfm_lineitem = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_LINEITEM_ROOT_URL, schema=lineitemSchema)

    dfm_lineitem.write.parquet(
        MEDIUM_SAMPLE_PROCESSED_DATASET_LINEITEM_ROOT_URL)

    # Nation
    dfm_nation = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_NATION_ROOT_URL, schema=nationSchema)

    dfm_nation.write.parquet(MEDIUM_SAMPLE_PROCESSED_DATASET_NATION_ROOT_URL)

    # Order
    dfm_order = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_ORDER_ROOT_URL, schema=orderSchema)

    dfm_order.write.parquet(MEDIUM_SAMPLE_PROCESSED_DATASET_ORDER_ROOT_URL)

    # Part
    dfm_part = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_PART_ROOT_URL, schema=partSchema)

    dfm_part.write.parquet(MEDIUM_SAMPLE_PROCESSED_DATASET_PART_ROOT_URL)

    # Partsupp
    dfm_partsupp = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_PARTSUPP_ROOT_URL, schema=partsuppSchema)

    dfm_partsupp.write.parquet(
        MEDIUM_SAMPLE_PROCESSED_DATASET_PARTSUPP_ROOT_URL)

    # Region
    dfm_region = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_REGION_ROOT_URL, schema=regionSchema)

    dfm_region.write.parquet(MEDIUM_SAMPLE_PROCESSED_DATASET_REGION_ROOT_URL)

    # Supplier
    dfm_supplier = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter=PIPE_DELIMITER) \
        .load(MEDIUM_SAMPLE_UNPROCESSED_DATASET_SUPPLIER_ROOT_URL, schema=supplierSchema)

    dfm_supplier.write.parquet(
        MEDIUM_SAMPLE_PROCESSED_DATASET_SUPPLIER_ROOT_URL)

    # 10 GB Datasets Trasnformations End =====================================================


if __name__ == '__main__':
    main()
