import os
import configparser

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

from pyspark.sql.types import *
import time

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

APP_NAME = 'spark-sql-stats'

SMALL_PARQUET_DATASET_CUSTOMER_ROOT_URL = 's3a://sample-processed/tpch/block/1/customer/*.parquet'
SMALL_SAMPLE_PROCESSED_DATASET_CUSTOMER_ROOT_URL = 's3a://sample-processed/tpch/block/1/customer/'
SMALL_SAMPLE_PROCESSED_DATASET_ORDERS_ROOT_URL = 's3a://sample-processed/tpch/block/1/orders/'
SMALL_SAMPLE_PROCESSED_DATASET_LINEITEM_ROOT_URL = 's3a://sample-processed/tpch/block/1/lineitem/'


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

    customerDF = getCustomerDF(spark, SMALL_PARQUET_DATASET_CUSTOMER_ROOT_URL)
    ordersDF = getOrdersDF(
        spark, SMALL_SAMPLE_PROCESSED_DATASET_ORDERS_ROOT_URL)
    lineitemDF = getLineitemDF(
        spark, SMALL_SAMPLE_PROCESSED_DATASET_LINEITEM_ROOT_URL)
    customerDF.registerTempTable('customer')
    ordersDF.registerTempTable('orders')
    lineitemDF.registerTempTable('lineitem')
    #SQLQuery = 'SELECT count(*) AS cnt FROM customer'
    #customerCount = sqlContext.sql(SQLQuery).first()['cnt']
    # print(f'########## customer count is {customerCount} #############')
    # customerCount.show()

    SQLQuery = 'from orders join lineitem on o_orderkey = l_orderkey \
            join customer on c_custkey = o_custkey \
            group by c_custkey, c_name \
            order by tot_qty \
            desc limit 10'
    # customerCount = sqlContext.sql(SQLQuery).first()['cnt']
    startTime = time.time()
    top10CustomersDF = sqlContext.sql(SQLQuery)
    endTime = time.time()
    queryExecutionTime = endTime - startTime
    top10CustomersDF.show()
    print(f'########## The query executed in {queryExecutionTime} ##########')


def getCustomerDF(spark, customerDataPathS3):
    customerDF = spark.read.parquet(customerDataPathS3)
    count = customerDF.count()
    print(f"************total count customer is {count}****************")
    return customerDF


def getOrdersDF(spark, ordersDataPathS3):
    ordersDF = spark.read.parquet(customerDataPathS3)
    count = ordersDF.count()
    print(f"************total count orders is {count}****************")
    return ordersDF


def getLineitemDF(spark, lineitemDataPathS3):
    lineitemDF = spark.read.parquet(customerDataPathS3)
    count = lineitemDF.count()
    print(f"************total count lineitem is {count}****************")
    return lineitemDF


if __name__ == "__main__":
    main()
