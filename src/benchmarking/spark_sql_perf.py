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

# This should be in the config file as well
CUSTOMER_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/customer/'
LINEITEM_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/lineitem/'
NATION_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/nation/'
ORDERS_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/orders/'
PART_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/part/'
PARTSUPP_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/partsupp/'
REGION_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/region/'
SUPPLIER_DATASET_PARQUET_URL = 's3a://optmark-processed/tpch/block/10/supplier/'

# This should be read from a separate file as well (e.g: <benchmarking_queries.sql>)
SQLQuery = 'SELECT \
            orders.o_orderkey, SUM(l_tax) as tax \
            FROM orders \
            LEFT JOIN lineitem \
            ON orders.o_orderkey = lineitem.l_orderkey \
            WHERE l_discount = 0 \
            GROUP BY orders.o_orderkey \
            ORDER BY o_orderkey DESC \
            LIMIT 10'


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

    # Refactor the below repititve code to write something like for each databaseTable in databaseTables
    # tables = [CUSTOMER_DATASET_PARQUET_URL, LINEITEM_DATASET_PARQUET_URL...]
    # dataframes = [customerDF, lineitemDF, ]
    customerDF = getDF(spark, CUSTOMER_DATASET_PARQUET_URL)
    lineitemDF = getDF(spark, LINEITEM_DATASET_PARQUET_URL)
    nationDF = getDF(spark, NATION_DATASET_PARQUET_URL)
    ordersDF = getDF(spark, ORDERS_DATASET_PARQUET_URL)
    partDF = getDF(spark, PART_DATASET_PARQUET_URL)
    partsuppDF = getDF(spark, PARTSUPP_DATASET_PARQUET_URL)
    regionDF = getDF(spark, REGION_DATASET_PARQUET_URL)
    supplierDF = getDF(spark, SUPPLIER_DATASET_PARQUET_URL)

    # register all the tables here in order to make them accessible
    customerDF.registerTempTable('customer')
    lineitemDF.registerTempTable('lineitem')
    nationDF.registerTempTable('nation')
    ordersDF.registerTempTable('orders')
    partDF.registerTempTable('part')
    partsuppDF.registerTempTable('partsupp')
    regionDF.registerTempTable('region')
    supplierDF.registerTempTable('supplier')

    top10CustomersDF = sqlContext.sql(SQLQuery)
    lazyStartTime = time.time()
    # This is where the SQLQuery will lazily evaluated
    top10CustomersDF.show()
    lazyEndTime = time.time()
    lazyExecutionTime = lazyEndTime - lazyStartTime

    print(f'########## The query executed in {lazyExecutionTime} ##########')


def getDF(spark, parquetDataTablePathInS3):
    df = spark.read.parquet(customerDataPathS3)
    count = df.count()
    print(f'************Total row count for the df is {count}****************')
    return df


if __name__ == "__main__":
    main()
