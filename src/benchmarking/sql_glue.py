from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

# .config("spark.sql.warehouse.dir", 's3a://sample-processed/tpch/block/1/') \

# Hadoop FS Config params
FS_S3_IMPL = 'fs.s3n.impl'
FS_S3_IMPL_CLASSNAME = 'org.apache.hadoop.fs.s3native.NativeS3FileSystem'
FS_S3_AWS_ACCESS_KEY_ID_KEY = 'fs.s3n.awsAccessKeyId'
FS_S3_AWS_SECRET_ACCESS_KEY_KEY = 'fs.s3n.awsSecretAccessKey'


def main():

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Glue integration example") \
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.sql.warehouse.dir", 's3a://sample-processed/tpch/block/1/') \
        .enableHiveSupport() \
        .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set(FS_S3_IMPL, FS_S3_IMPL_CLASSNAME)
    hadoop_conf.set(FS_S3_AWS_ACCESS_KEY_ID_KEY,
                    os.getenv('AWS_ACCESS_KEY_ID'))
    hadoop_conf.set(FS_S3_AWS_SECRET_ACCESS_KEY_KEY,
                    os.getenv('AWS_SECRET_ACCESS_KEY'))

    spark.catalog.setCurrentDatabase("s_tpch")
    # spark is an existing SparkSession
    df = spark.sql("select count(*) as custCount from customer")
    df.show()


if __name__ == "__main__":
    main()
