from pyspark.sql.types import StructType
from pyspark.sql.types import StructField


class TPCHSchema(object):

    def getCustomerSchema(self):
        customerSchema = StructType([
            StructField('c_custkey', StringType(), True),
            StructField('c_name', StringType(), True),
            StructField('c_address', StringType(), True),
            StructField('c_nationkey', StringType(), True),
            StructField('c_phone', StringType(), True),
            StructField('c_acctbal', StringType(), True),
            StructField('c_mktsegment', StringType(), True),
            StructField('c_comment', StringType(), True)])
        return customerSchema

    def getLineitemSchema(self):
        lineitemSchema = StructType([
            StructField('l_orderkey', LongType(), True),
            StructField('l_partkey', LongType(), True),
            StructField('l_suppkey', LongType(), True),
            StructField('l_linenumber', LongType(), True),
            StructField('l_quantity', DoubleType(), True),
            StructField('l_extendedprice', DoubleType(), True),
            StructField('l_discount', DoubleType(), True),
            StructField('l_tax', DoubleType(), True),
            StructField('l_returnflag', StringType(), True),
            StructField('l_linestatus', StringType(), True),
            StructField('l_shipdate', StringType(), True),
            StructField('l_commitdate', StringType(), True),
            StructField('l_receiptdate', StringType(), True),
            StructField('l_shipinstruct', StringType(), True),
            StructField('l_shipmode', StringType(), True),
            StructField('l_comment', StringType(), True)])
    # check if we need to use self.lineitemSchema and refactor the code using coding style guide
    # for python
    return lineitemSchema

    def getNationSchema(self):
        nationSchema = StructType([
            StructField('n_nationkey', LongType(), True),
            StructField('n_name', StringType(), True),
            StructField('n_regionkey', LongType(), True),
            StructField('n_comment', StringType(), True)])
        return nationSchema

    def getOrderSchema(self):
        orderSchema = StructType([
            StructField('o_orderkey', LongType(), True),
            StructField('o_custkey', LongType(), True),
            StructField('o_orderstatus', StringType(), True),
            StructField('o_totalprice', DoubleType(), True),
            StructField('o_orderdate', StringType(), True),
            StructField('o_orderpriority', StringType(), True),
            StructField('o_clerk', StringType(), True),
            StructField('o_shippriority', LongType(), True),
            StructField('o_comment', StringType(), True)])
        return orderSchema

    def getPartSchema(self):
        partSchema = StructType([
            StructField('p_partkey', LongType(), True),
            StructField('p_name', StringType(), True),
            StructField('p_mfgr', StringType(), True),
            StructField('p_brand', StringType(), True),
            StructField('p_type', StringType(), True),
            StructField('p_size', LongType(), True),
            StructField('p_container', StringType(), True),
            StructField('p_retailprice', DoubleType(), True),
            StructField('p_comment', StringType(), True)])
        return partSchema

    def getPartsuppSchema(self):
        partsuppSchema = StructType([
            StructField('ps_partkey', LongType(), True),
            StructField('ps_suppkey', LongType(), True),
            StructField('ps_availqty', LongType(), True),
            StructField('ps_supplycost', DoubleType(), True),
            StructField('ps_comment', StringType(), True)])
        return partsuppSchema

    def getRegionSchema(self):
        regionSchema = StructType([
            StructField('r_regionkey', LongType(), True),
            StructField('r_name', StringType(), True),
            StructField('r_comment', StringType(), True)])
        return regionSchema

    def getSupplierSchema(self):
        supplierSchema = StructType([
            StructField('s_suppkey', LongType(), True),
            StructField('s_name', StringType(), True),
            StructField('s_address', StringType(), True),
            StructField('s_nationkey', LongType(), True),
            StructField('s_phone', StringType(), True),
            StructField('s_acctbal', DoubleType(), True),
            StructField('s_comment', StringType(), True)])
        return supplierSchema
