import findspark
from pyspark.sql.types import IntegerType

findspark.init()
import pyspark
from pyspark.sql import SparkSession, functions, Window
from pyspark import SparkContext, SparkConf
# from pyspark.sql import functions
# from pyspark.sql.functions import *
from pyspark.sql.functions import col, asc, desc, row_number, regexp_replace  # pip install pyspark-stubs
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

# spark configuration
conf = SparkConf().set('spark.executor.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true').set(
    'spark.driver.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true').setAppName('pyspark_aws').setMaster(
    'local')

sc = SparkContext(conf=conf)
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

print('modules imported')

accessKeyId = 'UKfh8vjv0lYxTrBn'
secretAccessKey = '23T1g5flwWbj9hrLmS6W7PBzjZTXfyxt'
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('fs.s3a.endpoint', 'http://172.18.60.9:9007')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
spark = SparkSession(sc)

s3_df_cust = spark.read.csv('s3a://nasirianfarbucket/Question1/customer.csv/', header=False, inferSchema=True)
s3_df_order_info = spark.read.csv('s3a://nasirianfarbucket/Question1/order_info.csv/', header=False, inferSchema=True)
s3_df_orderline = spark.read.csv('s3a://nasirianfarbucket/Question1/orderline.csv/', header=False, inferSchema=True)
s3_df_product = spark.read.csv('s3a://nasirianfarbucket/Question1/product.csv/', header=False, inferSchema=True)

# cust
df_cust = s3_df_cust.selectExpr(
    '_c0 AS CUST_ID',
    '_c1 AS CUST_NAME',
    '_c2 AS CUST_ADDRESS',
    '_c3 AS CUST_CITY',
    '_c4 AS CUST_PROCS_TIME',
)

# order_info
df_order = s3_df_order_info.selectExpr(
    '_c0 AS ORDER_ID',
    '_c1 AS CUST_ID',
    '_c2 AS ORDER_PROCS_TIME',
)
df_order.show()
print(df_order.printSchema)

# orderline
df_orderline = s3_df_orderline.selectExpr(
    '_c0 AS ORDERLINE_ID',
    '_c1 AS ORDER_ID',
    '_c2 AS PRODUCT_ID',
    '_c3 AS NA_NA',
    '_c4 AS PAYMENT_AMOUNT',
)
df_orderline.show()
print(df_orderline.printSchema)

# product
df_product = s3_df_product.selectExpr(
    '_c0 AS PRODUCT_ID',
    '_c1 AS PRODUCT_NAME',
    '_c2 AS COUNT',
    '_c3 AS N_A',
    '_c4 AS PRODUCT_PROCS_TIME',
)
df_product.show()
# print(df_product.printSchema)

print("*" * 50 + "TRANSFORM phase" + "*" * 50)
print("*" * 50 + "TRANSFORM phase" + "*" * 50)
print("*" * 50 + "TRANSFORM phase" + "*" * 50)

# window = Window.partitionBy(df_cust["CUST_ID"]).orderBy(df_cust["CUST_PROCS_TIME"])

# PREPARING customer
df111 = df_cust.withColumn("CUST_ID", functions.regexp_replace(df_cust["CUST_ID"], "CUST-", ""))
df1222 = df111.withColumn('CUST_ID', functions.regexp_replace(df111["CUST_ID"], r'^[0]*', ''))

df_new = df1222.orderBy(col("CUST_PROCS_TIME").desc())
ready_cust = df_new.drop_duplicates(["CUST_ID"])

# print(ready_cust.show())

# PREPARING order info
df111 = df_order.withColumn("CUST_ID", functions.regexp_replace(df_order["CUST_ID"], "CUST-", ""))
ready_orderinfo = df111.withColumn('CUST_ID', functions.regexp_replace(df111["CUST_ID"], r'^[0]*', ''))

# print(ready_orderinfo.show())

# PREPARING order line

# JOIN order_info with order_line
df_order_and_orderline = ready_orderinfo.join(df_orderline, df_orderline['ORDER_ID'] == ready_orderinfo['ORDER_ID']) \
    .select([ready_orderinfo['CUST_ID'], ready_orderinfo['ORDER_ID'], df_orderline['PRODUCT_ID'],
             df_orderline['PAYMENT_AMOUNT'], ready_orderinfo['ORDER_PROCS_TIME']])

# print(df_order_and_orderline.show())

# join with customer

df_cus_and_result1 = df_order_and_orderline.join(ready_cust,
                                                 ready_cust['CUST_ID'] == df_order_and_orderline['CUST_ID']).select(
    ready_cust['CUST_ID'], ready_cust['CUST_NAME'], ready_cust['CUST_CITY'], df_order_and_orderline['ORDER_ID'],
    df_order_and_orderline['PRODUCT_ID'], df_order_and_orderline['PAYMENT_AMOUNT'],
    df_order_and_orderline['ORDER_PROCS_TIME']).orderBy(ready_cust['CUST_ID'])

print(df_cus_and_result1.show())

# join result1 with product
