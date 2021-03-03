from pyspark import SparkContext, SparkConf
from awsglue.context import GlueContext
try:
    sc=SparkContext().getOrCreate()
except: 
    pass
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sql('show tables in stage_fmtd_eda_all').show(10)