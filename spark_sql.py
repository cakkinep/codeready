from pyspark import SparkContext, SparkConf
from awsglue.context import GlueContext

conf = SparkConf()
conf.setAppName('testApp')
conf.set("spark.eventLog.enabled", "true")
conf.set("spark.history.fs.logDirectory","file:/tmp/spark-events")
conf.set("enableHiveSupport","true")
# sc = SparkContext(conf=conf)
sc=SparkContext(conf=conf).getOrCreate()
glueContext = GlueContext(sc)
# inputDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": ["s3://awsglue-datasets/examples/us-legislators/all/memberships.json"]}, format = "json")
# inputDF.toDF().show()
spark = glueContext.spark_session
spark.sql('show databases').show()