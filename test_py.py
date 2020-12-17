from pyspark import SparkContext, SparkConf
from awsglue.context import GlueContext

conf = SparkConf()
conf.setAppName('testApp')
conf.set("spark.eventLog.enabled", "true")
conf.set("spark.history.fs.logDirectory","file:/tmp/spark-events")
sc = SparkContext(conf=conf)
sc=SparkContext.getOrCreate()
glueContext = GlueContext(sc)
# creating input DF
inputDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": ["s3://awsglue-datasets/examples/us-legislators/all/memberships.json"]}, format = "json")
inputDF.toDF().show()