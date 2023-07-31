import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dataFrame = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .load("s3://datalake-gabrielpvicente-352855538020/raw-data/MICRODADOS_ENEM_2020.csv")

dataFrame.printSchema()
dataFrame.show(10)
dataFrame.write.parquet("s3://datalake-gabrielpvicente-352855538020/consumer-zone/MICRODADOS_ENEM_2020.parquet")
job.commit()
