import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Aaccelerometer Trusted
AaccelerometerTrusted_node1693589179570 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AaccelerometerTrusted_node1693589179570",
)

# Script generated for node Drop Duplicates User
DropDuplicatesUser_node1693589243104 = DynamicFrame.fromDF(
    AaccelerometerTrusted_node1693589179570.toDF().dropDuplicates(["user"]),
    glueContext,
    "DropDuplicatesUser_node1693589243104",
)

# Script generated for node Join
Join_node1693589327119 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=DropDuplicatesUser_node1693589243104,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693589327119",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.getSink(
    path="s3://khanhntn-udc-prj3-stedi/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node3",
)
CustomerCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node3.setFormat("json")
CustomerCurated_node3.writeFrame(Join_node1693589327119)
job.commit()
