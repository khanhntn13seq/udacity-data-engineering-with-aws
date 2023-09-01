import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1693582592283 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1693582592283",
)

# Script generated for node Join Customer Trusted
JoinCustomerTrusted_node1693582624409 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1693582592283,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerTrusted_node1693582624409",
)

# Script generated for node Drop Fields
DropFields_node1693583186920 = DropFields.apply(
    frame=JoinCustomerTrusted_node1693582624409,
    paths=["email", "phone"],
    transformation_ctx="DropFields_node1693583186920",
)

# Script generated for node Accelerometer Trusted Node
AccelerometerTrustedNode_node3 = glueContext.getSink(
    path="s3://khanhntn-udc-prj3-stedi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrustedNode_node3",
)
AccelerometerTrustedNode_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrustedNode_node3.setFormat("json")
AccelerometerTrustedNode_node3.writeFrame(DropFields_node1693583186920)
job.commit()
