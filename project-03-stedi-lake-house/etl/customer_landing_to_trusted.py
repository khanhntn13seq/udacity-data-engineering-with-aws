import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Share With Research
ShareWithResearch_node1693580565579 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node1693580565579",
)

# Script generated for node Customer Trusted
CustomerTrusted_node3 = glueContext.getSink(
    path="s3://khanhntn-udc-prj3-stedi/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node3",
)
CustomerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="cusomer_trusted"
)
CustomerTrusted_node3.setFormat("json")
CustomerTrusted_node3.writeFrame(ShareWithResearch_node1693580565579)
job.commit()
