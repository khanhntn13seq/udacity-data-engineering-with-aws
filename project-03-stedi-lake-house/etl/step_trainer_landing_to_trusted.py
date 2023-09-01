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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://khanhntn-udc-prj3-stedi/step-trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1693590541702 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1693590541702",
)

# Script generated for node Join Customer Curated
JoinCustomerCurated_node1693590579759 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1693590541702,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="JoinCustomerCurated_node1693590579759",
)

# Script generated for node Drop Fields
DropFields_node1693590714366 = DropFields.apply(
    frame=JoinCustomerCurated_node1693590579759,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1693590714366",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.getSink(
    path="s3://khanhntn-udc-prj3-stedi/step-trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node3",
)
StepTrainerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node3.setFormat("json")
StepTrainerTrusted_node3.writeFrame(DropFields_node1693590714366)
job.commit()
