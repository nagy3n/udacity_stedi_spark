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

# Script generated for node S3 customer trusted
S3customertrusted_node1681123000527 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nagy-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3customertrusted_node1681123000527",
)

# Script generated for node S3 accelerometer landing
S3accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nagy-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3accelerometerlanding_node1",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3accelerometerlanding_node1,
    frame2=S3customertrusted_node1681123000527,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1681123119897 = DropFields.apply(
    frame=Join_node2,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1681123119897",
)

# Script generated for node S3 customer curated
S3customercurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1681123119897,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nagy-stedi-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3customercurated_node3",
)

job.commit()
