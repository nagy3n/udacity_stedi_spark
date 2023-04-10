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

# Script generated for node S3 step trainer trusted
S3steptrainertrusted_node1681123000527 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nagy-stedi-lakehouse/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3steptrainertrusted_node1681123000527",
)

# Script generated for node S3 accelerometer trusted
S3accelerometertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nagy-stedi-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3accelerometertrusted_node1",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3accelerometertrusted_node1,
    frame2=S3steptrainertrusted_node1681123000527,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1681123119897 = DropFields.apply(
    frame=Join_node2, paths=[], transformation_ctx="DropFields_node1681123119897"
)

# Script generated for node S3 machine learning curated
S3machinelearningcurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1681123119897,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nagy-stedi-lakehouse/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3machinelearningcurated_node3",
)

job.commit()
