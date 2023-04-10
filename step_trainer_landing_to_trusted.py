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

# Script generated for node S3 customer curated
S3customercurated_node1681123000527 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nagy-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3customercurated_node1681123000527",
)

# Script generated for node S3 step trainer landing
S3steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nagy-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3steptrainerlanding_node1",
)

# Script generated for node Renamed keys for Join Step Trainer with Customer
RenamedkeysforJoinStepTrainerwithCustomer_node1681125648151 = ApplyMapping.apply(
    frame=S3customercurated_node1681123000527,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "`(right) shareWithPublicAsOfDate`",
            "bigint",
        ),
        ("birthDay", "string", "`(right) birthDay`", "string"),
        ("registrationDate", "bigint", "`(right) registrationDate`", "bigint"),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "`(right) shareWithResearchAsOfDate`",
            "bigint",
        ),
        ("customerName", "string", "`(right) customerName`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("lastUpdateDate", "bigint", "`(right) lastUpdateDate`", "bigint"),
        ("phone", "string", "`(right) phone`", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "`(right) shareWithFriendsAsOfDate`",
            "bigint",
        ),
    ],
    transformation_ctx="RenamedkeysforJoinStepTrainerwithCustomer_node1681125648151",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3steptrainerlanding_node1,
    frame2=RenamedkeysforJoinStepTrainerwithCustomer_node1681125648151,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1681123119897 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "`(right) serialNumber`",
        "`(right) shareWithPublicAsOfDate`",
        "`(right) birthDay`",
        "`(right) registrationDate`",
        "`(right) shareWithResearchAsOfDate`",
        "`(right) customerName`",
        "`(right) email`",
        "`(right) lastUpdateDate`",
        "`(right) phone`",
        "`(right) shareWithFriendsAsOfDate`",
    ],
    transformation_ctx="DropFields_node1681123119897",
)

# Script generated for node S3 step trainer trusted
S3steptrainertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1681123119897,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nagy-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3steptrainertrusted_node3",
)

job.commit()
