import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 paths
input_path = "s3://forex-data-bucket-v1/raw/"
output_path = "s3://forex-data-bucket-v1/processed/"

# Read raw data from S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="json"
)

# Convert to DataFrame for transformations
df = dynamic_frame.toDF()

# Print schema and sample data for verification
df.printSchema()
df.show(5)

# Define correct column names (Adjust based on your dataset)
correct_column_names = [
    "From_Currency", "From_Currency_Name", "To_Currency", "To_Currency_Name", 
    "Exchange_Rate", "Last_Refreshed", "Time_Zone", "Bid_Price", "Ask_Price"
]

# Rename columns dynamically to remove serial numbers
for index, new_col in enumerate(correct_column_names):
    df = df.withColumnRenamed(df.columns[index], new_col)

# Convert back to DynamicFrame
dynamic_frame_transformed = DynamicFrame.fromDF(df, glueContext)

# Write processed data to S3
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_transformed,
    connection_type="s3",
    connection_options={"path": output_path},
    format="json"
)

job.commit()
