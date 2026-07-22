import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    lit,
    input_file_name
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------------------------------
# Job Parameters
# ---------------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------------------------------------------------------------
# Read from Glue Catalog
# ---------------------------------------------------------------------------------

DATABASE = "realestate_db"
TABLE = "raw_properties"

raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=TABLE
)

df = raw_dyf.toDF()

# ---------------------------------------------------------------------------------
# Bronze Layer
# Preserve Raw Data
# ---------------------------------------------------------------------------------

bronze_df = (
    df
    .withColumn("bronze_ingestion_timestamp", current_timestamp())
    .withColumn("bronze_ingestion_date", current_date())
    .withColumn("source_file", input_file_name())
    .withColumn("pipeline_name", lit("RealEstate-Bronze"))
    .withColumn("record_status", lit("RAW"))
)

# ---------------------------------------------------------------------------------
# Validation Metrics
# ---------------------------------------------------------------------------------

print("=" * 80)
print("Bronze Layer Summary")
print("=" * 80)

print(f"Total Records : {bronze_df.count()}")

bronze_df.printSchema()

# ---------------------------------------------------------------------------------
# Write Bronze Layer
# ---------------------------------------------------------------------------------

OUTPUT_PATH = "s3://your-bronze-bucket/bronze/realestate/"

(
    bronze_df
    .write
    .mode("overwrite")
    .partitionBy("bronze_ingestion_date")
    .parquet(OUTPUT_PATH)
)

print("Bronze Layer Completed Successfully")

job.commit()