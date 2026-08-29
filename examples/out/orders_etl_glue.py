import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# WARNING: Job bookmarks need DynamicFrame reads with transformation_ctx; spark.sql / DataFrame reads do not bookmark.

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_PATH',
    'TARGET_PATH',
    'SOURCE_DATABASE',
    'SOURCE_TABLE',
    'TARGET_DATABASE',
    'TARGET_TABLE',
    'JOB_BOOKMARK_OPTION'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure logging
logger = glueContext.get_logger()
logger.info(f"Starting job: {args['JOB_NAME']}")

try:
    # Read source data using DataFrame
    source_df = spark.read.format("parquet").load(args['SOURCE_PATH'])

    # Apply SQL transformations
    source_df.createOrReplaceTempView("source_table")
    transformed_df = spark.sql('''
    SELECT
    o.customer_id::int AS customer_id,
    c.name,
    SUM(o.amount) AS total
FROM {schema}.orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'paid'
GROUP BY o.customer_id, c.name
HAVING SUM(o.amount) > 100

    ''')

    # Write transformed data
    transformed_df.write \
        .format("parquet") \
        .mode("overwrite").option("compression", "snappy") \
        .save(args['TARGET_PATH'])

    print(f"Job {args['JOB_NAME']} completed successfully")
    
    job.commit()
except Exception as e:
    print(f"Job failed with error: {str(e)}")
    logger.error(f"Job failed with error: {str(e)}")
    raise e

