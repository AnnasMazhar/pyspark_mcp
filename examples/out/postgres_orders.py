from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, avg, min, max, countDistinct,
    coalesce, concat, datediff, date_add, to_date,
    row_number, rank, lag, lead,
)
from pyspark.sql.window import Window

# Generated from POSTGRES SQL
spark = SparkSession.builder.appName('SQLToPySpark').getOrCreate()

# Load table: customers
customers_df = spark.table('customers')
# Load table: orders
orders_df = spark.table('orders')

# Main query
result_df = (orders_df.alias('o')
    .join(customers_df.alias('c'), (col('o.customer_id') == col('c.id')), 'inner')
    .filter((col('o.status') == lit('paid')))
    .groupBy(col('o.customer_id'), col('c.name'))
    .filter((SUM(o.amount) > lit(100)))  # HAVING clause
    .select((col('o.customer_id').cast('int')).alias('customer_id'), col('c.name'), (spark_sum(col('o.amount'))).alias('total')))
