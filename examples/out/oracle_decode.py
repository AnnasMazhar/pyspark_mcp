from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, avg, min, max, countDistinct,
    coalesce, concat, datediff, date_add, to_date,
    row_number, rank, lag, lead,
)
from pyspark.sql.window import Window

# Generated from ORACLE SQL
spark = SparkSession.builder.appName('SQLToPySpark').getOrCreate()

# Load table: customers
customers_df = spark.table('customers')
# Load table: orders
orders_df = spark.table('orders')

# Main query
result_df = (orders_df.alias('o')
    .join(customers_df.alias('c'), (col('o.customer_id') == col('c.id')), 'inner')
    .filter((col('o.order_date') >= DATE_STR_TO_DATE('2024-01-01')))
    .select(col('o.order_id'), (row_number().over(w)).alias('status_label'), (row_number().over(w)).alias('geo'), (coalesce(lit('none'))).alias('email')))
