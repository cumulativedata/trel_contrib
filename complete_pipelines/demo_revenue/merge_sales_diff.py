import pyspark
import pyspark.sql
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, TimestampType, FloatType
import pyspark.sql.functions as psf

import json, time, sys, yaml

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sales")
    parser.add_argument("--sales_diff")
    parser.add_argument("--output")
    args, _ = parser.parse_known_args()
    return args

if __name__ == '__main__':
    cli_args = parse_args()

    sc = SparkContext(conf=SparkConf())
    spark = SQLContext(sc)

    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('sale_date', TimestampType(), True),
        StructField('product_id', StringType(), True),
        StructField('price', FloatType(), True),
        StructField('returned_date', TimestampType(), True),
        StructField('created_ts', TimestampType(), True),
        StructField('modified_ts', TimestampType(), True),
    ])

    spark.read.json(cli_args.sales, schema=schema)\
              .createOrReplaceTempView('sales')
    spark.read.json(cli_args.sales_diff, schema=schema)\
              .createOrReplaceTempView('sales_diff')

    spark.sql("""
with
combined as (select * from sales UNION ALL select * from sales_diff)
,latest as (select *, row_number() over (partition by id order by modified_ts desc) r from combined)
,res as (select id, sale_date, product_id, price, returned_date, created_ts, modified_ts from latest where r = 1)

select * from res
    """).repartition(4)\
         .write.parquet(cli_args.output)

    sc.stop()
