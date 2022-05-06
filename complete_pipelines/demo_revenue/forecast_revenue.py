import pyspark
import pyspark.sql
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, TimestampType, FloatType
import pyspark.sql.functions as psf

import json, time, sys, yaml
import pandas
import numpy as np

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sales")
    parser.add_argument("--sales_stats")
    parser.add_argument("--_args")
    args, _ = parser.parse_known_args()
    return args

if __name__ == '__main__':
    cli_args = parse_args()
    args_fname = cli_args._args
    with open(args_fname) as f:
        args = yaml.load(f)
        print(args)
    s3_temp = args['temp_paths']['s3'] + str(time.time()) + '/'

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

    spark.read.json(cli_args.sales, schema=schema).createOrReplaceTempView('sales')

    spark.sql("""with
params as (select min(sale_date) min_date from sales)

select sale_date, datediff(sale_date, min_date) day_num, sum(price) total_sales
from sales cross join params
group by 1,2
order by sale_date
    """).cache().createOrReplaceTempView('daily_sales')
    sales_by_date = spark.sql("select * from daily_sales").toPandas()

    model = np.polyfit(sales_by_date.day_num, sales_by_date.total_sales, 1)
    predict = np.poly1d(model)

    new_x = np.arange(sales_by_date.day_num.max() + 1, sales_by_date.day_num.max() + 91)
    new_y = predict(new_x)
    predictions = pandas.DataFrame(np.vstack([new_x, new_y]).T, columns=['day_num','total_sales'])
    spark.createDataFrame(predictions).createOrReplaceTempView('predictions')

    spark.sql("""with
params as (select
  to_timestamp(unix_timestamp(max_sale_date) - 60 * 86400) as min_date
from (select max(sale_date) max_sale_date from sales))

select sale_date, total_sales from daily_sales
UNION ALL
select
  to_timestamp(unix_timestamp(min_date) + day_num * 86400) sale_date,
  total_sales
from predictions cross join params

    """).repartition(4).write.parquet(cli_args.sales_stats)
    sc.stop()
