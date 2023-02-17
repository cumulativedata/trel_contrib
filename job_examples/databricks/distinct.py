# Databricks notebook source
all_args = dbutils.notebook.entry_point.getCurrentBindings()
print(list(all_args.keys()))

# COMMAND ----------

import yaml

args = yaml.safe_load(all_args['args'])
print(all_args['args'])

# COMMAND ----------

sc.textFile(args['inputs']['request_log'][0]['uri']).distinct().saveAsTextFile(args['outputs']['request_log_distinct'][0]['uri'])
