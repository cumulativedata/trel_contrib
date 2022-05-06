name: forecast_revenue

execution.profile: emr_pyspark

sensor.source_code.main:
  class: github
  branch: main
  path: https://github.com/cumulativedata/trel_contrib.git
sensor.main_executable: _code/complete_pipelines/demo_revenue/forecast_revenue.py

repository_map:
  - sales: s3-us-east2

execution.output_generator:
  class: default
  outputs:
  - dataset_class: sales_stats

resource.name: EMRMapReduceResource
resource.memory_level: normal
resource.num_cores: 1
resource.args:
  region: us-east-2
  spot: true

scheduler:
  class: single_instance
  labels: [ master ]
  depends_on: [ sales ]
  instance_ts_precisions: [ D ]
  cron_constraint: "0 0 * * *"
  schedule_ts_min: "2022-01-01 00:00:00"
