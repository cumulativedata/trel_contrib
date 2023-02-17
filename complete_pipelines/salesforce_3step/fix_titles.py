import argparse, yaml
from treldev.gcputils import BigQueryURI
from treldev import get_args

if __name__ == '__main__':
    args = get_args()
    schedule_instance_ts = args['schedule_instance_ts']

    output_bq = BigQueryURI(args['outputs']['salesforce.Lead.fixed_titles'][0]['uri'])
    input_bq = BigQueryURI(args['inputs']['salesforce.Lead'][0]['uri'])

    output_bq.save_sql_results(f"""
with
lead as (SELECT * FROM `{input_bq.path}`)

SELECT * 
FROM (SELECT id,
    CASE 
    WHEN title = "Senior Vice President" THEN "SVP"
    WHEN title = "Vice President" THEN "VP"
    WHEN title = "Regional General Manager" THEN "Regional GM"
    ELSE NULL
    END as title
  FROM lead)
WHERE title is not NULL
""")
