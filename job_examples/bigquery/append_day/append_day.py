import argparse, yaml
from treldev.gcputils import BigQueryURI
from treldev import get_args

if __name__ == '__main__':
    args = get_args()
    
    output_bq = BigQueryURI(args['outputs']['output'][0]['uri'])
    transactions_bq = BigQueryURI(args['inputs']['transactions'][0]['uri'])
    t30_bq = BigQueryURI(args['inputs']['transactions_30day'][0]['uri'])

    output_bq.save_sql_results(f"""
with 

transactions as (SELECT * FROM `{transactions_bq.path}`)
,t30 as (SELECT * FROM `{t30_bq.path}`)
,max_ as (SELECT cast("{args['schedule_instance_ts']}" AS datetime) max_ts)

SELECT * FROM transactions 
UNION ALL 
SELECT t30.* FROM t30 
CROSS JOIN max_ 
WHERE ts >= DATETIME_SUB(max_ts, INTERVAL 29 DAY)
""")

    output_bq.validate(args, save=True,
                       sodacore_check=True,
                       sodacore_schema_check=True)
