import argparse, yaml
from treldev.gcputils import BigQueryURI
from treldev import get_args

if __name__ == '__main__':
    args = get_args()
    schedule_instance_ts = args['schedule_instance_ts']

    output_bq = BigQueryURI(args['outputs']['transactions_30day'][0]['uri'])
    transactions_bq = BigQueryURI(args['inputs']['transactions'][0]['uri'])
    t30_bq = BigQueryURI(args['inputs']['transactions_30day'][0]['uri'])

    t_schema = args['inputs']['transactions'][0]['schema_version']
    t30_schema = args['inputs']['transactions_30day'][0]['schema_version']
    output_schema = args['outputs']['transactions_30day'][0]['schema_version']

    if (t30_schema, t_schema, output_schema) in [('1','1','1'),('2','2','2')]:
        t_cols = "*"
        t30_cols = "*"
    elif (t30_schema, t_schema, output_schema) in [('1','2','2')]:
        t_cols = "*"
        t30_cols = "ts, user_id, product_id, amount, 0 as is_returned"
    else:
        raise Exception("Unknown schema provided.")
    
    output_bq.save_sql_results(f"""
with 

transactions as (SELECT {t_cols} FROM `{transactions_bq.path}`)
,t30 as (SELECT {t30_cols} FROM `{t30_bq.path}`)
,max_ as (SELECT cast("{schedule_instance_ts}" AS datetime) max_ts)

SELECT * FROM transactions 
UNION ALL 
SELECT t30.* FROM t30 
CROSS JOIN max_ 
WHERE ts >= DATETIME_SUB(max_ts, INTERVAL 29 DAY)
""")
