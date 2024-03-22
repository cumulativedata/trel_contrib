import argparse, yaml
from treldev.gcputils import BigQueryURI
from treldev import get_args

if __name__ == '__main__':
    args = get_args()
    
    output_bq = BigQueryURI(list(args['outputs'].values())[0][0]['uri'])
    # input1_bq = BigQueryURI(args['inputs']['___dataset class___'][0]['uri'])

    output_bq.save_sql_results(f"""
    """)

output_bq.validate(args, save=True,
                   sodacore_check=True,
                   sodacore_schema_check=False)
