import argparse, yaml, unittest, os, subprocess
from itertools import chain
from treldev.awsutils import AthenaURI, S3URI
from treldev import get_args


if __name__ == '__main__':
    args = get_args()
    schedule_instance_ts = args['schedule_instance_ts']

    known_dc = set()
    input_codes = {}
    for dc in args['inputs']:
        original_table = f"<{dc} table>"
        if original_table in input_codes:
            raise Exception(f"Table {original_table} is repeated in the input_map.")
        if dc in known_dc:
            raise Exception(f"Dataset class {dc} is repeated in the input_map.")
        if len(args['inputs'][dc]) == 1:
            code = AthenaURI(args['inputs'][dc][0]['uri']).path
        else:
            raise Exception("Currently only one URI per dataset class is supported.")
        input_codes[original_table] = code
        known_dc.add(dc)
    print(input_codes, known_dc)
    output_codes = {}
    output_dcs = set( v[0]['dataset_class'] for v in chain(args['outputs'].values()) )
    if len(output_dcs) != 1:
        raise Exception(f"There has to be exactly 1 dataet class in the output. Found: {output_dcs}")
    output_ath = AthenaURI(args['outputs']['output_athena'][0]['uri'])
    s3_output_uri = args['outputs']['output'][0]['uri']

    new_sql = []
    with open(args['parameters']['execution.main_sql_file']) as f:
        for line in f:
            if line.strip().endswith('-- :TREL_COMMENT:'):
                new_sql.append("-- "+ line)
                continue
            if line.startswith('-- :TREL_UNCOMMENT: '):
                line = line[len('-- :TREL_UNCOMMENT: '):]

            
            for original_table,code in input_codes.items():
                if original_table in line:
                    line = line.replace(original_table, code)

            if '{' in line:
                line = line.format(schedule_instance_ts=schedule_instance_ts)
                
            new_sql.append(line)

    #print(''.join(new_sql))
    output_ath.save_sql_results(s3_output_uri, ''.join(new_sql))
    output_ath.validate(args, save=True, sodacore_check=True)
