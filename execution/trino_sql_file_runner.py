import trino
from trino import TrinoURI,Trino
import argparse, yaml, unittest, os, subprocess
from treldev import get_args
import tempfile


""" Accepts an SQL file and information on how to parameterize it.

This runner can be used to run SQL scripts that were run as part of a data
transformation pipeline and overwrites existing tables. Only minimal changes
will need to be made to the SQL file.

The first part is replacement of tables in the SQL by Trel generate code. 
For this, you must provide the input tables in the SQL and the dataset classes 
that will replace them.

--input_map mycatalog.myschema.mytable1,dataset_class1 mycatalog.myschema.mytable2,dataset_class2
--output_map mycatalog.myschema.mytable3,dataset_class3 mycatalog.myschema.mytable4,dataset_class4

In addition,

Lines that end with "-- :TREL_COMMENT:" are commented out
Lines that start with "-- :TREL_UNCOMMENT: " are uncommented by removing this string.

So, if you have an SQL code as follows:

DROP TABLE IF EXISTS mycatalog.myschema.mytable2;
CREATE TABLE mycatalog.myschema.mytable2 
AS
SELECT user_id, count(*) c FROM mycatalog.myschema.mytable1 GROUP BY 1

To make this SQL compatible with this runner, the following changes are required

DROP TABLE IF EXISTS mycatalog.myschema.mytable2; -- :TREL_COMMENT:
CREATE TABLE mycatalog.myschema.mytable2
-- :TREL_UNCOMMENT: WITH ('external_location' = '{dataset_class2}', 'format' = 'PARQUET')   -- not required if the S3 location is not cataloged
AS
SELECT user_id, count(*) c FROM mycatalog.myschema.mytable1 GROUP BY 1

Then, the parameters can be as follows

--input_map mycatalog.myschema.mytable1,dataset_class1
--output_map mycatalog.myschema.mytable2,dataset_class2_trino

Now, this job can be registered with this scheduler

repository_map:
  - dataset_class1 : trino1

scheduler:
  class: single_instance
  depends_on:
   - dataset_class1
  labels: [ prod ]
  instance_ts_precisions: [ D ]
  cron_constraint: "0 0 * * *"
  schedule_ts_min: "2023-04-01 00:00:00"

execution.output_generator:
  class: default
  outputs:
  - dataset_class: dataset_class2
    repository: s2-e1
  - name: dataset_class2_trino
    repository: trino1
    link_to_previous: True

"""

class Test(unittest.TestCase):
    def test(self):
        
        with open('args.yml','w') as f:
            f.write("""
inputs:
 a:
  - uri: trino://myhost:8888/mycatalog/trel_tmp/t_5434
outputs:
 b_trino:
  - uri: trino://myhost:8888/mycatalog/trel_tmp/t_5434
 b:
  - uri: s3://mybucket/my_prefix/
parameters:
  execution.additional_arguments:
  - "--input_map"
  - mc.ms.mt_a,a
  - "--output_map"
  - mc.ms.mt_b,b_trino
  - "--sql_file=a.sql"
  execution.additional_arguments_cli: []
schedule_instance_ts: "2023-11-01 00:00:00"
    """)
        os.system('touch credentials.yml')

        with open('a.sql','w') as f:
            f.write("""
DROP TABLE IF EXISTS mc.ms.mt_b; -- :TREL_COMMENT:
CREATE TABLE  mc.ms.mt_b
-- :TREL_UNCOMMENT: WITH ('external_location' = '{b}', 'format' = 'PARQUET')
AS
SELECT user_id, count(*) c FROM mc.ms.mt_a GROUP BY 1

""")
        output = subprocess.check_output("python3 trino_sql_file_runner.py --_args=args.yml", shell=True)

        self.assertEqual(output.decode('utf-8').strip(),"""-- DROP TABLE IF EXISTS mc.ms.mt_b; -- :TREL_COMMENT:
CREATE TABLE  mycatalog.trel_tmp.t_5434
WITH ('external_location' = 's3://mybucket/my_prefix/', 'format' = 'PARQUET')
AS
SELECT user_id, count(*) c FROM mycatalog.trel_tmp.t_5434 GROUP BY 1""")



if __name__ == '__main__':
    args = get_args()
    schedule_instance_ts = args['schedule_instance_ts']

    cli_args_list = args['parameters']['execution.additional_arguments'] + \
        args['parameters']['execution.additional_arguments_cli']

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_map', nargs='+')
    parser.add_argument('--output_map', nargs='+')
    parser.add_argument('--sql_file', help='The SQL filename to run')
    parser.add_argument('--timeout',type = int, help='Seconds for timeout', default= 3600)
    cli_args = parser.parse_args(cli_args_list)
    new_sql = []

    known_dc = set()
    input_codes = {}
    for map1 in cli_args.input_map:
        original_table, dc = map1.split(',')
        if original_table in input_codes:
            raise Exception(f"Table {original_table} is repeated in the input_map.")
        if dc in known_dc:
            raise Exception(f"Dataset class {dc} is repeated in the input_map.")
        if len(args['inputs'][dc]) == 1:
            code = TrinoURI(args['inputs'][dc][0]['uri']).path
        else:
            raise Exception("Currently only one URI per dataset class is supported.")
        input_codes[original_table] = code
        known_dc.add(dc)

    output_codes = {}
    for map1 in cli_args.output_map:
        original_table, dc = map1.split(',')
        if original_table in input_codes or original_table in output_codes:
            raise Exception(f"Table {original_table} is repeated.")
        if dc in known_dc:
            raise Exception(f"Dataset class {dc} is repeated.")
        code = TrinoURI(args['outputs'][dc][0]['uri']).path
        output_codes[original_table] = code
        known_dc.add(dc)

    extra_mappings = {} # If paths are to be specified for the SQL script, we can get them pulled from the output path here
    for output_dc, outputs in args['outputs'].items():
        if output_dc not in known_dc:
            extra_mappings[output_dc] = outputs[0]['uri']

    with open(cli_args.sql_file) as f:
        for line in f:
            if line.strip().endswith('-- :TREL_COMMENT:'):
                new_sql.append("-- "+ line)
                continue
            if line.startswith('-- :TREL_UNCOMMENT: '):
                line = line[len('-- :TREL_UNCOMMENT: '):]

            
            for original_table,code in input_codes.items():
                if original_table in line:
                    line = line.replace(original_table, code)

            for original_table,code in output_codes.items():
                if original_table in line:
                    line = line.replace(original_table, code)

            if '{' in line:
                line = line.format(**extra_mappings)
            new_sql.append(line)

    print(''.join(new_sql))

    client = Trino.get_client()
    uri = list(args['outputs'].values())[0][0]['uri']
    trino_uri = TrinoURI(uri)
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.write(''.join(new_sql).encode('utf-8'))
        fp.close()
        client.execute_sql_script(trino_uri.host, trino_uri.port, trino_uri.catalog, fp.name, cli_args.timeout) 
       
