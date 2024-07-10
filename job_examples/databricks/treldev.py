import yaml, json

def get_sodacore_checks(job_args, entry, **kwargs):
    sodacore_checks = []
    schema = entry['schema']
    if kwargs.get('sodacore_check') is True:
        if not sodacore_checks:
            sodacore_checks = yaml.safe_load(schema['sodacore_checks'])
    if kwargs.get('sodacore_schema_check') is True:
        cols_str = ', '.join(column['name'] for column in schema['columns'])
        sodacore_checks.append(
            yaml.safe_load(f'''
schema:
  name: Confirm that required columns are present
  fail:
    when required column missing: [{cols_str}]'''))

    if kwargs.get('sodacore_extra_checks'):
        sodacore_checks += yaml.safe_load(kwargs['sodacore_extra_checks'])
    sodacore_checks_str = yaml.dump(sodacore_checks)
    print("Sodacore check")
    print(sodacore_checks_str)
    return(sodacore_checks)


def validate(table_name, job_args, entry, spark_session, save=True, sodacore_check=False, sodacore_schema_check=False, sodacore_extra_checks=None, return_folder='return'):

    sodacore_checks = get_sodacore_checks(job_args, entry, 
                                          sodacore_check=sodacore_check, 
                                          sodacore_schema_check=sodacore_schema_check,
                                          sodacore_extra_checks=sodacore_extra_checks)
    if len(sodacore_checks) > 0:
        from soda.scan import Scan

        scan = Scan()
        scan.add_spark_session(spark_session, data_source_name=table_name)
        scan.set_data_source_name(table_name)
        
        scan.add_variables(entry)
        table_str = table_name

        checks = yaml.dump({f'checks for {table_str}': sodacore_checks})
        print(checks)
        scan.add_sodacl_yaml_str(checks)

        scan.set_verbose(True)
        exit_code = scan.execute()

        scan.set_scan_definition_name(f"id_{entry['id_']}")

        results = scan.get_scan_results()

        r = {'metadata': {'sodacore': results },
                'has_validations': len(results['checks']) > 0,
                'has_validation_warnings': results['hasWarnings'],
                'has_validation_failures': results['hasFailures'],
        }
        if save:
            import boto3
            s3_client = boto3.client('s3')
            _,_,bucket,prefix = job_args['databricks_return_s3_path'].split('/',3)
            dsid = entry['id_']
            s3_client.put_object(Body=yaml.dump(r), Bucket=bucket, Key=(f'{prefix}/dsid_{dsid}.yml'))
        else:
            print('\n'.join( f'{v["timestamp"]} {v["message"]}' for v in results['logs']))

        if r['has_validation_failures']:
            print(yaml.dump(r))
            raise Exception(f"Dataset Validation failed for {entry['dataset_class']} {entry['uri']}.")
        