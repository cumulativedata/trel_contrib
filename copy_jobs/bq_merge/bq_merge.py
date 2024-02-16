import argparse, yaml, json
from treldev.gcputils import BigQueryURI, Storage, BigQuery
from treldev import get_args, SendSMTPMail

from google.cloud import bigquery
from typing import List

def union_tables_in_batches(client: bigquery.Client, output_table: str, input_tables: List[str], batch_size: int = 100):
    """
    Union all input tables into an output table in batches.
    
    :param client: BigQuery client object.
    :param output_table: ID of the output table (e.g., 'your-project.your_dataset.your_table').
    :param input_tables: List of input table IDs to union.
    :param batch_size: Number of tables to process in each batch.
    """
    
    # Define the query job configuration, especially useful for appending the results
    job_config = bigquery.QueryJobConfig()
    job_config.destination = output_table
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    print(f"Merging {len(input_tables)} inputs into {output_table}")

    for i in range(0, len(input_tables), batch_size):
        # Create a batch of tables
        batch = input_tables[i:i + batch_size]
        
        # Formulate the SQL query to UNION ALL tables in the current batch
        union_query = ' UNION ALL '.join([f'SELECT * FROM `{table}`' for table in batch])
        
        # Execute the query and append to the output table
        query_job = client.query(union_query, job_config=job_config)
        
        # Wait for the job to complete
        query_job.result()
        print(f"Batch {i // batch_size + 1} {'created' if i == 0 else 'appended'} successfully.")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND


if __name__ == '__main__':
    args = get_args()

    # cli_args_list = args['parameters']['execution.additional_arguments'] + \
    #     args['parameters']['execution.additional_arguments_cli']

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--arg1', nargs='+')
    # cli_args = parser.parse_args(cli_args_list)

    assert len(args['inputs']) == 1
    inputs = list(args['inputs'].values())[0]
    
    assert len(args['outputs']) == 1
    outputs = list(args['outputs'].values())[0]
    assert len(outputs) == 1

    
    output_bq = BigQueryURI(outputs[0]['uri'])

    BigQuery.set_args(args)
    client = BigQuery.get_client()
    output = output_bq.path
    inputs = [ BigQueryURI(v['uri']).path for v in inputs ]
    union_tables_in_batches(client, output, inputs, batch_size=1)
