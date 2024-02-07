import argparse, yaml, json
from treldev.gcputils import BigQueryURI, Storage, BigQuery
from treldev import get_args, SendSMTPMail

from google.cloud import bigquery
from typing import List

from datetime import datetime
import re

def recreate_view_if_needed(client: bigquery.Client, output_view: str, inputs: list, schedule_ts: datetime):
    """
    Recreates the output view with a UNION ALL of all input tables if the schedule_ts is newer.
    
    :param client: A BigQuery client object.
    :param output_view: The ID of the output view (e.g., 'project.dataset.view').
    :param inputs: A list of input table IDs.
    :param schedule_ts: A datetime object representing when the update is scheduled.
    """
    try:
        # Try to fetch the view metadata
        view = client.get_table(output_view)
        view_sql = view.view_query
        # Extract the timestamp from the view's comment
        timestamp_regex = r'-- (\d{8}_\d{4})$'
        match = re.search(timestamp_regex, view_sql)
        if match:
            view_ts_str = match.group(1)
            view_ts = datetime.strptime(view_ts_str, '%Y%m%d_%H%M')
            # Compare the timestamps
            if view_ts >= schedule_ts:
                print(f"No update needed. The view's timestamp {view_ts_str} is up-to-date or newer than the scheduled timestamp.")
                return
    except bigquery.NotFound:
        # View does not exist, proceed to create
        print("View does not exist. Creating a new one.")
    
    # Construct the new view SQL
    union_sql = ' UNION ALL '.join([f'SELECT * FROM `{table}`' for table in inputs])
    new_view_sql = f"{union_sql} -- {schedule_ts.strftime('%Y%m%d_%H%M')}"
    
    # Define the new view
    view = bigquery.Table(output_view)
    view.view_query = new_view_sql
    
    # Create or update the view
    client.create_table(view, exists_ok=True)
    print(f"View '{output_view}' has been created/updated successfully.")

if __name__ == '__main__':
    args = get_args()

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
    recreate_view_if_needed(client, output, inputs, args['schedule_instance_ts'])
