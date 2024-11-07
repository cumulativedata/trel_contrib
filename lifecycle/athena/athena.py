import json
import time
import sys
import yaml
import boto3
import tempfile
import os
import datetime
from multiprocessing import Pool, cpu_count
from typing import Dict, Any, List, Tuple
from treldev import get_args, S3Commands
from treldev.awsutils import AthenaURI  # Assuming your Athena utility classes are in athena_utils.py
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('delete_athena.log')
    ]
)
logger = logging.getLogger(__name__)

def delete_athena_table(athena_action: Dict[str, Any], credentials: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deletes an Athena table based on the provided action.

    :param athena_action: Dictionary containing Athena action details.
    :param credentials: AWS credentials for Athena access.
    :return: Updated athena_action with execution details.
    """
    athena_uri_str = athena_action.get('uri')
    action_requested = athena_action.get('action_requested', '').lower()
    
    if action_requested != 'delete':
        athena_action['before_state'] = []
        athena_action['after_state'] = []
        athena_action['error_message'] = f"{action_requested} is not a valid action."
        logger.warning(f"Invalid action requested: {action_requested}")
        return athena_action
    
    try:
        athena_uri = AthenaURI(athena_uri_str)
    except Exception as e:
        athena_action['before_state'] = []
        athena_action['after_state'] = []
        athena_action['error_message'] = f"Invalid Athena URI: {e}"
        logger.error(f"Invalid Athena URI: {athena_uri_str} | Error: {e}")
        return athena_action
    
    try:
        logger.info(f"Attempting to delete Athena table: {athena_uri.table} in database: {athena_uri.database}")
        exec_id = athena_uri.run_sql_command(
            sql=f"DROP TABLE IF EXISTS {athena_uri.path}",
            database=athena_uri.database,
            catalog=athena_uri.catalog,
            workgroup='primary',  # You can modify this based on your configuration
            client=AthenaURI.get_client(athena_uri.region),
            result_configuration={},
            quiet=True,
            is_query=True
        )
        logger.info(f"Successfully deleted table {athena_uri.table}. Execution ID: {exec_id}")
        athena_action['action_completed_ts'] = str(datetime.datetime.utcnow())
        athena_action['before_state'] = ['table_exists']
        athena_action['after_state'] = ['table_deleted']
    except Exception as e:
        logger.error(f"Failed to delete table {athena_uri.table}: {e}")
        athena_action['before_state'] = ['table_exists']
        athena_action['after_state'] = ['table_exists']
        athena_action['error_message'] = str(e)
    
    return athena_action

def process_action(args: Tuple[Dict[str, Any], Dict[str, Any]]) -> Dict[str, Any]:
    """
    Wrapper function to unpack arguments for multiprocessing.

    :param args: Tuple containing athena_action and credentials.
    :return: Updated athena_action with execution details.
    """
    athena_action, credentials = args
    return delete_athena_table(athena_action, credentials)

def main():
    """
    Main function to process Athena table deletion actions from S3.
    """
    args = get_args()
    start_time = time.time()
    
    pool_size = min(32, cpu_count() + 4)  # Adjust pool size based on CPU cores
    logger.info(f"Initializing multiprocessing pool with size: {pool_size}")
    
    with Pool(processes=pool_size) as pool:
        # Create temporary directories
        with tempfile.TemporaryDirectory() as temp_dir, tempfile.TemporaryDirectory() as output_folder:
            try:
                input_path = list(args['inputs'].values())[0][0]['uri']
                output_path = list(args['outputs'].values())[0][0]['uri']
            except IndexError:
                logger.error("Invalid input/output configuration.")
                sys.exit(1)
    
            s3_handler = S3Commands(credentials=args.get('credentials', {}))
            
            try:
                _, _, input_bucket, input_prefix = input_path.split('/', 3)
                _, _, output_bucket, output_prefix = output_path.split('/', 3)
            except ValueError:
                logger.error(f"Invalid S3 URI format for input: {input_path} or output: {output_path}")
                sys.exit(1)
    
            logger.info(f"Input S3 bucket: {input_bucket}, prefix: {input_prefix}")
            logger.info(f"Output S3 bucket: {output_bucket}, prefix: {output_prefix}")
    
            s3_input_bucket = s3_handler.s3r.Bucket(input_bucket)
            s3_output_bucket = s3_handler.s3r.Bucket(output_bucket)
    
            results = []
            action_counter = 0
    
            for s3_object in s3_input_bucket.objects.filter(Prefix=input_prefix):
                logger.info(f"Processing action file: {s3_object.key}")
                local_action_file = os.path.join(temp_dir, f"action_{action_counter}.json")
                try:
                    s3_input_bucket.download_file(s3_object.key, local_action_file)
                except Exception as e:
                    logger.error(f"Failed to download {s3_object.key}: {e}")
                    continue
                
                actions_to_process = []
                try:
                    with open(local_action_file, 'r') as f:
                        for line in f:
                            try:
                                action = json.loads(line)
                                actions_to_process.append((action, args['credentials']))
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON decode error in file {s3_object.key}: {e}")
                except Exception as e:
                    logger.error(f"Failed to read action file {local_action_file}: {e}")
                    continue
                
                # Process actions in batches
                if not actions_to_process:
                    logger.warning(f"No valid actions found in file {s3_object.key}. Skipping.")
                    continue
                
                logger.info(f"Submitting {len(actions_to_process)} actions to the pool.")
                processed_actions = pool.map(process_action, actions_to_process)
                results.extend(processed_actions)
                action_counter += 1
                os.remove(local_action_file)
    
            # Write results to output S3
            if results:
                output_filename = f'processed_actions_{int(time.time())}.json'
                local_output_file = os.path.join(output_folder, output_filename)
                try:
                    with open(local_output_file, 'w') as f_out:
                        for action_result in results:
                            json.dump(action_result, f_out)
                            f_out.write('\n')
                    s3_output_bucket.upload_file(local_output_file, f"{output_prefix}{output_filename}")
                    logger.info(f"Uploaded processed actions to {output_prefix}{output_filename}")
                except Exception as e:
                    logger.error(f"Failed to upload processed actions: {e}")
    
    logger.info(f"Execution completed in {time.time() - start_time:.2f} seconds.")

def test(temp_s3_path: str, num_paths: int = 20, num_files: int = 10):
    """
    Tests the Athena processing script by creating and uploading test action files.

    :param temp_s3_path: The S3 path to use for testing.
    :param num_paths: Number of S3 prefixes to create.
    :param num_files: Number of files per prefix.
    """
    temp_folder = tempfile.mkdtemp()
    # Create empty action files
    for i in range(num_files):
        filename = f"action_part_{i:05d}.json"
        open(os.path.join(temp_folder, filename), 'a').close()
    
    # Upload action files to multiple prefixes
    s3_handler = S3Commands(credentials={})  # Provide appropriate credentials
    for i in range(num_paths):
        prefix = f"{temp_s3_path}data_{i}/"
        for filename in os.listdir(temp_folder):
            local_file = os.path.join(temp_folder, filename)
            s3_handler.s3r.Bucket(s3_handler.s3r.meta.client.meta.endpoint_url).upload_file(local_file, f"{prefix}{filename}")
    
    # Create action records
    actions = [{'uri': f"athena://us-east-1/catalog_{i}/database_{i}/table_{i}", 'action_requested': 'delete_table'} for i in range(num_paths * num_files)]
    temp_folder2 = tempfile.mkdtemp()
    action_file_path = os.path.join(temp_folder2, 'processed_actions.json')
    try:
        with open(action_file_path, 'w') as f:
            for action in actions:
                json.dump(action, f)
                f.write('\n')
    except Exception as e:
        logger.error(f"Failed to create test action file: {e}")
        return
    
    # Upload the action file to input S3 path
    try:
        s3_handler.s3r.Bucket(s3_handler.s3r.meta.client.meta.endpoint_url).upload_file(
            action_file_path,
            f"{temp_s3_path}input/processed_actions.json"
        )
        logger.info(f"Uploaded test action file to {temp_s3_path}input/processed_actions.json")
    except Exception as e:
        logger.error(f"Failed to upload test action file: {e}")
        return
    
    # Execute the main script
    cmd = f"python3 delete_athena.py --_lifecycle.actions {temp_s3_path}input/ --_lifecycle.actions.complete {temp_s3_path}output/ --_args 1"
    logger.info(f"Executing command: {cmd}")
    os.system(cmd)

if __name__ == '__main__':
    main()
