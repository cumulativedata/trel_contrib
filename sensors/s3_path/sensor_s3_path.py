#!/usr/bin/env python3
'''
This sensor can monitor a given S3 path to look for data drops with an optional prefix and a date/time component.

E.g., Finding If data is placed into `s3://a/b/c/abc_20210201` and sensor is configured to with bucket as `a` and
prefix as `b/c`, this data path will be cataloged as ``<dataset_class>,,20210201,<label>,<repository>``

See example configuration file.

Credentials: ``aws.access_key``

'''


import argparse, os, sys, datetime, unittest
import treldev.awsutils
from os import listdir
from os.path import isfile, join, isdir

class S3PathSensor(treldev.Sensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.bucket = self.config['bucket']
        self.prefix = self.config.get('prefix')
        self.subfolder_prefix = self.config.get('subfolder_prefix',None)
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.instance_ts_format = self.config.get('instance_ts_format',"%Y%m%d")
        self.success_criteria = self.config.get('success_criteria','success_file')
        
        self.request_payer = self.config.get('request_payer',False)
        assert type(self.request_payer) is bool
        

        self.locking_seconds = self.config.get('locking_seconds',30)
        self.credentials = credentials
        self.known_contents = set([])
        self.s3_client = treldev.awsutils.S3.get_client(None)

        global boto3, ClientError
        import boto3
        from botocore.exceptions import ClientError
        
    def find_subfolders(self):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.bucket,
                                'Prefix': self.prefix+(self.subfolder_prefix if self.subfolder_prefix else ''),
                                'Delimiter': '/'}
        if self.request_payer:
            operation_parameters['RequestPayer'] = 'requester'

        page_iterator = paginator.paginate(**operation_parameters)

        subfolders = []
        for page in page_iterator:
            subfolders.extend(page.get('CommonPrefixes', []))

        # Extract the directory names from the CommonPrefixes result
        subfolder_names = set(folder['Prefix'].split('/')[-2] for folder in subfolders)

        return subfolder_names

    def _find_file(self, subfolder, file_name):
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f"{self.prefix}{subfolder}/{file_name}",
            **({'RequestPayer':'requester'} if self.request_payer else {})
        )

        for obj in response.get('Contents', []):
            if obj['Key'] == f"{self.prefix}{subfolder}/{file_name}":
                return obj['Key']

        return None

    def _find_file_with_suffix(self, subfolder, file_suffix):
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f"{self.prefix}{subfolder}/",
            **({'RequestPayer':'requester'} if self.request_payer else {})
        )

        for obj in response.get('Contents', []):
            if obj['Key'].endswith(file_suffix):
                return obj['Key']

        return None

    def verify_criteria_success_file(self, subfolder):
        return bool(self._find_file(subfolder, '_SUCCESS'))
    
    def verify_criteria_manifest_file(self, subfolder):
        # Check if '.manifest' file exists
        if self._find_file(subfolder, '.manifest'):
            return True
        
        # If '.manifest' file does not exist, check for any '.manifest' file
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f"{self.prefix}{subfolder}/",
            **({'RequestPayer':'requester'} if self.request_payer else {})
        )

        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.manifest'):
                return True

        return False


    def verify_criteria_manifest_file_with_replacement(self, subfolder):
        # Try to find both files
        manifest_file = self._find_file_with_suffix(subfolder, '.manifest')
        success_file = self._find_file(subfolder, '_SUCCESS')

        # Only manifest file is found
        if manifest_file and not success_file:
            # Upload _SUCCESS file
            self.s3_client.put_object(Bucket=self.bucket, Key=f"{self.prefix}{subfolder}/_SUCCESS",body=b'')
            success_file = f"{self.prefix}{subfolder}/_SUCCESS"

        # Both files are found
        if manifest_file and success_file:
            # Delete the manifest file
            self.s3_client.delete_object(Bucket=self.bucket, Key=manifest_file)
            manifest_file = None

        # Only _SUCCESS file is found
        if success_file and not manifest_file:
            return True

        return False

    def verify_criteria_min_files(self, subfolder, min_files):
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f"{self.prefix}{subfolder}/",
            **({'RequestPayer':'requester'} if self.request_payer else {})
        )
        return len(response.get('Contents', [])) >= min_files

    def verify_criteria_min_age(self, subfolder, min_age):
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f"{self.prefix}{subfolder}/",
            **({'RequestPayer':'requester'} if self.request_payer else {})
        )

        # If there are no Contents in the response, return False
        if 'Contents' not in response:
            return False

        current_time = datetime.datetime.now(datetime.timezone.utc)

        for obj in response['Contents']:
            time_diff = current_time - obj['LastModified'].replace(tzinfo=datetime.timezone.utc)
            if time_diff.total_seconds() < min_age:
                return False

        return True        
    
    def get_new_datasetspecs(self, datasets):
        ''' If there is data ready to be inserted, this should return a datasetspec. Else, return None '''
        subfolders = self.find_subfolders()
        #print(subfolders)
        new_subfolders = subfolders.difference(self.known_contents)
        subfolder_tss = {}
        for subfolder in new_subfolders:
            try:
                ts_portion = subfolder[(0 if self.subfolder_prefix is None else len(self.subfolder_prefix)):]
                instance_ts = datetime.datetime.strptime(ts_portion, self.instance_ts_format)
            except:
                print(f"Unable to parse ts from subfolder {subfolder}",file=sys.stderr)
                continue
            subfolder_tss[instance_ts] = subfolder
        existing_tss = set([ ds['instance_ts'] for ds in datasets ])
        if self.debug:
            self.logger.debug(f"exiting_tss {existing_tss}")
        for instance_ts, subfolder in sorted(subfolder_tss.items()):
            #print("Examining", subfolder)
            
            if str(instance_ts) in existing_tss:
                if self.debug:
                    self.logger.debug(f"For subfolder {subfolder} with instance_ts {instance_ts}, we found entry in existing_tss. Skipping.")
                self.known_contents.add(subfolder)
                continue
            else:
                if self.debug:
                    self.logger.debug(f"For subfolder {subfolder} with instance_ts {instance_ts}, we found no entry in existing_tss.")
                
            if self.max_instance_age_seconds is not None and \
               instance_ts < self.round_now - datetime.timedelta(seconds=self.max_instance_age_seconds):
                self.known_contents.add(subfolder)
                continue
            if self.min_instance_ts is not None and str(instance_ts) < self.min_instance_ts:
                self.known_contents.add(subfolder)
                continue

            if self.success_criteria == 'success_file' and not self.verify_criteria_success_file(subfolder):
                continue
            if self.success_criteria == 'manifest_file' and not self.verify_criteria_manifest_file(subfolder):
                continue
            if self.success_criteria == 'manifest_file_with_replacement' \
               and not self.verify_criteria_manifest_file_with_replacement(subfolder):
                continue
            if type(self.success_criteria) is dict and 'min_files' in self.success_criteria \
               and not self.verify_criteria_min_files(subfolder, **self.success_criteria):
                continue
            if type(self.success_criteria) is dict and 'min_age' in self.success_criteria \
               and not self.verify_criteria_min_age(subfolder, **self.success_criteria):
                continue
                
            yield subfolder, { 'instance_prefix':self.instance_prefix,
                               'instance_ts':str(instance_ts),
                               'instance_ts_precision':self.instance_ts_precision,
                               'locking_seconds': self.locking_seconds,
                               'alt_uri':f's3://{self.bucket}/{self.prefix}{subfolder}/'
            }

    def save_data_to_path(self, load_info, uri, **kwargs):
        ''' Nothing to do, as this sensor only registers. '''
        pass

def setup_for_test():
    import subprocess, json, boto3
    credentials_data = subprocess.check_output(['trel','--output_json','credentials']).decode('utf-8')
    aws_creds = None
    credentials = {}
    for credential_string in credentials_data.split('\n'):
        if credential_string:
            credential = json.loads(credential_string)
            value = credential['value']
            credentials[credential['key']] = value
            if credential['key'] == 'aws.access_key':
                aws_creds = json.loads(value)
    assert aws_creds is not None

    from unittest.mock import MagicMock
    treldev.awsutils.S3.get_client = MagicMock(return_value=boto3.client(
        's3',
        aws_access_key_id=aws_creds['key'],
        aws_secret_access_key=aws_creds['skey']
    ))
    return credentials

class Test(unittest.TestCase):
    def test_sensor(self):
        ''' Test the s3_path sensor against a pre-determined s3 path with various configs.
        The sensor is asked to monitor s3://trel-contrib-unittests/public/s3_path/set1/
        This is a public folder with "requestor pays" setup. Take a look at the folder contents
        to better understand the assertions.'''
        setup_for_test()
        config = {
            'bucket':'trel-contrib-unittests',
            'prefix':'public/s3_path/set1/',
            'instance_ts_precision':'D',
            'instance_ts_format':'%Y%m%d',
            'success_criteria':'success_file',
            'dataset_class':'unittest_class',
            'label':'test',
            'instance_prefix':'set1',
            'repository': 'some-s3-repo',
            'request_payer': True,
            }
        s = S3PathSensor(config,{},None,[])
        res = list(s.get_new_datasetspecs([]))
        res.sort()
        self.assertEqual(len(res),2)
        self.assertEqual(res[0][0],'20230101')
        self.assertEqual(res[0][1]['alt_uri'],'s3://trel-contrib-unittests/public/s3_path/set1/20230101/')
        self.assertEqual(res[0][1]['instance_ts'],'2023-01-01 00:00:00')
        self.assertEqual(res[0][1]['instance_prefix'],'set1')
        self.assertEqual(res[1][0],'20230102')

        s = S3PathSensor(config,{},None,[])
        res = list(s.get_new_datasetspecs([{'instance_ts':datetime.datetime(2023,1,1)}]))                   
        self.assertEqual(len(res),1)
        self.assertEqual(res[0][0],'20230102')

        config['success_criteria'] = None
        s = S3PathSensor(config,{},None,[])
        res = list(s.get_new_datasetspecs([{'instance_ts':datetime.datetime(2023,1,1)}]))                   
        res.sort()
        self.assertEqual(len(res),2)
        self.assertEqual(res[0][0],'20230102')
        self.assertEqual(res[1][0],'20230103')

        config['success_criteria'] = {'min_files': 2}
        s = S3PathSensor(config,{},None,[])
        res = list(s.get_new_datasetspecs([]))             
        res.sort()
        self.assertEqual(len(res),2)
        self.assertEqual(res[0][0],'20230102')
        self.assertEqual(res[1][0],'20230103')
        
        config['success_criteria'] = {'min_age': 0}
        s = S3PathSensor(config,{},None,[])
        res = list(s.get_new_datasetspecs([]))             
        res.sort()
        self.assertEqual(len(res),3)
        
        config['success_criteria'] = {'min_age': 100000000000}
        s = S3PathSensor(config,{},None,[])
        res = list(s.get_new_datasetspecs([]))             
        res.sort()
        self.assertEqual(len(res),0)
        
        config['prefix'] = 'public/s3_path/set2/'
        config['subfolder_prefix'] = 'a_'
        config['instance_prefix'] = 'set2/a'
        config['success_criteria'] = None
        s = S3PathSensor(config,{},None,[])
        res = list(s.get_new_datasetspecs([]))
        res.sort()
        self.assertEqual(len(res),2)
        self.assertEqual(res[0][0],'a_20230101')
        self.assertEqual(res[0][1]['alt_uri'],'s3://trel-contrib-unittests/public/s3_path/set2/a_20230101/')
        self.assertEqual(res[0][1]['instance_ts'],'2023-01-01 00:00:00')
        self.assertEqual(res[0][1]['instance_prefix'],'set2/a')
        self.assertEqual(res[1][0],'a_20230102')
                                               
if __name__ == '__main__':
    treldev.Sensor.init_and_run(S3PathSensor)
    
