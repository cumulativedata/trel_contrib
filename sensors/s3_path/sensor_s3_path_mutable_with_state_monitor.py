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
from treldev.awsutils import S3Helper
from os import listdir
from os.path import isfile, join, isdir
from sensor_s3_path import S3PathSensor, setup_for_test

class S3PathSensorMutable(treldev.Sensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)

        self.mutable_data_path = self.config['mutable_data_path']
        assert S3Helper._is_valid_s3_path(self.mutable_data_path)
        
        self.state_path_to_monitor = self.config['state_path_to_monitor']
        assert S3Helper._is_valid_s3_path(self.state_path_to_monitor)
        _,_, self.state_path_bucket, self.state_path_prefix = self.state_path_to_monitor.split('/',3)
        
        self.state_ts_key = self.config.get('state_ts_key','ts')
        self.locking_seconds = self.config.get('locking_seconds',30)
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.instance_ts_format = self.config.get('instance_ts_format',"%Y-%m-%d %H:%M:%S")
        self.request_payer = self.config.get('request_payer',False)
        
        self.credentials = credentials
        self.known_contents = set([])
        self.s3_client = treldev.awsutils.S3.get_client(None)
        self.s3_helper = S3Helper(self.s3_client, request_payer = self.request_payer)

        global boto3, ClientError
        import boto3
        from botocore.exceptions import ClientError
        
    def get_new_datasetspecs(self, datasets):
        ''' If there is data ready to be inserted, this should return a datasetspec. Else, return None '''
        state = self.s3_helper._load_file_as_dict(self.state_path_bucket, self.state_path_prefix)
        if not state:
            raise Exception(f"Unable to load state from {self.state_path_to_monitor}")
        existing_tss = set([ ds['instance_ts'] for ds in datasets ])
        ts = datetime.datetime.strptime(state[self.state_ts_key], self.instance_ts_format)
        if ts in existing_tss:
            return
        yield str(ts), { 'instance_prefix':self.instance_prefix,
                         'instance_ts':str(ts),
                         'instance_ts_precision':self.instance_ts_precision,
                         'locking_seconds': self.locking_seconds,
                         'alt_uri': self.mutable_data_path
        }

    def save_data_to_path(self, load_info, uri, **kwargs):
        ''' Nothing to do, as this sensor only registers. '''
        pass

class Test(unittest.TestCase):
    def test_sensor(self):
        ''' Test the s3_path sensor against a pre-determined s3 path with various configs.
        The sensor is asked to monitor s3://trel-contrib-unittests/public/s3_path/set1/
        This is a public folder with "requestor pays" setup. Take a look at the folder contents
        to better understand the assertions.'''
        setup_for_test()
        config = {
            'mutable_data_path':'s3://trel-contrib-unittests/public/s3_path/set1/',
            'state_path_to_monitor':'s3://trel-contrib-unittests/public/s3_path/state',
            'instance_ts_precision':'D',
            'dataset_class':'unittest_class',
            'label':'test',
            'repository': 'some-s3-repo',
            }
        s = S3PathSensorMutable(config,{},None,[])
        res = list(s.get_new_datasetspecs([]))
        res.sort()
        self.assertEqual(len(res),1)
        self.assertEqual(res[0][0],'2023-01-01 00:00:00')
        self.assertEqual(res[0][1]['alt_uri'],'s3://trel-contrib-unittests/public/s3_path/set1/')
        self.assertEqual(res[0][1]['instance_ts'],'2023-01-01 00:00:00')
        self.assertEqual(res[0][1]['instance_prefix'], None)
                                               
if __name__ == '__main__':
    treldev.Sensor.init_and_run(S3PathSensorMutable)
    
