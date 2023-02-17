#!/usr/bin/env python3

'''
This sensor loads from Salesforce. See the config.yml file below for possible options.

.. literalinclude:: ../../GIT/trel_contrib/sensors/salesforce/config.yml
   :language: yaml


The behavior is as follows:

Supported repository classes as destination:

1. ``bigquery``

'''

import argparse, os, sys
import treldev, tempfile, json, datetime, subprocess, traceback
from os import listdir
from os.path import isfile, join, isdir
import sflib

class SalesforceSensor(treldev.ClockBasedSensor):
    
    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.credentials = credentials

        self.credentials_str = self.credentials[self.config.get('credentials.source_key','salesforce')]
        self.dataset_class_prefix = self.config['dataset_class_prefix']

        self.table_whitelist = self.config.get('table_whitelist',None)
        self.table_blacklist = self.config.get('table_blacklist',None)
        self.ignore_recommended_excluded_tables = self.config.get('ignore_recommended_excluded_tables', False)
        self.table_details = self.config.get('table_details',{})
        self.batch_rows = self.config.get('batch_rows',100000)
        self.known_contents = set([])
        self.cron_constraint = self.config['cron_constraint']
        self.lookback_seconds = self.config['max_instance_age_seconds'] - 1 # how far we should backfill missing datasets
        self.locking_seconds = self.config.get('locking_seconds',3600)
        self.mandatory_load_tables = self.config.get('mandatory_load_tables',[])

    def get_dataset_classes(self, load_info):
        sf = sflib.instantiate_from_creds(self.credentials_str)
        total_tables = set(sflib.get_tables(sf))
        if self.table_whitelist is not None:
            total_tables = total_tables.intersection(self.table_whitelist)
        if self.table_blacklist is not None:
            total_tables = total_tables.difference(self.table_blacklist)
        if not self.ignore_recommended_excluded_tables:
            total_tables = total_tables.difference(sflib.recommended_excluded_tables)
        for i,table_name in enumerate(sorted(total_tables)):
            load_info_copy = load_info.copy()
            load_info_copy['table_name'] = table_name
            yield self.dataset_class_prefix+table_name, load_info_copy
        
    def save_data_to_path(self, load_info, uri, dataset=None, **kwargs):
        sf = sflib.instantiate_from_creds(self.credentials_str)
        table_name = load_info['table_name']
        print("processing table", load_info['table_name'], file=sys.stderr)
        non_queryable = False
        failed = False
        try:
            sflib.load_table(sf, table_name, uri, self,
                             cols=self.table_details.get(table_name,{}).get('columns'))
        except TableNotQueryableException as ex:
            print(ex, file=sys.stderr)
            non_queryable = True
        except Exception as ex:
            traceback.print_exc()
            failed = True
        sf.session.close()
        if failed and table_name in self.mandatory_load_tables:
            raise Exception(f"The following mandatory table failed to load: {table_name}")
        
if __name__ == '__main__':
    treldev.Sensor.init_and_run(SalesforceSensor)
    
