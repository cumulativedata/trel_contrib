#!/usr/bin/env python3

'''
This sensor loads from ODBC data sources. See the config.yml file below for possible options.

.. literalinclude:: ../../GIT/trel_contrib/sensors/odbc_table_load/config.yml
   :language: yaml


The behavior is as follows:

1. The sensor queries the relevant datasets are already exist in the catalog. It uses the following parameters for this:

 * dataset_class
 * instance_ts_precision
 * label
 * repository
 * instance_prefix
 * max_instance_age_seconds
    
2. The sensor looks at the clock and identifies which datasets are relevant to load. The parameters of relevance are,

 * cron_constraint
 * delay_seconds
 * max_instance_age_seconds
 * backfill_newest_first (true / false)

 Here, treat the ``cron_constraint`` as a set rather than a timer. It consists of a set of timestamps. The ``cron_constraint`` defines the periods. Each period starts at a timestamp in the ``cron_constraint`` and ends before the next one. 

This sensor will try to create a dataset for each period. The resulting dataset's ``instance_ts`` will match the period start.
 
 A period is considered ready to load ``delay_seconds`` after the end of the period. Sensor will not try to load a period whose start is older than ``max_instance_age_seconds`` from now.

3. Once a dataset that should be created is identified, a connection is established to the ODBC data source

 * driver
 * server
 * port
 * username
 * password
 * credentials.requested_name
 * credentials.source_key -> odbc.<server_name>

4. Then the relevant data is downloaded in pieces and uploaded to the destination

 * database
 * table
 * custom_sql
 * repository
 * credentials.requested_name

.. admonition:: Warning
    :class: warning

    When using ``custom_sql`` is used, make sure that for tabular destinations such as BigQuery, the result has the same schema as the table. 

    This restriction does not apply to object store destinations such as S3 and Google Storage.

Custom SQL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``custom_sql`` that is provided will be formatted using python's ``.format`` rather than ODBC's SQL parameterization. This is largely for convenience and readability. The rationale for not worrying about SQL injestion is that the SQL is provided by the developer and parameterized by Trel using strings not selectable by anyone. 

Only three parameters are possible:

1. ``table``: This is the table selected by the developer in this config.
2. ``instance_ts``: This corresponds to the dataset and is a datetime object
3. ``instance_ts_precision``: This takes one of 4 values: 'D', 'H', 'M' and 'S'.
4. ``period_end``: This is the end of this period. The period starts at ``instance_ts``.

Please take a look at :ref:`odbc_data_sources` for a list of pre-installed ODBC drivers in the Trel instance for the ``driver`` parameter.

Supported repository classes as destination:

1. ``s3``
2. ``bigquery``

'''

import argparse, os, sys
import treldev, tempfile, json, datetime, subprocess
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
        self.table_details = self.config.get('table_details',[])
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
            total_tables = sorted(total_tables.intersection(self.table_whitelist))
        if self.table_blacklist is not None:
            total_tables = sorted(total_tables.difference(self.table_blacklist))
        if not self.ignore_recommended_excluded_tables:
            total_tables = sorted(total_tables.difference(sflib.recommended_excluded_tables))
        for i,table_name in enumerate(sorted(total_tables)):
            load_info_copy = load_info.copy()
            load_info_copy['table_name'] = table_name
            yield self.dataset_class_prefix+table_name, load_info_copy
        
    def save_data_to_path(self, load_info, uri, dataset=None, **kwargs):
        sf = sflib.instantiate_from_creds(self.credentials_str)
        table_name = load_info['table_name']
        print("processing table", load_info['table_name'])
        non_queryable = False
        failed = False
        try:
            sflib.load_table(sf, table_name, uri, self,
                             cols=self.table_details.get(table_name,{}).get('columns'))
        except TableNotQueryableException as ex:
            print(ex)
            non_queryable = True
        except Exception as ex:
            print(ex)
            failed = True
        sf.session.close()
        if failed and table_name in self.mandatory_load_tables:
            raise Exception(f"The following mandatory table failed to load: {table_name}")
        
