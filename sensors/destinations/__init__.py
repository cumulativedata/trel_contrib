import argparse, os, sys
import treldev, pyodbc, tempfile, json, datetime, subprocess, collections
from os import listdir
from os.path import isfile, join, isdir

class DestinationProtocol(object):

    registered = {}
    
    @classmethod
    def register(cls):
        cls.registered[cls.protocol] = cls
    
    @classmethod
    def get_class_from_protocol(cls, protocol):
        try:
            return cls.registered[protocol]
        except KeyError:
            logger.debug(f"{protocol} is not a registered destination protocol. Only found {cls.registered}")
            raise

    def get_schema_format_name(self):
        ''' Return a string that clearly specifies where the data is going and what are the capabilities of the columns if any. E.g., s3 is not enough. s3_csv or s3_json or s3_parquet or s3_avro. However, bq is enough.
        '''
        pass
        
    @classmethod
    def get_object_from_uri(cls, uri, sensor):
        try:
            uri_protocol,_ = uri.split(':',1)
            return cls.registered[uri_protocol](uri, sensor)
        except KeyError as ex:
            msg = (f"{uri_protocol} is not a registered destination protocol. Only found {sorted(cls.registered)}")
            sensor.logger.error(msg)
            raise Exception(msg) from ex
        
    def __init__(self, uri, sensor):
        self.uri = uri
        self.sensor = sensor
    
    def set_column_format(self, destination_format):
        self.destination_format = destination_format
    
    def prepare(self):
        self.batch_num = 0
        self.prepare_inner()

    def append_data(self, file_name):
        self.append_data_inner(file_name)
        self.batch_num += 1

    def finish(self):
        self.finish_inner()

    def write_row_to_file(self, row, f):
        for i in range(len(row)):
            if type(row[i]) is datetime.datetime:
                row[i] = str(row[i])
        json.dump(dict(filter((lambda x: x[1] is not None), zip(self.col_names, row))), f)
        f.write('\n')
    
    def get_next_batch_num(self):
        return self.batch_num

class S3Destination(DestinationProtocol):

    protocol = 's3'

    def prepare_inner(self):
        self.s3_commands = treldev.S3Commands(credentials=self.sensor.credentials)

    
    def append_data_inner(self, filename):
        if self.sensor.compression == 'gz':
            subprocess.check_call(f"gzip {filename}", shell=True)
            filename = filename + '.gz'
            file_uri = self.uri + f"part-{self.batch_num:>05}.gz"
        else:
            file_uri = self.uri + f"part-{self.batch_num:>05}"
        self.s3_commands.upload_file(filename, file_uri)
        os.remove(filename)

    def finish_inner(self):
        with tempfile.NamedTemporaryFile('w') as f:
            self.s3_commands.upload_file(f.name, self.uri+'_SUCCESS')
            f.close()
S3Destination.register()

            
class BigQueryDestination(DestinationProtocol):

    protocol = 'bq'
    type_mapping = {
        pyodbc.SQL_CHAR: 'STRING',
        pyodbc.SQL_VARCHAR: 'STRING',
        pyodbc.SQL_LONGVARCHAR: 'STRING',
        pyodbc.SQL_WCHAR: 'STRING',
        pyodbc.SQL_WVARCHAR: 'STRING',
        pyodbc.SQL_WLONGVARCHAR: 'STRING',
        pyodbc.SQL_GUID: 'STRING',
        pyodbc.SQL_TYPE_DATE: 'DATE',
        pyodbc.SQL_TYPE_TIME: 'TIME',
        pyodbc.SQL_TYPE_TIMESTAMP: 'DATETIME',
        #pyodbc.SQL_TYPE_UTCDATETIME: 'DATETIME',
        #pyodbc.SQL_TYPE_UTCTIME: 'DATETIME',
        pyodbc.SQL_BINARY: 'BYTES',
        pyodbc.SQL_VARBINARY: 'BYTES',
        pyodbc.SQL_DECIMAL: 'DECIMAL',
        pyodbc.SQL_NUMERIC: 'DECIMAL',
        pyodbc.SQL_SMALLINT: 'INTEGER',
        pyodbc.SQL_INTEGER: 'INTEGER',
        pyodbc.SQL_BIT: 'INTEGER', # test
        pyodbc.SQL_TINYINT: 'INTEGER',
        pyodbc.SQL_BIGINT: 'INTEGER',
        pyodbc.SQL_REAL: 'FLOAT64',
        pyodbc.SQL_FLOAT: 'FLOAT64',
        pyodbc.SQL_DOUBLE: 'FLOAT64',
        pyodbc.SQL_INTERVAL_MONTH: 'INTERVAL',
        pyodbc.SQL_INTERVAL_YEAR: 'INTERVAL',
        pyodbc.SQL_INTERVAL_YEAR_TO_MONTH: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY: 'INTERVAL',
        pyodbc.SQL_INTERVAL_HOUR: 'INTERVAL',
        pyodbc.SQL_INTERVAL_MINUTE: 'INTERVAL',
        pyodbc.SQL_INTERVAL_SECOND: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY_TO_HOUR: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY_TO_MINUTE: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY_TO_SECOND: 'INTERVAL',
        pyodbc.SQL_INTERVAL_HOUR_TO_MINUTE: 'INTERVAL',
        pyodbc.SQL_INTERVAL_HOUR_TO_SECOND: 'INTERVAL',
        pyodbc.SQL_INTERVAL_MINUTE_TO_SECOND: 'INTERVAL',
    }

    def get_schema_format_name(self):
        return 'bq'

    def prepare_inner(self):
        global bigquery, BigQuery, BigQueryURI, base64

        #assert self.sensor.output_format == 'json'
        
        from treldev.gcputils import BigQuery, BigQueryURI
        import treldev.gcputils
        from google.cloud import bigquery
        import base64

        self.client = treldev.gcputils.BigQuery.get_client()
        for col in self.destination_format:
            print(f"{col},", file=sys.stderr,end='')
        print("", file=sys.stderr)

        self.schema = []
        self.bquri = BigQueryURI(self.uri)
        for col in self.destination_format:
            #bq_type = self.type_mapping[col.data_type]
            self.schema.append( bigquery.SchemaField(col['name'], col['type'], mode=("NULLABLE" if col['nullable'] else "REQUIRED")) )
        self.client.delete_table(self.bquri.path, not_found_ok=True)
        table = bigquery.Table(self.bquri.path, schema=self.schema)
        
        table = self.client.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id), file=sys.stderr)
        
    def write_row_to_file(self, row, f):
        for i in range(len(row)):
            bq_type = self.destination_format[i]['type'].upper()
            if bq_type in ('DATE','TIME','DATETIME','INTERVAL'):
                if row[i] is not None:
                    row[i] = str(row[i])
            elif bq_type == 'BYTES':
                if row[i] is not None:
                    row[i] = base64.b64encode(row[i]).decode('utf-8')
        json.dump(dict(filter((lambda x: x[1] is not None), zip(self.col_names, row))), f)
        f.write('\n')
    
    def write_dict_row_to_file(self, row, f):
        for col in self.destination_format:
            bq_type = col['type'].upper()
            col_name = col['name']
            if bq_type in ('DATE','TIME','DATETIME','INTERVAL'):
                if row[col_name] is not None:
                    row[col_name] = str(row[col_name])
            elif bq_type == 'BYTES':
                if row[col_name] is not None:
                    row[col_name] = base64.b64encode(row[col_name]).decode('utf-8')
            if bq_type == 'STRING' and type(row[col_name]) in (list, dict, collections.OrderedDict):
                row[col_name] = json.dumps(row[col_name])
        row = dict(row)
        for k in list(row):
            if row[k] is None:
                del row[k]
        json.dump(row, f)
        f.write('\n')
    
    def append_data_inner(self, filename):
        loadjob_config_dict = {
            'write_disposition': bigquery.WriteDisposition.WRITE_APPEND,
            'source_format': bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            }
        self.bquri.load_file(filename, loadjob_config_dict)
        os.remove(filename)
        
    def finish_inner(self):
        pass
BigQueryDestination.register()
        
