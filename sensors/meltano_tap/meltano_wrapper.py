import argparse, os, sys
import treldev, pyodbc, tempfile, json, datetime, subprocess
from os import listdir
from os.path import isfile, join, isdir


class MeltanoWrapper(treldev.ClockBasedSensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        global destinations
        import destinations
        
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.credentials = credentials

        self.tap_config = self.config['tap_config']
        
        self.cron_constraint = self.config['cron_constraint']
        self.lookback_seconds = self.config['max_instance_age_seconds'] - 1 # how far we should backfill missing datasets
        self.locking_seconds = self.config.get('locking_seconds',600)
        self.compression = self.config.get('compression','gz')
        self.compression = None
        
        try:
            self.current_directory = os.getcwd()
            self.logger.debug(f"Current working directory: {self.current_directory}")
            subprocess.check_output(f"~/platform_venv/bin/meltano init test-meltalo", shell=True)
            self.logger.debug("meltano project created successfully")
            result = subprocess.check_output(f"~/platform_venv/bin/meltano add extractor {self.config['meltano_tap']}", cwd=f"{self.current_directory}/test-meltalo", shell=True)
            self.logger.debug(f"Extractor {self.config['meltano_tap']} added successfully")
        except Exception as e:
            raise

        if 'tap_credentials' in self.config:
            self.tap_creds = self.config["tap_credentials"]
            for i in self.tap_creds:
                tap_cred_value = json.loads(self.credentials[i])
                cred_val = tap_creds[self.tap_creds[i]]
                setattr(self, self.tap_creds[i], tap_cred_value[self.tap_creds[i]])
                subprocess.check_output(f"~/platform_venv/bin/meltano config {self.config['meltano_tap']} set {self.tap_creds[i]} \'{tap_cred_value}\'", cwd=f"{self.current_directory}/test-meltalo", shell=True)
                self.logger.debug(f"{self.tap_creds[i]} set successcully")


        for k in self.tap_config:
            setattr(self, k, self.tap_config[k])
            # self.logger.debug(f"~/platform_venv/bin/meltano config {self.config['meltano_tap']} set user_usernames \'{self.tap_config[k]}\'")
            # self.logger.debug(f'~/platform_venv/bin/meltano config tap-github set user_usernames \'["{self.user_usernames}"]\'')
            result = subprocess.check_output(f"~/platform_venv/bin/meltano config {self.config['meltano_tap']} set {k} \'{self.tap_config[k]}\'", cwd=f"{self.current_directory}/test-meltalo", shell=True)
            self.logger.debug(f"{k} \'{self.tap_config[k]}\' set successcully")
            

    def save_data_to_path(self, load_info, uri, dataset=None, **kwargs): 
        destination_folder_path = f"{self.current_directory}/loader_data_destinations"        
        _config_target = {
            "parquet": {
                "loader": "target-parquet",
                "config": {
                "destination_path": destination_folder_path
                }
            },"csv":{
                "loader":"target-csv",
                "config":{
                "destination_path": destination_folder_path
                }
            },"json":{
                "loader":"target-jsonl",
                "config":{
                "destination_path": destination_folder_path
                }
            }
            }

        minute = load_info['instance_ts']

        destination = destinations.DestinationProtocol.get_object_from_uri(uri, self)
        destination.prepare()
        os.makedirs(destination_folder_path, exist_ok=True)
        self.logger.debug("destination folder created")

        meltano_target_format = self.config['meltano_target_format']
        _target = _config_target[meltano_target_format]
        if meltano_target_format not in ["csv", "parquet", "json"]:
            raise "please provide valid target from csv, parquet or json"
        else:
            subprocess.check_output(f"~/platform_venv/bin/meltano add loader {_target['loader']}", cwd=f"{self.current_directory}/test-meltalo", shell=True)
            self.logger.debug("loader added successcully")
            for key,val in _target['config'].items():
                subprocess.check_output(f"~/platform_venv/bin/meltano config {_target['loader']} set {key} {val}", cwd=f"{self.current_directory}/test-meltalo", shell=True)
                self.logger.debug(f"{key} set successcully")

        
        subprocess.check_output(f"~/platform_venv/bin/meltano el {self.config['meltano_tap']} {_target['loader']}", cwd=f"{self.current_directory}/test-meltalo", shell=True)
        self.logger.debug("elt completed successcully")
        
       
        all_files = []
        for root, dirs, files in os.walk(destination_folder_path):
            for file in files:
                full_path = os.path.join(root, file)
                all_files.append(full_path)

        for file in all_files:
            destination.append_data(file)
        destination.finish()



if __name__ == '__main__':
    treldev.Sensor.init_and_run(MeltanoWrapper)
