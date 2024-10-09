import argparse, os, sys
import treldev, pyodbc, tempfile, json, datetime, subprocess
from os import listdir
from os.path import isfile, join, isdir


class GoogleSheetSensor(treldev.ClockBasedSensor):

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
            # result = subprocess.run(['pwd'], capture_output=True, text=True, check=True)
            self.current_directory = os.getcwd()
            self.logger.debug(f"Current working directory: {self.current_directory}")
            subprocess.check_output(f"~/platform_venv/bin/meltano init test-meltalo", shell=True)
            self.logger.debug("meltano project created successfully")
            result = subprocess.check_output(f"~/platform_venv/bin/meltano add extractor {self.config['meltano_tap']}", cwd=f"{self.current_directory}/test-meltalo", shell=True)
            self.logger.debug(f"Extractor {config['meltano_tap']} added successfully")
        except Exception as e:
            raise

        if 'tap_credentials' in self.config:
            self.tap_creds = self.config["tap_credentials"]
            for i in self.tap_creds:
                tap_creds = json.loads(self.credentials[i])
                cred_val = tap_creds[self.tap_creds[i]]
                setattr(self, self.tap_creds[i], tap_creds[self.tap_creds[i]])
                subprocess.check_output(f"~/platform_venv/bin/meltano config {self.config['meltano_tap']} set {self.tap_creds[i]} \'{cred_val}\'", cwd=f"{self.current_directory}/test-meltalo", shell=True)
                self.logger.debug(f"{self.tap_creds[i]} set successcully")
        else:
            self.auth_token = self.config['auth_token']
            self.user_usernames = self.config['user_usernames']


        for k in self.tap_config:
            setattr(self, k, self.tap_config[k])
            self.logger.debug(f"~/platform_venv/bin/meltano config {config['meltano_tap']} set user_usernames \'{self.tap_config[k]}\'")
            # self.logger.debug(f'~/platform_venv/bin/meltano config tap-github set user_usernames \'["{self.user_usernames}"]\'')
            result = subprocess.check_output(f"~/platform_venv/bin/meltano config {self.config['meltano_tap']} set {k} \'{self.tap_config[k]}\'", cwd=f"{self.current_directory}/test-meltalo", shell=True)
            self.logger.debug(f"{k} \'{self.tap_config[k]}\' set successcully")
            

    def save_data_to_path(self, load_info, uri, dataset=None, **kwargs):
        
        minute = load_info['instance_ts']
        destination_obj = destinations.DestinationProtocol(uri, self,original_file_name = True)

        destination = destination_obj.get_object_from_uri(uri, self)
        destination.prepare()
        
        subprocess.check_output('~/platform_venv/bin/meltano add loader target-parquet', cwd=f"{self.current_directory}/test-meltalo", shell=True)
        self.logger.debug("loader added successcully")

        # subprocess.check_output('~/platform_venv/bin/meltano add loader target-parquet', cwd=f"{self.current_directory}/test-meltano", shell=True)
        # self.logger.debug("loader added successcully")
        # subprocess.check_output(f'~/platform_venv/bin/meltano config target-parquet set compression_method gzip', cwd=f"{self.current_directory}/test-meltalo", shell=True)
        # self.logger.debug("compression method set successcully")
        done = False
        # while not done:
        #     with tempfile.NamedTemporaryFile('w+', delete=False) as f:
        #         self.logger.debug(f"{f.name}")
        subprocess.check_output(f'~/platform_venv/bin/meltano config target-parquet set destination_path {self.current_directory}', cwd=f"{self.current_directory}/test-meltalo", shell=True)
        self.logger.debug("destination path set successcully")
        # print(f"Uploading batch {destination.get_next_batch_num()} data from {f.name} to {uri}",file=sys.stderr)
        #         destination.append_data(f.name)
        #         sys.stderr.flush()
        
        subprocess.check_output(f"~/platform_venv/bin/meltano el {self.config['meltano_tap']} target-parquet", cwd=f"{self.current_directory}/test-meltalo", shell=True)
        self.logger.debug("elt completed successcully")
        folder_path = f"{self.current_directory}/users"
        all_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                full_path = os.path.join(root, file)
                all_files.append(full_path)

        for file in all_files:
            destination.append_data(file)
        destination.finish()



if __name__ == '__main__':
    treldev.Sensor.init_and_run(GoogleSheetSensor)

        
        
        

        
        
# prepare, append_data, finish