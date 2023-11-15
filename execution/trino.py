import treldev, json, subprocess, os

class Trino(treldev.SQLExecutor):
    ''' 
Connects to Trino by wrapping credentials management.'''

    client = None
    
    @classmethod
    def import_modules(cls):
        global connect, TrinoQueryError
        # from trino.dbapi import connect
        # from trino.exceptions import TrinoQueryError
    
    @classmethod
    def get_client(cls):
        '''Returns an authenticated Trino Client object. Also saves it as cls.client'''
        cls.import_modules()
        credentials = cls.get_credentials()
        try:
            credentials = json.loads(credentials['trino'])
        except KeyError:
            raise Exception("Running Trino SQL requires trino credential, which is missing")
        cls.client = TrinoJavaClientWrapper(**credentials)
        return cls.client

class TrinoJavaClientWrapper(object):
    def __init__(self,  user, password, executable_jar, jks_path=None, jks_password=None):
        self.user = user
        self.password = password
        self.executable_jar = executable_jar
        self.jks_path = jks_path
        self.jks_password = jks_password

    def execute_sql_script(self, host, port, catalog, script_path, timeout_seconds):
        cmd = ['java','-jar', self.executable_jar,
               '--server',f'https://{host}:{port}',
               '--user', self.user, '--password',
               '--catalog', 'hive',
               '--file', script_path, '--debug']
        if self.jks_path:
            cmd.extend(['--keystore-path', self.jks_path,
                        '--keystore-password', self.jks_password])

        print(' '.join(cmd))
        process = subprocess.Popen(cmd, env=dict(os.environ, TRINO_PASSWORD=self.password))
        
        try:
            process.wait(timeout=timeout_seconds)
            return process.returncode
        except subprocess.TimeoutExpired:
            process.terminate()  # Terminate the process if it runs beyond the timeout
            process.wait()  # Optionally, wait for the process to actually terminate
            print(f"Command exceeded the {timeout_seconds} seconds timeout and was terminated.")
            return treldev.ReturnCode.TIMEOUT_ERROR  # Return a special code to indicate timeout
    

class TrinoURI(object):
    def __init__(self, uri, client=None):
        self.uri = uri
        prefix,_,host_port,self.catalog, self.schema, self.table = uri.split('/')
        if prefix != 'trino:':
            raise Exception(f"Given uri {uri} should start with trino://")
        self.host, self.port = host_port.split(':')
        self.client = client
        self.path = f"{self.catalog}.{self.schema}.{self.table}"
        

    def validate(self):
        pass

    def save(self, sql):
        pass    

