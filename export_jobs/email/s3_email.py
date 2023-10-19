import argparse, yaml, shutil, unittest, os, tempfile, json
from treldev.awsutils import S3URI
from treldev import get_args, S3Commands, SendSMTPMail

class Test(unittest.TestCase):
    def test(self):
        ''' Test the s3 report e-mailing script by asking to download a report and email it.'''
        global get_args
        with open(os.path.expanduser('~/.trel.unittest.config')) as f:
            unittest_config = yaml.safe_load(f)
        args = {'schedule_instance_ts':'2023-01-02 00:00:00',
                'parameters': {
                    'execution.additional_arguments':[
                        unittest_config['email_test_destination'],
                        'Unittest: Annual Sales Report',
                        'Hi,\n\nPFA report.\n\nThanks,\n\nAdmin',
                        '--exclude=_SUCCESS'
                    ],
                    'execution.additional_arguments_cli':[]
                },
                'credentials':{},
                'inputs':{
                    'input_dc': [{
                        'uri': 's3://trel-contrib-unittests/public/email/report/'
                    }]
                }
        }
        import subprocess, json, boto3
        credentials_data = subprocess.check_output(['trel','--output_json','credentials']).decode('utf-8')
        aws_creds = None
        for credential_string in credentials_data.split('\n'):
            if credential_string:
                credential = json.loads(credential_string)
                value = credential['value']
                args['credentials'][credential['key']] = value
                if credential['key'] == 'aws.access_key':
                    aws_creds = json.loads(value)
        assert aws_creds is not None
        
        from unittest.mock import MagicMock
        get_args = MagicMock(return_value=args)
        main()

def main():
    args = get_args()
    schedule_instance_ts = args['schedule_instance_ts']

    cli_args_list = args['parameters']['execution.additional_arguments'] + \
        args['parameters']['execution.additional_arguments_cli']

    parser = argparse.ArgumentParser()
    parser.add_argument('receiver')
    parser.add_argument('subject')
    parser.add_argument('body')
    parser.add_argument('--exclude', help='A comma separated list of files to exclude.')
    cli_args = parser.parse_args(cli_args_list)
    exclude_list = set(cli_args.exclude.split(','))

    s3_commands = S3Commands(credentials = args['credentials'])
    input_s3 = list(args['inputs'].values())[0][0]['uri']

    temp_dir = tempfile.mkdtemp()
    s3_commands.download_folder(input_s3, temp_dir)
    files_to_attach = []
    for root,dirs,files in os.walk(temp_dir):
            rel_path = os.path.relpath(root, temp_dir)
            for file_ in files:
                if file_ not in exclude_list:
                    files_to_attach.append(os.path.join(root,file_))
    print(files_to_attach)
    sendmail = SendSMTPMail(credentials=json.loads(args['credentials']['smtp']))
    sendmail.send_mail(cli_args.receiver, cli_args.subject, cli_args.body,
                       attachments = files_to_attach)

    # shutil.rmtree(temp_dir)

if __name__ == '__main__':
    main()
