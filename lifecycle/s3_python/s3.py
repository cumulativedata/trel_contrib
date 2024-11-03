import json, time, sys, yaml, boto3, tempfile, os, datetime
import multiprocessing.pool
from treldev import get_args, S3Commands

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--lifecycle.actions", dest='input_path')
    parser.add_argument("--lifecycle.actions.complete", dest='output_path')
    parser.add_argument("--_args")
    args, _ = parser.parse_known_args()
    return args

def do_action(s3_action, credentials):
    s3_handler = S3Commands(credentials= credentials)
    # fill in before_state, after_state, action_completed_ts (if SUCCESS) , error_message (if FAILED)
    if s3_action['action_requested'] == 'delete':
        _,_,bucket, prefix = s3_action['uri'].split('/',3)
        s3_bucket = s3_handler.s3r.Bucket(bucket)
        empty = True
        for s3_object in s3_bucket.objects.filter(Prefix=prefix):
            print("Deleting ",s3_object.key)
            s3_object.delete()
            s3_object.wait_until_not_exists()
            empty = False
        s3_action['action_completed_ts'] = str(datetime.datetime.utcnow())
        s3_action['before_state'] = [ ('empty' if empty else 'not_empty') ]
        s3_action['after_state'] = ['empty']
    else:
        s3_action['before_state'] = []
        s3_action['after_state'] = []
        s3_action['error_message'] = s3_action['action_requested'] + " is not a valid action"
    return s3_action


def process_action(s3_action, credentials):
    return do_action(s3_action, credentials)


def main():
    args = get_args()
    start_time = time.time()
    pool = multiprocessing.pool.Pool(processes=100)
    fd, filename = tempfile.mkstemp()
    output_folder = tempfile.mkdtemp()

    input_path = list(args['inputs'].values())[0][0]['uri']
    output_path = list(args['outputs'].values())[0][0]['uri']

    s3_handler = S3Commands(credentials= args['credentials'])
    
    # s3 = boto3.resource('s3')
    _,_,bucket, prefix = input_path.split('/',3)
    _,_,output_bucket, output_prefix = output_path.split('/',3)
    res = []
    i = 0
    s3_output_bucket = s3_handler.s3r.Bucket(output_bucket)
    s3_bucket = s3_handler.s3r.Bucket(bucket)
    for s3_object in s3_bucket.objects.filter(Prefix=prefix):
        print("Processing action file w prefix "+ s3_object.key)
        s3_bucket.download_file(s3_object.key,filename)
        subset = []
        with open(filename) as f:
            for line in f:
                subset.append( json.loads(line) )
                if len(subset) == 10000:
                    print("Asking pool to process a subset of length {}".format(len(subset)))
                    res += pool.starmap(process_action, [(action, args['credentials']) for action in subset])
                    subset.clear()
        print("Asking pool to process a subset of length {}".format(len(subset)))
        res += pool.starmap(process_action, [(action, args['credentials']) for action in subset])
        output_filename = 'part-{:05d}'.format(i)
        full_output_filename = os.path.join(output_folder, output_filename)
        with open(full_output_filename,'w') as f:
            for v in res:
                f.write(json.dumps(v))
                f.write('\n')
        s3_output_bucket.upload_file(full_output_filename,output_prefix+output_filename)
        os.close(fd)
        os.system("rm {0}".format(full_output_filename))
        i += 1
    print("Execution took {} seconds".format(time.time() - start_time))

def test(temp_s3_path, num_paths=20, num_files=10):
    temp_folder = tempfile.mkdtemp()
    # make some files
    os.system("""for fname in `echo | py -l '[ ("part_"+str(i)) for i in range({num_files})]'`; do touch {temp_folder}/$fname; done""".format(**locals()))
    # upload them in 10 places
    res = []
    for i in range(num_paths):
        prefix = "{temp_s3_path}data_{i}/".format(**locals())
        os.system("aws s3 cp --recursive {temp_folder} {prefix}".format(**locals()))
        res.append({'uri':prefix,'action_requested':'delete'})
        
    temp_folder2 = tempfile.mkdtemp()
    #put the places in a file
    with open(os.path.join(temp_folder2,'part-00000'),'w') as f:
        for d in res:
            json.dump(d,f)
            f.write('\n')
    # upload the file
    os.system("aws s3 cp --recursive {temp_folder2} {temp_s3_path}input/".format(**locals()))
    # call the script
    cmd = ("time python3 delete_s3.py --_lifecycle.actions {temp_s3_path}input/ --_lifecycle.actions.complete {temp_s3_path}output/ --_args 1".format(**locals()))
    print(cmd)
    os.system(cmd)
        
if __name__ == '__main__':
    main()

'''
    # 27 seconds with (300,10) ThreadPool


'''
