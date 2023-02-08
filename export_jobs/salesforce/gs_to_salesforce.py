#!/usr/bin/env python3
''' A Trel job script that can push data from Google Storage to Salesforce. See sample registration file.

In GS, provide as NEWLINE_DELIMITED_JSON. The appropriate action to take for each row is determined as follows:

1. Create: If a row has keys but not the "id" key, do a create.
2. Update: If a row has 2 or more keys and one of them is the "id" key, do an update.
3. Delete: If a row has only one key and it is the "id" key, delete it.
 '''

import treldev.gcputils
import argparse, sys, json, tempfile
import sflib

if __name__ == '__main__':
    args = treldev.get_args()
    #treldev.SQLExecutor.set_args(args)
    sf = sflib.instantiate_from_credentials_file()
    
    input_ = list(args['inputs'].values())[0][0]
    output = list(args['outputs'].values())[0][0]

    input_gs_uri = treldev.gcputils.GSURI(input_['uri'])
    cli_args_list = args['parameters']['execution.additional_arguments'] + \
        args['parameters']['execution.additional_arguments_cli']

    parser = argparse.ArgumentParser()
    parser.add_argument('--target_table', required=True)
    cli_args = parser.parse_args(cli_args_list)
    target_table = cli_args.target_table
    sf_table = getattr(sf,target_table)
    sf_table.describe() # To throw an error message quickly

    gs_client = treldev.gcputils.Storage.get_client()
    for blob in gs_client.list_blobs(input_gs_uri.bucket, prefix=input_gs_uri.key):
        with tempfile.NamedTemporaryFile('wb+', delete=True) as f:
            gs_client.download_blob_to_file(blob, f)
            f.flush()
            f.seek(0)
            for line in f:
                d = json.loads(line)
                id_key = 'id' if 'id' in d else ('Id' if 'Id' in d else None)
                if id_key is not None:
                    if len(d) == 1:
                        # delete
                        try:
                            sf_table.delete(d[id_key])
                        except:
                            print(f"Unable to delete entry {d[id_key]}", file=sys.stderr)
                    else:
                        # update
                        id_ = d[id_key]
                        del d[id_key]
                        try:
                            sf_table.update(id_, d)
                        except:
                            print(f"Unable to update entry {id_}", file=sys.stderr)
                else:
                    # insert
                    try:
                        sf_table.create(d)
                    except:
                        print(f"Unable to create/insert entry {d}", file=sys.stderr)


                
