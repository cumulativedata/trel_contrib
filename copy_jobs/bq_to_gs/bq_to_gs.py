import treldev.gcputils
import argparse, sys, json, tempfile

if __name__ == '__main__':
    args = treldev.get_args()
    treldev.SQLExecutor.set_args(args)
    bq_client = treldev.gcputils.BigQuery.get_client()

    input_ = list(args['inputs'].values())[0][0]
    output = list(args['outputs'].values())[0][0]

    input_bq_uri = treldev.gcputils.BigQueryURI(input_['uri'])
    output_gs_uri = treldev.gcputils.GSURI(output['uri'])

    output_uri = output['uri']+'part-*' if output['uri'].endswith('/') else output['uri']

    input_bq_uri.export_to_gs([output['uri']+'part-*'],
                              {'destination_format':'NEWLINE_DELIMITED_JSON'})
                            
