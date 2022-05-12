import treldev.awsutils

if __name__ == '__main__':
    args = treldev.get_args()
    treldev.SQLExecutor.set_args(args)
    
    input_ = list(args['inputs'].values())[0][0]
    output = list(args['outputs'].values())[0][0]

    input_s3_uri = treldev.awsutils.S3URI(input_['uri'])
    output_athena_uri = treldev.awsutils.AthenaURI(output['uri'])

    output_athena_uri.create_from_s3_uri(input_s3_uri, schema=output['schema'])
