'''
 Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 SPDX-License-Identifier: MIT-0
'''
'''
Modified by srlixin@gmail.com for AWS lambda map-reduce test.
add batch_creator_for_ny_trip and find_prev_new_line_position to support devide large files to small parts
'''

import boto3
import botocore
import os
import math

class LambdaManager(object):
    def __init__ (self, l, s3, region, codepath, job_id, fname, handler,layers, lmem=3008):
        self.awslambda = l
        self.region = "us-east-1" if region is None else region
        self.s3 = s3
        self.codefile = codepath
        self.job_id = job_id
        self.function_name = fname
        self.handler = handler
        self.role = os.environ.get('serverless_mapreduce_role')
        self.memory = lmem 
        self.timeout = 900
        self.function_arn = None # set after creation
        self.layers = layers


    # TracingConfig parameter switches X-Ray tracing on/off.
    # Change value to 'Mode':'PassThrough' to switch it off
    def create_lambda_function(self):
        runtime = 'python3.6'
        response = self.awslambda.create_function(
                      FunctionName = self.function_name, 
                      Code = { 
                        "ZipFile": open(self.codefile, 'rb').read()
                      },
                      Handler =  self.handler,
                      Role =  self.role, 
                      Runtime = runtime,
                      Description = self.function_name,
                      MemorySize = self.memory,
                      Timeout =  self.timeout,
                      Layers = self.layers,
                      TracingConfig={'Mode':'PassThrough'}
                    )
        self.function_arn = response['FunctionArn']
        print(response)

    def update_function(self):
        '''
        Update lambda function
        '''
        response = self.awslambda.update_function_code(
                FunctionName = self.function_name, 
                ZipFile=open(self.codefile, 'rb').read(),
                Publish=True
                )
        updated_arn = response['FunctionArn']
        # parse arn and remove the release number (:n) 
        arn = ":".join(updated_arn.split(':')[:-1])
        self.function_arn = arn 
        print(response)

    def update_code_or_create_on_noexist(self):
        '''
        Update if the function exists, else create function
        '''
        try:
            self.create_lambda_function()
        except botocore.exceptions.ClientError as e:
            # parse (Function already exist) 
            self.update_function()

    def add_lambda_permission(self, sId, bucket):
        resp = self.awslambda.add_permission(
          Action = 'lambda:InvokeFunction', 
          FunctionName = self.function_name, 
          Principal = 's3.amazonaws.com', 
          StatementId = '%s' % sId,
          SourceArn = 'arn:aws:s3:::' + bucket
        )
        print(resp)

    def create_s3_eventsource_notification(self, bucket, prefix=None):
        if not prefix:
            prefix = self.job_id +"/task";

        self.s3.put_bucket_notification_configuration(
          Bucket =  bucket, 
          NotificationConfiguration = { 
            'LambdaFunctionConfigurations': [
              {
                  'Events': [ 's3:ObjectCreated:*'],
                  'LambdaFunctionArn': self.function_arn,
                   'Filter' : {
                    'Key':    {
                        'FilterRules' : [
                      {
                          'Name' : 'prefix',
                          'Value' : prefix
                      },
                    ]
                  }
                }
              }
            ],
            #'TopicConfigurations' : [],
            #'QueueConfigurations' : []
          }
        )

    def delete_function(self):
        self.awslambda.delete_function(FunctionName=self.function_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        '''
        Delete all Lambda log group and log streams for a given function

        '''
        log_client = boto3.client('logs')
        #response = log_client.describe_log_streams(logGroupName='/aws/lambda/' + func_name)
        response = log_client.delete_log_group(logGroupName='/aws/lambda/' + func_name)
        return response

def compute_batch_size(keys, lambda_memory, concurrent_lambdas):
    max_mem_for_data = 0.6 * lambda_memory * 1000 * 1000; 
    size = 0.0
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            size += key.size
    avg_object_size = size/len(keys)
    print("max mem : %s" %(max_mem_for_data))
    print("Dataset size: %s, nKeys: %s, avg: %s" %(size, len(keys), avg_object_size))
    if avg_object_size < max_mem_for_data and len(keys) < concurrent_lambdas:
        b_size = 1
    else:
        b_size = int(round(max_mem_for_data/avg_object_size))
    return b_size

def batch_creator_for_ny_trip(keys, lambda_memory, concurrent_lambdas):
    '''
        for ny trip large files, divied large files into smaller chunk to fit in one map
    '''
    max_mem_for_data = 0.5 * lambda_memory * 1000 * 1000 
    # this is the biggest max mem for one mapper (lamabda size 3008) tested..
    max_mem_for_data = 150000000
    batches = []
    


    for key in keys:
        batch = []
        if key.size <= max_mem_for_data:
            batch.append({"key":key,"range":"bytes=0-"})
            batches.append(batch)
        else:
            split_num = math.ceil(key.size/max_mem_for_data)

            range_start = 0
            range_end = int(max_mem_for_data)
            for i in range(split_num):
                
                range_end_fix = find_prev_new_line_position(key, range_end, 300)

                byte_range = "bytes=" + str(range_start) + "-" + ("" if range_end_fix==0 else str(range_end_fix))
                print(byte_range)
                batch.append({"key":key, "range": byte_range})
                batches.append(batch)

                range_start = range_end_fix
                range_end = 0 if i == split_num-2 else range_start + int(max_mem_for_data)
                batch=[]

    return batches

def find_prev_new_line_position(key, position, seek_range):
    '''
        given a position of a file, find the nearest new line position in the range before given position, return 0 if found none
    '''

    start_search_position = position - seek_range if position >= seek_range else 0
    Bytes_range = "bytes=" + str(start_search_position) + "-" + str(position) 
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=key.bucket_name,Key=key.key, Range=Bytes_range)
    contents = response['Body'].read().decode('utf-8')

    offset = 0
    for i in range(len(contents)):
        if contents[len(contents)-i-1] == '\n':
            offset = i
            break

    return position - offset


def batch_creator(all_keys, batch_size):
    '''
    '''
    # TODO: Create optimal batch sizes based on key size & number of keys

    batches = []
    batch = []
    for i in range(len(all_keys)):
        batch.append(all_keys[i]);
        if (len(batch) >= batch_size):
            batches.append(batch)
            batch = []

    if len(batch):
        batches.append(batch)
    return batches
