'''
Python reducer function

Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''
'''
Modified by srlixin@gmail.com for AWS lambda map-reduce test.
This reducer function takes in multiple files which are mapper phase outputs , writes back to one parquet file in s3
'''

import boto3
import json
import random
import resource
from io import StringIO
import time
import awswrangler as wr
import pandas as pd


# create an S3 & Dynamo session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# constants
TASK_MAPPER_PREFIX = "task/mapper/";
TASK_REDUCER_PREFIX = "task/reducer/";

def write_to_s3(bucket, key, data, metadata):
    # Write to S3 Bucket
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

def write_pandas_parquet_to_s3(df, bucketName, keyName):

    path = "s3://mapreduce-lambda-ny-trip/parquet_test/" + str(keyName)
    # dummy dataframe
    wr.s3.to_parquet(
        df=df,
        path=path
    )


def lambda_handler(event, context):
    
    start_time = time.time()
    
    job_bucket = event['jobBucket']
    bucket = event['bucket']
    reducer_keys = event['keys']
    job_id = event['jobId']
    r_id = event['reducerId']
    step_id = event['stepId']
    n_reducers = event['nReducers']
    
    # aggr 
    results = {}
    line_count = 0

    final_df = pd.DataFrame()

    # INPUT CSV => OUTPUT PARQUET

    # Download and process all keys
    for key in reducer_keys:
        response = s3_client.get_object(Bucket=job_bucket, Key=key)
        contents = response['Body'].read().decode('utf-8') 

        data =  contents.split('\n')[1:-1]
        df = pd.DataFrame(data,columns=['row'])
        #print(df.shape)
        df[['row_number','VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','pickup_longitude','pickup_latitude','RatecodeID','store_and_fwd_flag','dropoff_longitude','dropoff_latitude','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','LocationID']] = df.row.str.split(",",expand=True)
        df.drop(['row','row_number'],axis=1,inplace=True)

        #type convert
        df['VendorID'] = pd.to_numeric(df['VendorID'])
        df['passenger_count'] = pd.to_numeric(df['passenger_count'])
        df['trip_distance'] = pd.to_numeric(df['trip_distance'])
        df['pickup_latitude'] = pd.to_numeric(df['pickup_latitude'])
        df['pickup_longitude'] = pd.to_numeric(df['pickup_longitude'])

        df['RatecodeID'] = pd.to_numeric(df['RatecodeID'])
        df['dropoff_longitude'] = pd.to_numeric(df['dropoff_longitude'])
        df['dropoff_latitude'] = pd.to_numeric(df['dropoff_latitude'])      

        df['fare_amount'] = pd.to_numeric(df['fare_amount'])
        df['extra'] = pd.to_numeric(df['extra'])
        df['mta_tax'] = pd.to_numeric(df['mta_tax'])  
        df['tip_amount'] = pd.to_numeric(df['tip_amount'])
        df['tolls_amount'] = pd.to_numeric(df['tolls_amount'])
        df['improvement_surcharge'] = pd.to_numeric(df['improvement_surcharge'])  
        df['total_amount'] = pd.to_numeric(df['total_amount']) 
        df['LocationID'] = pd.to_numeric(df['LocationID']) 


        #union back to final
        final_df = pd.concat([df, final_df])

        # try:
        #     for pickup_date, val in json.loads(contents).items():
        #         line_count +=1

        #         if pickup_date not in results:
        #             results[pickup_date] = {'total_trip_num':0}

        #         results[pickup_date]['total_trip_num'] += val['total_trip_num']
        #except Exception as e:
        #    print(e)
    
    #df = pd.DataFrame(results, columns =['date', 'total_trip_num'], dtype = float)

    time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9 
    #s3DownloadTime = 0
    #totalProcessingTime = 0 
    line_count = final_df.shape[0]
    pret = [len(reducer_keys), line_count, time_in_secs]
    print("Reducer ouputput", pret)

    if n_reducers == 1:
        # Last reducer file, final result
        fname = "%s/result" % job_id
    else:
        fname = "%s/%s%s/%s" % (job_id, TASK_REDUCER_PREFIX, step_id, r_id)
    
    metadata = {
                    "linecount":  '%s' % line_count,
                    "processingtime": '%s' % time_in_secs,
                    "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
               }

    #write_to_s3(job_bucket, fname, json.dumps(results), metadata)

    write_pandas_parquet_to_s3(final_df,'mapreduce-lambda-ny-trip',r_id)
    return pret

'''
ev = {
    "bucket": "-useast-1",
    "jobBucket": "-useast-1",
    "jobId": "py-biglambda-1node-3",
    "nReducers": 1,
    "keys": ["py-biglambda-1node-3/task/mapper/1"],
    "reducerId": 1, 
    "stepId" : 1
}
lambda_handler(ev, {});
'''
