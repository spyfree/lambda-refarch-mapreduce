'''
Python mapper function

Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

'''
Modified by srlixin@gmail.com for AWS lambda map-reduce test.
This mapper function takes in a partial part of csv file , doing geo spatial join and some data clean job, writes back to csv in s3
'''

import boto3
import json
import random
import resource
from io import StringIO
import time
import geopandas as gpd
from shapely.geometry import Point
import rtree 
import warnings
import pandas as pd

#rtree for test , make sure find the right rtree package

#supress geopandas Future warning
warnings.filterwarnings("ignore", category=FutureWarning)

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# constants
TASK_MAPPER_PREFIX = "task/mapper/";

def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

def lambda_handler(event, context):
    
    start_time = time.time()

    job_bucket = event['jobBucket']
    src_bucket = event['bucket']
    src_keys = event['keys']
    job_id = event['jobId']
    mapper_id = event['mapperId']
   
    # aggr 
    output = {}
    line_count = 0
    err = ''

    # need put other sbx,shx etc. files in the same directory 
    s3_shape_file = "s3://mapreduce-lambda-ny-trip/taxi_zones.shp"
    shape_df = gpd.read_file(s3_shape_file).to_crs({'init': 'epsg:4326'})
    shape_df.drop(['OBJECTID', "Shape_Area", "Shape_Leng", "borough", "zone"],
                  axis=1, inplace=True)


    # INPUT CSV => OUTPUT CSV

    # Download and process all keys
    for key in src_keys:
        key_dict = json.loads(key)
        file_key = key_dict['key']
        Bytes_range = key_dict['range']
        #Bytes_range="bytes=0-150000000" #for test in case of oom
        #control 
        response = s3_client.get_object(Bucket=src_bucket,Key=file_key, Range=Bytes_range)
        contents = response['Body'].read().decode('utf-8') 
        #print(contents)
        
        #get the month in the file name
        key_month = file_key[26:33]

        #skip the first line (csv header and splited empty new line)
        data =  contents.split('\n')[1:]
        df = pd.DataFrame(data,columns=['row'])
        print(df.shape)
        df[['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','pickup_longitude','pickup_latitude','RatecodeID','store_and_fwd_flag','dropoff_longitude','dropoff_latitude','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']] = df.row.str.split(",",expand=True)
        df.drop(['row'],axis=1,inplace=True)

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

        #some clean before join
        #filter some attr which < 0 and limit the pickup_time is in the same month
        df = df[(df.fare_amount > 0) & (df.trip_distance > 0) & (df.passenger_count < 10) & (df.tpep_pickup_datetime.str.contains(key_month))]

        #spatial join
        local_gdf = gpd.GeoDataFrame(df, crs={'init': 'epsg:4326'},geometry=[Point(xy) for xy in zip(df['pickup_longitude'], df['pickup_latitude'])])
        local_gdf = gpd.sjoin(local_gdf, shape_df, how='left', op='within')

        # more cleaning, only keep in the right geometry data
        local_gdf.drop(['geometry','index_right'],axis=1,inplace=True)
        local_gdf = local_gdf[(local_gdf.LocationID > 0)]

        print(local_gdf.shape)
        line_count=local_gdf.shape[0]
        out = local_gdf.to_csv()
        
        # for line in contents.split('\n')[1:]:
        #     line_count +=1
        #     try:
        #         data = line.split(',')
        #         pickup_date = data[1][:10]

        #         if pickup_date[:7] != key_month:
        #             continue

        #         longitude = data[5]
        #         latitude = data[6]
        #         pt = Point(float(longitude), float(latitude))
        #         re = gpd.sjoin(gpd.GeoDataFrame(crs={'init': 'epsg:4326'},geometry=[pt]),df, how='left', op='within')
        #         #print(re)
        #         zone = re['zone'][0]

        #         if zone not in output:
        #             output[zone] = {'total_trip_num':0}
        #         output[zone]['total_trip_num'] += 1
        #     except Exception as e:
        #         print(e)
        #         #err += '%s' % e

    time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9 
    #s3DownloadTime = 0
    #totalProcessingTime = 0 
    
    mapper_fname = "%s/%s%s" % (job_id, TASK_MAPPER_PREFIX, mapper_id) 
    metadata = {
                    "linecount":  '%s' % line_count,
                    "processingtime": '%s' % time_in_secs,
                    "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
               }
    pret = [len(src_keys), line_count, time_in_secs,resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, err]
    print("metadata", metadata)
    #write_to_s3(job_bucket, mapper_fname, json.dumps(output), metadata)
    write_to_s3(job_bucket, mapper_fname, out, metadata)
    return pret

'''
ev = {
   "bucket": "-useast-1", 
   "keys": ["key.sample"],
   "jobId": "pyjob",
   "mapperId": 1,
   "jobBucket": "-useast-1"
   }
lambda_handler(ev, {});
'''
