# Serverless Reference Architecture: MapReduce

## Why choose this framework at first place.

The nyc taxi trip ETL process require using MapReduce to accomplish it.MapReduce is a computation mechanism, which divide data process phase in mapper phase and reducer phases,sometimes with a shuffle phase. 

After the data exploration, we can get a clue what we need to do in the ETL process. Which is 
    *extract* :
        read csv data files from S3
    *transform* :
        clean some odd value, such as null or the money is a negetive value etc. 
        The most important part is to convert the GPS data(longitude,latitude) before 2016-07 to locationId, which is in consistent with the data after.
    *load* :
        load the data back to S3, which prefer to be in parquet (GZIP/snappy compressed) format.

The MapReduce traditional method is do it in a hadoop cluster. We need to spin up a hadoop EMR cluster to do the job. I once wrote a [Hive UDF](https://github.com/spyfree/hive_udf_gps_to_city/blob/master/GeoGps.java) to process the GPS data to geo location. And nowadays a Spark notebook in EMR cluster seems also to be a straightforward method to do the job. All of these methods need to spin up a EMR cluster, which is quick and environment ready. But this time i want to try a serverless way to do the MapReduce.

I found out this [project](https://github.com/awslabs/lambda-refarch-mapreduce)  in github. This serverless MapReduce reference architecture demonstrates how to use AWS Lambda in conjunction with Amazon S3 to build a MapReduce framework that can process data stored in S3. 

By leveraging this framework, you can build a cost-effective pipeline to run ad hoc MapReduce jobs. The price-per-query model and ease of use make it very suitable for data scientists and developers alike. 


## The problems and fixs and improvment

The [code directory](https://github.com/spyfree/lambda-refarch-mapreduce/tree/master/src/python)

### change Python2 to Python3
This mainly fix some print/exception syntax. 

### the origin mapper take one file at least, for large files, lambda OOM sometimes.
We need to do some split in case of large files. And the split point must be
at the newline position(so we don't get the incomplete lines).In the
[code](https://github.com/spyfree/lambda-refarch-mapreduce/blob/master/src/python/lambdautils.py),
i added two new function `batch_creator_for_ny_trip` and
`find_prev_new_line_position` to do the split caculation. And in the mapper
phase, with the *Range* parameter of `get_object` method, we can read a partial file
from S3.

### need to write as parquet file.
Parquet is a columnar storage format, with compression it can save a lot of
stroage space. In Python you need PyArrow library to read/write to parquet
files. PyArrow is not available in the Lambda Python environment. Fortunately
with Awswrangler and Lambda layer, we can create a customized environment to
use the library to write as parquet files.

### do the geo spatial join
Given the GPS point to find out the locationid is a [point in polygon problem](https://en.wikipedia.org/wiki/Point_in_polygon),which is very computationally expensive. I used the GeoPandas and Shapely library to do the job. The diffcult part here is you  need costomized Python&C environment to use these two library.
there are two steps need to be done:
1. Use a docker image to create a lambda layer zip files for most python libraries.
'''
docker run  -v $(pwd):/foo -w /foo lambci/lambda:build-python3.6 pip install -r requirements.txt --no-deps -t ${PKG_DIR}
'''

2. The rtree library need a C library called `libspatialindex_c`. To fix this,i spin up a [EC2 Linux AMI](https://aws.amazon.com/premiumsupport/knowledge-center/lambda-linux-binary-package/) which is same as the Lambda execution environment. Extract the compiled so files from this instance and added to the rtree zip package. rtree use `find_library` of ctypes.util to find the correspondent so files name, then use ctypes.CDLL to load the c types definded in the so file. To change this, just alter the rtree code to direct use the so files extracted from ec2 instance.


## the results 
I used the `yellow_tripdata_2016-01.csv` as test file, the size of the file is
1.7G, if i don't use geospatial join to process the csv files, the size of data can
be processed is roughed 0.6 of the mapper memory, and finished within one minute. if i added the geospatial join part, the data can be processed is much smaller, about 150M.

the total cost of processing this file is about $0.09.

the test result:
Reducer L 0.0146904375
Lambda Cost 0.08707021886653707
S3 Storage Cost 1.183175685686807e-05
S3 Request Cost 3.7199999999999996e-05
S3 Cost 4.9031756856868064e-05
Total Cost:  0.08711925062339394
Total Lines: 10673343



