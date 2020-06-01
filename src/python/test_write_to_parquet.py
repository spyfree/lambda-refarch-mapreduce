import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    data = {0: {"data1": "value1"}}
    df = pd.DataFrame.from_dict(data, orient='index')
    write_pandas_parquet_to_s3(
        df, "bucket", "trip-ny-test/test_parquet/file.parquet", "test.parquet")


def write_pandas_parquet_to_s3(df, bucketName, keyName, fileName):
    # dummy dataframe
    table = pa.Table.from_pandas(df)
    pq.write_table(table, fileName)

    # upload to s3
    s3 = boto3.client("s3")
    BucketName = bucketName
    with open(fileName) as f:
       object_data = f.read()
       s3.put_object(Body=object_data, Bucket=BucketName, Key=keyName)


if __name__ == "__main__":
    main()
