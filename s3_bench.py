import boto3
import time
import os

# Set up S3 client
session = boto3.Session(profile_name='abml')
s3 = session.client('s3')

# Define S3 bucket and file to download
bucket_name = 'tmp-grachev'
object_key = 'random10g'

# Define local file path to save the downloaded file
local_file_path = '1670782505139937.data'

# Download the file and measure the download speed
start_time = time.time()

s3.download_file(bucket_name, object_key, local_file_path, Config=boto3.s3.transfer.TransferConfig(max_concurrency=20))

end_time = time.time()
download_time = end_time - start_time
file_size = os.path.getsize(local_file_path)
download_speed = file_size / download_time / 1024 / 1024 # Convert to MB/s
print(f"Downloaded {file_size} bytes in {download_time} seconds")
print(f"Download speed: {download_speed} MB/s")

