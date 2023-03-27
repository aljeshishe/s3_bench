import time

import boto3
import os
import threading

# Define the S3 bucket and object key for the file
bucket_name = 'tmp-grachev'
object_key = 'random10g'

session = boto3.Session(profile_name='abml')

# Define the number of threads to use for downloading the file
num_threads = 10

# Define a function to download a chunk of the file
def download_chunk(start_byte, end_byte, buffer):
    s3 = session.client('s3')
    range_header = f"bytes={start_byte}-{end_byte}"
    response = s3.get_object(Bucket=bucket_name, Key=object_key, Range=range_header)
    data = response['Body'].read()
    # buffer[start_byte:end_byte+1] = data

# Get the total size of the file
s3 = session.client('s3')
response = s3.head_object(Bucket=bucket_name, Key=object_key)
file_size = response['ContentLength']

# Create a buffer to hold the file data
# buffer = bytearray(file_size)
buffer = None

# Divide the file into chunks and download them in parallel
start_time = time.time()
chunk_size = file_size // num_threads
threads = []
for i in range(num_threads):
    start = i * chunk_size
    end = start + chunk_size - 1
    if i == num_threads - 1:
        end = file_size - 1
    thread = threading.Thread(target=download_chunk, args=(start, end, buffer))
    thread.start()
    threads.append(thread)

# Wait for all threads to finish
for thread in threads:
    thread.join()
end_time = time.time()


# Save the downloaded file to disk
#with open('downloaded-file', 'wb') as f:
#    f.write(buffer)

download_time = end_time - start_time
download_speed = file_size / download_time / 1024 / 1024 # Convert to MB/s
print(f"Downloaded {file_size} bytes in {download_time} seconds")
print(f"Download speed: {download_speed} MB/s")
