import os
import boto3
import environ
from pathlib import PurePath

env = environ.Env()
environ.Env.read_env()

AWS_ACCESS_KEY_ID = env('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = env('AWS_SECRET_KEY')
BUCKET = env('BUCKET_NAME')
BUCKET_SUBFOLDER = env('BUCKET_PREFIX')
LOCATION_DIR = env('LOCATION_DIR')

# Define save_file function before it's called in download_files_from_s3 function
def save_file(location_dir_path, file_name, content):
    file_dir_path = PurePath(location_dir_path, file_name)
    dir_path = os.path.dirname(file_dir_path)
    
    #Check if the file exists, else create it
    
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(file_dir_path, 'wb') as f:
        for chunk in content.iterate_chunks(chunk_size=4096):
            f.write(chunk)

def download_files_from_s3(bucket_name, prefix_name):
    client = boto3.client(
        's3',
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    )
    # Return a list of all the objects in the bucket!
    # It should also include files and folders
    
    obj_list = client.list_objects_v2(
        Bucket= bucket_name,
        Prefix= prefix_name
    )
    
    for obj in obj_list['Contents']:
        response = client.get_object(
            Bucket=bucket_name,
            Key=obj['Key'],
        )

        if 'application/x-directory' not in response['ContentType']:
            save_file(LOCATION_DIR, obj['Key'], response['Body'])

if __name__ == '__main__':
    download_files_from_s3(BUCKET, BUCKET_SUBFOLDER)
