import boto3
import requests
import sys
import logging
from datetime import date
from botocore.exceptions import ClientError


def download_file(args):
    if len(args) != 2:
    	raise Exception("Parametros insuficientes")
    s3_filename = args[0]
    url = args[1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(s3_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return s3_filename


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
    
    
def main(args):
	file = download_file(args) 
	s3 = boto3.client("s3")
	with open(file, "rb") as f:
	    s3.upload_fileobj(f, "p3-tet-prod-us-east-1", "Raw/"+file)
	
	

today = date.today()
d1 = today.strftime("%d-%m-%Y")
main(["datos" + d1 + ".csv", "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"])
