import os

import boto3

import pandas as pd

import io

import uuid

# import the LaunchDarkly SDK client
import ldclient
from ldclient.config import Config

# Initialize the S3 client
s3 = boto3.client('s3')

# Initialize the LaunchDarkly SDK client
sdkKey = os.environ.get('sdkKey')
ldclient.set_config(Config(sdkKey))
client = ldclient.get()

def lambda_handler(event, context):
    from ldclient import Context
    
    # generate a random UUID for the user context
    uuidValue = uuid.uuid4()
    contextKey = str(uuidValue)
    context = Context.builder(contextKey).name("Random Test Context").build()
    
    # evaluate the feature flag
    flagKey = os.environ.get('flagKey')
    flagValue = client.variation(flagKey, context, 100)

    # Specify the S3 bucket and object key of the CSV file
    bucketName = os.environ.get('bucketName')
    if flagValue == '100':
        fileKey = os.environ.get('fileKey1')
    elif flagValue == '1000':
        fileKey = os.environ.get('fileKey2')
    elif flagValue == '10000':
        fileKey = os.environ.get('fileKey3')

    try:
        # Read the CSV file from S3
        response = s3.get_object(Bucket=bucketName, Key=fileKey)
        csvContent = response['Body'].read().decode('utf-8')

        # Create a Pandas DataFrame
        df = pd.read_csv(io.StringIO(csvContent))

        # Now you have your DataFrame (df) for further processing
        # Example: Print the first 5 rows
        print(df.head(5))

        return {
            'statusCode': 200,
            'body': 'File read successfully into DataFrame.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }