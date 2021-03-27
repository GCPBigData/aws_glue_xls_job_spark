from datetime import datetime, timedelta, timezone
from awsglue.utils import getResolvedOptions
import pandas as pd
import boto3
import sys
import io
import sys

args = getResolvedOptions(sys.argv, ['bucket', 'path'])

bucket = args["bucket"]
path = args["path"]

s3 = boto3.client('s3')

yesterday = datetime.now(timezone.utc) - timedelta(days=1)

new_filenames = []

paginator = s3.get_paginator('list_objects_v2')

page_iterator = paginator.paginate(
    Bucket=bucket,
    Prefix=path.format(yesterday.year, yesterday.month)
)
#filter recently sent files
for page in page_iterator:
    if 'Contents' in page:
        for content in page['Contents']:
            age = datetime.now(timezone.utc)-content['LastModified']
            if (age < timedelta(days=1) and not content['Key'].endswith('/')):
                print(content['Key'])
                new_filenames.append(content['Key'])

    else:
        print('No object was found')
        print('Check bucket and path arguments')

for file in new_filenames:
    s3_output = file.replace('raw', 'clean').replace('.xls','.csv')

    obj = boto3.client('s3').get_object(Bucket=bucket, Key=file)
    data = obj['Body'].read()
    df = pd.read_excel(io.BytesIO(data),skiprows=3)

    columns=[]
    for col in df.columns:       
        col = col.replace('\n','')
        col = col.replace('-','')
        columns.append(col)
    df.columns = columns

    df['Observações'] = df['Observações'].replace('\n',';', regex=True)
    df = df.replace('\n','', regex=True)

    df.drop(df[df['REF. IMB']==0].index,axis=0,inplace=True)

    #print(df.head(),df.info())

    df.to_csv('/tmp/converted.csv', sep=',', index=False)

    boto3.resource('s3').Bucket(bucket).upload_file('/tmp/converted.csv', s3_output)

    print("Arquivo convertido com sucesso. Caminho => {}".format(s3_output))