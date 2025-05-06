import boto3
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        source_key = record['s3']['object']['key']

        if not source_key.endswith('.csv'):
            print(f"Arquivo ignorado: {source_key}")
            continue

        destination_bucket = "timesync-backup-841051091018312111099"
        destination_key = source_key

        try:
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }

            s3_client.copy_object(
                Bucket=destination_bucket,
                Key=destination_key,
                CopySource=copy_source
            )

            print(f"Arquivo copiado: {source_key} -> {destination_bucket}/{destination_key}")

        except Exception as e:
            print(f"Erro ao copiar arquivo {source_key}: {str(e)}")
            raise e
