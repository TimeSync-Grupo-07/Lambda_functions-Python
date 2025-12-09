import boto3
import os
import urllib.parse

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        # Processa apenas arquivos JSON
        if not source_key.lower().endswith(".json"):
            print(f"Ignorado (não é JSON): {source_key}")
            continue

        # Buckets definidos por variáveis de ambiente
        backup_bucket = os.environ["BACKUP_BUCKET"]
        raw_bucket = os.environ["RAW_BUCKET"]

        copy_source = {"Bucket": source_bucket, "Key": source_key}

        # Copiar para RAW
        try:
            s3.copy_object(
                Bucket=raw_bucket,
                Key=source_key,
                CopySource=copy_source
            )
            print(f"Copiado para RAW: {source_key} -> s3://{raw_bucket}/{source_key}")
        except Exception as e:
            print(f"Erro ao copiar para RAW: {e}")
            raise e

        # Copiar para BACKUP
        try:
            s3.copy_object(
                Bucket=backup_bucket,
                Key=source_key,
                CopySource=copy_source
            )
            print(f"Copiado para BACKUP: {source_key} -> s3://{backup_bucket}/{source_key}")
        except Exception as e:
            print(f"Erro ao copiar para BACKUP: {e}")
            raise e
