import boto3
import os
import urllib.parse

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        # Apenas arquivos JSON
        if not source_key.lower().endswith(".json"):
            print(f"Ignorado: não é JSON -> {source_key}")
            continue

        # Variáveis de ambiente
        raw_bucket = os.environ["RAW_BUCKET"]
        trusted_bucket = os.environ["TRUSTED_BUCKET"]

        # Confirma que o evento veio do bucket RAW
        if source_bucket != raw_bucket:
            print(f"Ignorado: evento veio de outro bucket ({source_bucket})")
            continue

        copy_source = {
            "Bucket": source_bucket,
            "Key": source_key
        }

        # Copiar RAW → TRUSTED
        try:
            s3.copy_object(
                Bucket=trusted_bucket,
                Key=source_key,
                CopySource=copy_source
            )
            print(f"Copiado: s3://{source_bucket}/{source_key} → s3://{trusted_bucket}/{source_key}")

        except Exception as e:
            print(f"Erro ao copiar arquivo: {e}")
            raise e
