import boto3
import os

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = record["s3"]["object"]["key"]

        # Apenas arquivos JSON
        if not source_key.endswith(".json"):
            print(f"Arquivo ignorado (não é JSON): {source_key}")
            continue

        # Buckets definidos no Terraform
        raw_bucket = os.environ["RAW_BUCKET"]
        trusted_bucket = os.environ["TRUSTED_BUCKET"]

        # O destino manterá o mesmo path/nome
        destination_key = source_key

        try:
            copy_source = {
                "Bucket": source_bucket,
                "Key": source_key
            }

            s3.copy_object(
                Bucket=trusted_bucket,
                Key=destination_key,
                CopySource=copy_source
            )

            print(f"[OK] JSON movido do RAW para TRUSTED:")
            print(f"     {source_bucket}/{source_key} -> {trusted_bucket}/{destination_key}")

        except Exception as e:
            print(f"[ERRO] Falha ao mover {source_key} para TRUSTED: {str(e)}")
            raise e
