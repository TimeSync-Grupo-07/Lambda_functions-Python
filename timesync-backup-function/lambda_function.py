import boto3
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        source_key = record['s3']['object']['key']

        # Alterado para filtrar JSON em vez de CSV
        if not source_key.endswith('.json'):
            print(f"Arquivo ignorado: {source_key}")
            continue

        # Usando as variáveis de ambiente definidas no Terraform
        destination_bucket = os.environ['BACKUP_BUCKET']
        raw_bucket = os.environ['RAW_BUCKET']

        # Mantém a mesma estrutura de chave no bucket de backup
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

            print(f"Arquivo JSON copiado: {source_key} -> {destination_bucket}/{destination_key}")

        except Exception as e:
            print(f"Erro ao copiar arquivo {source_key}: {str(e)}")
            raise e
        
