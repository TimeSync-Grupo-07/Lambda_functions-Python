import boto3
import os

def lambda_handler(event, context):
    """
    Esta função é executada quando novos arquivos são adicionados ao bucket RAW.
    Ela pode ler o conteúdo, transformar os dados e salvá-los em outro bucket.
    """
    raw_bucket = os.environ.get('RAW_BUCKET')
    s3 = boto3.client('s3')

    print(f"Evento recebido: {event}")

    # Exemplo de leitura de arquivo enviado ao bucket
    for record in event.get('Records', []):
        source_bucket = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        print(f"Processando arquivo: {object_key} do bucket {source_bucket}")

        response = s3.get_object(Bucket=source_bucket, Key=object_key)
        data = response['Body'].read().decode('utf-8')

        # TODO: Adicionar lógica de transformação aqui

        print(f"Arquivo lido com sucesso:\n{data[:200]}")  # Mostra só os primeiros 200 caracteres

    return {
        'statusCode': 200,
        'body': 'Processamento do bucket RAW concluído com sucesso.'
    }
