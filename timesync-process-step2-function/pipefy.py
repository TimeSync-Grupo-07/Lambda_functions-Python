import boto3
import pandas as pd
import json

def buscar_arquivo_pipefy_json(bucket_name):
    s3 = boto3.client('s3')
    prefix = 'pipefy/'
    arquivo_procurado = 'pipefy.json'
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    arquivos_encontrados = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith(arquivo_procurado):
            arquivos_encontrados.append(key)
    return arquivos_encontrados


def json_s3_para_csv(bucket_name, key, csv_path):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = json.load(obj['Body'])
    # Se o JSON for uma lista de objetos:
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)
    print(f'Arquivo salvo em {csv_path}')


def enviar_csv_para_trusted(bucket_trusted, csv_path, key_destino):
    s3 = boto3.client('s3')
    s3.upload_file(csv_path, bucket_trusted, key_destino)
    print(f'CSV enviado para s3://{bucket_trusted}/{key_destino}')


# Exemplo de uso para buscar o arquivo JSON do Pipefy:
bucket_raw = 'timesync-raw-841051091018312111099'
print(buscar_arquivo_pipefy_json(bucket_raw))

# Exemplo de uso para buscar o arquivo JSON do Pipefy e convertê-lo para CSV:
arquivos = buscar_arquivo_pipefy_json(bucket_raw)
if arquivos:
    json_s3_para_csv(bucket_raw, arquivos[0], 'pipefy.csv')


    
# Exemplo de uso para enviar o CSV para o bucket TRUSTED:
bucket_trusted = 'timesync-trusted-841051091018312111099'
enviar_csv_para_trusted(bucket_trusted, 'pipefy.csv', 'pipefy/pipefy.csv')
