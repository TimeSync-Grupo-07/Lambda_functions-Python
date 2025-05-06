import pandas as pd
import boto3
from io import StringIO

def formatar_csv(df):
    # Aplicar strip e title em colunas de texto
    for coluna in df.select_dtypes(include=['object']).columns:
        df[coluna] = df[coluna].str.strip()
        df[coluna] = df[coluna].str.title()

    # Tentar converter colunas para data
    for coluna in df.columns:
        try:
            df[coluna] = pd.to_datetime(df[coluna], errors='raise')
            df[coluna] = df[coluna].dt.strftime('%Y-%m-%d')
            print(f"Coluna '{coluna}' formatada como data.")
        except Exception:
            pass

    return df

def lambda_handler(event, context):
    # Obter nomes dos buckets das variáveis de ambiente
    bucket_raw = 'timesync-raw-841051091018312111099'
    bucket_trusted = 'timesync-trusted-841051091018312111099'
    
    s3 = boto3.client('s3')
    
    print(f"Evento recebido: {event}")

    object_key = event['file_key']

    print(f"Processando arquivo: {object_key} do bucket {source_bucket}")

    try:
        # Baixar CSV do bucket RAW
        response = s3.get_object(Bucket=bucket_raw, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        # Ler CSV no pandas
        df = pd.read_csv(StringIO(csv_content))
        # Formatar o CSV
        df_formatado = formatar_csv(df)
        # Salvar em memória como CSV
        csv_buffer = StringIO()
        df_formatado.to_csv(csv_buffer, index=False)
        # Enviar para o bucket TRUSTED
        s3.put_object(
            Bucket=bucket_trusted,
            Key=object_key,
            Body=csv_buffer.getvalue()
        )
        print(f"Arquivo '{object_key}' formatado e enviado para o bucket Trusted!")

    except Exception as e:
        print(f"Erro ao processar o arquivo {object_key}: {str(e)}")
        raise e

    return {
        'statusCode': 200,
        'body': 'Processamento concluído com sucesso.'
    }