import pandas as pd
import boto3
from io import StringIO

def formatar_csv(df):
   
    for coluna in df.select_dtypes(include=['object']).columns:
        df[coluna] = df[coluna].str.strip()
        df[coluna] = df[coluna].str.title()

    
    for coluna in df.columns:
        try:
            df[coluna] = pd.to_datetime(df[coluna], errors='raise')
            df[coluna] = df[coluna].dt.strftime('%Y-%m-%d')
            print(f"Coluna '{coluna}' formatada como data.")
        except Exception:
            pass

    return df

def processar_csv_s3(bucket_raw, bucket_trusted, nome_arquivo):

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_raw, Key=nome_arquivo)
    csv_content = obj['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_content))
    df_formatado = formatar_csv(df)

    csv_buffer = StringIO()
    df_formatado.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=bucket_trusted,
        Key=nome_arquivo,
        Body=csv_buffer.getvalue()
    )

    print(f"Arquivo '{nome_arquivo}' formatado e enviado para o bucket Trusted!")


bucket_raw = 'timesync-raw-841051091018312111099'
bucket_trusted = 'timesync-trusted-841051091018312111099'
nome_arquivo = ''

processar_csv_s3(bucket_raw, bucket_trusted, nome_arquivo)
