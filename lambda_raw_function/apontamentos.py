import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime

def buscar_csv_apontamentos(bucket_name):
    s3 = boto3.client('s3')
    prefix = 'apontamentos/'
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    arquivos_csv = []
    
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.csv'):
            arquivos_csv.append(key)
    return arquivos_csv

def tratar_e_enviar_para_trusted(bucket_origem, file_key, bucket_destino='trusted'):
    s3 = boto3.client('s3')
    
    obj = s3.get_object(Bucket=bucket_origem, Key=file_key)
    data = obj['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(data))

    mapeamento_colunas = {
        'ID': 'id',
        'Nome': 'nome',
        'Email': 'email',
        'Detalhamento': 'detalhes',
        'Responsável → Name': 'nome_responsavel',
        'Responsável → Email': 'email_responsavel',
        'Como você se sente com nosso atendimento?': 'opiniao',
        'Como acredita que possamos melhorar?': 'sugestao',
        'DataHora': 'data_hora_abertura',
        'DataHora fechamento': 'data_hora_fechamento'
    }
    df = df.rename(columns={k: v for k, v in mapeamento_colunas.items() if k in df.columns})
    
    
    df = df.fillna({
        'nome_responsavel': 'Não informado',
        'email_responsavel': 'Não informado',
        'opiniao': 'Não opinou',
        'sugestao': 'Não sugeriu',
        'data_hora_fechamento': ''
    })
    

    for col in ['data_hora_abertura', 'data_hora_fechamento']:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                df[col] = df[col].astype(str)
    
    
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'])
    

    output = BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    
    nome_arquivo = file_key.split('/')[-1] 
    destino_key = f"apontamentos/{nome_arquivo}"
    

    s3.put_object(
        Bucket=bucket_destino,
        Key=destino_key,
        Body=output,
        ContentType='text/csv'
    )
    
    return {
        'arquivo_origem': file_key,
        'arquivo_destino': destino_key,
        'registros_processados': len(df),
        'bucket_destino': bucket_destino
    }


bucket_raw = 'timesync-raw-841051091018312111099'
bucket_trusted = 'timesync-trusted-841051091018312111099' 

arquivos = buscar_csv_apontamentos(bucket_raw)

if arquivos:
    resultados = []
    for arquivo in arquivos:
        print(f"Processando: {arquivo}")
        resultado = tratar_e_enviar_para_trusted(bucket_raw, arquivo, bucket_trusted)
        resultados.append(resultado)
        print(f"Enviado para: s3://{bucket_trusted}/{resultado['arquivo_destino']}")
    
    print("\nResumo do processamento:")
    for res in resultados:
        print(f"{res['arquivo_origem']} -> {res['arquivo_destino']} ({res['registros_processados']} registros)")
else:
    print("Nenhum arquivo CSV encontrado no bucket de origem.")