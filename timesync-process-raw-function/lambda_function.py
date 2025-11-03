import os
import mysql.connector
import pandas as pd
import boto3
import io

def lambda_handler(event, context):
    """
    Esta função é executada quando um arquivo é salvo no bucket TRUSTED.
    Ela extrai os dados, realiza análises e salva no MySQL.
    """

    trusted_bucket = os.environ.get('TRUSTED_BUCKET')

    db_config = {
        'host': os.environ.get('MYSQL_HOST'),
        'user': os.environ.get('MYSQL_USER'),
        'password': os.environ.get('MYSQL_PASSWORD'),
        'database': os.environ.get('MYSQL_DB')
    }

    s3 = boto3.client('s3')

    print(f"Evento recebido: {event}")

    for record in event.get('Records', []):
        object_key = record['s3']['object']['key']
        bucket_name = record['s3']['bucket']['name']

        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))

        print(f"Arquivo lido com sucesso. Linhas: {len(df)}")

        # TODO: Lógica para enviar ao MySQL
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            for _, row in df.iterrows():
                # Exemplo: ajuste para seu schema real
                cursor.execute("INSERT INTO tabela (coluna1, coluna2) VALUES (%s, %s)", (row['coluna1'], row['coluna2']))

            conn.commit()
            cursor.close()
            conn.close()

            print("Dados inseridos com sucesso no banco MySQL.")

        except Exception as e:
            print(f"Erro ao conectar/inserir no MySQL: {e}")

    return {
        'statusCode': 200,
        'body': 'Processamento do bucket TRUSTED concluído com sucesso.'
    }
