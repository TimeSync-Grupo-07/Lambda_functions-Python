import boto3
import os
import json
from datetime import timezone

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    """
    Função Lambda que processa eventos do S3, gera um relatório dos objetos no bucket
    e envia notificação via SNS com formato HTML.
    
    :param event: Dados do evento que acionou a função
    :param context: Informações de runtime
    :return: Dicionário com statusCode e mensagem
    """
    try:
        print(f" Evento recebido: {json.dumps(event, indent=2)}")
        print(f"Contexto da função: {context}")
        
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        event_name = event['Records'][0]['eventName']
        event_time = event['Records'][0]['eventTime']
        object_key = event['Records'][0]['s3']['object']['key']
        
        print(f"Detalhes do evento S3:")
        print(f"- Bucket: {bucket_name}")
        print(f"- Tipo de Evento: {event_name}")
        print(f"- Objeto: {object_key}")
        print(f"- Horário do Evento: {event_time}")
        
        print(f"Listando objetos no bucket {bucket_name}...")
        response = s3.list_objects_v2(Bucket=bucket_name)
        objects = response.get('Contents', [])
        
        object_count = len(objects)
        print(f"Encontrados {object_count} objetos no bucket {bucket_name}")
        
        if not objects:
            file_list_html = "<p><strong>Nenhum arquivo encontrado no bucket.</strong></p>"
        else:
            file_list_html = "<ul>"
            for idx, obj in enumerate(objects, 1):
                file_name = obj['Key']
                created_time = obj['LastModified'].astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                file_list_html += f"<li><strong>{file_name}</strong> — criado em {created_time}</li>"
                
                if idx <= 10:
                    print(f"Objeto {idx}/{object_count}: {file_name} - Última modificação: {created_time}")
                elif idx == 11:
                    print(f"... omitindo logs dos objetos restantes ...")
            file_list_html += "</ul>"
        
        # HTML bonito para o e-mail
        html_message = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; background: #f9f9f9; }}
                .container {{ padding: 20px; background: white; border-radius: 8px; }}
                h2 {{ color: #2e6c80; }}
                .footer {{ font-size: 12px; color: #999; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>📂 Relatório de Upload - Sucesso</h2>
                <p>Evento <strong>{event_name}</strong> detectado no bucket <strong>{bucket_name}</strong>.</p>
                <p>Objeto modificado: <strong>{object_key}</strong></p>
                <p>Horário do evento: <strong>{event_time}</strong></p>
                <p>Lista de arquivos atuais no bucket ({object_count} objetos):</p>
                {file_list_html}
                <div class="footer">
                    Esta é uma notificação automática da AWS Lambda.
                </div>
            </div>
        </body>
        </html>
        """
        
        subject = f"📂 {event_name} - Bucket: {bucket_name} - Sucesso"
        
        response = sns.publish(
            TopicArn=os.environ['SNS_TOPIC_ARN'],
            Message=json.dumps({
                'default': f"{event_name} detectado no bucket {bucket_name}. Objeto: {object_key}",
                'html': html_message
            }),
            Subject=subject,
            MessageStructure='json'
        )
        
        print(f"Mensagem publicada com sucesso no SNS. MessageId: {response['MessageId']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Relatório enviado com sucesso!',
                'sns_message_id': response['MessageId'],
                'bucket': bucket_name,
                'object': object_key,
                'event_type': event_name
            })
        }
        
    except Exception as e:
        error_msg = f"Erro ao processar evento: {str(e)}"
        print(error_msg)
        print(f"Traceback: {json.dumps(e.__dict__, indent=2)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'event': event,
                'context': str(context)
            })
        }