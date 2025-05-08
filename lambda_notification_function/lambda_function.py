import boto3
import os
import json
from datetime import timezone

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']

    response = s3.list_objects_v2(Bucket=bucket_name)
    objects = response.get('Contents', [])

    if not objects:
        file_list_html = "<p><strong>Nenhum arquivo encontrado no bucket.</strong></p>"
    else:
        file_list_html = "<ul>"
        for obj in objects:
            file_name = obj['Key']
            created_time = obj['LastModified'].astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            file_list_html += f"<li><strong>{file_name}</strong> — criado em {created_time}</li>"
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
            <p>O bucket <strong>{bucket_name}</strong> recebeu um novo arquivo.</p>
            <p>Lista de arquivos atuais no bucket:</p>
            {file_list_html}
            <div class="footer">
                Esta é uma notificação automática da AWS Lambda.
            </div>
        </div>
    </body>
    </html>
    """

    subject = "📂 Relatório de Upload - Sucesso"

    response = sns.publish(
        TopicArn=os.environ['SNS_TOPIC_ARN'],
        Message=json.dumps({
            'default': f"Upload detectado no bucket {bucket_name}",
            'html': html_message
        }),
        Subject=subject,
        MessageStructure='json'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Relatório enviado com sucesso!')
    }
