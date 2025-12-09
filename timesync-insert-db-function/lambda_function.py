import json
import pymysql
import boto3
import traceback
import os
from datetime import datetime
import uuid


def get_db_connection():
    """Conecta ao MySQL usando variáveis de ambiente."""
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor
    )


def lambda_handler(event, context):
    try:
        # =======================
        # 1. ENTENDER EVENTO S3
        # =======================
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        print(f"Lendo arquivo S3: s3://{bucket}/{key}")

        s3 = boto3.client("s3")
        file_obj = s3.get_object(Bucket=bucket, Key=key)
        content = file_obj["Body"].read().decode("utf-8")

        data = json.loads(content)

        # =======================
        # 2. EXTRAIR DADOS DO JSON
        # =======================
        employee = data.get("header_info", {}).get("employee", {})
        period = data.get("header_info", {}).get("period", {})
        daily_records = data.get("daily_records", [])
        summary = data.get("period_summary", {})

        employee_name = employee.get("name")
        employee_reg = employee.get("registration")

        if not employee_reg:
            raise Exception("Matrícula não encontrada no JSON.")

        print(f"Processando colaborador: {employee_name} ({employee_reg})")

        # =======================
        # 3. CONECTAR AO BANCO
        # =======================
        conn = get_db_connection()
        cursor = conn.cursor()

        # =======================
        # 4. GARANTIR QUE O USUÁRIO EXISTE
        # =======================
        cursor.execute("SELECT matricula FROM usuarios WHERE matricula=%s", (employee_reg,))
        exists = cursor.fetchone()

        if not exists:
            print("Usuário não existe, criando...")

            cursor.execute("""
                INSERT INTO usuarios (
                    matricula,
                    nome_completo_usuario,
                    email_usuario,
                    senha_usuario,
                    data_criacao_usuario,
                    data_atualizacao_usuario,
                    id_estado_dado
                ) VALUES (%s, %s, %s, 'default', NOW(), NOW(), UNHEX(REPLACE(UUID(),'-','')))
            """, (employee_reg, employee_name, f"{employee_name}@empresa.com"))
        
        # =======================
        # 5. REGISTRO DO PERÍODO
        # (Você pode adaptar ao seu modelo real)
        # =======================
        periodo_id = uuid.uuid4().hex

        cursor.execute("""
            INSERT INTO periodos_ponto (
                id_periodo,
                matricula_usuario,
                data_inicio,
                data_fim,
                total_horas_trabalhadas,
                total_horas_extras,
                total_horas_projetos
            ) VALUES (
                UNHEX(%s), %s, %s, %s, %s, %s, %s
            )
        """, (
            periodo_id,
            employee_reg,
            period.get("start"),
            period.get("end"),
            summary.get("total_work_hours", "0:00"),
            summary.get("total_overtime_hours", "0:00"),
            summary.get("total_project_hours", "0:00")
        ))

        # =======================
        # 6. REGISTROS DIÁRIOS
        # =======================
        for day in daily_records:
            day_id = uuid.uuid4().hex

            cursor.execute("""
                INSERT INTO registros_dia (
                    id_registro_dia,
                    id_periodo,
                    data_dia,
                    dia_compensado
                ) VALUES (
                    UNHEX(%s), UNHEX(%s), %s, %s
                )
            """, (day_id, periodo_id, day["date"], 1 if day["is_compensated"] else 0))

            # Registros internos
            for r in day["records"]:
                cursor.execute("""
                    INSERT INTO registros_horas (
                        id_registro,
                        id_registro_dia,
                        tipo_ocorrencia,
                        justificativa,
                        projetos,
                        ticket,
                        hora_inicio,
                        hora_fim,
                        inatividade,
                        horas_trabalhadas,
                        entrada_manual,
                        hora_extra
                    ) VALUES (
                        UNHEX(REPLACE(UUID(),'-','')), 
                        UNHEX(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    day_id,
                    r.get("occurrence_type"),
                    r.get("justification"),
                    r.get("projects"),
                    r.get("ticket"),
                    r.get("start_time"),
                    r.get("end_time"),
                    r.get("inactive_time"),
                    r.get("hours"),
                    1 if r.get("is_manual_entry") else 0,
                    1 if r.get("is_overtime") else 0
                ))

        conn.commit()

        print("Processamento finalizado com sucesso.")
        return {"status": "ok"}

    except Exception as e:
        print("Erro na Lambda:", e)
        print(traceback.format_exc())
        return {"status": "error", "message": str(e)}
