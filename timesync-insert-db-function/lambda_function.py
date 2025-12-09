import json
import mysql.connector
import boto3
import traceback
import os
from datetime import datetime
import uuid


def get_db_connection():
    """Conecta ao MySQL usando MySQL Connector da layer."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME")
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

        # Verificar erro
        if data.get("error"):
            print(f"Erro no processamento do PDF: {data.get('message')}")
            return {"status": "error", "message": data.get("message")}

        # =======================
        # 2. EXTRAIR DADOS
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
        cursor = conn.cursor(dictionary=True)

        # =======================
        # 4. GARANTIR ESTADO_DADO ATIVO
        # =======================
        # Primeiro, garantir que existe um estado "Ativo"
        cursor.execute("SELECT id_estado_dado FROM estado_dados WHERE nome_estado_dado='Ativo' LIMIT 1")
        estado_result = cursor.fetchone()
        
        if not estado_result:
            # Criar estado "Ativo" se não existir
            estado_uuid = uuid.uuid4().bytes
            cursor.execute("""
                INSERT INTO estado_dados (id_estado_dado, nome_estado_dado)
                VALUES (%s, 'Ativo')
            """, (estado_uuid,))
            estado_id = estado_uuid
        else:
            estado_id = estado_result['id_estado_dado']
        
        # =======================
        # 5. GARANTIR QUE O USUÁRIO EXISTE
        # =======================
        # Converter matrícula para int
        try:
            matricula_int = int(employee_reg)
        except ValueError:
            print(f"Matrícula inválida: {employee_reg}")
            return {"status": "error", "message": f"Matrícula inválida: {employee_reg}"}
        
        cursor.execute("SELECT matricula FROM usuarios WHERE matricula=%s", (matricula_int,))
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
                ) VALUES (%s, %s, %s, 'default123', NOW(), NOW(), %s)
            """, (matricula_int, employee_name, f"{employee_reg}@empresa.com", estado_id))

            print(f"Usuário {employee_reg} criado com sucesso")
        else:
            print(f"Usuário {employee_reg} já existe")

        # =======================
        # 6. PROCESSAR REGISTROS DO JSON CORRETO
        # =======================
        # Você precisa usar os dados do raw_lines, não do daily_records (que está vazio)
        raw_lines = data.get("raw_lines", [])
        
        # O formato parece ser: Data, Ocorrência, Justificativa, Projeto, Ticket, Início, Saída, Inativo, Horas, Motivo
        # Pular cabeçalho (5 primeiras linhas são cabeçalho)
        records_to_process = []
        for i in range(5, len(raw_lines)):
            line_parts = raw_lines[i].split(", ")
            if len(line_parts) >= 10:
                records_to_process.append({
                    "date": line_parts[0],
                    "occurrence_type": line_parts[1],
                    "justification": line_parts[2] if line_parts[2] != "-" else None,
                    "projects": line_parts[3] if line_parts[3] != "-" else None,
                    "ticket": line_parts[4] if line_parts[4] != "-" else None,
                    "start_time": line_parts[5] if line_parts[5] != "-" else None,
                    "end_time": line_parts[6] if line_parts[6] != "-" else None,
                    "inactive_time": line_parts[7] if line_parts[7] != "-" else None,
                    "hours": line_parts[8] if line_parts[8] != "-" else None,
                    "reason": line_parts[9] if len(line_parts) > 9 and line_parts[9] != "-" else None
                })
        
        print(f"Encontrados {len(records_to_process)} registros em raw_lines")

        for record in records_to_process:
            try:
                # Converter data
                date_str = record["date"]
                date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                
                # Criar datetime (usando hora padrão 00:00:00)
                formatted_datetime = date_obj.strftime("%Y-%m-%d 00:00:00")
                
                # Calcular horas totais
                horas_totais = 0.0
                horas_str = record.get("hours")
                if horas_str:
                    try:
                        if ":" in horas_str:
                            h, m = map(float, horas_str.split(":"))
                            horas_totais = h + m / 60
                        else:
                            horas_totais = float(horas_str)
                    except:
                        pass
                
                # Determinar ocorrência
                ocorrencia = record.get("occurrence_type", "Relógio Web")
                if "Hora Extra" in ocorrencia or "Hora Extra" in (record.get("reason") or ""):
                    ocorrencia = "Hora Extra"
                elif "Manual" in ocorrencia or "Manual" in (record.get("reason") or ""):
                    ocorrencia = "Manual"
                
                # Processar projeto
                projetos_str = record.get("projects")
                id_projeto = None
                
                if projetos_str and projetos_str.strip() and projetos_str != "-":
                    projeto_id = projetos_str.strip()
                    
                    # Verificar se projeto existe
                    cursor.execute(
                        "SELECT id_projeto FROM projetos WHERE id_projeto=%s",
                        (projeto_id,)
                    )
                    projeto_existe = cursor.fetchone()
                    
                    if not projeto_existe:
                        # Criar projeto
                        cursor.execute("""
                            INSERT INTO projetos (
                                id_projeto,
                                nome_projeto,
                                data_entrega_projeto,
                                data_inicio_projeto,
                                id_estado_dado
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                            projeto_id,
                            projeto_id,  # Usando ID como nome temporário
                            date_obj.strftime("%Y-%m-%d"),
                            date_obj.strftime("%Y-%m-%d"),
                            estado_id
                        ))
                    
                    id_projeto = projeto_id
                
                # Inserir apontamento
                apontamento_id = uuid.uuid4().bytes
                
                cursor.execute("""
                    INSERT INTO apontamentos (
                        id_apontamento,
                        data_apontamento,
                        ocorrencia_apontamento,
                        justificativa_apontamento,
                        id_projeto,
                        hora_inicio_apontamento,
                        hora_fim_apontamento,
                        horas_totais_apontamento,
                        motivo_apontamento,
                        usuarios_matricula,
                        id_estado_dado
                    ) VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                """, (
                    apontamento_id,
                    formatted_datetime,
                    ocorrencia,
                    record.get("justification"),
                    id_projeto,
                    record.get("start_time"),
                    record.get("end_time"),
                    horas_totais,
                    record.get("reason"),
                    matricula_int,
                    estado_id
                ))
                
                print(f"Apontamento inserido para {date_str}")
                
            except Exception as e:
                print(f"Erro ao processar registro: {e}")
                print(f"Dados do registro: {record}")
                continue

        # =======================
        # 7. LOGAR ESTATÍSTICAS
        # =======================
        print("Estatísticas do período:", summary)

        conn.commit()
        cursor.close()
        conn.close()

        print("Processamento finalizado com sucesso.")
        return {
            "status": "ok",
            "employee": employee_reg,
            "records_processed": len(records_to_process)
        }

    except Exception as e:
        print("Erro na Lambda:", e)
        print(traceback.format_exc())
        try:
            if "conn" in locals():
                conn.rollback()
                conn.close()
        except:
            pass
        return {"status": "error", "message": str(e)}