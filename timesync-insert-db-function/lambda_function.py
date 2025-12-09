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

        # Verificar se há erro na extração
        if data.get("error"):
            print(f"Erro no processamento do PDF: {data.get('message')}")
            return {"status": "error", "message": data.get("message")}

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
            
            # Primeiro verificar se existe um estado_dado padrão
            cursor.execute("SELECT id_estado_dado FROM estado_dados WHERE nome_estado_dado='Ativo' LIMIT 1")
            estado_result = cursor.fetchone()
            
            if not estado_result:
                # Criar estado padrão se não existir
                estado_id = uuid.uuid4().hex
                cursor.execute("""
                    INSERT INTO estado_dados (id_estado_dado, nome_estado_dado) 
                    VALUES (UNHEX(%s), 'Ativo')
                """, (estado_id,))
                estado_id_hex = estado_id
            else:
                estado_id_hex = estado_result['id_estado_dado'].hex()

            cursor.execute("""
                INSERT INTO usuarios (
                    matricula,
                    nome_completo_usuario,
                    email_usuario,
                    senha_usuario,
                    data_criacao_usuario,
                    data_atualizacao_usuario,
                    id_estado_dado
                ) VALUES (%s, %s, %s, 'default123', NOW(), NOW(), UNHEX(%s))
            """, (employee_reg, employee_name, f"{employee_reg}@empresa.com", estado_id_hex))
            
            print(f"Usuário {employee_reg} criado com sucesso")
        else:
            print(f"Usuário {employee_reg} já existe no banco")

        # =======================
        # 5. PROCESSAR CADA DIA E SEUS REGISTROS
        # =======================
        for day in daily_records:
            date_str = day["date"]
            
            # Converter data do formato dd/mm/yyyy para yyyy-mm-dd
            try:
                date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                formatted_date = date_obj.strftime("%Y-%m-%d")
            except ValueError as e:
                print(f"Erro ao converter data {date_str}: {e}")
                continue

            # Processar cada registro do dia
            for record in day["records"]:
                # Gerar ID para o apontamento
                apontamento_id = uuid.uuid4().hex
                
                # Converter horas para formato float
                horas_totais = 0.0
                horas_str = record.get("hours")
                if horas_str:
                    try:
                        # Converter formato "H:MM" para float
                        if ":" in horas_str:
                            hours, minutes = horas_str.split(":")
                            horas_totais = float(hours) + (float(minutes) / 60)
                        else:
                            horas_totais = float(horas_str)
                    except ValueError as e:
                        print(f"Erro ao converter horas {horas_str}: {e}")
                
                # Determinar tipo de ocorrência
                ocorrencia = record.get("occurrence_type", "Trabalho")
                
                # Verificar se é compensado
                if day.get("is_compensated"):
                    ocorrencia = "Compensado"
                
                # Verificar se é hora extra
                if record.get("is_overtime"):
                    ocorrencia = "Hora Extra"
                
                # Verificar se é entrada manual
                if record.get("is_manual_entry"):
                    ocorrencia = "Manual"
                
                # Processar projetos (se houver)
                projetos_str = record.get("projects")
                id_projeto = None
                
                if projetos_str and len(projetos_str.strip()) > 0:
                    # Verificar se o projeto já existe
                    cursor.execute("SELECT id_projeto FROM projetos WHERE id_projeto=%s", (projetos_str[:6],))
                    projeto_existe = cursor.fetchone()
                    
                    if not projeto_existe and len(projetos_str) >= 6:
                        # Criar projeto se não existir
                        projeto_id = projetos_str[:6]
                        cursor.execute("""
                            INSERT INTO projetos (
                                id_projeto,
                                nome_projeto,
                                data_entrega_projeto,
                                data_inicio_projeto,
                                id_estado_dado
                            ) VALUES (%s, %s, %s, %s, UNHEX(%s))
                        """, (
                            projeto_id,
                            projetos_str,
                            date_obj.strftime("%Y-%m-%d"),
                            date_obj.strftime("%Y-%m-%d"),
                            estado_id_hex
                        ))
                        id_projeto = projeto_id
                    elif projeto_existe:
                        id_projeto = projetos_str[:6]
                
                # Inserir apontamento
                try:
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
                            UNHEX(%s), 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            UNHEX(%s)
                        )
                    """, (
                        apontamento_id,
                        formatted_date,
                        ocorrencia,
                        record.get("justification"),
                        id_projeto,
                        record.get("start_time"),
                        record.get("end_time"),
                        horas_totais,
                        record.get("reason"),
                        employee_reg,
                        estado_id_hex
                    ))
                    
                    print(f"Apontamento inserido para {employee_reg} em {formatted_date}: {ocorrencia}")
                    
                except Exception as e:
                    print(f"Erro ao inserir apontamento: {e}")
                    print(f"Dados do apontamento: {record}")

        # =======================
        # 6. INSERIR ESTATÍSTICAS DO PERÍODO
        # =======================
        try:
            # Inserir ou atualizar estatísticas (se você tiver uma tabela para isso)
            # Aqui estou apenas logando as estatísticas
            print(f"Estatísticas do período para {employee_reg}:")
            print(f"- Horas trabalhadas: {summary.get('total_work_hours', '0:00')}")
            print(f"- Horas extras: {summary.get('total_overtime_hours', '0:00')}")
            print(f"- Horas projetos: {summary.get('total_project_hours', '0:00')}")
            print(f"- Dias úteis: {summary.get('work_days_count', 0)}")
            
        except Exception as e:
            print(f"Erro ao processar estatísticas: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        print("Processamento finalizado com sucesso.")
        return {"status": "ok", "employee": employee_reg, "records_processed": len(daily_records)}

    except Exception as e:
        print("Erro na Lambda:", e)
        print(traceback.format_exc())
        
        # Tentar fechar conexão se ainda estiver aberta
        try:
            if 'conn' in locals():
                conn.rollback()
                conn.close()
        except:
            pass
            
        return {"status": "error", "message": str(e)}