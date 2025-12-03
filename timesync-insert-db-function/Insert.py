import json
import mysql.connector
from datetime import datetime, date

# ============================
# CONFIGURAÇÃO DO BANCO
# ============================
db_config = {
    "host": "localhost",
    "user": "usuario_app",
    "password": "senha_app",
    "database": "Timesync"
}

# ============================
# FUNÇÃO PARA CALCULAR HORAS
# ============================
def calcular_horas(hora_inicio, hora_fim):
    if not hora_inicio or not hora_fim:
        return 0.0
    dt_inicio = datetime.combine(date.today(), hora_inicio)
    dt_fim = datetime.combine(date.today(), hora_fim)
    diff = dt_fim - dt_inicio
    return round(diff.total_seconds() / 3600, 2)

# ============================
# CONEXÃO MYSQL
# ============================
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# ============================
# GARANTIR EXISTÊNCIA DO ESTADO "Ativo"
# ============================
cursor.execute("SELECT id_estado_dado FROM estado_dados WHERE nome_estado_dado = 'Ativo'")
row = cursor.fetchone()

if row:
    id_estado_padrao = row[0]
    print(f"Estado 'Ativo' encontrado: {id_estado_padrao}")
else:
    print("Estado 'Ativo' não existe — criando automaticamente...")
    cursor.execute("""
        INSERT INTO estado_dados (id_estado_dado, nome_estado_dado)
        VALUES (UNHEX(REPLACE(UUID(), '-', '')), 'Ativo')
    """)
    conn.commit()
    cursor.execute("SELECT id_estado_dado FROM estado_dados WHERE nome_estado_dado = 'Ativo'")
    id_estado_padrao = cursor.fetchone()[0]
    print(f"Estado 'Ativo' criado: {id_estado_padrao}")

# ============================
# LER JSON
# ============================
with open("giovanna_aavila_2025-11-04.json", "r", encoding="utf-8") as f:
    data = json.load(f)

colaborador = data["header_info"]["employee"]["name"]
matricula = int(data["header_info"]["employee"]["registration"])

print("\n=======================================")
print(f"Inserindo apontamentos de {colaborador} (mat {matricula})")
print("=======================================\n")

# ============================
# MAPEAR PROJETOS DO BANCO
# ============================
cursor.execute("SELECT id_projeto, nome_projeto FROM projetos")
projetos_db = {nome.upper(): pid for pid, nome in cursor.fetchall()}

print("Projetos disponíveis no banco:")
for nome, pid in projetos_db.items():
    print(f"  • {nome} → {pid}")
print("\n")

# ============================
# INSERIR APONTAMENTOS
# ============================
for dia in data["daily_records"]:
    data_ap = datetime.strptime(dia["date"], "%d/%m/%Y")

    print(f"\nDia: {dia['date']}")

    for r in dia["records"]:
        tipo = r["occurrence_type"]
        justificativa = r["justification"]
        motivo = r.get("reason")

        nome_projeto = r.get("projects", "").upper().strip()
        id_projeto = projetos_db.get(nome_projeto)

        if not id_projeto:
            print(f"Projeto '{nome_projeto}' não encontrado no banco! Usando NULL.")
        else:
            print(f"Projeto identificado: {nome_projeto} -> {id_projeto}")

        # converter horários
        def to_time(h):
            if h and ":" in h:
                return datetime.strptime(h, "%H:%M").time()
            return None

        hora_inicio = to_time(r.get("start_time"))
        hora_fim = to_time(r.get("end_time"))
        horas_totais = calcular_horas(hora_inicio, hora_fim)

        print(f"Horários: {r.get('start_time')} → {r.get('end_time')} | Total: {horas_totais} h")
        print(f"Tipo: {tipo} | Justificativa: {justificativa}")

        # INSERIR APONTAMENTO
        cursor.execute("""
            INSERT INTO apontamentos (
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
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s
            )
        """, (
            data_ap,
            tipo,
            justificativa,
            id_projeto,
            hora_inicio,
            hora_fim,
            horas_totais,
            motivo,
            matricula,
            id_estado_padrao
        ))

        print("Apontamento inserido!\n")

conn.commit()
cursor.close()
conn.close()

print("\n====================================================")
print("Todos os apontamentos foram inseridos com sucesso!")
print("====================================================")
