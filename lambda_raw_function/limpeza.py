import pandas as pd
import unicodedata
import chardet
import boto3 
import io

# config AWS
INPUT_KEY = "arquivoRaw.csv"
OUTPUT_KEY = "arquivoLimpo.xlsx"

s3_client = boto3.client("s3")

# padronização de texto
def remover_acentos(txt):
    if pd.isna(txt):
        return txt
    txt = str(txt)
    txt_norm = unicodedata.normalize("NFD", txt)
    return "".join(c for c in txt_norm if unicodedata.category(c) != "Mn")

def padronizar_texto(txt):
    if pd.isna(txt):
        return txt
    return remover_acentos(txt).strip().upper()

# leitura de csv raw
obj = s3_client.get_object( Key=INPUT_KEY)
raw_bytes = obj["Body"].read()

# config encoding
enc = chardet.detect(raw_bytes[:50000])["encoding"]

#  de CSV para DataFrame
df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, sep=";", on_bad_lines="skip")

# conformidade a dicionário de dados
colunas_mapeadas = {
    "Data": "data_apontamento",
    "Ocorrencia": "ocorrencia_apontamento",
    "Justificativa": "email_usuario",
    "Projetos": "projetos",
    "Tiket": "id_projeto",
    "Inicio": "hora_inicio",
    "Saida": "hora_saida",
    "Inativo": "inativo",
    "Horas": "horas_totais",
    "Motivo": "motivo"
}

# alinhando colunas do CSV para as do dicionário
df = df.rename(columns=colunas_mapeadas)

# padroniando dados
for col in df.select_dtypes(include="object").columns:
    if col.startswith("data_") or col.startswith("hora_") or col.startswith("id_"):
        continue
    df[col] = df[col].apply(padronizar_texto)

# # dt
for col in [c for c in df.columns if "data" in c]:
    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

# # hr
for col in [c for c in df.columns if "hora" in c]:
    df[col] = pd.to_datetime(df[col], format="%H:%M:%S", errors="coerce").dt.strftime("%H:%M:%S")

# # numero
for col in [c for c in df.columns if "matricula" in c]:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")

# tratamento de NULL
for campo_obrigatorio in ["data_apontamento", "ocorrencia_apontamento", "email_usuario","id_projeto","hora_inicio","hora_saida","inativo","horas_totais","motivo"]:
    df = df[df[campo_obrigatorio].notna()]

# remoção de duplicadas
    df = df.drop_duplicates()

# salvamento em S3
output_buffer = io.BytesIO()
df.to_excel(output_buffer, index=False)
s3_client.put_object(Key=OUTPUT_KEY, Body=output_buffer.getvalue())