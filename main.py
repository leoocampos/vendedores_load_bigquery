import os
import io
import pandas as pd
import pytz
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import storage, bigquery

app = Flask(__name__)

# Configurações
TIME_SP = 'America/Sao_Paulo'
DATASET_ID = 'BRONZE'
TABLE_ID = 'vendedores'
BUCKET_NAME = "sample-track-files"

@app.post("/vendedores_load_bigquery")
def load_vendedores_to_bq():
    try:
        # 1. Obter nome do arquivo do JSON enviado (ou fixo se preferir)
        data = request.get_json()
        file_name = data.get('file_name', 'vendedores.xlsx')
        bucket_name = BUCKET_NAME

        # 2. Conectar aos Clientes
        storage_client = storage.Client()
        bq_client = bigquery.Client()
        
        # 3. Ler arquivo do Cloud Storage para a memória
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_bytes()

        # 4. Processar com Pandas (Lendo os bytes do Excel)
        df_vendedores = pd.read_excel(io.BytesIO(content), engine='openpyxl')
        
        # Adicionar coluna de data de carga e formatar STRING
        dat_ref = datetime.now(tz=pytz.timezone(TIME_SP)).date()
        df_vendedores = df_vendedores.assign(dat_ref_carga=pd.to_datetime(dat_ref))
        df_vendedores['dat_ref_carga'] = df_vendedores['dat_ref_carga'].dt.strftime('%Y-%m-%d')
        df_vendedores = df_vendedores.astype(str)

        # 5. Carregar no BigQuery
        # table_id completo: projeto.dataset.tabela
        table_full_id = f"{bq_client.project}.{DATASET_ID}.{TABLE_ID}"
        
        # Configuração da carga (Append para não apagar o que já existe)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        job = bq_client.load_table_from_dataframe(df_vendedores, table_full_id, job_config=job_config)
        job.result()  # Espera a carga terminar

        logging.info(f"Carga concluída: {len(df_vendedores)} linhas inseridas em {table_full_id}")
        
        return jsonify({"status": "success", "rows": len(df_vendedores)}), 200

    except Exception as e:
        logging.error(f"Erro na carga: {e}")
        return jsonify({"status": "error", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)