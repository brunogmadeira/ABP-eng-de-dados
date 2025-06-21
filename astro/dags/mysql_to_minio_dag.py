from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook  
from datetime import datetime
import pandas as pd
import pymysql
import boto3

MINIO_CONFIG = {
    'endpoint_url': 'http://minio:9000',
    'aws_access_key_id': 'minio',
    'aws_secret_access_key': 'minio123',
    'region_name': 'us-east-1',
    'bucket_name': 'landing',
    'folder': 'dados'
}

TABLES = ['avaliacoes', 'cancelamentos', 'dispositivos', 'eventos', 'favoritos', 'generos', 'pagamentos', 'planos', 'usuarios', 'videos', 'visualizacoes'] 

def extract_and_upload(table_name: str, **context):
    hook = MySqlHook(mysql_conn_id="mysql_conn")
    conn = hook.get_conn()

    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    # Conversão para CSV
    csv_data = df.to_csv(index=False)

    # Upload para MinIO
    s3 = boto3.client('s3', **{k: v for k, v in MINIO_CONFIG.items() if k != 'bucket_name' and k != 'folder'})
    s3.put_object(
        Bucket=MINIO_CONFIG['bucket_name'],
        Key=f"{MINIO_CONFIG['folder']}/{table_name}.csv",
        Body=csv_data
    )

with DAG(
    dag_id='mysql_multiple_tables_to_minio',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['mysql', 'minio']
) as dag:

    for table in TABLES:
        PythonOperator(
            task_id=f'process_{table}',
            python_callable=extract_and_upload,
            op_kwargs={'table_name': table}
        )
