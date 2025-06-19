from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Configuração de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dag(
    dag_id="landing_to_bronze_minio_optimized",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["minio", "pyspark", "elt", "bronze"],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        'execution_timeout': timedelta(minutes=15),
    }
)
def landing_to_bronze_dag():

    @task()
    def process_landing_to_bronze():
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
        landing_bucket = os.getenv("MINIO_LANDING_BUCKET", "landing")
        bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
        
        # A pasta onde os arquivos estão armazenados
        subfolder = "dados"  # Nome fixo da pasta onde estão os arquivos
        
        logger.info(f"Configurando conexão com MinIO: {minio_endpoint}")

        try:
            spark = (
                SparkSession.builder
                .appName("LandingToBronze")
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
                .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .getOrCreate()
            )

            # Caminhos corrigidos para incluir a pasta 'dados'
            landing_path = f"s3a://{landing_bucket}/{subfolder}/"
            bronze_path = f"s3a://{bronze_bucket}/{subfolder}/"

            logger.info(f"Procurando arquivos em: {landing_path}")
            
            file_list = [f for f in spark.sparkContext.binaryFiles(landing_path + "*.csv").keys().collect()]
            
            if not file_list:
                logger.warning(f"Nenhum arquivo CSV encontrado em {landing_path}!")
                return

            logger.info(f"Encontrados {len(file_list)} arquivos para processar")
            
            for file_path in file_list:
                # Extrai o nome da tabela do caminho do arquivo
                # Exemplo: 's3a://landing/dados/clientes.csv' -> 'clientes'
                table_name = os.path.basename(file_path).replace(".csv", "")
                logger.info(f"Processando: {table_name}")
                
                df = spark.read.csv(file_path, header=True, inferSchema=True)
                df = df.withColumn("_ingestion_timestamp", current_timestamp())
                
                # Caminho de saída mantendo a estrutura de pastas
                output_path = f"{bronze_path}{table_name}"
                
                df.write.format("delta") \
                    .mode("overwrite") \
                    .save(output_path)
                
                logger.info(f"{table_name} processado com sucesso! Salvo em: {output_path}")

        except Exception as e:
            logger.error("Erro fatal no processamento", exc_info=True)
            raise
        finally:
            if 'spark' in locals():
                spark.stop()
                logger.info("Sessão Spark encerrada")

    process_landing_to_bronze()

dag = landing_to_bronze_dag()