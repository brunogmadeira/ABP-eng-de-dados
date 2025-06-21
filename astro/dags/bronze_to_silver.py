from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import sys
import subprocess
import traceback
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col

# Configuração explícita do ambiente Java
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ['PATH']}"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configuração de logs
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Mapeamento completo de colunas
COLUMN_MAPPING = {
    "visualizacoes": {"data_hora": "data_hora_visualizacao"},
    "pagamentos": {"data_pagamento": "data_hora_pagamento"},
    "favoritos": {"data_favorito": "data_hora_favorito"},
    "cancelamentos": {"data_cancelamento": "data_hora_cancelamento"},
    "eventos": {"data_hora": "data_hora_evento"}
}

# Configurações do MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
MINIO_SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "silver")
DATA_FOLDER = os.getenv("DATA_FOLDER", "dados")

# Log das configurações
logger.info(f"Configurações MinIO:")
logger.info(f"Endpoint: {MINIO_ENDPOINT}")
logger.info(f"Access Key: {MINIO_ACCESS_KEY}")
logger.info(f"Bronze Bucket: {MINIO_BRONZE_BUCKET}")
logger.info(f"Silver Bucket: {MINIO_SILVER_BUCKET}")
logger.info(f"Data Folder: {DATA_FOLDER}")

# Função para criar sessão Spark
def create_spark_session():
    # Versões compatíveis testadas
    DELTA_VERSION = "2.4.0"
    HADOOP_VERSION = "3.3.4"
    AWS_SDK_VERSION = "1.12.262"
    
    # Configurações de memória otimizadas
    spark_config = {
        "spark.jars.packages": f"io.delta:delta-core_2.12:{DELTA_VERSION},"
                               f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION},"
                               f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.parquet.enableVectorizedReader": "false",
        "spark.jars.repositories": "https://repo1.maven.org/maven2"
    }

    try:
        # Verificar conexão com repositório Maven
        logger.info("Verificando acesso ao repositório Maven...")
        response = requests.get("https://repo1.maven.org/maven2", timeout=10)
        if response.status_code != 200:
            logger.warning("Acesso ao Maven Central pode estar bloqueado")
    except Exception as e:
        logger.error(f"Erro ao acessar Maven Central: {str(e)}")

    try:
        builder = SparkSession.builder.appName("BronzeToSilver")
        for key, value in spark_config.items():
            builder.config(key, value)
            
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session criada com sucesso!")
        return spark
        
    except Exception as e:
        logger.error("Erro ao criar Spark Session com pacotes Delta. Tentando modo fallback...")
        
        # Modo fallback sem pacotes Delta
        try:
            spark = SparkSession.builder \
                .appName("BronzeToSilver_Fallback") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .getOrCreate()
                
            logger.warning("Sessão Spark criada em modo fallback (sem suporte a Delta Lake)")
            return spark
        except Exception as fallback_error:
            logger.error("Falha ao criar Spark Session mesmo em modo fallback", exc_info=True)
            raise RuntimeError(f"Erro crítico ao criar Spark Session: {str(fallback_error)}")

@dag(
    dag_id='bronze_to_silver_processing_resilient',
    description='Processa dados Bronze para Silver com resiliência a erros',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['minio', 'spark', 'silver'],
    default_args={
        'owner': 'admin',
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    }
)
def resilient_bronze_to_silver_dag():

    @task
    def verify_minio_connection():
        """Verificação da conexão com o MinIO"""
        try:
            response = requests.get(MINIO_ENDPOINT, timeout=10)
            if response.status_code in [200, 403, 404]:
                logger.info("Conexão com MinIO bem-sucedida")
                return "SUCCESS: Conexão MinIO OK"
            else:
                raise Exception(f"Resposta inesperada: {response.status_code}")
        except Exception as e:
            logger.error("Falha na conexão com MinIO", exc_info=True)
            raise RuntimeError("Não foi possível conectar ao MinIO") from e

    @task
    def process_single_table(table_name):
        spark = None
        result = {
            "table": table_name,
            "status": "SUCCESS",
            "message": "Processamento concluído",
            "exception": None,
            "traceback": None
        }
        
        try:
            logger.info(f"=== Iniciando processamento para tabela: {table_name} ===")
            spark = create_spark_session()
            
            bronze_path = f"s3a://{MINIO_BRONZE_BUCKET}/{DATA_FOLDER}/{table_name}"
            silver_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/{table_name}"
            
            logger.info(f"Lendo dados bronze de: {bronze_path}")
            
            # Verificar se o caminho existe
            logger.info("Verificando existência do caminho...")
            try:
                hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark.sparkContext._jvm.java.net.URI.create(bronze_path), 
                    hadoop_conf
                )
                path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(bronze_path)
                
                if not fs.exists(path):
                    raise FileNotFoundError(f"Caminho não encontrado: {bronze_path}")
            except Exception as e:
                logger.warning(f"Erro ao verificar caminho: {str(e)}. Continuando...")
            
            # Tentar ler dados bronze em diferentes formatos
            df = None
            for format in ["delta", "parquet", "json"]:
                try:
                    logger.info(f"Tentando ler como {format.upper()}...")
                    df = spark.read.format(format).load(bronze_path)
                    break
                except Exception as e:
                    logger.warning(f"Falha ao ler como {format.upper()}: {str(e)}")
            
            if df is None:
                raise RuntimeError(f"Falha ao ler dados bronze para {table_name}")
            
            row_count = df.count()
            logger.info(f"Leitura concluída! Total de linhas: {row_count}")
            
            if row_count == 0:
                result["message"] = f"Aviso: Tabela {table_name} vazia"
                logger.warning(f"A tabela {table_name} está vazia!")
            
            # Renomear colunas conforme mapeamento
            if table_name in COLUMN_MAPPING:
                logger.info(f"Aplicando mapeamento de colunas para {table_name}")
                for old_col, new_col in COLUMN_MAPPING[table_name].items():
                    if old_col in df.columns:
                        logger.info(f"Renomeando coluna: {old_col} -> {new_col}")
                        df = df.withColumnRenamed(old_col, new_col)
                    else:
                        logger.warning(f"Coluna {old_col} não encontrada na tabela {table_name}")
            
            # Adicionar metadados
            df = df.withColumn("data_processamento_silver", current_timestamp()) \
                   .withColumn("fonte_bronze", lit(table_name))
            
            # Remover colunas temporárias
            colunas_para_remover = ["_ingestion_timestamp", "data_processamento_bronze"]
            for col_name in colunas_para_remover:
                if col_name in df.columns:
                    logger.info(f"Removendo coluna temporária: {col_name}")
                    df = df.drop(col_name)
            
            # Validação específica por tabela
            logger.info(f"Validando estrutura da tabela {table_name}")
            required_cols_map = {
                "visualizacoes": ["id_visualizacao", "id_usuario", "id_video", "data_hora_visualizacao"],
                "videos": ["id_video", "titulo", "duracao", "id_genero"],
                "usuarios": ["id_usuario", "nome", "email", "id_plano"],
                "planos": ["id_plano", "nome", "valor", "tipo"],
                "pagamentos": ["id_pagamento", "id_usuario", "valor", "data_hora_pagamento"],
                "generos": ["id_genero", "nome"],
                "favoritos": ["id_favorito", "id_usuario", "id_video", "data_hora_favorito"],
                "eventos": ["id_evento", "id_usuario", "acao", "data_hora_evento"],
                "dispositivos": ["id_dispositivo", "id_usuario", "tipo", "sistema_operacional"],
                "cancelamentos": ["id_cancelamento", "id_usuario", "motivo", "data_hora_cancelamento"],
                "avaliacoes": ["id_avaliacao", "id_usuario", "id_video", "nota", "comentario"]
            }
            
            required_cols = required_cols_map.get(table_name, [])
            missing_cols = [c for c in required_cols if c not in df.columns]
            
            if missing_cols:
                logger.warning(f"Colunas obrigatórias faltantes: {', '.join(missing_cols)}")
                # Tentar corrigir automaticamente
                for col_name in missing_cols:
                    if col_name not in df.columns:
                        df = df.withColumn(col_name, lit(None).cast("string"))
                        logger.info(f"Coluna {col_name} adicionada com valores nulos")
            
            # Escrever dados silver
            logger.info(f"Escrevendo dados silver em: {silver_path}")
            write_success = False
            for format in ["delta", "parquet"]:
                try:
                    (df.write
                        .format(format)
                        .mode("overwrite")
                        .option("mergeSchema", "true")
                        .save(silver_path))
                    write_success = True
                    logger.info(f"Escrita como {format.upper()} bem-sucedida!")
                    break
                except Exception as e:
                    logger.warning(f"Falha ao escrever como {format.upper()}: {str(e)}")
            
            if not write_success:
                raise RuntimeError("Falha ao escrever dados silver em todos os formatos")
            
            logger.info(f"Tabela {table_name} processada com sucesso! Linhas processadas: {row_count}")
            return result
        
        except Exception as e:
            error_msg = f"Erro ao processar {table_name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            result["status"] = "ERROR"
            result["message"] = error_msg
            result["exception"] = str(e)
            result["traceback"] = traceback.format_exc()
            
            return result
        
        finally:
            if spark:
                logger.info(f"Encerrando sessão Spark para {table_name}")
                try:
                    spark.stop()
                except Exception as e:
                    logger.error(f"Erro ao encerrar sessão: {str(e)}")

    @task
    def finalize(results):
        success_tables = []
        error_details = []
        
        for result in results:
            if result["status"] == "SUCCESS":
                success_tables.append(result["table"])
            else:
                error_details.append(
                    f"=== ERRO EM {result['table']} ===\n"
                    f"Mensagem: {result['message']}\n"
                    f"Exceção: {result['exception']}\n"
                )
        
        success_count = len(success_tables)
        error_count = len(error_details)
        
        logger.info(f"\n{'='*50}")
        logger.info(f"RESUMO FINAL DO PROCESSAMENTO")
        logger.info(f"Total de tabelas processadas: {len(results)}")
        logger.info(f"Tabelas com sucesso: {success_count}")
        logger.info(f"Tabelas com erro: {error_count}")
        logger.info(f"{'='*50}\n")
        
        if success_tables:
            logger.info("Tabelas processadas com sucesso:\n- " + "\n- ".join(success_tables))
        
        if error_details:
            consolidated_errors = "\n".join(error_details)
            logger.error("Detalhes dos erros:\n" + consolidated_errors)
            
            # Criar mensagem resumida para exceção
            error_tables = [result["table"] for result in results if result["status"] == "ERROR"]
            error_msg = f"Falha no processamento de {error_count} tabelas: {', '.join(error_tables)}"
            
            if error_count == len(results):
                raise RuntimeError(f"FALHA CRÍTICA: {error_msg}")
            elif error_count > 0:
                logger.error(f"AVISO: {error_msg}")
        
        return f"Processamento concluído: {success_count} sucessos, {error_count} erros"

    # Verificar conexão com MinIO primeiro
    minio_check = verify_minio_connection()
    
    # Lista completa de tabelas para processar
    tabelas = [
        "visualizacoes", "videos", "usuarios", "planos", 
        "pagamentos", "generos", "favoritos", "eventos", 
        "dispositivos", "cancelamentos", "avaliacoes"
    ]
    
    # Processar tabelas
    processing_tasks = []
    for tabela in tabelas:
        process_task = process_single_table(tabela)
        process_task.set_upstream(minio_check)
        processing_tasks.append(process_task)
    
    # Consolidar resultados
    finalize(processing_tasks)

# Instanciar o DAG
bronze_to_silver_dag = resilient_bronze_to_silver_dag()