from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, count, sum, avg, to_date, year, month, dayofmonth

# Configuração explícita do ambiente Java
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ['PATH']}"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configuração de logs
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configurações do MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "silver")
MINIO_GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "gold")
DATA_FOLDER = os.getenv("DATA_FOLDER", "dados")

# Log das configurações
logger.info(f"Configurações MinIO:")
logger.info(f"Endpoint: {MINIO_ENDPOINT}")
logger.info(f"Access Key: {MINIO_ACCESS_KEY}")
logger.info(f"Silver Bucket: {MINIO_SILVER_BUCKET}")
logger.info(f"Gold Bucket: {MINIO_GOLD_BUCKET}")
logger.info(f"Data Folder: {DATA_FOLDER}")

# Função para criar sessão Spark (reutilizada de bronze_to_silver)
def create_spark_session():
    DELTA_VERSION = "2.4.0"
    HADOOP_VERSION = "3.3.4"
    AWS_SDK_VERSION = "1.12.262"

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
        logger.info("Verificando acesso ao repositório Maven...")
        response = requests.get("https://repo1.maven.org/maven2", timeout=10)
        if response.status_code != 200:
            logger.warning("Acesso ao Maven Central pode estar bloqueado")
    except Exception as e:
        logger.error(f"Erro ao acessar Maven Central: {str(e)}")

    try:
        builder = SparkSession.builder.appName("SilverToGold")
        for key, value in spark_config.items():
            builder.config(key, value)
            
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session criada com sucesso!")
        return spark
        
    except Exception as e:
        logger.error("Erro ao criar Spark Session com pacotes Delta. Tentando modo fallback...")
        
        try:
            spark = SparkSession.builder \
                .appName("SilverToGold_Fallback") \
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
    dag_id='silver_to_gold_processing',
    description='Processa dados Silver para Gold, criando tabelas agregadas para consumo',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['minio', 'spark', 'gold', 'aggregation'],
    default_args={
        'owner': 'admin',
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    }
)
def silver_to_gold_dag():

    @task
    def verify_minio_connection():
        """Verificação da conexão com o MinIO."""
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
    def process_gold_tables():
        spark = None
        results = []
        try:
            spark = create_spark_session()
            
            # --- Tabela Gold: Total de Visualizações por Usuário e por Dia ---
            try:
                logger.info("Processando tabela Gold: visualizacoes_diarias_por_usuario")
                silver_visualizacoes_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/visualizacoes"
                gold_visualizacoes_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/visualizacoes_diarias_por_usuario"

                df_visualizacoes = spark.read.format("delta").load(silver_visualizacoes_path)
                
                df_visualizacoes_gold = df_visualizacoes \
                    .withColumn("data_visualizacao", to_date(col("data_hora_visualizacao"))) \
                    .groupBy("id_usuario", "data_visualizacao") \
                    .agg(count("id_visualizacao").alias("total_visualizacoes")) \
                    .withColumn("data_processamento_gold", current_timestamp())

                df_visualizacoes_gold.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .save(gold_visualizacoes_path)
                
                logger.info("Tabela 'visualizacoes_diarias_por_usuario' criada com sucesso na camada Gold.")
                results.append({"table": "visualizacoes_diarias_por_usuario", "status": "SUCCESS", "message": "Processamento concluído"})
            except Exception as e:
                logger.error(f"Erro ao processar visualizacoes_diarias_por_usuario: {str(e)}", exc_info=True)
                results.append({"table": "visualizacoes_diarias_por_usuario", "status": "ERROR", "message": str(e)})

            # --- Tabela Gold: Receita Total por Mês ---
            try:
                logger.info("Processando tabela Gold: receita_mensal")
                silver_pagamentos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/pagamentos"
                silver_planos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/planos"
                gold_receita_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/receita_mensal"

                df_pagamentos = spark.read.format("delta").load(silver_pagamentos_path)
                df_planos = spark.read.format("delta").load(silver_planos_path)

                # Join para obter o valor do plano associado ao pagamento (se necessário)
                # Neste exemplo, vamos usar o valor do pagamento diretamente para simplificar
                # mas em um cenário real, você pode querer correlacionar pagamentos com planos ativos.
                df_receita_mensal = df_pagamentos \
                    .withColumn("ano", year(col("data_hora_pagamento"))) \
                    .withColumn("mes", month(col("data_hora_pagamento"))) \
                    .groupBy("ano", "mes") \
                    .agg(sum("valor").alias("receita_total_mensal")) \
                    .withColumn("data_processamento_gold", current_timestamp())

                df_receita_mensal.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .save(gold_receita_path)

                logger.info("Tabela 'receita_mensal' criada com sucesso na camada Gold.")
                results.append({"table": "receita_mensal", "status": "SUCCESS", "message": "Processamento concluído"})
            except Exception as e:
                logger.error(f"Erro ao processar receita_mensal: {str(e)}", exc_info=True)
                results.append({"table": "receita_mensal", "status": "ERROR", "message": str(e)})

            # --- Tabela Gold: Usuários Ativos por Dia (Exemplo Simples) ---
            # Consideramos "ativo" como qualquer usuário que realizou um evento ou visualização no dia
            try:
                logger.info("Processando tabela Gold: usuarios_ativos_diarios")
                silver_eventos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/eventos"
                silver_visualizacoes_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/visualizacoes"
                gold_usuarios_ativos_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/usuarios_ativos_diarios"

                df_eventos = spark.read.format("delta").load(silver_eventos_path)
                df_visualizacoes = spark.read.format("delta").load(silver_visualizacoes_path)

                df_eventos_ativos = df_eventos.withColumn("data_atividade", to_date(col("data_hora_evento"))) \
                                            .select("id_usuario", "data_atividade") \
                                            .distinct()

                df_visualizacoes_ativos = df_visualizacoes.withColumn("data_atividade", to_date(col("data_hora_visualizacao"))) \
                                                        .select("id_usuario", "data_atividade") \
                                                        .distinct()

                df_usuarios_ativos = df_eventos_ativos.union(df_visualizacoes_ativos).distinct()

                df_usuarios_ativos_gold = df_usuarios_ativos \
                    .groupBy("data_atividade") \
                    .agg(count("id_usuario").alias("total_usuarios_ativos")) \
                    .withColumn("data_processamento_gold", current_timestamp())

                df_usuarios_ativos_gold.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .save(gold_usuarios_ativos_path)

                logger.info("Tabela 'usuarios_ativos_diarios' criada com sucesso na camada Gold.")
                results.append({"table": "usuarios_ativos_diarios", "status": "SUCCESS", "message": "Processamento concluído"})
            except Exception as e:
                logger.error(f"Erro ao processar usuarios_ativos_diarios: {str(e)}", exc_info=True)
                results.append({"table": "usuarios_ativos_diarios", "status": "ERROR", "message": str(e)})

        except Exception as e:
            error_msg = f"Erro geral no processamento da camada Gold: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results.append({"table": "general_gold_processing", "status": "ERROR", "message": error_msg})
        finally:
            if spark:
                logger.info("Encerrando sessão Spark para processamento Gold.")
                spark.stop()
        return results

    @task
    def finalize_gold_processing(results):
        success_tables = [r["table"] for r in results if r["status"] == "SUCCESS"]
        error_tables = [r["table"] for r in results if r["status"] == "ERROR"]
        
        logger.info(f"\n{'='*50}")
        logger.info(f"RESUMO FINAL DO PROCESSAMENTO GOLD")
        logger.info(f"Total de tabelas Gold processadas: {len(results)}")
        logger.info(f"Tabelas Gold com sucesso: {len(success_tables)}")
        logger.info(f"Tabelas Gold com erro: {len(error_tables)}")
        logger.info(f"{'='*50}\n")
        
        if success_tables:
            logger.info("Tabelas Gold processadas com sucesso:\n- " + "\n- ".join(success_tables))
        
        if error_tables:
            logger.error("Detalhes dos erros nas tabelas Gold:\n")
            for r in results:
                if r["status"] == "ERROR":
                    logger.error(f"=== ERRO EM {r['table']} ===\n"
                                 f"Mensagem: {r['message']}")
            # A linha abaixo vai levantar um erro se houver qualquer erro no processamento das tabelas gold.
            # Se você quer que a DAG continue mesmo com alguns erros, pode remover ou ajustar isso.
            raise RuntimeError(f"Falha no processamento de {len(error_tables)} tabelas Gold: {', '.join(error_tables)}")
        
        return f"Processamento Gold concluído: {len(success_tables)} sucessos, {len(error_tables)} erros"

    # Definir a ordem das tarefas e a passagem de dados
    minio_check_result = verify_minio_connection() # Salva o resultado do verify_minio_connection

    # A tarefa process_gold_tables só deve começar após verify_minio_connection
    # e não precisa do resultado do check diretamente como argumento se não o usar.
    gold_processing_results = process_gold_tables() 

    # finalize_gold_processing recebe o resultado de gold_processing_results como argumento.
    # A dependência é criada implicitamente por essa passagem de argumento.
    finalize_gold_processing(gold_processing_results)

# Instanciar o DAG
silver_to_gold_dag_instance = silver_to_gold_dag()