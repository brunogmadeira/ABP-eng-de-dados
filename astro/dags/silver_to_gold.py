from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import sys
import traceback
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, count, sum as _sum, 
    when, avg, year, month, dayofmonth, 
    dayofweek, quarter, weekofyear, current_timestamp
)

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
logger.info(f"Configurações MinIO Gold:")
logger.info(f"Endpoint: {MINIO_ENDPOINT}")
logger.info(f"Access Key: {MINIO_ACCESS_KEY}")
logger.info(f"Silver Bucket: {MINIO_SILVER_BUCKET}")
logger.info(f"Gold Bucket: {MINIO_GOLD_BUCKET}")
logger.info(f"Data Folder: {DATA_FOLDER}")

# Função para criar sessão Spark com resiliência
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
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.parquet.enableVectorizedReader": "false",
        "spark.jars.repositories": "https://repo1.maven.org/maven2",
        "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
        "spark.executor.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true"
    }

    try:
        builder = SparkSession.builder.appName("SilverToGold")
        for key, value in spark_config.items():
            builder.config(key, value)
            
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session criada com sucesso para Gold!")
        
        # Configurar manualmente o sistema de arquivos
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
        hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        return spark
        
    except Exception as e:
        logger.error("Erro ao criar Spark Session. Tentando modo fallback...", exc_info=True)
        
        # Modo fallback sem pacotes Delta
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
                
            # Configurar manualmente o sistema de arquivos
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
            hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
            hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
            hadoop_conf.set("fs.s3a.path.style.access", "true")
            hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
            hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            
            logger.warning("Sessão Spark criada em modo fallback (sem suporte a Delta Lake)")
            return spark
        except Exception as fallback_error:
            logger.error("Falha ao criar Spark Session mesmo em modo fallback", exc_info=True)
            raise RuntimeError(f"Erro crítico ao criar Spark Session: {str(fallback_error)}")

@dag(
    dag_id='silver_to_gold_processing',
    description='Transforma dados Silver para camada Gold com tabelas analíticas',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['minio', 'spark', 'gold'],
    default_args={
        'owner': 'admin',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def silver_to_gold_dag():

    @task
    def create_dim_tempo():
        spark = None
        try:
            spark = create_spark_session()
            logger.info("Processando dim_tempo...")
            
            # Coletar todas as datas relevantes do sistema
            date_sources = {
                "visualizacoes": "data_hora_visualizacao",
                "pagamentos": "data_hora_pagamento",
                "favoritos": "data_hora_favorito",
                "cancelamentos": "data_hora_cancelamento",
                "eventos": "data_hora_evento"
            }
            
            dfs = []
            for table, date_col in date_sources.items():
                try:
                    path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/{table}"
                    logger.info(f"Lendo datas de: {table}.{date_col} em {path}")
                    
                    # Verificar se o caminho existe
                    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                        spark.sparkContext._jvm.java.net.URI.create(path), 
                        hadoop_conf
                    )
                    path_obj = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
                    
                    if not fs.exists(path_obj):
                        logger.warning(f"Caminho não encontrado: {path}")
                        continue
                    
                    # Tentar ler dados em diferentes formatos
                    df = None
                    for format in ["parquet", "delta"]:  # Priorizar Parquet
                        try:
                            logger.info(f"Tentando ler como {format.upper()}...")
                            df = spark.read.format(format).load(path)
                            break
                        except Exception as e:
                            logger.warning(f"Falha ao ler como {format.upper()}: {str(e)}")
                    
                    if df is None:
                        logger.warning(f"Falha ao ler dados para {table}")
                        continue
                    
                    if date_col not in df.columns:
                        logger.warning(f"Coluna {date_col} não encontrada na tabela {table}")
                        continue
                        
                    df = df.select(col(date_col).alias("data_hora"))
                    dfs.append(df)
                    
                except Exception as e:
                    logger.error(f"Erro ao processar {table}: {str(e)}", exc_info=True)
            
            if not dfs:
                logger.error("Nenhuma fonte de datas disponível")
                return "ERROR: dim_tempo - Nenhuma fonte de datas disponível"
            
            union_df = dfs[0]
            for df in dfs[1:]:
                union_df = union_df.union(df)
            
            # Extrair atributos temporais
            dim_tempo = union_df.distinct() \
                .withColumn("id_tempo", date_format(col("data_hora"), "yyyyMMdd").cast("int")) \
                .withColumn("data", col("data_hora").cast("date")) \
                .withColumn("ano", year("data")) \
                .withColumn("mes", month("data")) \
                .withColumn("dia", dayofmonth("data")) \
                .withColumn("dia_semana", dayofweek("data")) \
                .withColumn("trimestre", quarter("data")) \
                .withColumn("semana_ano", weekofyear("data")) \
                .drop("data_hora")
            
            # Salvar na Gold como Parquet
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_tempo"
            dim_tempo.write.format("parquet") \
                .mode("overwrite") \
                .save(gold_path)
                
            logger.info(f"dim_tempo salva em: {gold_path}")
            return "SUCCESS: dim_tempo"
            
        except Exception as e:
            logger.error(f"Erro ao criar dim_tempo: {str(e)}", exc_info=True)
            return f"ERROR: dim_tempo - {str(e)}"
        finally:
            if spark:
                spark.stop()

    @task
    def create_dim_videos():
        spark = None
        try:
            spark = create_spark_session()
            logger.info("Processando dim_videos...")
            
            # Ler dados silver
            videos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/videos"
            generos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/generos"
            
            # Função para ler com resiliência
            def read_table(path, table_name):
                # Verificar existência do caminho
                hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark.sparkContext._jvm.java.net.URI.create(path), 
                    hadoop_conf
                )
                path_obj = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
                
                if not fs.exists(path_obj):
                    raise FileNotFoundError(f"Caminho não encontrado: {path}")
                
                # Tentar ler em diferentes formatos
                for format in ["parquet", "delta"]:  # Priorizar Parquet
                    try:
                        logger.info(f"Lendo {table_name} como {format.upper()}...")
                        return spark.read.format(format).load(path)
                    except Exception as e:
                        logger.warning(f"Falha ao ler {table_name} como {format.upper()}: {str(e)}")
                raise RuntimeError(f"Falha ao ler dados para {table_name}")
            
            df_videos = read_table(videos_path, "videos")
            df_generos = read_table(generos_path, "generos")
            
            # Enriquecer com gêneros
            dim_videos = df_videos.join(
                df_generos, 
                df_videos.id_genero == df_generos.id_genero,
                "left"
            ).select(
                df_videos.id_video,
                df_videos.titulo,
                df_videos.duracao,
                df_generos.id_genero.alias("id_genero"),
                df_generos.nome.alias("genero")
            )
            
            # Salvar na Gold como Parquet
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_videos"
            dim_videos.write.format("parquet") \
                .mode("overwrite") \
                .save(gold_path)
                
            logger.info(f"dim_videos salva em: {gold_path}")
            return "SUCCESS: dim_videos"
            
        except Exception as e:
            logger.error(f"Erro ao criar dim_videos: {str(e)}", exc_info=True)
            return f"ERROR: dim_videos - {str(e)}"
        finally:
            if spark:
                spark.stop()

    @task
    def create_dim_usuarios():
        spark = None
        try:
            spark = create_spark_session()
            logger.info("Processando dim_usuarios...")
            
            # Ler dados silver
            usuarios_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/usuarios"
            planos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/planos"
            
            # Função para ler com resiliência
            def read_table(path, table_name):
                # Verificar existência do caminho
                hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark.sparkContext._jvm.java.net.URI.create(path), 
                    hadoop_conf
                )
                path_obj = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
                
                if not fs.exists(path_obj):
                    raise FileNotFoundError(f"Caminho não encontrado: {path}")
                
                # Tentar ler em diferentes formatos
                for format in ["parquet", "delta"]:  # Priorizar Parquet
                    try:
                        logger.info(f"Lendo {table_name} como {format.upper()}...")
                        return spark.read.format(format).load(path)
                    except Exception as e:
                        logger.warning(f"Falha ao ler {table_name} como {format.upper()}: {str(e)}")
                raise RuntimeError(f"Falha ao ler dados para {table_name}")
            
            df_usuarios = read_table(usuarios_path, "usuarios")
            df_planos = read_table(planos_path, "planos")
            
            # Enriquecer com planos
            dim_usuarios = df_usuarios.join(
                df_planos,
                df_usuarios.id_plano == df_planos.id_plano,
                "left"
            ).select(
                df_usuarios.id_usuario,
                df_usuarios.nome,
                df_usuarios.email,
                df_planos.id_plano.alias("id_plano"),
                df_planos.nome.alias("plano"),
                df_planos.valor.alias("valor_plano"),
                df_planos.tipo.alias("tipo_plano")
            )
            
            # Salvar na Gold como Parquet
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_usuarios"
            dim_usuarios.write.format("parquet") \
                .mode("overwrite") \
                .save(gold_path)
                
            logger.info(f"dim_usuarios salva em: {gold_path}")
            return "SUCCESS: dim_usuarios"
            
        except Exception as e:
            logger.error(f"Erro ao criar dim_usuarios: {str(e)}", exc_info=True)
            return f"ERROR: dim_usuarios - {str(e)}"
        finally:
            if spark:
                spark.stop()

    @task
    def create_fato_visualizacoes():
        spark = None
        try:
            spark = create_spark_session()
            logger.info("Processando fato_visualizacoes...")
            
            # Ler dados silver
            visualizacoes_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/visualizacoes"
            
            # Verificar existência do caminho
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI.create(visualizacoes_path), 
                hadoop_conf
            )
            path_obj = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(visualizacoes_path)
            
            if not fs.exists(path_obj):
                raise FileNotFoundError(f"Caminho não encontrado: {visualizacoes_path}")
            
            # Tentar ler em diferentes formatos
            df_visualizacoes = None
            for format in ["parquet", "delta"]:  # Priorizar Parquet
                try:
                    logger.info(f"Lendo visualizacoes como {format.upper()}...")
                    df_visualizacoes = spark.read.format(format).load(visualizacoes_path)
                    break
                except Exception as e:
                    logger.warning(f"Falha ao ler como {format.upper()}: {str(e)}")
            
            if df_visualizacoes is None:
                raise RuntimeError("Falha ao ler dados de visualizações")
            
            # Criar id_tempo
            fato = df_visualizacoes.withColumn(
                "id_tempo",
                date_format(col("data_hora_visualizacao"), "yyyyMMdd").cast("int")
            )
            
            # Salvar na Gold como Parquet
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/fato_visualizacoes"
            fato.write.format("parquet") \
                .partitionBy("id_tempo") \
                .mode("overwrite") \
                .save(gold_path)
                
            logger.info(f"fato_visualizacoes salva em: {gold_path}")
            return "SUCCESS: fato_visualizacoes"
            
        except Exception as e:
            logger.error(f"Erro ao criar fato_visualizacoes: {str(e)}", exc_info=True)
            return f"ERROR: fato_visualizacoes - {str(e)}"
        finally:
            if spark:
                spark.stop()

    @task
    def create_agg_metricas():
        spark = None
        try:
            spark = create_spark_session()
            logger.info("Processando agg_metricas...")
            
            # Ler dados gold
            fato_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/fato_visualizacoes"
            dim_videos_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_videos"
            dim_usuarios_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_usuarios"
            
            # Função para ler tabelas gold com resiliência
            def read_gold_table(path, table_name):
                # Verificar existência do caminho
                hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark.sparkContext._jvm.java.net.URI.create(path), 
                    hadoop_conf
                )
                path_obj = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
                
                if not fs.exists(path_obj):
                    raise FileNotFoundError(f"Caminho não encontrado: {path}")
                
                # Tentar ler em diferentes formatos
                for format in ["parquet", "delta"]:  # Priorizar Parquet
                    try:
                        logger.info(f"Lendo {table_name} como {format.upper()}...")
                        return spark.read.format(format).load(path)
                    except Exception as e:
                        logger.warning(f"Falha ao ler {table_name} como {format.upper()}: {str(e)}")
                raise RuntimeError(f"Falha ao ler dados para {table_name}")
            
            df_fato = read_gold_table(fato_path, "fato_visualizacoes")
            df_videos = read_gold_table(dim_videos_path, "dim_videos")
            df_usuarios = read_gold_table(dim_usuarios_path, "dim_usuarios")
            
            # Juntar dimensões
            df = df_fato.join(
                df_videos, 
                "id_video", 
                "left"
            ).join(
                df_usuarios,
                "id_usuario",
                "left"
            )
            
            # Calcular métricas
            agg_metricas = df.groupBy(
                "id_tempo", 
                "genero", 
                "plano"
            ).agg(
                count("*").alias("total_visualizacoes"),
                avg("duracao").alias("duracao_media"),
                _sum(when(col("genero").isNotNull(), 1)).alias("visualizacoes_com_genero")
            )
            
            # Adicionar timestamp de processamento
            agg_metricas = agg_metricas.withColumn(
                "data_processamento_gold", 
                current_timestamp()
            )
            
            # Salvar na Gold como Parquet
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/agg_metricas"
            agg_metricas.write.format("parquet") \
                .mode("overwrite") \
                .save(gold_path)
                
            logger.info(f"agg_metricas salva em: {gold_path}")
            return "SUCCESS: agg_metricas"
            
        except Exception as e:
            logger.error(f"Erro ao criar agg_metricas: {str(e)}", exc_info=True)
            return f"ERROR: agg_metricas - {str(e)}"
        finally:
            if spark:
                spark.stop()

    @task
    def finalize(results):
        success_count = sum(1 for r in results if r.startswith("SUCCESS"))
        error_count = sum(1 for r in results if r.startswith("ERROR"))
        
        logger.info(f"\n{'='*50}")
        logger.info("RESUMO DA TRANSFORMAÇÃO SILVER TO GOLD")
        logger.info(f"Tabelas processadas: {len(results)}")
        logger.info(f"Sucessos: {success_count}")
        logger.info(f"Erros: {error_count}")
        
        if error_count > 0:
            logger.error("Detalhes dos erros:")
            for r in results:
                if r.startswith("ERROR"):
                    logger.error(r)
            logger.error("Processamento concluído com erros")
            return "Processamento concluído com erros"
        else:
            logger.info("Todas as tabelas Gold processadas com sucesso!")
            return "Processamento Gold concluído"

    # Orquestração das tarefas
    dim_tempo = create_dim_tempo()
    dim_videos = create_dim_videos()
    dim_usuarios = create_dim_usuarios()
    fato_visualizacoes = create_fato_visualizacoes()
    
    # Dependências
    fato_visualizacoes.set_upstream([dim_tempo])
    
    agg_metricas = create_agg_metricas()
    agg_metricas.set_upstream([dim_videos, dim_usuarios, fato_visualizacoes])
    
    final_results = finalize([
        dim_tempo,
        dim_videos,
        dim_usuarios,
        fato_visualizacoes,
        agg_metricas
    ])

# Instanciar o DAG
silver_to_gold_processing = silver_to_gold_dag()