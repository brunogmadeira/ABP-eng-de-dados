from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import sys
import subprocess
import traceback
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuração explícita do ambiente Java
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ['PATH']}"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_HOME'] = '/usr/local/lib/python3.12/site-packages/pyspark'

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
logger.info(f"Java Home: {os.environ['JAVA_HOME']}")
logger.info(f"Spark Home: {os.environ['SPARK_HOME']}")

# Função para criar sessão Spark
def create_spark_session():
    # Configurações otimizadas
    spark_config = {
        "spark.jars.packages": "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.sql.shuffle.partitions": "4",
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.driver.maxResultSize": "512m",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.host": "localhost",
        "spark.driver.port": "7077",
        "spark.ui.port": "4040"
    }

    try:
        # Tentar inicializar findspark se disponível
        try:
            import findspark
            findspark.init()
            logger.info("findspark initialized successfully")
        except ImportError:
            logger.warning("findspark not installed. Skipping initialization.")
        
        # Verificar versão do Java
        java_version = subprocess.run(["java", "-version"], stderr=subprocess.PIPE, text=True)
        logger.info(f"Java version detected:\n{java_version.stderr}")
        
        builder = SparkSession.builder.appName("SilverToGold")
        for key, value in spark_config.items():
            builder.config(key, value)
            
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session criada com sucesso!")
        logger.info(f"Spark version: {spark.version}")
        return spark
        
    except Exception as e:
        logger.error("Erro ao criar Spark Session", exc_info=True)
        # Verificação adicional do Java
        try:
            java_check = subprocess.run(["java", "-version"], stderr=subprocess.PIPE, text=True)
            logger.error(f"Java version check failed: {java_check.stderr}")
        except Exception as java_err:
            logger.error("Java not found or not accessible", exc_info=True)
        raise RuntimeError(f"Falha ao criar Spark Session: {str(e)}")

@dag(
    dag_id='silver_to_gold_processing',
    description='Transforma dados da camada Silver para Gold com modelagem dimensional',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['minio', 'spark', 'gold', 'data-warehouse'],
    default_args={
        'owner': 'data-engineer',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def silver_to_gold_dag():

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
    def test_java_connection():
        """Verifica se o Java está disponível e a versão"""
        try:
            result = subprocess.run(
                ["java", "-version"],
                stderr=subprocess.PIPE,  # Java envia a versão para stderr
                text=True,
                check=True
            )
            java_version = result.stderr.split('\n')[0] if result.stderr else "Unknown"
            logger.info(f"Java testado com sucesso: {java_version}")
            return "SUCCESS: Java test"
        except Exception as e:
            logger.error("Teste do Java falhou", exc_info=True)
            return f"ERROR: Java test - {str(e)}"

    @task
    def create_dim_tempo():
        """Cria dimensão de tempo para 10 anos (2020-2030)"""
        spark = None
        try:
            logger.info("Criando dimensão tempo...")
            spark = create_spark_session()
            
            # Criar dataframe com range de datas
            start_date = "2020-01-01"
            end_date = "2030-12-31"
            
            # Usar seq para criar range de datas
            dates = spark.sql(f"""
                SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as data_completa
            """)
            
            dim_tempo = dates.select(
                F.date_format("data_completa", "yyyyMMdd").cast("int").alias("id_tempo"),
                F.date_format("data_completa", "yyyy-MM-dd").alias("data"),
                F.year("data_completa").alias("ano"),
                F.quarter("data_completa").alias("trimestre"),
                F.month("data_completa").alias("mes"),
                F.weekofyear("data_completa").alias("semana_ano"),
                F.dayofyear("data_completa").alias("dia_ano"),
                F.dayofmonth("data_completa").alias("dia_mes"),
                F.dayofweek("data_completa").alias("dia_semana"),
                F.when(F.dayofweek("data_completa").isin(1, 7), "Fim de Semana")
                 .otherwise("Dia Útil").alias("tipo_dia"),
                F.date_format("data_completa", "MMMM").alias("nome_mes"),
                F.date_format("data_completa", "EEEE").alias("nome_dia_semana")
            )
            
            # Salvar dimensão tempo
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_tempo"
            (dim_tempo.write
                .format("delta")
                .mode("overwrite")
                .save(gold_path))
            
            logger.info(f"Dimensão tempo criada com {dim_tempo.count()} registros")
            return "SUCCESS: dim_tempo"
            
        except Exception as e:
            error_msg = f"Erro ao criar dim_tempo: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return f"ERROR: dim_tempo - {str(e)}"
        
        finally:
            if spark:
                spark.stop()

    @task
    def create_dim_usuarios():
        """Cria dimensão de usuários"""
        spark = None
        try:
            logger.info("Criando dimensão usuários...")
            spark = create_spark_session()
            
            # Ler dados silver
            usuarios_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/usuarios"
            planos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/planos"
            
            df_usuarios = spark.read.format("delta").load(usuarios_path)
            df_planos = spark.read.format("delta").load(planos_path)
            
            # Enriquecer dados
            dim_usuarios = df_usuarios.join(
                df_planos, 
                "id_plano",
                "left"
            ).select(
                "id_usuario",
                "nome",
                "email",
                "id_plano",
                F.col("nome").alias("nome_plano"),
                "valor",
                "tipo"
            ).withColumn(
                "tipo_plano",
                F.when(F.col("tipo") == "P", "Premium")
                 .when(F.col("tipo") == "B", "Básico")
                 .when(F.col("tipo") == "G", "Grátis")
                 .otherwise("Desconhecido")
            ).drop("tipo")
            
            # Salvar dimensão
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_usuarios"
            (dim_usuarios.write
                .format("delta")
                .mode("overwrite")
                .save(gold_path))
            
            logger.info(f"Dimensão usuários criada com {dim_usuarios.count()} registros")
            return "SUCCESS: dim_usuarios"
            
        except Exception as e:
            error_msg = f"Erro ao criar dim_usuarios: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return f"ERROR: dim_usuarios - {str(e)}"
        
        finally:
            if spark:
                spark.stop()

    @task
    def create_dim_videos():
        """Cria dimensão de vídeos"""
        spark = None
        try:
            logger.info("Criando dimensão vídeos...")
            spark = create_spark_session()
            
            # Ler dados silver
            videos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/videos"
            generos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/generos"
            
            df_videos = spark.read.format("delta").load(videos_path)
            df_generos = spark.read.format("delta").load(generos_path)
            
            # Enriquecer dados
            dim_videos = df_videos.join(
                df_generos,
                "id_genero",
                "left"
            ).select(
                "id_video",
                "titulo",
                "duracao",
                "id_genero",
                F.col("nome").alias("genero")
            ).withColumn(
                "duracao_minutos",
                F.round(F.col("duracao") / 60, 2)
            ).withColumn(
                "faixa_duracao",
                F.when(F.col("duracao_minutos") < 5, "Curto (0-5min)")
                 .when((F.col("duracao_minutos") >= 5) & (F.col("duracao_minutos") < 15), "Médio (5-15min)")
                 .when((F.col("duracao_minutos") >= 15) & (F.col("duracao_minutos") < 30), "Longo (15-30min)")
                 .otherwise("Muito longo (30+min)")
            )
            
            # Salvar dimensão
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/dim_videos"
            (dim_videos.write
                .format("delta")
                .mode("overwrite")
                .save(gold_path))
            
            logger.info(f"Dimensão vídeos criada com {dim_videos.count()} registros")
            return "SUCCESS: dim_videos"
            
        except Exception as e:
            error_msg = f"Erro ao criar dim_videos: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return f"ERROR: dim_videos - {str(e)}"
        
        finally:
            if spark:
                spark.stop()

    @task
    def create_fato_visualizacoes():
        """Cria fato de visualizações"""
        spark = None
        try:
            logger.info("Criando fato visualizações...")
            spark = create_spark_session()
            
            # Ler dados silver
            visualizacoes_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/visualizacoes"
            videos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/videos"
            
            df_visualizacoes = spark.read.format("delta").load(visualizacoes_path)
            df_videos = spark.read.format("delta").load(videos_path)
            
            # Juntar e enriquecer dados
            fato_visualizacoes = df_visualizacoes.join(
                df_videos,
                "id_video",
                "left"
            ).select(
                "id_visualizacao",
                "id_usuario",
                "id_video",
                "data_hora_visualizacao",
                "duracao"
            ).withColumn(
                "data",
                F.to_date("data_hora_visualizacao")
            ).withColumn(
                "hora",
                F.date_format("data_hora_visualizacao", "HH:mm:ss")
            ).withColumn(
                "id_tempo",
                F.date_format("data", "yyyyMMdd").cast("int")
            ).withColumn(
                "duracao_assistida",
                F.expr("least(duracao, 60)")  # Exemplo: limita a 60 segundos
            ).drop("duracao")
            
            # Salvar fato
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/fato_visualizacoes"
            (fato_visualizacoes.write
                .format("delta")
                .partitionBy("data")
                .mode("overwrite")
                .save(gold_path))
            
            logger.info(f"Fato visualizações criado com {fato_visualizacoes.count()} registros")
            return "SUCCESS: fato_visualizacoes"
            
        except Exception as e:
            error_msg = f"Erro ao criar fato_visualizacoes: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return f"ERROR: fato_visualizacoes - {str(e)}"
        
        finally:
            if spark:
                spark.stop()

    @task
    def create_fato_pagamentos():
        """Cria fato de pagamentos"""
        spark = None
        try:
            logger.info("Criando fato pagamentos...")
            spark = create_spark_session()
            
            # Ler dados silver
            pagamentos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/pagamentos"
            
            df_pagamentos = spark.read.format("delta").load(pagamentos_path)
            
            # Enriquecer dados
            fato_pagamentos = df_pagamentos.select(
                "id_pagamento",
                "id_usuario",
                "valor",
                "data_hora_pagamento"
            ).withColumn(
                "data",
                F.to_date("data_hora_pagamento")
            ).withColumn(
                "id_tempo",
                F.date_format("data", "yyyyMMdd").cast("int")
            ).withColumn(
                "mes_ano",
                F.date_format("data", "yyyy-MM")
            )
            
            # Calcular métricas recorrentes
            window = Window.partitionBy("id_usuario").orderBy("data")
            fato_pagamentos = fato_pagamentos.withColumn(
                "pagamento_anterior",
                F.lag("valor").over(window)
            ).withColumn(
                "variacao_valor",
                F.when(F.col("pagamento_anterior").isNull(), 0)
                 .otherwise(F.col("valor") - F.col("pagamento_anterior"))
            )
            
            # Salvar fato
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/fato_pagamentos"
            (fato_pagamentos.write
                .format("delta")
                .partitionBy("mes_ano")
                .mode("overwrite")
                .save(gold_path))
            
            logger.info(f"Fato pagamentos criado com {fato_pagamentos.count()} registros")
            return "SUCCESS: fato_pagamentos"
            
        except Exception as e:
            error_msg = f"Erro ao criar fato_pagamentos: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return f"ERROR: fato_pagamentos - {str(e)}"
        
        finally:
            if spark:
                spark.stop()

    @task
    def create_fato_avaliacoes():
        """Cria fato de avaliações"""
        spark = None
        try:
            logger.info("Criando fato avaliações...")
            spark = create_spark_session()
            
            # Ler dados silver
            avaliacoes_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/avaliacoes"
            
            df_avaliacoes = spark.read.format("delta").load(avaliacoes_path)
            
            # Enriquecer dados
            fato_avaliacoes = df_avaliacoes.select(
                "id_avaliacao",
                "id_usuario",
                "id_video",
                "nota",
                "comentario",
                F.current_timestamp().alias("data_processamento")
            ).withColumn(
                "sentimento",
                F.when(F.col("nota") >= 4, "Positivo")
                 .when(F.col("nota") == 3, "Neutro")
                 .otherwise("Negativo")
            )
            
            # Salvar fato
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/fato_avaliacoes"
            (fato_avaliacoes.write
                .format("delta")
                .mode("overwrite")
                .save(gold_path))
            
            logger.info(f"Fato avaliações criado com {fato_avaliacoes.count()} registros")
            return "SUCCESS: fato_avaliacoes"
            
        except Exception as e:
            error_msg = f"Erro ao criar fato_avaliacoes: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return f"ERROR: fato_avaliacoes - {str(e)}"
        
        finally:
            if spark:
                spark.stop()

    @task
    def create_agg_engajamento():
        """Cria agregação de engajamento"""
        spark = None
        try:
            logger.info("Criando agregação de engajamento...")
            spark = create_spark_session()
            
            # Ler dados necessários
            visualizacoes_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/visualizacoes"
            favoritos_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/favoritos"
            avaliacoes_path = f"s3a://{MINIO_SILVER_BUCKET}/{DATA_FOLDER}/avaliacoes"
            
            df_visualizacoes = spark.read.format("delta").load(visualizacoes_path)
            df_favoritos = spark.read.format("delta").load(favoritos_path)
            df_avaliacoes = spark.read.format("delta").load(avaliacoes_path)
            
            # Agregar visualizações por vídeo
            agg_visualizacoes = df_visualizacoes.groupBy("id_video").agg(
                F.count("*").alias("total_visualizacoes"),
                F.avg("duracao").alias("media_duracao_assistida")
            )
            
            # Agregar favoritos por vídeo
            agg_favoritos = df_favoritos.groupBy("id_video").agg(
                F.count("*").alias("total_favoritos")
            )
            
            # Agregar avaliações por vídeo
            agg_avaliacoes = df_avaliacoes.groupBy("id_video").agg(
                F.count("*").alias("total_avaliacoes"),
                F.avg("nota").alias("media_avaliacoes")
            )
            
            # Juntar todas as agregações
            agg_engajamento = agg_visualizacoes.join(
                agg_favoritos, "id_video", "left"
            ).join(
                agg_avaliacoes, "id_video", "left"
            ).fillna(0, ["total_favoritos", "total_avaliacoes"])
            
            # Calcular taxa de engajamento
            agg_engajamento = agg_engajamento.withColumn(
                "taxa_engajamento",
                F.round((F.col("total_favoritos") + F.col("total_avaliacoes")) / 
                F.greatest(F.col("total_visualizacoes"), F.lit(1)) * 100, 2)
            )
            
            # Salvar agregação
            gold_path = f"s3a://{MINIO_GOLD_BUCKET}/{DATA_FOLDER}/agg_engajamento_videos"
            (agg_engajamento.write
                .format("delta")
                .mode("overwrite")
                .save(gold_path))
            
            logger.info(f"Agregação de engajamento criada com {agg_engajamento.count()} registros")
            return "SUCCESS: agg_engajamento"
            
        except Exception as e:
            error_msg = f"Erro ao criar agg_engajamento: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return f"ERROR: agg_engajamento - {str(e)}"
        
        finally:
            if spark:
                spark.stop()

    @task
    def finalize(results):
        success_count = sum("SUCCESS" in result for result in results)
        error_count = sum("ERROR" in result for result in results)
        
        logger.info(f"\n{'='*50}")
        logger.info(f"RESUMO FINAL DA CAMADA GOLD")
        logger.info(f"Total de tabelas processadas: {len(results)}")
        logger.info(f"Tabelas com sucesso: {success_count}")
        logger.info(f"Tabelas com erro: {error_count}")
        logger.info(f"{'='*50}\n")
        
        for result in results:
            if "SUCCESS" in result:
                logger.info(f"✅ {result}")
            else:
                logger.error(f"❌ {result}")
        
        if error_count > 0:
            raise RuntimeError(f"Ocorreram {error_count} erros no processamento Gold")
        
        return "Camada Gold processada com sucesso!"

    # Verificar conexão com MinIO primeiro
    minio_check = verify_minio_connection()
    
    # Criar dimensões
    dim_tempo = create_dim_tempo()
    dim_usuarios = create_dim_usuarios()
    dim_videos = create_dim_videos()
    
    # Criar fatos
    fato_visualizacoes = create_fato_visualizacoes()
    fato_pagamentos = create_fato_pagamentos()
    fato_avaliacoes = create_fato_avaliacoes()
    
    # Criar agregações
    agg_engajamento = create_agg_engajamento()
    
    # Definir dependências
    dim_tempo.set_upstream(minio_check)
    dim_usuarios.set_upstream(minio_check)
    dim_videos.set_upstream(minio_check)
    
    # Consolidar resultados
    results = [dim_tempo, dim_usuarios, dim_videos, 
               fato_visualizacoes, fato_pagamentos, fato_avaliacoes,
               agg_engajamento]
    
    finalize(results)


# Instanciar o DAG
silver_to_gold_dag = silver_to_gold_dag()