from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# configuração de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    spark = SparkSession.builder \
        .appName("IngestaoMultiplaMySQLParaLanding") \
        .config("spark.jars.packages", "org.mysql:mysql-connector-java:8.0.33") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .getOrCreate()

    logging.info("Sessão Spark iniciada com sucesso.")

    jdbc_url = "jdbc:mysql://mysql8:3306/pipelinestreaming"
    properties = {
        "user": "user",
        "password": "user123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    id_colunas = {
        "avaliacoes": "id_avaliacao",
        "planos": "id_plano",
        "cancelamentos": "id_cancelamento",
        "dispositivos": "id_dispositivo",
        "eventos": "id_evento",
        "generos": "id_genero",
        "pagamentos": "id_pagamento",
        "usuarios": "id_usuario",
        "videos": "id_video",
        "visualizacoes": "id_visualizacao",
        "favoritos": "id_favorito"
    }

    tabelas = [
        "avaliacoes"
    ]

    for tabela in tabelas:
        try:
            logging.info(f"Iniciando ingestão da tabela: {tabela}")

            coluna_id = id_colunas.get(tabela, "")

            if coluna_id:
                bounds_query = f"(SELECT MIN({coluna_id}) AS min_id, MAX({coluna_id}) AS max_id FROM {tabela}) AS bounds"
                bounds_df = spark.read.jdbc(url=jdbc_url, table=bounds_query, properties=properties)
                bounds = bounds_df.collect()[0]
                lowerBound = bounds['min_id'] if bounds['min_id'] is not None else 1
                upperBound = bounds['max_id'] if bounds['max_id'] is not None else 1_000_000

                df = spark.read.jdbc(
                    url=jdbc_url,
                    table=tabela,
                    column=coluna_id,
                    lowerBound=lowerBound,
                    upperBound=upperBound,
                    numPartitions=4,
                    properties=properties
                )
            else:
                df = spark.read.jdbc(
                    url=jdbc_url,
                    table=tabela,
                    properties=properties
                )

            # adicionando uma coluna de data de ingestão
            df = df.withColumn("data_ingestao", current_date())

            df.write.mode("overwrite").parquet(f"s3a://landing/{tabela}/")

            logging.info(f"Tabela {tabela} salva com sucesso em s3a://landing/{tabela}/")

        except Exception as e:
            logging.error(f"Erro na ingestão da tabela {tabela}: {e}", exc_info=True)

except Exception as e:
    logging.error(f"Erro geral no script: {e}", exc_info=True)

finally:
    if 'spark' in locals(): # verifica se a sessão Spark foi criada antes de tentar parar ela
        spark.stop()
        logging.info("Sessão Spark finalizada.")