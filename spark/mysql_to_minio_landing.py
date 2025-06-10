from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IngestaoMultiplaMySQLParaLanding") \
    .config("spark.jars.packages", "org.mysql:mysql-connector-java:8.0.33") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")  \
    .getOrCreate()

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
        print(f"Iniciando ingestão da tabela: {tabela}")

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

        df.write.mode("overwrite").parquet(f"s3a://landing/{tabela}/")

        print(f"Tabela {tabela} salva com sucesso em s3a://landing/{tabela}/")

    except Exception as e:
        print(f"Erro na ingestão da tabela {tabela}: {e}")

spark.stop()
