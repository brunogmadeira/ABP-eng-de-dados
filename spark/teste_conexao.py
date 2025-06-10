from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TesteConexaoS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.mysql:mysql-connector-java:8.0.33") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .getOrCreate()

# Nível de log para debug
spark.sparkContext.setLogLevel("DEBUG")

# Criar um DataFrame de exemplo
data = [("João", 29), ("Maria", 33), ("Pedro", 25)]
df = spark.createDataFrame(data, ["nome", "idade"])

# Gravar parquet no bucket/dir landing/teste
df.write.mode("overwrite").parquet("s3a://landing/teste/")

print("Gravação concluída com sucesso!")

spark.stop()

#spark-submit --packages mysql:mysql-connector-java:8.0.33 /scripts/teste_conexao.py