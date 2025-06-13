import os
import mysql.connector
import traceback

# Pega automaticamente o diretório do usuário
home_dir = os.path.expanduser("~")

# Junta com o restante do caminho
pasta_csv = os.path.join(home_dir, "Documents", "ABP-eng-de-dados", "data")

# Conexão com o MySQL no Docker
conn = mysql.connector.connect(
    host='localhost',
    port=3307,
    user='user',
    password='user123',
    database='pipelinestreaming',
    allow_local_infile=True,
    use_pure=True
)

cursor = conn.cursor()

print(f"📂 Lendo arquivos de: {pasta_csv}")

for arquivo in os.listdir(pasta_csv):
    if arquivo.endswith(".csv"):
        caminho_absoluto = os.path.abspath(os.path.join(pasta_csv, arquivo))
        nome_tabela = os.path.splitext(arquivo)[0]

        print(f"\n📤 Importando '{arquivo}' para a tabela '{nome_tabela}'...")
        print(f"🛤️ Caminho do arquivo: {caminho_absoluto}")

        sql = f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE {nome_tabela}
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS;
        """
        try:
            cursor.execute(sql, (caminho_absoluto,))
            conn.commit()
            print(f"✔️ {arquivo} importado com sucesso.")
            cursor.execute(f"SELECT COUNT(*) FROM {nome_tabela}")
            print(f"📊 Total de registros na tabela {nome_tabela}: {cursor.fetchone()[0]}")
        except Exception as e:
            print(f"❌ Erro ao importar {arquivo}:")
            traceback.print_exc()

cursor.close()
conn.close()
