SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA INFILE '/arquivos_csv/planos.csv' # Caminho corrigido de /data/ para /arquivos_csv/
INTO TABLE planos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/usuarios.csv' # Caminho corrigido
INTO TABLE usuarios
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/dispositivos.csv' # Caminho corrigido
INTO TABLE dispositivos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/generos.csv' # Caminho corrigido
INTO TABLE generos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/videos.csv' # Caminho corrigido
INTO TABLE videos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/pagamentos.csv' # Caminho corrigido
INTO TABLE pagamentos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/cancelamentos.csv' # Caminho corrigido
INTO TABLE cancelamentos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/eventos.csv' # Caminho corrigido
INTO TABLE eventos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/favoritos.csv' # Caminho corrigido
INTO TABLE favoritos
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/avaliacoes.csv' # Caminho corrigido
INTO TABLE avaliacoes
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/arquivos_csv/visualizacoes.csv' # Caminho corrigido
INTO TABLE visualizacoes
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

SET FOREIGN_KEY_CHECKS = 1;
