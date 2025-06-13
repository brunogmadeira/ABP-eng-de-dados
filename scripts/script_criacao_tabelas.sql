CREATE TABLE planos (
    id_plano INT PRIMARY KEY,
    nome VARCHAR(255),
    valor DECIMAL(10, 2),
    tipo VARCHAR(100)
);

CREATE TABLE usuarios (
    id_usuario INT PRIMARY KEY,
    nome VARCHAR(255),
    email VARCHAR(255),
    id_plano INT,
    FOREIGN KEY (id_plano) REFERENCES planos(id_plano)
);

CREATE TABLE generos (
    id_genero INT PRIMARY KEY,
    nome VARCHAR(100)
);

CREATE TABLE videos (
    id_video INT PRIMARY KEY,
    titulo VARCHAR(255),
    duracao INT,
    id_genero INT,
    FOREIGN KEY (id_genero) REFERENCES generos(id_genero)
);

CREATE TABLE visualizacoes (
    id_visualizacao INT PRIMARY KEY,
    id_usuario INT,
    id_video INT,
    data_hora DATETIME,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario),
    FOREIGN KEY (id_video) REFERENCES videos(id_video)
);

CREATE TABLE avaliacoes (
    id_avaliacao INT PRIMARY KEY,
    id_usuario INT,
    id_video INT,
    nota INT,
    comentario TEXT,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario),
    FOREIGN KEY (id_video) REFERENCES videos(id_video)
);

CREATE TABLE favoritos (
    id_favorito INT PRIMARY KEY,
    id_usuario INT,
    id_video INT,
    data_favorito DATETIME,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario),
    FOREIGN KEY (id_video) REFERENCES videos(id_video)
);

CREATE TABLE dispositivos (
    id_dispositivo INT PRIMARY KEY,
    id_usuario INT,
    tipo VARCHAR(100),
    sistema_operacional VARCHAR(100),
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario)
);

CREATE TABLE cancelamentos (
    id_cancelamento INT PRIMARY KEY,
    id_usuario INT,
    motivo TEXT,
    data_cancelamento DATETIME,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario)
);

CREATE TABLE eventos (
    id_evento INT PRIMARY KEY,
    id_usuario INT,
    acao VARCHAR(255),
    data_hora DATETIME,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario)
);

CREATE TABLE pagamentos (
    id_pagamento INT PRIMARY KEY,
    id_usuario INT,
    valor DECIMAL(10, 2),
    data_pagamento DATETIME,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario)
);
