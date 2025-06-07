from faker import Faker
import pandas as pd
import random
import os

fake = Faker('pt_BR')

# Caminho completo da pasta onde salvar
diretorio = os.path.abspath(os.path.join('popular_dados', 'arquivos_csv'))

# Garante que a pasta existe, se não cria
os.makedirs(diretorio, exist_ok=True)

# ------------------ Planos ------------------
planos = ['Básico', 'Padrão', 'Premium']
df_planos = pd.DataFrame({
    'id_plano': list(range(1, len(planos) + 1)),
    'nome': planos,
    'valor': [19.9, 29.9, 39.9],
    'tipo': ['mensal'] * len(planos)
})

# ------------------ Usuários ------------------
usuarios = []
for i in range(1, 1001):
    usuarios.append({
        'id_usuario': i,
        'nome': fake.name(),
        'email': fake.email(),
        'id_plano': random.randint(1, len(planos))
    })
df_usuarios = pd.DataFrame(usuarios)

# ------------------ Gêneros ------------------
generos = ['Ação', 'Comédia', 'Drama', 'Terror', 'Romance']
df_generos = pd.DataFrame({
    'id_genero': list(range(1, len(generos)+1)),
    'nome': generos
})

# ------------------ Vídeos ------------------
videos = []
for i in range(1, 5001):
    videos.append({
        'id_video': i,
        'titulo': fake.sentence(nb_words=3),
        'duracao': random.randint(60, 180),
        'id_genero': random.randint(1, len(generos))
    })
df_videos = pd.DataFrame(videos)

# ------------------ Visualizações ------------------
visualizacoes = []
for i in range(1, 20001):
    visualizacoes.append({
        'id_visualizacao': i,
        'id_usuario': random.randint(1, 1000),
        'id_video': random.randint(1, 5000),
        'data_hora': fake.date_time_between(start_date='-3y', end_date='now')
    })
df_visualizacoes = pd.DataFrame(visualizacoes)

# ------------------ Avaliações ------------------
avaliacoes = []
for i in range(1, 10001):
    avaliacoes.append({
        'id_avaliacao': i,
        'id_usuario': random.randint(1, 1000),
        'id_video': random.randint(1, 5000),
        'nota': random.randint(1, 5),
        'comentario': fake.sentence()
    })
df_avaliacoes = pd.DataFrame(avaliacoes)

# ------------------ Favoritos ------------------
favoritos = []
for i in range(1, 10001):
    favoritos.append({
        'id_favorito': i,
        'id_usuario': random.randint(1, 1000),
        'id_video': random.randint(1, 5000),
        'data_favorito': fake.date_time_between(start_date='-3y', end_date='now')
    })
df_favoritos = pd.DataFrame(favoritos)

# ------------------ Dispositivos ------------------
tipos = ['Smartphone', 'SmartTV', 'Tablet', 'Notebook']
sistemas = ['Android', 'iOS', 'Windows', 'Linux']
dispositivos = []
for i in range(1, 2001):
    dispositivos.append({
        'id_dispositivo': i,
        'id_usuario': random.randint(1, 1000),
        'tipo': random.choice(tipos),
        'sistema_operacional': random.choice(sistemas)
    })
df_dispositivos = pd.DataFrame(dispositivos)

# ------------------ Eventos ------------------
acoes = ['login', 'logout', 'play', 'pause', 'like', 'dislike']
eventos = []
for i in range(1, 10001):
    eventos.append({
        'id_evento': i,
        'id_usuario': random.randint(1, 1000),
        'acao': random.choice(acoes),
        'data_hora': fake.date_time_between(start_date='-3y', end_date='now')
    })
df_eventos = pd.DataFrame(eventos)

# ------------------ Pagamentos ------------------
pagamentos = []
for i in range(1, 5001):
    pagamentos.append({
        'id_pagamento': i,
        'id_usuario': random.randint(1, 1000),
        'valor': random.choice([19.9, 29.9, 39.9]),
        'data_pagamento': fake.date_between(start_date='-3y', end_date='today')
    })
df_pagamentos = pd.DataFrame(pagamentos)

# ------------------ Cancelamentos ------------------
motivos = ['Problemas técnicos', 'Muito caro', 'Pouco conteúdo', 'Não usa mais']
cancelamentos = []
for i in range(1, 1001):
    cancelamentos.append({
        'id_cancelamento': i,
        'id_usuario': random.randint(1, 1000),
        'motivo': random.choice(motivos),
        'data_cancelamento': fake.date_between(start_date='-3y', end_date='today')
    })
df_cancelamentos = pd.DataFrame(cancelamentos)

# Salvar arquivos CSV
csv_paths = {}
for name, df in [
    ("planos", df_planos),
    ("usuarios", df_usuarios),
    ("generos", df_generos),
    ("videos", df_videos),
    ("visualizacoes", df_visualizacoes),
    ("avaliacoes", df_avaliacoes),
    ("favoritos", df_favoritos),
    ("dispositivos", df_dispositivos),
    ("eventos", df_eventos),
    ("pagamentos", df_pagamentos),
    ("cancelamentos", df_cancelamentos)
]:
    path = os.path.join(diretorio, f"{name}.csv")
    df.to_csv(path, index=False)
    csv_paths[name] = path

csv_paths
