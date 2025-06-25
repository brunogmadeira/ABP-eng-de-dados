# 🚀 Configuração do Ambiente - ABP Engenharia de Dados

## 📋 Pré-requisitos

Antes de iniciar, certifique-se de ter instalado em sua máquina:

- 🐍 **Python 3.11.9**
- 🐳 **Docker Desktop** (manter aberto durante o uso)
- 📁 **Git**
- 💻 **VS Code**
- 🚀 **Astro CLI**

## ⚙️ Configuração do Ambiente

### 📂 Preparação do Workspace

1. Crie a pasta `ABP-eng-de-dados` dentro da pasta `Documents`
2. Abra o VS Code na pasta `C:\Users\{seu-usuario}\Documents`

### 📥 Clone do Repositório

Execute o seguinte comando no terminal do VS Code:

```bash
git clone https://github.com/brunogmadeira/ABP-eng-de-dados.git
```

### ⬇️ Instalação do Astro CLI

Execute o PowerShell como **administrador** e rode o comando:

```powershell
winget install -e --id Astronomer.Astro
```

## 🔧 Configuração dos Serviços

### 🐳 Inicialização do Docker

Na pasta **raiz** do projeto, execute:

```bash
docker compose up -d
```

### 🌪️ Configuração do Airflow

1. Navegue até a pasta `astro` dentro do projeto:
   ```bash
   cd astro
   ```

2. Inicie o ambiente Airflow:
   ```bash
   astro dev start
   ```

3. Acesse o Airflow através do navegador na porta **8080**: `http://localhost:8080` 🌐

### 📦 Instalação de Dependências Python

Execute o comando para instalar o conector MySQL:

```bash
pip install mysql-connector-python
```

### 📊 Ingestão de Dados

Execute o script Python para realizar a ingestão dos dados CSV:

```bash
python ingestao_dados_mysql.py
```

## 📝 Observações Importantes

- 🐳 Mantenha o **Docker Desktop** aberto durante todo o processo
- 📁 Certifique-se de estar na pasta correta ao executar cada comando
- 🌐 O Airflow estará disponível na porta **8080** após a inicialização

## 🛠️ Resolução de Problemas

Se encontrar algum erro durante a configuração:

1. Verifique se todos os pré-requisitos estão instalados
2. Confirme se o Docker Desktop está executando
3. Certifique-se de estar executando os comandos nas pastas corretas
4. Verifique se as portas necessárias não estão sendo utilizadas por outros serviços

---