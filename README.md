# Streaming de vídeos - Pipeline Completo

## 📊 Visão Geral

Este projeto implementa uma solução completa de engenharia de dados, abrangendo desde a ingestão e transformação de dados até a criação de dashboards interativos para análise e monitoramento. A arquitetura foi desenvolvida utilizando tecnologias modernas de big data, incluindo Apache Spark, Data Lakes e ferramentas de visualização avançadas. A temática escolhida — plataforma de streaming de vídeo — permite trabalhar com dados realistas e ricos em volume e variedade.

## 🏗️ Arquitetura da Solução

O projeto segue uma arquitetura de **Data Lake** com processamento em camadas:

- **📥 Ingestão**: Coleta automatizada de dados de múltiplas fontes
- **🥉 Bronze Layer**: Dados brutos no formato original (Delta/Iceberg)
- **🥈 Silver Layer**: Dados limpos e estruturados
- **🥇 Gold Layer**: Dados agregados e otimizados para consumo
- **📈 Dashboards**: Visualizações e KPIs para tomada de decisão

## 📋 Pré-requisitos

Antes de iniciar, certifique-se de ter instalado em sua máquina:

- **Python 3.11.9**
- **Docker Desktop** (manter aberto durante o uso)
- **Git**
- **VS Code**
- **Astro CLI**

## 🚀 Configuração do Ambiente

### 1. Preparação do Workspace

1. Crie a pasta `ABP-eng-de-dados` dentro da pasta `Documents`
2. Abra o VS Code na pasta `C:\Users\{seu-usuario}\Documents`

### 2. Clone do Repositório

Execute o seguinte comando no terminal do VS Code:

```bash
git clone https://github.com/brunogmadeira/ABP-eng-de-dados.git
```

### 3. Instalação do Astro CLI

Execute o PowerShell como **administrador** e rode o comando:

```powershell
winget install -e --id Astronomer.Astro
```

## 🔧 Configuração dos Serviços

### 4. Inicialização do Docker

Na pasta **raiz** do projeto, execute:

```bash
docker compose up -d
```

### 5. Configuração do Airflow

1. Navegue até a pasta `astro` dentro do projeto:
   ```bash
   cd astro
   ```

2. Inicie o ambiente Airflow:
   ```bash
   astro dev start
   ```

3. Acesse o Airflow através do navegador na porta **8080**: `http://localhost:8080`

### 6. Instalação de Dependências Python

Execute o comando para instalar o conector MySQL:

```bash
pip install mysql-connector-python
```

### 7. Ingestão de Dados

Execute o script Python para realizar a ingestão dos dados CSV:

```bash
python ingestao_dados_mysql.py
```

## 📝 Observações Importantes

- Mantenha o **Docker Desktop** aberto durante todo o processo
- Certifique-se de estar na pasta correta ao executar cada comando
- O Airflow estará disponível na porta **8080** após a inicialização

## 🛠️ Resolução de Problemas

Se encontrar algum erro durante a configuração:

1. Verifique se todos os pré-requisitos estão instalados
2. Confirme se o Docker Desktop está executando
3. Certifique-se de estar executando os comandos nas pastas corretas
4. Verifique se as portas necessárias não estão sendo utilizadas por outros serviços

---

**Desenvolvido para o curso de Engenharia de Dados**

## ✍️ Autores

* Bruno Girardi Madeira - [brunogmadeira] (https://github.com/brunogmadeira)
* João Gabriel Rosso - [joaodagostin] (https://github.com/joaodagostin)
* João Pedro Taufembach - [JoaoAcordi] (https://github.com/JoaoAcordi)
* Rafael Frassetto - [rafafrassetto] (https://github.com/rafafrassetto)
* Yasmin Elias Michels - [YasminMichels] (https://github.com/YasminMichels)


