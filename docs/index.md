# 🎬 Streaming de Vídeos - Pipeline Completo

## 📊 Visão Geral

Este projeto implementa uma solução completa de engenharia de dados, abrangendo desde a ingestão e transformação de dados até a criação de dashboards interativos para análise e monitoramento. 

A arquitetura foi desenvolvida utilizando tecnologias modernas de big data, incluindo **Apache Spark**, **Data Lakes** e ferramentas de visualização avançadas. A temática escolhida — plataforma de streaming de vídeo — permite trabalhar com dados realistas e ricos em volume e variedade.

## 🏗️ Arquitetura da Solução

O projeto segue uma arquitetura de **Data Lake** com processamento em camadas:

### 📥 Camada de Ingestão
- Coleta automatizada de dados de múltiplas fontes
- Processamento em tempo real e batch
- Validação e qualidade dos dados

### 🥉 Bronze Layer
- Dados brutos no formato original
- Armazenamento em formatos Delta/Iceberg
- Histórico completo de todas as informações

### 🥈 Silver Layer
- Dados limpos e estruturados
- Aplicação de regras de negócio
- Padronização e normalização

### 🥇 Gold Layer
- Dados agregados e otimizados para consumo
- Métricas de negócio calculadas
- Pronto para análises e relatórios

### 📈 Camada de Visualização
- Dashboards interativos
- KPIs para tomada de decisão
- Relatórios automatizados

## 🎯 Objetivos do Projeto

- **📊 Análise de Comportamento**: Entender padrões de consumo de conteúdo
- **🔍 Monitoramento**: Acompanhar métricas de performance em tempo real
- **💡 Insights**: Gerar recomendações baseadas em dados
- **⚡ Escalabilidade**: Arquitetura preparada para crescimento

## 🛠️ Tecnologias Utilizadas

- **🐍 Python**: Linguagem principal para processamento
- **🌪️ Apache Airflow**: Orquestração de workflows
- **🐳 Docker**: Containerização dos serviços
- **🗄️ MySQL**: Banco de dados relacional
- **📊 Power BI/Grafana**: Visualização de dados
- **☁️ Apache Spark**: Processamento distribuído

## 📋 Pré-requisitos

Antes de iniciar, certifique-se de ter instalado em sua máquina:

- 🐍 **Python 3.11.9**
- 🐳 **Docker Desktop** (manter aberto durante o uso)
- 📁 **Git**
- 💻 **VS Code**
- 🚀 **Astro CLI**

## 🚀 Como Começar

1. **📖 Leia a documentação** de configuração do ambiente
2. **⚙️ Configure** seu ambiente de desenvolvimento
3. **🔄 Execute** os pipelines de dados
4. **📊 Explore** os dashboards criados

## 🎓 Contexto Acadêmico

Este projeto foi desenvolvido como parte do curso de **Engenharia de Dados**, aplicando conceitos modernos de:

- **Data Engineering**: Pipelines robustos e escaláveis
- **Data Architecture**: Padrões de mercado
- **DevOps**: Automação e monitoramento
- **Analytics**: Insights orientados por dados

---

🚀 **Pronto para começar?** Acesse a seção de [Configuração do Ambiente](config-ambiente.md) e siga o passo a passo!