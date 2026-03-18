# 📊 Competitor Intelligence Lakehouse – Data Engineering Project

Pipeline de dados baseado em arquitetura **Lakehouse**, desenvolvido para ingestão, processamento e análise de dados de concorrência no setor alimentício.

---

## 🚀 Overview

Este projeto implementa um **pipeline de dados escalável utilizando Databricks e Delta Lake**, responsável por processar dados de múltiplas fontes e transformá-los em insights analíticos.

O pipeline foi projetado seguindo boas práticas modernas de engenharia de dados, incluindo:

* Arquitetura em camadas (**Bronze, Silver e Gold**)
* Processamento com SQL no Databricks
* Armazenamento otimizado com Delta Lake
* Consumo analítico estruturado

---

## 🧠 Problem Statement

Empresas do setor alimentício enfrentam dificuldades para entender seus concorrentes de forma estratégica.

Este projeto resolve esse problema através de:

* Centralização de dados de múltiplas fontes
* Tratamento e padronização dos dados
* Cálculo de métricas relevantes de negócio
* Disponibilização de dados prontos para análise

Principais perguntas respondidas:

* Qual a faixa de preço dos concorrentes?
* Como é o fluxo de pessoas nesses locais?
* Qual a densidade demográfica das regiões?

---

## ⚙️ Pipeline Architecture

<img src='[ARCHITECTURE] competitor-intelligence-lakehouse.jpg' alt='Arquitetura Lakehouse'>

---

## 🔄 Data Pipeline Stages

### 🔹 1. Data Ingestion

Responsável pela ingestão de dados diretamente do Amazon S3:

* Leitura de arquivos em formatos:

  * JSON
  * CSV
  * ZIP
* Armazenamento inicial na camada **Bronze**
* Preservação dos dados brutos (raw)

---

### 🔹 2. Data Processing (Silver Layer)

* Limpeza e padronização dos dados
* Validação de tipos
* Tratamento de valores nulos

---

### 🔹 3. Data Enrichment (Gold Layer)

* Criação de métricas analíticas:

  * Fluxo de pessoas (média, máximo, mínimo)
  * Segmentação por:

    * Dia da semana
    * Período (manhã, tarde, noite)

* Cálculo de densidade demográfica:

```text
densidade = população / área
```

* Preparação de dados prontos para consumo analítico

---

### 🔹 4. Data Storage

* Armazenamento em **Delta Lake**
* Formato otimizado (Parquet)
* Suporte a queries performáticas

---

### 🔹 5. Data Consumption

* Consulta via SQL, Python, Pyspark no Databricks
* Base pronta para:

  * BI
  * Dashboards
  * Análises exploratórias

---

## 🗂 Project Structure

```bash
competitor-intelligence-lakehouse/
│
├── config/
│   └── structure.ipynb
│
├── sql/
│   └── pipeline_competitor_intelligence_lakehouse.sql
│
├── .gitignore
├── .gitattributes
└── README.md
```

---

## 🛠 Tech Stack

* **Python**
* **SQL**
* **Databricks**
* **Delta Lake**
* **Amazon S3**

---

## 📊 Key Engineering Concepts

Este projeto demonstra na prática:

* Lakehouse Architecture
* Data Pipeline Design
* Data Modeling (Bronze / Silver / Gold)
* Batch Processing
* Data Quality
* SQL-based Transformations
* Data Enrichment
* Analytical Data Serving

---

## ▶️ Execution

O pipeline é executado dentro do ambiente Databricks:

* Execução do script SQL:

```sql
-- pipeline DLT principal
sql/pipeline_competitor_intelligence_lakehouse.sql
```

---

## 📈 Output

* Tabelas analíticas na camada Gold
* Métricas de fluxo de pessoas
* Dados de densidade demográfica
* Base pronta para consumo por analistas

---

<div align="center">

**Feito por [@Igor Santos](www.linkedin.com/in/igor-santos-50791a227) 😁**

</div>
