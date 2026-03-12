-- Databricks notebook source
-- MAGIC %md
-- MAGIC BRONZE

-- COMMAND ----------

-- DBTITLE 1,BRONZE BAIRROS
CREATE OR REFRESH STREAMING LIVE TABLE competitor_intelligence_dev.bronze.bronze_bairros
AS
SELECT *,
CURRENT_DATE() AS consumed_date
FROM cloud_files(
  "/Volumes/workspace/default/raw/bairros",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
)

-- COMMAND ----------

-- DBTITLE 1,BRONZE  CONCORRENTES
CREATE OR REFRESH STREAMING LIVE TABLE competitor_intelligence_dev.bronze.bronze_concorrentes
AS
SELECT *,
CURRENT_DATE() AS consumed_date
FROM cloud_files(
  "/Volumes/workspace/default/raw/concorrentes",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
)

-- COMMAND ----------

-- DBTITLE 1,BRONZE EVENTOS DE FLUXO
CREATE OR REFRESH STREAMING LIVE TABLE competitor_intelligence_dev.bronze.bronze_eventos_de_fluxo
AS
SELECT *,
CURRENT_DATE() AS consumed_date
FROM cloud_files(
  "/Volumes/workspace/default/raw/eventos_de_fluxo/",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
)

-- COMMAND ----------

-- DBTITLE 1,BRONZE POPULACAO
CREATE OR REFRESH STREAMING LIVE TABLE competitor_intelligence_dev.bronze.bronze_populacao
AS
SELECT *,
CURRENT_DATE() AS consumed_date
FROM cloud_files(
  "/Volumes/workspace/default/raw/populacao/",
  "json",
  map(
    "inferSchema", "true"
  )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SILVER

-- COMMAND ----------

-- DBTITLE 1,SILVER BAIRROS
CREATE OR REFRESH LIVE TABLE competitor_intelligence_dev.silver.silver_bairros AS
SELECT
  codigo AS id_bairros_treated,
  UPPER(TRANSLATE(nome, 'áàãâäéèêëíìîïóòõôöúùûüç', 'aaaaaeeeeiiiiooooouuuuc')) AS bairro_treated,
  UPPER(TRANSLATE(municipio, 'áàãâäéèêëíìîïóòõôöúùûüç', 'aaaaaeeeeiiiiooooouuuuc')) AS municipio_treated,
  uf AS uf_treated,
  TRY_CAST(area AS DECIMAL(10, 6)) AS area_treated
FROM
  LIVE.bronze.bronze_bairros

-- COMMAND ----------

-- DBTITLE 1,SILVER CONCORRENTES
CREATE OR REFRESH LIVE TABLE competitor_intelligence_dev.silver.silver_concorrentes AS
SELECT
  codigo AS id_concorrentes_treated,
  TRIM(UPPER(TRANSLATE(nome, 'áàãâäéèêëíìîïóòõôöúùûüç', 'aaaaaeeeeiiiiooooouuuuc'))) AS nome_estabelecimento_treated,
  TRIM(UPPER(categoria)) AS categoria_treated,
  TRY_CAST(faixa_preco AS TINYINT) AS faixa_preco_treated,
  TRIM(UPPER(TRANSLATE(endereco, 'áàãâäéèêëíìîïóòõôöúùûüç', 'aaaaaeeeeiiiiooooouuuuc'))) AS endereco_completo_treated,
  REGEXP_EXTRACT(endereco, '\\d{5}-?\\d{3}', 0) AS cep_treated,
  REGEXP_EXTRACT(
    REGEXP_REPLACE(endereco, '\\d{5}-?\\d{3}', ''),
    ',\\s*(\\d+),',
    1) AS num_casa_treated,
  UPPER(municipio) AS municipio_treated,
  UPPER(uf) AS uf_treated,
  codigo_bairro AS id_bairros_treated
FROM LIVE.bronze.bronze_concorrentes

-- COMMAND ----------

-- DBTITLE 1,SILVER EVENTOS DE FLUXO


-- COMMAND ----------

-- DBTITLE 1,SILVER POPULACAO


-- COMMAND ----------

-- MAGIC %md
-- MAGIC GOLD
