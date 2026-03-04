-- Databricks notebook source
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
CREATE OR REFRESH STREAMING LIVE TABLE competitor_intelligence_dev.bronze.populacao
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
