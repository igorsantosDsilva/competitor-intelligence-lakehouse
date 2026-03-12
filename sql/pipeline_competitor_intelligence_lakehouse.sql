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
CREATE OR REFRESH LIVE TABLE competitor_intelligence_dev.silver.silver_eventos_de_fluxo AS
SELECT
  codigo AS codigo_evento_treated,
  DATE(datetime) AS data_evento_treated,
  DATE_FORMAT(datetime, 'HH:mm:ss') AS hora_evento_treated,
  codigo_concorrente AS id_concorrentes_treated
FROM
  LIVE.bronze.bronze_eventos_de_fluxo
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY codigo, datetime, codigo_concorrente
    ORDER BY datetime
  ) = 1

-- COMMAND ----------

-- DBTITLE 1,SILVER POPULACAO
CREATE OR REFRESH LIVE TABLE competitor_intelligence_dev.silver.silver_populacao AS
SELECT
  TRIM(codigo) AS id_bairros_treated,
  TRY_CAST(populacao AS INT) AS populacao_treated,
  YEAR(consumed_date) AS year,
  MONTH(consumed_date) AS month,
  DAY(consumed_date) AS day
FROM
  LIVE.bronze.bronze_populacao

-- COMMAND ----------

-- MAGIC %md
-- MAGIC GOLD

-- COMMAND ----------

-- DBTITLE 1,GOLD_CONCORRENTES
CREATE OR REFRESH LIVE TABLE competitor_intelligence_dev.gold.gold_concorrentes AS
SELECT
  c.id_concorrentes_treated AS id_concorrentes,
  c.nome_estabelecimento_treated AS nome_estabelecimento,
  c.categoria_treated AS categoria,
  c.faixa_preco_treated AS faixa_preco,
  c.endereco_completo_treated AS endereco_completo,
  c.cep_treated AS cep_treated,
  c.num_casa_treated AS num_casa,
  c.municipio_treated AS municipio,
  c.uf_treated AS uf,
  b.id_bairros_treated AS id_bairros,
  b.bairro_treated AS bairro,
  b.area_treated AS area,
  p.populacao_treated AS populacao,
  p.populacao_treated / b.area_treated AS densidade_demografica
FROM
  LIVE.silver.silver_concorrentes c
    LEFT JOIN LIVE.silver.silver_bairros b
      ON c.id_bairros_treated = b.id_bairros_treated
    LEFT JOIN LIVE.silver.silver_populacao p
      ON b.id_bairros_treated = p.id_bairros_treated

-- COMMAND ----------

-- DBTITLE 1,GOLD_FLUXO_CONCORRENTES
CREATE OR REFRESH LIVE TABLE competitor_intelligence_dev.gold.gold_fluxo_concorrentes AS
SELECT

    id_concorrentes_treated,

    DAYOFWEEK(data_evento_treated) AS dia_semana,

    CASE
        WHEN HOUR(hora_evento_treated) BETWEEN 6 AND 11 THEN 'MANHA'
        WHEN HOUR(hora_evento_treated) BETWEEN 12 AND 17 THEN 'TARDE'
        ELSE 'NOITE'
    END AS periodo_dia,

    COUNT(*) AS total_visitas,
    AVG(COUNT(*)) OVER(PARTITION BY id_concorrentes_treated) AS media_visitas,
    MAX(COUNT(*)) OVER(PARTITION BY id_concorrentes_treated) AS max_visitas,
    MIN(COUNT(*)) OVER(PARTITION BY id_concorrentes_treated) AS min_visitas

FROM LIVE.silver.silver_eventos_de_fluxo

GROUP BY
    id_concorrentes_treated,
    dia_semana,
    periodo_dia

-- COMMAND ----------

-- DBTITLE 1,GOLD_DEMOGRAFIA
CREATE OR REFRESH LIVE TABLE competitor_intelligence_dev.gold.gold_demografia_bairros AS
SELECT

    b.id_bairros_treated,
    b.bairro_treated,
    b.municipio_treated,
    b.uf_treated,

    b.area_treated,
    p.populacao_treated,

    p.populacao_treated / b.area_treated AS densidade_demografica

FROM LIVE.silver.silver_bairros b

LEFT JOIN LIVE.silver.silver_populacao p
ON b.id_bairros_treated = p.id_bairros_treated
