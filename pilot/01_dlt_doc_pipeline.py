# Databricks notebook source
# Phase 2 (Pilot): DLT Pipeline for Document Processing
# Autoloader → ai_parse_document → extraction → governed Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC # Document Processing Pipeline (Lakeflow Declarative Pipeline)
# MAGIC
# MAGIC **Phase 2 takes the demo notebooks into a repeatable, scheduled pipeline.**
# MAGIC
# MAGIC | Layer | What It Does | Databricks Resource |
# MAGIC |-------|-------------|-------------------|
# MAGIC | Bronze | Ingest PDFs from UC Volume | Autoloader (cloudFiles, binaryFile) |
# MAGIC | Silver | Parse documents | `ai_parse_document` (AI Function) |
# MAGIC | Gold | Extract structured fields | Foundation Model API via `ai_query` |
# MAGIC | Governed | Apply ABAC policies | UC row filters + column masks |
# MAGIC
# MAGIC **Key design decisions:**
# MAGIC - `dlt.create_sink` + `@dlt.append_flow` to avoid re-parsing on full refresh
# MAGIC - `failOnError => false` so individual doc failures don't crash the pipeline
# MAGIC - DLT expectations enforce data quality at each layer

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("pipeline.catalog", "retail_insight_demo_catalog")
SCHEMA = spark.conf.get("pipeline.schema", "governance_demo")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_documents"
LLM_ENDPOINT = "databricks-claude-sonnet-4"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Ingest PDFs from UC Volume
# MAGIC
# MAGIC Autoloader watches the Volume for new files. `binaryFile` format reads
# MAGIC entire PDFs as binary — required input for `ai_parse_document`.

# COMMAND ----------

@dlt.table(
    name="bronze_raw_documents",
    comment="Raw PDF files ingested from UC Volume via Autoloader"
)
def bronze_raw_documents():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(VOLUME_PATH)
        .select(
            F.col("path").alias("file_path"),
            "content",
            F.col("modificationTime").alias("file_modified_at"),
            F.col("length").alias("file_size_bytes"),
            F.current_timestamp().alias("ingested_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Parse Documents with ai_parse_document
# MAGIC
# MAGIC **Critical:** Use `failOnError => false` so a single corrupted PDF
# MAGIC doesn't stop the entire pipeline. Failed parses are captured for review.

# COMMAND ----------

doc_parse_quality = {
    "valid_parse": "parsed_result IS NOT NULL",
    "has_text": "length(full_text) > 50",
    "no_parse_error": "parse_error IS NULL",
}

@dlt.table(
    name="silver_parsed_documents",
    comment="Parsed document text extracted via ai_parse_document"
)
@dlt.expect_all(doc_parse_quality)
def silver_parsed_documents():
    return (
        dlt.readStream("bronze_raw_documents")
        .withColumn("parsed_result", F.expr(
            "ai_parse_document(content, failOnError => false)"
        ))
        .withColumn("parse_error", F.expr(
            "CASE WHEN parsed_result:error IS NOT NULL THEN parsed_result:error:message ELSE NULL END"
        ))
        .withColumn("full_text", F.expr("""
            CONCAT_WS('\\n\\n',
                TRANSFORM(
                    FROM_JSON(
                        TO_JSON(variant_get(parsed_result, '$.document.elements')),
                        'ARRAY<STRUCT<content: STRING, type: STRING>>'
                    ),
                    x -> x.content
                )
            )
        """))
        .withColumn("page_count", F.expr(
            "variant_get(parsed_result, '$.document.page_count', 'INT')"
        ))
        .select(
            "file_path", "full_text", "page_count", "parse_error",
            "file_modified_at", "ingested_at",
            F.current_timestamp().alias("parsed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sink: Persist Parsed Output (Survives Full Refresh)
# MAGIC
# MAGIC DLT streaming tables recompute on full refresh. Without a sink,
# MAGIC every pipeline restart re-parses ALL PDFs — expensive with ai_parse_document.
# MAGIC The sink persists results in an external Delta table.

# COMMAND ----------

dlt.create_sink(
    name="persisted_parsed_sink",
    format="delta",
    options={"tableName": f"{CATALOG}.{SCHEMA}.parsed_documents"}
)

@dlt.append_flow(target="persisted_parsed_sink")
def persist_parsed():
    return dlt.readStream("silver_parsed_documents").filter("parse_error IS NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Extract Structured Fields
# MAGIC
# MAGIC Uses Foundation Model API via `ai_query` to extract key fields from
# MAGIC the parsed text. The prompt is loaded from MLflow Prompt Registry.

# COMMAND ----------

EXTRACTION_PROMPT = """You are a financial document extraction specialist.
Extract structured data from the following document text.

Required fields:
- doc_type: Document type (KIID, Fund Factsheet, Trade Confirmation, Compliance Report, Other)
- fund_name: Full official name (null if not a fund document)
- isin: ISIN code (null if not present)
- risk_rating: Numeric risk indicator 1-7 (null if not present)
- ongoing_charges: As percentage string e.g. "1.45%" (null if not present)
- doc_date: Publication/trade date as YYYY-MM-DD
- client_name: Client or counterparty name (null if not present)
- jurisdiction: Country or region
- compliance_status: If mentioned (null if not present)

Return ONLY valid JSON. Use null for missing fields.

Document:
{doc_text}"""

extraction_quality = {
    "valid_extraction": "extracted_json IS NOT NULL",
    "no_extract_error": "extraction_error IS NULL",
}

@dlt.table(
    name="gold_extracted_entities",
    comment="Structured entities extracted from parsed documents"
)
@dlt.expect_all(extraction_quality)
def gold_extracted_entities():
    return (
        dlt.readStream("silver_parsed_documents")
        .filter("parse_error IS NULL AND length(full_text) > 50")
        .withColumn("extraction_result", F.expr(f"""
            ai_query(
                '{LLM_ENDPOINT}',
                CONCAT('{EXTRACTION_PROMPT.replace(chr(10), " ").replace("{doc_text}", "' , full_text, '")}'),
                failOnError => false
            )
        """))
        .withColumn("extraction_error", F.col("extraction_result.errorMessage"))
        .withColumn("extracted_json", F.expr("""
            CASE WHEN extraction_result.errorMessage IS NULL
                 THEN from_json(extraction_result, 'STRUCT<doc_type: STRING, fund_name: STRING, isin: STRING, risk_rating: INT, ongoing_charges: STRING, doc_date: STRING, client_name: STRING, jurisdiction: STRING, compliance_status: STRING>')
                 ELSE NULL END
        """))
        .select(
            "file_path", "full_text", "extracted_json.*", "extraction_error",
            "page_count", "parsed_at",
            F.current_timestamp().alias("extracted_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary
# MAGIC
# MAGIC ```
# MAGIC UC Volume (PDFs)
# MAGIC     │ Autoloader (binaryFile)
# MAGIC     ▼
# MAGIC Bronze: raw binary files
# MAGIC     │ ai_parse_document (failOnError => false)
# MAGIC     ▼
# MAGIC Silver: parsed text + page count
# MAGIC     │── Sink: persisted_parsed_docs (survives refresh)
# MAGIC     │
# MAGIC     │ ai_query with extraction prompt
# MAGIC     ▼
# MAGIC Gold: structured entities (doc_type, isin, jurisdiction, ...)
# MAGIC     │
# MAGIC     ▼
# MAGIC UC ABAC policies apply automatically to downstream queries
# MAGIC ```
# MAGIC
# MAGIC **To deploy this pipeline:**
# MAGIC ```
# MAGIC databricks pipelines create --json '{
# MAGIC   "name": "doc-processing-pilot",
# MAGIC   "catalog": "retail_insight_demo_catalog",
# MAGIC   "target": "governance_demo",
# MAGIC   "continuous": false,
# MAGIC   "libraries": [{"notebook": {"path": "/path/to/this/notebook"}}],
# MAGIC   "configuration": {
# MAGIC     "pipeline.catalog": "retail_insight_demo_catalog",
# MAGIC     "pipeline.schema": "governance_demo"
# MAGIC   }
# MAGIC }'
# MAGIC ```
