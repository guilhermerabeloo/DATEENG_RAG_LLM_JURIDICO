import pypdf
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from langchain_text_splitters import RecursiveCharacterTextSplitter

spark = SparkSession.builder \
    .appName("RAG_LLM_JURIDICO_SILVER") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ====================
## FUNCOES AUXILIARES
# ====================

# 1. PARSING: Extraindo texto do PDF binário
@F.udf(returnType=StringType())
def extract_pdf_text(content):
    import io
    try:
        reader = pypdf.PdfReader(io.BytesIO(content))
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text
    except Exception as e:
        return None

# 2. LIMPEZA: Removendo ruídos de documentos digitalizados
@F.udf(returnType=StringType())
def clean_legal_text(raw_text):
    if not raw_text:
        return None
    
    # Remover números de página (e.g., "1", "página 1", "p. 1")
    text = re.sub(r'(?:p(?:á|a)?g(?:e|ina)?\.?\s*\d+|\b\d+\b(?:\s*$))', '', raw_text, flags=re.MULTILINE)
    
    # Remover cabeçalhos 
    text = re.sub(r'(?i)(tribunal|corte|poder\s+judiciário|justiça.*?estado).*?(?:\n|$)', '', text)
    
    # Remover rodapés
    text = re.sub(r'(?i)(assinado\s+digitalmente|documento\s+assinado|certificado\s+digital).*?(?:\n|$)', '', text)
    
    # Remover espaços em branco
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    # Retornar None se o texto fica muito pequeno após limpeza
    if len(text) < 50:
        return None
    
    return text

# 3. CHUNKING: Dividindo em chunks com overlap
@F.udf(returnType=ArrayType(StringType()))
def chunk_text(cleaned_text):
    if not cleaned_text:
        return None
    
    try:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=100,
            separators=["\n\n", "\n", " ", ""]
        )
        chunks = splitter.split_text(cleaned_text)
        return chunks if chunks else None
    except Exception as e:
        return None



# ====================
## ETL
# ====================

# Extract
df_bronze = spark.read.format("delta").load("/opt/bitnami/spark/data/delta/bronze/boate_kiss_files")

df_extracted = df_bronze.withColumn("raw_text", extract_pdf_text(F.col("content"))) \
                         .filter(F.col("raw_text").isNotNull())


# Transform
df_cleaned = df_extracted.withColumn("cleaned_text", clean_legal_text(F.col("raw_text"))) \
                          .filter(F.col("cleaned_text").isNotNull())

df_chunked = df_cleaned.withColumn("chunks", chunk_text(F.col("cleaned_text"))) \
                        .filter(F.col("chunks").isNotNull())

df_silver = df_chunked.withColumn("chunk", F.explode(F.col("chunks"))) \
                       .withColumn("processing_timestamp", F.current_timestamp()) \
                       .select(
                           F.col("file_name"),
                           F.col("chunk").alias("text"),
                           F.col("cleaned_text").alias("full_text"),
                           F.col("ingestion_timestamp"),
                           F.col("processing_timestamp")
                       ) # Explodir chunks (1 linha por chunk) e adicionar metadados

# Load
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/opt/bitnami/spark/data/delta/silver/boate_kiss_refined")

