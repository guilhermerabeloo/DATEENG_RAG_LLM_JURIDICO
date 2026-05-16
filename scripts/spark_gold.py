import requests
import chromadb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime


spark = SparkSession.builder \
    .appName("RAG_LLM_JURIDICO_GOLD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ====================
## FUNÇÕES AUXILIARES
# ====================

# 1. EMBEDDING: Gerando vetor via Ollama (API local)
@F.udf(returnType=ArrayType(FloatType()))
def generate_embedding(text):
    """
    Chama o container do Ollama para gerar o embedding do texto.
    """
    if not text:
        return None
    
    url = "http://ollama:11434/api/embeddings"
    payload = {
        "model": "nomic-embed-text", 
        "prompt": text
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            return response.json().get("embedding")
        return None
    except Exception as e:
        return None

# 2. LOAD: Função para carregar dados no ChromaDB por partição
def upsert_to_chroma(partition):
    """
    Função executada em cada partição do Spark para inserir dados no Chroma.
    """
    client = chromadb.HttpClient(host='chromadb', port=8000)
    collection = client.get_or_create_collection(name="boate_kiss_index")
    
    for row in partition:
        # Gerar um ID único baseado no nome do arquivo e hash do texto ou timestamp
        unique_id = f"{row['file_name']}_{hash(row['text'])}"
        
        collection.upsert(
            ids=[unique_id],
            embeddings=[row['embedding']],
            metadatas=[{
                "file_name": row['file_name'],
                "ingestion_timestamp": str(row['ingestion_timestamp'])
            }],
            documents=[row['text']]
        )

# ====================
## ETL (GOLD)
# ====================

# 1. Extract: Lendo da Silver (Delta)
print(f"[{datetime.now()}] Lendo dados da camada Silver...")
df_silver = spark.read.format("delta").load("/opt/bitnami/spark/data/delta/silver/boate_kiss_refined")

# 2. Transform: Gerando Embeddings
print(f"[{datetime.now()}] Gerando embeddings via Ollama (nomic-embed-text)...")
df_gold = df_silver.withColumn("embedding", generate_embedding(F.col("text"))) \
                   .filter(F.col("embedding").isNotNull())

# 3. Load: Ingestão no ChromaDB
print(f"[{datetime.now()}] Iniciando ingestão no ChromaDB...")
df_gold.foreachPartition(upsert_to_chroma)
