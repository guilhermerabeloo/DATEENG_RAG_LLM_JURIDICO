from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


spark = SparkSession.builder \
    .appName("RAG_LLM_JURIDICO_BRONZE") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

RAW_PATH = "/opt/spark/data/pdf/"
BRONZE_OUTPUT_PATH = "/opt/spark/data/delta/bronze/boate_kiss_files"

def ingest_pdfs_to_bronze(input_path, output_path):
    """
    Lê arquivos PDF como binários e salva na camada Bronze com metadados.
    """
    print(f"[{datetime.now()}] Iniciando ingestão de: {input_path}")
    
    # Lendo arquivos em modo binário
    df_raw = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.pdf") \
        .option("recursiveFileLookup", "true") \
        .load(input_path)

    # 3. Adição de Metadados
    df_bronze = df_raw.select(
        F.col("path").alias("source_path"),
        F.col("content"), # O PDF bruto em binário
        F.current_timestamp().alias("ingestion_timestamp"),
        F.input_file_name().alias("file_name")
    )
    
    df_bronze.show()

    # 4. Escrita na Camada Bronze 
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)
    
    print(f"[{datetime.now()}] Ingestão concluída com sucesso em: {output_path}")

if __name__ == "__main__":
    ingest_pdfs_to_bronze(RAW_PATH, BRONZE_OUTPUT_PATH)