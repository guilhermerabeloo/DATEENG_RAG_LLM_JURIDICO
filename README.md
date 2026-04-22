# PROJETO: LEGAL-RAG – PLATAFORMA DE IA JURÍDICA
**Foco:** Engenharia de Dados para LLMs (Large Language Models)

## 1. Visão Geral
Este projeto consiste na criação de uma plataforma de busca semântica e assistente inteligente para o setor jurídico. O objetivo é transformar documentos jurídicos "sujos" e não estruturados em uma base de conhecimento vetorial altamente performática, permitindo consultas em linguagem natural com citação de fontes e conformidade com a LGPD.

## 2. Arquitetura Técnica e Stack
O projeto segue o padrão de Engenharia de Dados moderna (Data-Centric AI) e utiliza:

*   **Infraestrutura:** Terraform (Azure/Fabric) e Docker (Stack isolada).
*   **Orquestração:** Apache Airflow.
*   **Motor de Processamento:** PySpark (Medallion Architecture).
*   **Camada de IA:** Ollama (Llama 3 e Nomic-Embed-Text).
*   **Banco de Vetores:** ChromaDB.
*   **Framework RAG:** LangChain.
*   **Interface:** Streamlit (UI para chat).

## 3. Fluxo de Dados (Pipeline)
1. **Bronze (Raw):** Ingestão de JSON/PDF jurídicos (Ex: DataJud/Kaggle) via Airflow para OneLake/Local.
2. **Silver (Clean):** ETL com PySpark para limpeza, normalização e anonimização de PII (LGPD).
3. **Gold (Vector):** Segmentação de texto (Chunking) e geração de Embeddings via Spark + Ollama.
4. **Serving (RAG):** Interface de consulta onde o LangChain orquestra a busca no ChromaDB e a geração de resposta pela LLM.

## 4. Diferenciais de Portfólio
*   **Escalabilidade:** Uso de Spark para processamento distribuído de embeddings.
*   **Governança:** Camada de anonimização de dados sensíveis.
*   **Automação:** Provisionamento via Terraform e Docker.
*   **Custo Zero:** Execução 100% local via Ollama ou via Fabric Trial.

## 5. Cronograma de Desenvolvimento
*   **Fase 1:** Configuração do ambiente Docker (Airflow, Spark, ChromaDB, Ollama).
*   **Fase 2:** Desenvolvimento do pipeline de limpeza e chunking em PySpark.
*   **Fase 3:** Implementação da lógica de RAG com LangChain.
*   **Fase 4:** Criação da interface Streamlit e automação com Terraform.
