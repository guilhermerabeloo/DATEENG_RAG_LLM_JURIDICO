from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum


DEFAULT_ARGS = {
    'owner': 'Guilherme',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': pendulum.duration(minutes=5),
}

@dag(
    dag_id='etl_spark_rag_juridico',
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 5, 9, tz="UTC"),
    catchup=False,
)

def spark_dag():
    
    start = EmptyOperator(task_id='start')

    spark_bronze = SparkSubmitOperator(
        task_id='spark_submit_bronze',
        application="/opt/airflow/scripts/spark_bronze.py",
        packages='io.delta:delta-spark_2.12:3.1.0',
        conn_id='spark_standalone',
        conf={
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.hadoop.fs.permissions.umask-mode': '000',
        },
    )
    
    spark_silver = SparkSubmitOperator(
        task_id='spark_submit_silver',
        application="/opt/airflow/scripts/spark_silver.py",
        packages='io.delta:delta-spark_2.12:3.1.0',
        conn_id='spark_standalone',
        conf={
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.hadoop.fs.permissions.umask-mode': '000',
        },
    )
    
    spark_gold = SparkSubmitOperator(
        task_id='spark_submit_gold',
        application="/opt/airflow/scripts/spark_gold.py",
        packages='io.delta:delta-spark_2.12:3.1.0',
        conn_id='spark_standalone',
        conf={
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.hadoop.fs.permissions.umask-mode': '000',
        },
    )

    end = EmptyOperator(task_id='end')



    start >> spark_bronze >> spark_silver >> spark_gold >> end


spark_dag()