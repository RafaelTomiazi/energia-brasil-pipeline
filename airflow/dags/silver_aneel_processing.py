"""
DAG: Silver — Processamento ANEEL MMGD (versão corrigida)
==========================================================

Mesma mudança do silver_ons_processing: usa BashOperator + docker exec
em vez de SparkSubmitOperator, pra evitar dependência de Java no Airflow.

Esse job é mais pesado por causa do volume da ANEEL (~1.4 GB).
Tempo estimado: 5-10 minutos.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_SUBMIT_CMD = """
docker exec spark-master /opt/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
    --executor-memory 1g \\
    --driver-memory 500m \\
    --executor-cores 2 \\
    /opt/jobs/silver_aneel.py
"""


with DAG(
    dag_id="silver_aneel_processing",
    description="Processa bronze ANEEL MMGD -> silver com PySpark (via docker exec)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "aneel", "spark", "energia", "mmgd"],
    doc_md=__doc__,
) as dag:

    processar_silver_aneel = BashOperator(
        task_id="processar_silver_aneel",
        bash_command=SPARK_SUBMIT_CMD,
    )
