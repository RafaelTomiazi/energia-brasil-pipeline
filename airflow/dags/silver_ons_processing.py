"""
DAG: Silver — Processamento ONS Geração Horária (versão corrigida)
====================================================================

Mudança vs versão anterior:
  - Antes: SparkSubmitOperator (precisa de Java no container do Airflow)
  - Agora: BashOperator + docker exec (executa spark-submit DENTRO do
           container spark-master, que já tem Java)

Por que essa mudança:
  A imagem oficial do Airflow não traz JVM, e instalar Java no startup
  é caro (200 MB extras + tempo). É mais limpo deixar o spark-master
  rodar o submit ele mesmo (é o trabalho dele) e o Airflow só dá a
  ordem via docker exec.

Como funciona:
  1. Airflow scheduler executa o BashOperator
  2. BashOperator chama: docker exec spark-master spark-submit ...
  3. O comando entra no container spark-master e roda o spark-submit lá
  4. Spark-submit conecta no master local (já está dentro!) e dispara o job
  5. Logs do Spark aparecem no log da task do Airflow

Pré-requisito:
  O socket do Docker do host precisa estar mapeado dentro do container do
  Airflow (já configurado no docker-compose.yml).
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


# Comando que será executado pelo BashOperator.
# Note os pacotes: hadoop-aws permite ler/gravar do MinIO via API S3.
SPARK_SUBMIT_CMD = """
docker exec spark-master /opt/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
    --executor-memory 1g \\
    --driver-memory 500m \\
    --executor-cores 2 \\
    /opt/jobs/silver_ons.py
"""


with DAG(
    dag_id="silver_ons_processing",
    description="Processa bronze ONS -> silver com PySpark (via docker exec)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "ons", "spark", "energia"],
    doc_md=__doc__,
) as dag:

    processar_silver_ons = BashOperator(
        task_id="processar_silver_ons",
        bash_command=SPARK_SUBMIT_CMD,
    )
