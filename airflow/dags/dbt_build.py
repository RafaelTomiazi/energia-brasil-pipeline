"""
DAG: dbt build (via container dedicado)
========================================

Atualização vs versão anterior:
  Antes: rodava `dbt build` direto dentro do Airflow scheduler
  Agora: chama via `docker exec dbt dbt build` no container dedicado

Por quê:
  dbt-postgres tinha conflito de dependências com Airflow (ResolutionTooDeep).
  Container dedicado isola dbt e elimina esse problema. É o padrão usado
  em produção quando se quer isolamento entre ferramentas.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


# Roda dbt build dentro do container dbt.
# --quiet pra reduzir ruído de log; sem ele, dbt imprime banner colorido
# ANSI que polui os logs do Airflow.
DBT_CMD = """
docker exec dbt dbt build --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
"""


with DAG(
    dag_id="dbt_build",
    description="Roda dbt build (models + tests) — via container dedicado",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "warehouse", "energia"],
    doc_md=__doc__,
) as dag:

    rodar_dbt = BashOperator(
        task_id="dbt_build",
        bash_command=DBT_CMD,
    )
