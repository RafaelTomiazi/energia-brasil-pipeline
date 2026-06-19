"""
DAG: ONS — Geração por Usina em Base Horária (Bronze)
======================================================

O que esse DAG faz, em uma frase:
  Baixa o CSV mensal de geração horária das usinas do SIN (Sistema Interligado
  Nacional) publicado pelo ONS e materializa o arquivo bruto na camada bronze
  do data lake (MinIO), particionado por ano e mês.

Por que importa para o projeto:
  Esse é o "first principles" da camada bronze — guardar o dado EXATAMENTE como
  veio da fonte, sem transformação. Se algum dia o ONS mudar o formato do CSV
  ou o cálculo, o histórico bronze continua sendo a fonte da verdade. As
  transformações vêm depois, na silver.

Fonte oficial:
  https://dados.ons.org.br/dataset/geracao-usina-2

Como funciona o fluxo:
  1. Consulta a API CKAN do portal de dados abertos do ONS para listar todos
     os arquivos (resources) do dataset "geracao-usina-2".
  2. Filtra o resource cujo nome contém o ano e mês desejados.
  3. Baixa o CSV (via streaming, para não estourar a memória).
  4. Sobe o arquivo para s3://bronze/ons/geracao_usina_horaria/year=YYYY/month=MM/...
     usando o S3Hook do Airflow apontando para o MinIO.

Para rodar:
  - Suba o ambiente: docker compose up -d
  - Acesse http://localhost:8080 (admin / admin)
  - Ative o DAG "ons_geracao_horaria_bronze" e dispare manualmente.
"""
from __future__ import annotations

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task

# ---------------------------------------------------------------------
# Configurações do DAG (em projetos reais, isso vai pra um arquivo .yaml
# ou para Airflow Variables; aqui está inline para você ler tudo de uma vez)
# ---------------------------------------------------------------------
ONS_DATASET_ID = "geracao-usina-2"
CKAN_API_URL = (
    f"https://dados.ons.org.br/api/3/action/package_show?id={ONS_DATASET_ID}"
)
S3_BUCKET = "bronze"
S3_PREFIX = "ons/geracao_usina_horaria"
MINIO_CONN_ID = "minio_s3"

# Para a primeira execução, vamos ingerir um mês fixo (dezembro/2024).
# Em uma versão mais madura, isso seria parametrizado pela data lógica
# do DAG run (logical_date) usando a feature de "data interval" do Airflow.
TARGET_YEAR = 2024
TARGET_MONTH = 12

logger = logging.getLogger(__name__)


@dag(
    dag_id="ons_geracao_horaria_bronze",
    description="Ingere geração horária das usinas do ONS para a camada bronze",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ons", "energia"],
    doc_md=__doc__,
)
def ons_geracao_horaria_bronze():

    @task
    def descobrir_recurso(year: int, month: int) -> dict:
        """
        Consulta a API CKAN do ONS e encontra a URL do CSV do mês desejado.

        Retorna um dicionário com a URL e o nome do recurso, que será passado
        adiante para a próxima task via XCom (mecanismo padrão do Airflow para
        comunicação entre tasks).
        """
        logger.info("Consultando metadados do dataset no CKAN: %s", CKAN_API_URL)
        response = requests.get(CKAN_API_URL, timeout=30)
        response.raise_for_status()

        payload = response.json()
        resources = payload["result"]["resources"]
        logger.info("Dataset tem %d recursos no total", len(resources))

        # Os arquivos do ONS seguem padrões como "GERACAO_USINA-2_2024_12.csv".
        # Procuramos qualquer recurso que contenha "YYYY_MM" no nome ou na URL.
        target = f"{year}_{month:02d}"
        for r in resources:
            nome = (r.get("name") or "").upper()
            url = r.get("url") or ""
            formato = (r.get("format") or "").upper()

            if formato != "CSV":
                continue
            if target in nome or target in url.upper():
                logger.info("Recurso encontrado: %s -> %s", r["name"], url)
                return {"url": url, "name": r["name"]}

        # Se não achou, falha alto e claro — uma silenciosa seria pior.
        raise ValueError(
            f"Nenhum recurso CSV encontrado para {year}-{month:02d}. "
            f"Verifique o portal: https://dados.ons.org.br/dataset/{ONS_DATASET_ID}"
        )

    @task
    def baixar_e_persistir(recurso: dict, year: int, month: int) -> str:
        """
        Baixa o CSV do ONS e envia para o bucket bronze do MinIO,
        particionado por ano e mês (padrão Hive partitioning).

        Retorna o caminho S3 completo, útil para auditoria e XCom.
        """
        # Importação adiada: o S3Hook só fica disponível depois que o
        # provider apache-airflow-providers-amazon é instalado no startup.
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        url = recurso["url"]
        nome = recurso["name"]
        logger.info("Iniciando download: %s", url)

        # stream=True evita carregar o arquivo inteiro na memória.
        # Útil porque os CSVs do ONS podem passar de 100 MB.
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()
        conteudo = response.content
        logger.info("Download concluído. Tamanho: %.2f MB", len(conteudo) / 1024 / 1024)

        # Particionamento estilo Hive: year=YYYY/month=MM
        # Esse layout faz com que ferramentas como Spark, Athena e DuckDB
        # consigam fazer "partition pruning" automaticamente depois.
        chave_s3 = f"{S3_PREFIX}/year={year}/month={month:02d}/{nome}.csv"

        # Conexão com MinIO usando S3Hook — funciona porque o MinIO
        # implementa a API S3, e o conn_id "minio_s3" foi configurado
        # no docker-compose com endpoint_url apontando pro container do MinIO.
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        s3.load_bytes(
            bytes_data=conteudo,
            key=chave_s3,
            bucket_name=S3_BUCKET,
            replace=True,  # idempotência: rodar de novo sobrescreve
        )

        caminho_completo = f"s3://{S3_BUCKET}/{chave_s3}"
        logger.info("Upload concluído: %s", caminho_completo)
        return caminho_completo

    # ---- Definição do grafo de dependências ----
    # Aqui você "monta" o DAG: a saída de uma task vira a entrada da próxima.
    # O Airflow descobre as dependências automaticamente pela passagem de args.
    recurso = descobrir_recurso(year=TARGET_YEAR, month=TARGET_MONTH)
    baixar_e_persistir(recurso=recurso, year=TARGET_YEAR, month=TARGET_MONTH)


# Esta linha registra o DAG no scheduler. Sem ela, o Airflow não enxerga.
ons_geracao_horaria_bronze()
