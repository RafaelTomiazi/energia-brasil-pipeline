"""
DAG: ANEEL — MMGD Geração Distribuída (Bronze)
================================================

O que esse DAG faz, em uma frase:
  Baixa o CSV da Relação de Empreendimentos de Mini e Micro Geração
  Distribuída (MMGD) publicado pela ANEEL e materializa um snapshot
  na camada bronze do data lake.

Por que importa para o projeto:
  Esse dataset é a fonte da verdade sobre a explosão da solar fotovoltaica
  de telhado no Brasil. Toda usina pequena (≤5 MW) conectada à rede está
  listada — com município, distribuidora, fonte, potência e data de conexão.
  É o complemento exato dos zeros de "Pequenas Usinas (MMGD)" que vimos no
  dado do ONS.

Diferença em relação ao DAG anterior (ons_geracao_horaria_bronze):
  ─ Lá: discovery via API CKAN (descobre URL do recurso por mês/ano).
  ─ Aqui: download direto de URL fixa (a ANEEL mantém URL estável por UUID).

  Lá: partição por year=YYYY/month=MM (o dado tem temporalidade embutida).
  Aqui: partição por snapshot_date=YYYY-MM-DD (o dado é "estado atual"
  da base completa, então salvamos um snapshot a cada run pra permitir
  cálculo de deltas depois — padrão "snapshot fact" em data warehouse).

  Lá: arquivo de ~65 MB.
  Aqui: arquivo de ~500 MB+ (toda a base histórica). Por isso o download
  é em streaming (chunks de 8 MB), pra não estourar a memória do worker.

Fonte oficial:
  https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida
"""
from __future__ import annotations

import logging
from datetime import datetime
from io import BytesIO

import requests
from airflow.decorators import dag, task

# ---------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------
# URL estável do recurso na ANEEL — UUID-based, não muda.
# Se um dia der 404, vá ao portal e copie a URL atualizada do botão "Download":
#   https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida
ANEEL_URL = (
    "https://dadosabertos.aneel.gov.br/dataset/"
    "5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/"
    "b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/"
    "empreendimento-geracao-distribuida.csv"
)

S3_BUCKET = "bronze"
S3_PREFIX = "aneel/mmgd"
MINIO_CONN_ID = "minio_s3"

# Tamanho do chunk de download — 8 MB equilibra memória e overhead de rede.
# Aumentar não acelera download (gargalo é rede), mas consome mais RAM.
DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024

# Timeout generoso porque o servidor da ANEEL é lento em horário de pico.
DOWNLOAD_TIMEOUT_SECONDS = 600

logger = logging.getLogger(__name__)


@dag(
    dag_id="aneel_mmgd_bronze",
    description="Snapshot mensal da base MMGD da ANEEL para a camada bronze",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "aneel", "energia", "mmgd"],
    doc_md=__doc__,
)
def aneel_mmgd_bronze():

    @task
    def baixar_e_persistir(**context) -> str:
        """
        Faz streaming do CSV da ANEEL e sobe pro MinIO como um snapshot
        datado. Streaming evita carregar 500MB+ na memória de uma vez.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        # A Airflow injeta automaticamente o "context" do run.
        # `ds` é a data lógica do run no formato YYYY-MM-DD — usamos como
        # data do snapshot. Isso garante idempotência: rodar o mesmo run
        # de novo sobrescreve o mesmo arquivo, em vez de criar duplicata.
        snapshot_date = context["ds"]
        logger.info(f"Snapshot date: {snapshot_date}")
        logger.info(f"URL: {ANEEL_URL}")

        # ----- Download em streaming -----
        # Não usamos response.content direto porque isso carrega o arquivo
        # inteiro na RAM. Em vez disso, percorremos chunks e gravamos num
        # BytesIO, que é estruturalmente igual mas mais legível.
        buffer = BytesIO()
        bytes_baixados = 0
        ultimo_log_mb = 0

        with requests.get(
            ANEEL_URL,
            stream=True,
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
        ) as response:
            response.raise_for_status()

            tamanho_total = response.headers.get("Content-Length")
            if tamanho_total:
                logger.info(
                    f"Tamanho declarado pela ANEEL: "
                    f"{int(tamanho_total) / 1024 / 1024:.1f} MB"
                )

            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if not chunk:
                    continue
                buffer.write(chunk)
                bytes_baixados += len(chunk)

                # Loga progresso a cada ~50 MB pra não poluir o log
                mb_baixados = bytes_baixados // (1024 * 1024)
                if mb_baixados - ultimo_log_mb >= 50:
                    logger.info(f"  Progresso: {mb_baixados} MB baixados")
                    ultimo_log_mb = mb_baixados

        logger.info(
            f"Download concluído. Total: "
            f"{bytes_baixados / 1024 / 1024:.1f} MB"
        )

        # Sanidade básica: arquivo não pode estar vazio nem ser HTML
        # (sinal de erro 200 OK com página de erro no body — acontece).
        if bytes_baixados == 0:
            raise ValueError("Download retornou 0 bytes")

        primeiros_bytes = buffer.getvalue()[:100].decode("utf-8", errors="replace")
        if "<html" in primeiros_bytes.lower():
            raise ValueError(
                f"Servidor retornou HTML em vez de CSV. "
                f"Primeiros 100 bytes: {primeiros_bytes!r}"
            )

        # ----- Upload para o MinIO -----
        # Padrão de partição "snapshot fact": cada execução grava um
        # arquivo carimbado pela data do run. Isso permite calcular deltas
        # entre snapshots (quantas usinas novas conectaram em março × abril?)
        # depois, na camada silver.
        chave_s3 = (
            f"{S3_PREFIX}/snapshot_date={snapshot_date}/"
            f"empreendimento-geracao-distribuida.csv"
        )

        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        s3.load_bytes(
            bytes_data=buffer.getvalue(),
            key=chave_s3,
            bucket_name=S3_BUCKET,
            replace=True,  # idempotência
        )

        caminho_completo = f"s3://{S3_BUCKET}/{chave_s3}"
        logger.info(f"Upload concluído: {caminho_completo}")
        return caminho_completo

    baixar_e_persistir()


aneel_mmgd_bronze()
