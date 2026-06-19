"""
DAG: Carrega Silver (Parquet/MinIO) para Warehouse (Postgres)
==============================================================

O que esse DAG faz:
  Lê os arquivos Parquet da camada silver no MinIO e copia pro
  postgres-warehouse no schema "raw". É a ponte silver -> warehouse.

Por que DuckDB como ferramenta de carga:
  DuckDB é um banco analítico embarcado (tipo SQLite mas pra OLAP).
  Tem suporte NATIVO pra ler Parquet do S3 e gravar em Postgres via
  extensão. Em 30 linhas faz o que demoraria centenas com pandas + sqlalchemy.

Fluxo:
  1. Conecta no DuckDB (em memória)
  2. Configura credenciais S3 (MinIO)
  3. Instala extensão postgres
  4. Executa: CREATE TABLE postgres.raw.X AS SELECT * FROM 's3://silver/X/...'
  5. DuckDB streaming lê Parquet, converte tipos, manda pro Postgres em batches

Pré-requisitos:
  - Silver populada (jobs Spark já rodaram)
  - postgres-warehouse no ar
"""
from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="load_silver_to_warehouse",
    description="Carrega Parquet silver -> Postgres warehouse via DuckDB",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["warehouse", "duckdb", "energia"],
    doc_md=__doc__,
)
def load_silver_to_warehouse():

    @task
    def carregar_dados():
        import duckdb

        # Conexão DuckDB em memória — não persiste, é só a engine
        con = duckdb.connect(":memory:")

        # Configura conexão com MinIO (S3-compatible)
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute("SET s3_endpoint='minio:9000';")
        con.execute("SET s3_access_key_id='minioadmin';")
        con.execute("SET s3_secret_access_key='minioadmin';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")

        # Configura conexão com Postgres warehouse
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")
        con.execute("""
            ATTACH 'host=postgres-warehouse port=5432 dbname=energia user=warehouse password=warehouse'
            AS pg (TYPE postgres);
        """)

        # Cria schema "raw" (idempotente)
        con.execute("CREATE SCHEMA IF NOT EXISTS pg.raw;")
        print(">>> Schema raw garantido")

        # ---- Carrega ONS ----
        # DROP + CREATE = full refresh. Em produção, considerar incremental.
        print(">>> Carregando ons_geracao_horaria...")
        con.execute("DROP TABLE IF EXISTS pg.raw.ons_geracao_horaria;")
        con.execute("""
            CREATE TABLE pg.raw.ons_geracao_horaria AS
            SELECT * FROM read_parquet(
                's3://silver/ons_geracao_horaria/**/*.parquet',
                hive_partitioning = true
            );
        """)
        count = con.execute("SELECT COUNT(*) FROM pg.raw.ons_geracao_horaria;").fetchone()[0]
        print(f"  {count:,} linhas carregadas em raw.ons_geracao_horaria")

        # ---- Carrega ANEEL ----
        print(">>> Carregando aneel_mmgd...")
        con.execute("DROP TABLE IF EXISTS pg.raw.aneel_mmgd;")
        con.execute("""
            CREATE TABLE pg.raw.aneel_mmgd AS
            SELECT * FROM read_parquet(
                's3://silver/aneel_mmgd/**/*.parquet',
                hive_partitioning = true
            );
        """)
        count = con.execute("SELECT COUNT(*) FROM pg.raw.aneel_mmgd;").fetchone()[0]
        print(f"  {count:,} linhas carregadas em raw.aneel_mmgd")

        con.close()
        print(">>> Carregamento completo.")

    carregar_dados()


load_silver_to_warehouse()
