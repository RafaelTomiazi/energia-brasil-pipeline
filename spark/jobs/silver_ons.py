"""
Job Spark — Silver ONS Geração Horária
========================================

O que esse job faz, em uma frase:
  Lê o CSV bronze do ONS, aplica limpezas e enriquecimentos, e grava
  como Parquet particionado na camada silver.

Por que esse job existe (a tese da camada silver):
  Bronze é dado bruto, intocado, "verdade vinda de fora". Silver é dado
  pronto pra consumo — limpo, tipado corretamente, padronizado, enriquecido.
  O contrato da silver é: "se você ler daqui, pode confiar".

Transformações aplicadas:
  1. Schema explícito (não confiar na inferência do Spark)
  2. Encoding ISO-8859-1 -> tudo vira UTF-8 internamente
  3. Filtra registros "Pequenas Usinas (MMGD)" com geração 0 (ruído)
  4. Adiciona coluna `eh_renovavel` (boolean derivado do tipo de combustível)
  5. Adiciona colunas `ano` e `mes` extraídas do timestamp (pra particionar)
  6. Trata o subsistema "PARAGUAI" (Itaipu) marcando explicitamente
  7. Grava em Parquet particionado por ano/mês

Como rodar:
  - Manual: chamado pelo SparkSubmitOperator no DAG `silver_ons_processing`
  - Direto (debug): docker compose exec spark-master spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/jobs/silver_ons.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

# ---------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------
# Endereço do MinIO. Note "minio" como hostname, não "localhost":
# isso é o nome do container na rede interna do docker-compose.
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"

# Caminhos de input (bronze) e output (silver).
# s3a:// é o "esquema" do conector Hadoop pra S3 — funciona com MinIO
# porque o MinIO implementa a API do S3.
BRONZE_PATH = "s3a://bronze/ons/geracao_usina_horaria/"
SILVER_PATH = "s3a://silver/ons_geracao_horaria/"


def criar_spark_session() -> SparkSession:
    """
    Cria a SparkSession com a configuração necessária pra falar com MinIO.

    Em produção, várias dessas configs viriam de spark-defaults.conf ou de
    properties do submit. Aqui colocamos inline pra você ler tudo num lugar.
    """
    return (
        SparkSession.builder
        .appName("silver_ons_geracao_horaria")
        # Conector Hadoop S3A — necessário pra ler/gravar do MinIO
        .config(
            "spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT
        )
        .config(
            "spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY
        )
        # Path style necessário pro MinIO (URL bucket.minio.com não funciona)
        .config(
            "spark.hadoop.fs.s3a.path.style.access", "true"
        )
        # Implementação do "filesystem" S3
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # Sem SSL pra dev local
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
        )
        .getOrCreate()
    )


def definir_schema_ons() -> StructType:
    """
    Define o schema explícito do CSV do ONS.

    Por que explícito e não inferido?
      - Inferência é lenta (Spark precisa ler 2x o arquivo)
      - Inferência erra em casos extremos (ex: coluna que sempre tem NULL no início)
      - Schema explícito é AUTODOCUMENTADO — quem lê o código sabe os tipos

    Tipos que importam:
      - din_instante: TimestampType pra permitir comparações temporais
      - val_geracao: DoubleType pra agregações numéricas
      - resto: StringType (chaves e descritivos)
    """
    return StructType([
        StructField("din_instante", TimestampType(), nullable=False),
        StructField("id_subsistema", StringType(), nullable=True),
        StructField("nom_subsistema", StringType(), nullable=True),
        StructField("id_estado", StringType(), nullable=True),
        StructField("nom_estado", StringType(), nullable=True),
        StructField("cod_modalidadeoperacao", StringType(), nullable=True),
        StructField("nom_tipousina", StringType(), nullable=True),
        StructField("nom_tipocombustivel", StringType(), nullable=True),
        StructField("nom_usina", StringType(), nullable=True),
        StructField("id_ons", StringType(), nullable=True),
        StructField("ceg", StringType(), nullable=True),
        StructField("val_geracao", DoubleType(), nullable=True),
    ])


def aplicar_transformacoes(df):
    """
    Aplica todas as transformações de negócio pra silver.

    Lembrando: nada disso EXECUTA aqui. Spark só anota a receita.
    A execução só acontece no df.write() lá embaixo.
    """

    # 1. Filtra MMGD com geração 0 (ruído — a verdade da MMGD vem da ANEEL)
    df = df.filter(
        ~(
            (F.col("cod_modalidadeoperacao") == "Pequenas Usinas (MMGD)")
            & (F.col("val_geracao") == 0)
        )
    )

    # 2. Marca subsistema PARAGUAI explicitamente
    # Itaipu é binacional. Pra agregação Brasil-only, vamos sinalizar.
    df = df.withColumn(
        "eh_brasil",
        F.when(F.col("nom_subsistema") == "PARAGUAI", F.lit(False))
        .otherwise(F.lit(True)),
    )

    # 3. Classifica fonte como renovável ou não
    # Renováveis: hidráulica, eólica, fotovoltaica, biomassa
    # Não-renováveis: gás, carvão, óleo, diesel, nuclear (controverso, mas
    # tradicionalmente classificado como "limpa" mas não "renovável")
    fontes_renovaveis = [
        "Hidráulica",
        "Eólica",
        "Fotovoltaica",
        "Biomassa",
    ]
    df = df.withColumn(
        "eh_renovavel",
        F.col("nom_tipocombustivel").isin(fontes_renovaveis),
    )

    # 4. Extrai ano e mês pra particionamento
    # year() e month() são funções built-in do Spark, super otimizadas
    df = df.withColumn("ano", F.year("din_instante"))
    df = df.withColumn("mes", F.month("din_instante"))

    # 5. Adiciona timestamp de processamento (lineage / debugging)
    # Toda tabela silver/gold deve ter isso — quando dado parecer estranho,
    # você consegue saber QUANDO foi processado.
    df = df.withColumn("processed_at", F.current_timestamp())

    return df


def main():
    spark = criar_spark_session()

    # Reduz verbosidade do log do Spark — só warnings e erros
    spark.sparkContext.setLogLevel("WARN")

    print(">>> Lendo CSV bronze do ONS...")
    df_raw = (
        spark.read
        .option("delimiter", ";")
        .option("header", "true")
        # ATENÇÃO: o CSV do ONS é UTF-8. O da ANEEL é ISO-8859-1.
        .option("encoding", "UTF-8")
        # Note: schema deve ser passado como argumento, não como .option
        .schema(definir_schema_ons())
        .csv(BRONZE_PATH)
    )

    print(">>> Aplicando transformações...")
    df_silver = aplicar_transformacoes(df_raw)

    # ----- Aqui sim a execução acontece -----
    # O .write é uma AÇÃO. Spark vai pegar toda a receita acima
    # (read + filter + withColumn x5) e finalmente executar.
    print(f">>> Gravando Parquet em {SILVER_PATH}...")
    (
        df_silver.write
        .mode("overwrite")  # idempotência: rodar de novo sobrescreve
        .partitionBy("ano", "mes")
        .parquet(SILVER_PATH)
    )

    # Conferência final — quantas linhas escrevemos
    total = df_silver.count()
    print(f">>> Concluído. {total:,} linhas gravadas na silver.")

    spark.stop()


if __name__ == "__main__":
    main()
