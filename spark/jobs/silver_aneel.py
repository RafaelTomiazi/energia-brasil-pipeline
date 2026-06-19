"""
Job Spark — Silver ANEEL MMGD
==============================

O que esse job faz:
  Lê o CSV bronze da ANEEL (1.4 GB!), aplica limpezas, drop de PII,
  conversão de números/datas em formato BR, e grava como Parquet
  particionado na camada silver.

Principais desafios atacados aqui (tudo que vimos na amostra):
  1. Encoding ISO-8859-1 (latin1), não UTF-8
  2. Números BR com vírgula decimal: "32,50" -> 32.50
  3. Coordenadas geográficas no mesmo formato BR: "-10,00" -> -10.00
  4. PII anonimizada pela ANEEL — vamos dropar mesmo assim por segurança
  5. Datas em formato YYYY-MM-DD (fácil) e MM/YYYY (médio)

Como rodar:
  Chamado pelo SparkSubmitOperator no DAG `silver_aneel_processing`.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
)

# ---------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"

BRONZE_PATH = "s3a://bronze/aneel/mmgd/"
SILVER_PATH = "s3a://silver/aneel_mmgd/"


def criar_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_aneel_mmgd")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def definir_schema_aneel() -> StructType:
    """
    Schema do CSV da ANEEL.

    Decisão importante: TUDO como String inicialmente, mesmo as colunas
    que parecem numéricas. Por quê?
      - Números vêm em formato BR ("32,50") — DoubleType nem leria
      - Coordenadas idem
      - Datas vêm como string em vários formatos diferentes

    A conversão acontece nas transformações, depois.
    Esse padrão "ler como String, converter depois" é canônico
    pra ingestão de dado sujo de mundo real.

    Listamos só as colunas que vamos USAR. As de PII serão dropadas
    no momento de leitura (não pedindo elas no schema).
    Spread + select pra quem vier depois ler:
    """
    return StructType([
        StructField("DatGeracaoConjuntoDados", StringType(), True),
        StructField("AnmPeriodoReferencia", StringType(), True),
        StructField("NumCNPJDistribuidora", StringType(), True),
        StructField("SigAgente", StringType(), True),
        StructField("NomAgente", StringType(), True),
        StructField("CodClasseConsumo", StringType(), True),
        StructField("DscClasseConsumo", StringType(), True),
        StructField("CodSubGrupoTarifario", StringType(), True),
        StructField("DscSubGrupoTarifario", StringType(), True),
        StructField("CodUFibge", IntegerType(), True),
        StructField("SigUF", StringType(), True),
        StructField("CodRegiao", IntegerType(), True),
        StructField("NomRegiao", StringType(), True),
        StructField("CodMunicipioIbge", LongType(), True),
        StructField("NomMunicipio", StringType(), True),
        # ----- PII (dropamos no select) -----
        StructField("CodCEP", StringType(), True),
        StructField("SigTipoConsumidor", StringType(), True),
        StructField("NumCPFCNPJ", StringType(), True),
        StructField("NomTitularEmpreendimento", StringType(), True),
        # ----- Continua -----
        StructField("CodEmpreendimento", StringType(), True),
        StructField("DthAtualizaCadastralEmpreend", StringType(), True),
        StructField("SigModalidadeEmpreendimento", StringType(), True),
        StructField("DscModalidadeHabilitado", StringType(), True),
        StructField("QtdUCRecebeCredito", IntegerType(), True),
        StructField("SigTipoGeracao", StringType(), True),
        StructField("DscFonteGeracao", StringType(), True),
        StructField("DscPorte", StringType(), True),
        StructField("NumCoordNEmpreendimento", StringType(), True),
        StructField("NumCoordEEmpreendimento", StringType(), True),
        StructField("MdaPotenciaInstaladaKW", StringType(), True),
        StructField("NomSubEstacao", StringType(), True),
        StructField("NumCoordESub", StringType(), True),
        StructField("NumCoordNSub", StringType(), True),
    ])


def converter_numero_br(coluna):
    """
    Converte coluna string "32,50" -> double 32.50.

    regexp_replace: substitui vírgula por ponto.
    cast: converte string numérica em double.
    """
    return F.regexp_replace(F.col(coluna), ",", ".").cast("double")


def aplicar_transformacoes(df):
    # 1. DROP DE PII — primeiro de tudo, antes de qualquer processamento.
    # Princípio de privacy by design: dado pessoal não circula no pipeline.
    colunas_pii = [
        "CodCEP",
        "SigTipoConsumidor",
        "NumCPFCNPJ",
        "NomTitularEmpreendimento",
    ]
    df = df.drop(*colunas_pii)

    # 2. Renomeia colunas pra snake_case (padrão Python, e padrão de DW)
    # Nomes camelCase em SQL ficam horríveis. silver/gold deve usar snake_case.
    renomear = {
        "DatGeracaoConjuntoDados": "data_geracao_conjunto",
        "AnmPeriodoReferencia": "periodo_referencia",
        "NumCNPJDistribuidora": "cnpj_distribuidora",
        "SigAgente": "sigla_agente",
        "NomAgente": "nome_agente",
        "CodClasseConsumo": "cod_classe_consumo",
        "DscClasseConsumo": "classe_consumo",
        "CodSubGrupoTarifario": "cod_subgrupo_tarifario",
        "DscSubGrupoTarifario": "subgrupo_tarifario",
        "CodUFibge": "cod_uf_ibge",
        "SigUF": "sigla_uf",
        "CodRegiao": "cod_regiao",
        "NomRegiao": "regiao",
        "CodMunicipioIbge": "cod_municipio_ibge",
        "NomMunicipio": "municipio",
        "CodEmpreendimento": "cod_empreendimento",
        "DthAtualizaCadastralEmpreend": "data_conexao_str",
        "SigModalidadeEmpreendimento": "sigla_modalidade",
        "DscModalidadeHabilitado": "modalidade",
        "QtdUCRecebeCredito": "qtd_uc_recebe_credito",
        "SigTipoGeracao": "sigla_tipo_geracao",
        "DscFonteGeracao": "fonte_geracao",
        "DscPorte": "porte",
        "NumCoordNEmpreendimento": "lat_str",
        "NumCoordEEmpreendimento": "lon_str",
        "MdaPotenciaInstaladaKW": "potencia_kw_str",
        "NomSubEstacao": "subestacao",
        "NumCoordESub": "lon_sub_str",
        "NumCoordNSub": "lat_sub_str",
    }
    for antigo, novo in renomear.items():
        df = df.withColumnRenamed(antigo, novo)

    # 3. Converte números BR -> double
    df = df.withColumn("potencia_kw", converter_numero_br("potencia_kw_str"))
    df = df.withColumn("latitude", converter_numero_br("lat_str"))
    df = df.withColumn("longitude", converter_numero_br("lon_str"))

    # 4. Converte data de conexão e extrai ano/mês pra particionamento
    df = df.withColumn(
        "data_conexao",
        F.to_date(F.col("data_conexao_str"), "yyyy-MM-dd"),
    )
    df = df.withColumn("ano_conexao", F.year("data_conexao"))
    df = df.withColumn("mes_conexao", F.month("data_conexao"))

    # 5. Drop das colunas string originais (já temos as convertidas)
    df = df.drop(
        "potencia_kw_str", "lat_str", "lon_str",
        "lon_sub_str", "lat_sub_str", "data_conexao_str",
    )

    # 6. Lineage: timestamp de processamento
    df = df.withColumn("processed_at", F.current_timestamp())

    # 7. Filtra registros sem data de conexão (não dá pra particionar)
    df = df.filter(F.col("ano_conexao").isNotNull())

    return df


def main():
    spark = criar_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(">>> Lendo CSV bronze da ANEEL (1.4 GB)...")
    df_raw = (
        spark.read
        .option("delimiter", ";")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")  # CRÍTICO: não é UTF-8
        .option("quote", '"')
        .schema(definir_schema_aneel())
        .csv(BRONZE_PATH)
    )

    print(">>> Aplicando transformações...")
    df_silver = aplicar_transformacoes(df_raw)

    print(f">>> Gravando Parquet em {SILVER_PATH}...")
    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("ano_conexao")  # só ano (mês daria muitos arquivos pequenos)
        .parquet(SILVER_PATH)
    )

    total = df_silver.count()
    print(f">>> Concluído. {total:,} usinas distribuídas processadas.")

    spark.stop()


if __name__ == "__main__":
    main()
