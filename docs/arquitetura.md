# Arquitetura do Projeto

## Visão geral

Pipeline em arquitetura **medallion** (Bronze → Silver → Gold), orquestrado por Airflow, com armazenamento no MinIO (S3-compatível) e modelagem no PostgreSQL via dbt.

```
                        Cross-cutting: Docker · GitHub Actions · Great Expectations
─────────────────────────────────────────────────────────────────────────────────────

   Fontes              Ingestão           Bronze            Silver               Gold              Serving
┌──────────┐    ┌────────────────┐   ┌──────────┐    ┌──────────┐    ┌─────────────────┐    ┌──────────────┐
│  APIs +  │ -> │    Airflow     │ -> │  MinIO   │ -> │  Spark   │ -> │ dbt + Postgres  │ -> │   Power BI   │
│   CSVs   │    │ (orquestração) │    │  (raw)   │    │ (limpo)  │    │   (modelado)    │    │   FastAPI    │
└──────────┘    └────────────────┘    └──────────┘    └──────────┘    └─────────────────┘    └──────────────┘
```

## Camadas

### Bronze — dado bruto, imutável

- **O que entra:** arquivos CSV/JSON exatamente como vieram da fonte, sem nenhuma transformação.
- **Por que:** auditável, reprocessável. Se descobrirmos um bug na transformação meses depois, podemos reprocessar a partir do bronze. É a única camada onde a verdade vem de fora.
- **Onde:** bucket `bronze` no MinIO, particionado por `year=YYYY/month=MM/`.

### Silver — dado limpo, conformado

- **O que entra:** dados do bronze após limpeza, deduplicação, conformação de tipos, padronização de chaves (códigos IBGE para municípios, ISO para estados, fuso horário UTC), e validação com Great Expectations.
- **Por que:** dados confiáveis para serem usados como blocos de construção. Quem consome silver não precisa saber dos quirks de cada fonte.
- **Onde:** bucket `silver` no MinIO em formato Parquet (mais eficiente que CSV), particionado por dimensões úteis (data, fonte de geração, UF).

### Gold — dado modelado para consumo

- **O que entra:** modelos dimensionais (fatos e dimensões) construídos via dbt sobre os dados silver.
- **Por que:** otimizado para perguntas de negócio. Tabelas amplas, métricas pré-calculadas, semântica explícita.
- **Onde:** PostgreSQL (warehouse), com documentação automática gerada pelo dbt.

## Fontes

| Fonte | O que tem | Atualização | Formato |
|---|---|---|---|
| **ONS** — `dados.ons.org.br` | Geração horária por usina, curva de carga, geração térmica por motivo de despacho | Mensal | CSV, API CKAN |
| **ANEEL** — `dadosabertos.aneel.gov.br` | Geração distribuída (MMGD), capacidade instalada (SIGA), tarifas | Mensal | CSV |
| **ABVE** — `abve.org.br` | Vendas de EVs, eletropostos por município | Mensal | BI público (extração via web scraping) |

## Decisões técnicas

**Por que MinIO e não S3 direto.** Pra desenvolvimento local, MinIO simula a API do S3 sem custo. Quando o projeto for pra produção, troca-se a credencial e o endpoint — o código não muda (S3Hook abstrai).

**Por que LocalExecutor no Airflow e não CeleryExecutor.** Pra dev de uma máquina só, LocalExecutor é mais simples (sem Redis, sem Celery workers). Em produção, considera-se Celery ou Kubernetes Executor.

**Por que dbt em vez de SQL puro.** dbt traz versionamento, testes em SQL, documentação automática, lineage gráfico — tudo que falta no SQL puro pra um projeto de DE escalar.

**Por que Spark e não só Python/Pandas.** Pandas é ótimo até alguns GB. Os datasets do ONS rapidamente passam disso quando agregamos vários anos de dados horários. Spark escala horizontalmente e é a habilidade mais cobrada em vagas de DE.
