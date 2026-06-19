-- =====================================================================
-- stg_ons_geracao_horaria — staging do ONS
-- =====================================================================
-- Camada staging: 1-pra-1 com a fonte, só padroniza nomes e tipos.
-- Não faz joins. Não agrega. Não inventa coluna.
-- A única "regra" é deixar o dado pronto pra ser usado nos próximos models.
--
-- Materializado como VIEW (config no dbt_project.yml) — sem custo de disco
-- =====================================================================

with fonte as (
    select * from {{ source('raw', 'ons_geracao_horaria') }}
)

select
    -- chaves
    din_instante                      as ts_geracao,
    id_subsistema                     as cod_subsistema,
    nom_subsistema                    as nome_subsistema,
    id_estado                         as sigla_uf,
    nom_estado                        as nome_estado,

    -- atributos da usina
    cod_modalidadeoperacao            as modalidade_operacao,
    nom_tipousina                     as tipo_usina,
    nom_tipocombustivel               as fonte_combustivel,
    nom_usina                         as nome_usina,
    id_ons                            as id_usina,
    ceg                               as ceg,

    -- métricas
    val_geracao                       as geracao_mwh,

    -- flags derivadas (já vieram do Spark)
    eh_renovavel                      as eh_renovavel,
    eh_brasil                         as eh_brasil,

    -- partição como colunas explícitas (vinham implícitas do path)
    ano                               as ano_geracao,
    mes                               as mes_geracao,

    -- lineage
    processed_at                      as bronze_processed_at

from fonte
where
    -- Filtros de qualidade que devem rodar sempre
    val_geracao is not null
    and val_geracao >= 0   -- valores negativos = erro de medição
