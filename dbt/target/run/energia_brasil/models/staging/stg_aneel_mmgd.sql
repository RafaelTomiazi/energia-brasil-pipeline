
  create view "energia"."public_staging"."stg_aneel_mmgd__dbt_tmp"
    
    
  as (
    -- =====================================================================
-- stg_aneel_mmgd — staging da ANEEL (corrigido)
-- =====================================================================
-- Mudança vs versão anterior:
--   Adicionado filtro 'sigla_uf is not null' no WHERE
--   Motivo: 2 registros nos ~5M totais vinham com sigla_uf NULL —
--   bug detectado pelo teste not_null_stg_aneel_mmgd_sigla_uf no dbt
-- =====================================================================

with fonte as (
    select * from "energia"."raw"."aneel_mmgd"
)

select
    cod_empreendimento                as id_empreendimento,
    sigla_uf                          as sigla_uf,
    cod_uf_ibge                       as cod_uf_ibge,
    regiao                            as nome_regiao,
    municipio                         as nome_municipio,
    cod_municipio_ibge                as cod_municipio_ibge,

    sigla_agente                      as sigla_distribuidora,
    nome_agente                       as nome_distribuidora,

    classe_consumo                    as classe_consumo,
    subgrupo_tarifario                as subgrupo_tarifario,
    sigla_tipo_geracao                as sigla_tipo_geracao,
    fonte_geracao                     as fonte_geracao,
    porte                             as porte,
    sigla_modalidade                  as sigla_modalidade,
    modalidade                        as modalidade,
    qtd_uc_recebe_credito             as qtd_uc_recebe_credito,

    potencia_kw                       as potencia_kw,

    latitude                          as latitude,
    longitude                         as longitude,

    data_conexao                      as data_conexao,
    ano_conexao                       as ano_conexao,
    mes_conexao                       as mes_conexao,

    processed_at                      as bronze_processed_at

from fonte
where
    potencia_kw is not null
    and potencia_kw > 0
    and data_conexao is not null
    and sigla_uf is not null   -- NOVO: descarta registros sem UF (2 registros lixo)
  );