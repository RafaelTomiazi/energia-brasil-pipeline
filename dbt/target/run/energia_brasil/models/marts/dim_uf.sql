
  
    

  create  table "energia"."public_marts"."dim_uf__dbt_tmp"
  
  
    as
  
  (
    -- =====================================================================
-- dim_uf — dimensão das UFs brasileiras (corrigido)
-- =====================================================================
-- Mudança vs versão anterior:
--   Antes: SELECT DISTINCT sigla_uf, nome_estado, nome_subsistema, ...
--          → algumas UFs apareciam 2x porque combinações de subsistema
--          variavam dentro da mesma UF
--   Agora: GROUP BY sigla_uf + MAX() pros descritivos
--          → garante 1 linha por UF (chave primária verdadeira)
--
-- Bug detectado pelo teste unique_dim_uf_sigla_uf no dbt
-- =====================================================================

with ufs_ons as (
    select
        sigla_uf,
        max(nome_estado)        as nome_estado,
        max(nome_subsistema)    as nome_subsistema,
        max(cod_subsistema)     as cod_subsistema,
        bool_or(eh_brasil)      as eh_brasil
    from "energia"."public_staging"."stg_ons_geracao_horaria"
    where sigla_uf is not null
    group by sigla_uf
)

select
    sigla_uf,
    nome_estado,
    nome_subsistema,
    cod_subsistema,
    eh_brasil,
    row_number() over (order by sigla_uf) as sk_uf
from ufs_ons
  );
  