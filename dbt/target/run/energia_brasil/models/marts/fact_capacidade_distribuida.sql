
  
    

  create  table "energia"."public_marts"."fact_capacidade_distribuida__dbt_tmp"
  
  
    as
  
  (
    -- =====================================================================
-- fact_capacidade_distribuida — capacidade instalada de GD por UF/mês
-- =====================================================================
-- Cada linha: UF + ano-mês de conexão + fonte = potência somada das
-- usinas distribuídas conectadas naquele mês.
--
-- Útil pra responder: "quanta solar foi instalada em SP em 2024?"
-- Cruzamento natural com fact_geracao_mensal pelo (sigla_uf + ano_mes).
-- =====================================================================

with cadastro as (
    select
        sigla_uf,
        ano_conexao,
        mes_conexao,
        fonte_geracao,
        classe_consumo,
        porte,
        count(*)                       as num_empreendimentos_novos,
        sum(potencia_kw)               as potencia_kw_novos,
        avg(potencia_kw)               as potencia_media_kw
    from "energia"."public_staging"."stg_aneel_mmgd"
    group by 1, 2, 3, 4, 5, 6
)

select
    c.sigla_uf,
    c.ano_conexao,
    c.mes_conexao,
    c.fonte_geracao,
    c.classe_consumo,
    c.porte,
    c.num_empreendimentos_novos,
    c.potencia_kw_novos,
    round(c.potencia_media_kw::numeric, 2) as potencia_media_kw,
    -- chave estrangeira
    u.sk_uf
from cadastro c
left join "energia"."public_marts"."dim_uf" u on c.sigla_uf = u.sigla_uf
  );
  