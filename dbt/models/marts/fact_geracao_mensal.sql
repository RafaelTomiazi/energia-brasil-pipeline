-- =====================================================================
-- fact_geracao_mensal — fato principal de geração energética
-- =====================================================================
-- Agregação por (UF + ano-mês + fonte). Granularidade mensal,
-- não horária — é o que faz sentido pro dashboard final.
--
-- Conceito: fato é o "evento mensurável". Aqui = quanto cada UF
-- gerou de cada fonte em cada mês.
-- =====================================================================

with geracao as (
    select
        sigla_uf,
        ano_geracao,
        mes_geracao,
        fonte_combustivel,
        sum(geracao_mwh)                 as geracao_total_mwh,
        sum(case when eh_renovavel then geracao_mwh else 0 end) as geracao_renovavel_mwh,
        count(distinct id_usina)         as num_usinas_ativas,
        count(*)                         as num_horas_medidas
    from {{ ref('stg_ons_geracao_horaria') }}
    where eh_brasil = true   -- exclui Itaipu/Paraguai
    group by 1, 2, 3, 4
)

select
    g.sigla_uf,
    g.ano_geracao,
    g.mes_geracao,
    g.fonte_combustivel,
    g.geracao_total_mwh,
    g.geracao_renovavel_mwh,
    g.num_usinas_ativas,
    g.num_horas_medidas,
    -- chaves estrangeiras pra dimensões
    u.sk_uf,
    f.sk_fonte,
    -- métricas calculadas úteis
    round((g.geracao_renovavel_mwh / nullif(g.geracao_total_mwh, 0) * 100)::numeric, 2)
        as pct_renovavel
from geracao g
left join {{ ref('dim_uf') }}            u on g.sigla_uf = u.sigla_uf
left join {{ ref('dim_fonte_energia') }} f on g.fonte_combustivel = f.fonte_combustivel
