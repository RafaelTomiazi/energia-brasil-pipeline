-- =====================================================================
-- dim_fonte_energia — classificação das fontes energéticas
-- =====================================================================
-- Classificação consolidada de cada fonte:
--   - Renovável vs Não-renovável
--   - Limpa vs Suja (renováveis + nuclear vs combustíveis fósseis)
--   - Categoria simplificada (Solar, Eólica, Hídrica, Térmica, Nuclear)
-- =====================================================================

with fontes_distintas as (
    select distinct
        fonte_combustivel,
        eh_renovavel
    from {{ ref('stg_ons_geracao_horaria') }}
    where fonte_combustivel is not null
)

select
    fonte_combustivel                    as fonte_combustivel,
    eh_renovavel                         as eh_renovavel,
    -- Categoria simplificada (gera bons gráficos)
    case
        when fonte_combustivel ilike '%hidr%' then 'Hídrica'
        when fonte_combustivel ilike '%eól%' or fonte_combustivel ilike '%eol%' then 'Eólica'
        when fonte_combustivel ilike '%solar%' or fonte_combustivel ilike '%fotovolt%' then 'Solar'
        when fonte_combustivel ilike '%nuclear%' then 'Nuclear'
        when fonte_combustivel ilike '%biomass%' then 'Biomassa'
        else 'Térmica/Outras'
    end                                  as categoria_simplificada,
    -- Limpa = não emite CO2 na geração (renováveis + nuclear)
    case
        when eh_renovavel then true
        when fonte_combustivel ilike '%nuclear%' then true
        else false
    end                                  as eh_limpa,
    row_number() over (order by fonte_combustivel) as sk_fonte
from fontes_distintas
