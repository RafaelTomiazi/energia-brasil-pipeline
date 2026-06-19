
    
    

with child as (
    select sigla_uf as from_field
    from "energia"."public_marts"."fact_geracao_mensal"
    where sigla_uf is not null
),

parent as (
    select sigla_uf as to_field
    from "energia"."public_marts"."dim_uf"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


