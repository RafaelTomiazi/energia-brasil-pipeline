
    
    

with all_values as (

    select
        categoria_simplificada as value_field,
        count(*) as n_records

    from "energia"."public_marts"."dim_fonte_energia"
    group by categoria_simplificada

)

select *
from all_values
where value_field not in (
    'Hídrica','Eólica','Solar','Nuclear','Biomassa','Térmica/Outras'
)


