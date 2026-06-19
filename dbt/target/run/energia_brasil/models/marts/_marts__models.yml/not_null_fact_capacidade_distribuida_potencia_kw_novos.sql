select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select potencia_kw_novos
from "energia"."public_marts"."fact_capacidade_distribuida"
where potencia_kw_novos is null



      
    ) dbt_internal_test