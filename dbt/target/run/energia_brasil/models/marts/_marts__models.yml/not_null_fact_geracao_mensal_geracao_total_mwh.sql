select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select geracao_total_mwh
from "energia"."public_marts"."fact_geracao_mensal"
where geracao_total_mwh is null



      
    ) dbt_internal_test