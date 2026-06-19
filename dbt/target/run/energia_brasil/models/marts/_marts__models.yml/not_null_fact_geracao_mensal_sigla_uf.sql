select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sigla_uf
from "energia"."public_marts"."fact_geracao_mensal"
where sigla_uf is null



      
    ) dbt_internal_test