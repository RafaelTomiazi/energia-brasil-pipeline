select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select geracao_mwh
from "energia"."public_staging"."stg_ons_geracao_horaria"
where geracao_mwh is null



      
    ) dbt_internal_test