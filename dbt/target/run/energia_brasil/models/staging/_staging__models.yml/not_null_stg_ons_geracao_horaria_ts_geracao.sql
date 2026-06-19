select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ts_geracao
from "energia"."public_staging"."stg_ons_geracao_horaria"
where ts_geracao is null



      
    ) dbt_internal_test