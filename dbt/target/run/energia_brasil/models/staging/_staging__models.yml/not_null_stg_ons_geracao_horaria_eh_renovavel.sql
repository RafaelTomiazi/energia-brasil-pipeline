select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select eh_renovavel
from "energia"."public_staging"."stg_ons_geracao_horaria"
where eh_renovavel is null



      
    ) dbt_internal_test