select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select potencia_kw
from "energia"."public_staging"."stg_aneel_mmgd"
where potencia_kw is null



      
    ) dbt_internal_test