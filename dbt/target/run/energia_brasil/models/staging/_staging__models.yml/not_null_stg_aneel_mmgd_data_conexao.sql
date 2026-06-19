select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select data_conexao
from "energia"."public_staging"."stg_aneel_mmgd"
where data_conexao is null



      
    ) dbt_internal_test