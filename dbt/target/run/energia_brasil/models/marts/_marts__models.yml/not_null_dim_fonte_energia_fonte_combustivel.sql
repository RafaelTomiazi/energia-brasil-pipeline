select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fonte_combustivel
from "energia"."public_marts"."dim_fonte_energia"
where fonte_combustivel is null



      
    ) dbt_internal_test