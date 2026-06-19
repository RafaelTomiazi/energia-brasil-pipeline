select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select categoria_simplificada
from "energia"."public_marts"."dim_fonte_energia"
where categoria_simplificada is null



      
    ) dbt_internal_test