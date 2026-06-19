select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sk_uf
from "energia"."public_marts"."dim_uf"
where sk_uf is null



      
    ) dbt_internal_test