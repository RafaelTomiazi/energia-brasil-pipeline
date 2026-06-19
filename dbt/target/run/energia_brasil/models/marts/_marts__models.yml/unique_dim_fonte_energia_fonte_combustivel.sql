select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    fonte_combustivel as unique_field,
    count(*) as n_records

from "energia"."public_marts"."dim_fonte_energia"
where fonte_combustivel is not null
group by fonte_combustivel
having count(*) > 1



      
    ) dbt_internal_test