select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    sk_uf as unique_field,
    count(*) as n_records

from "energia"."public_marts"."dim_uf"
where sk_uf is not null
group by sk_uf
having count(*) > 1



      
    ) dbt_internal_test