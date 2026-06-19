select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    sigla_uf as unique_field,
    count(*) as n_records

from "energia"."public_marts"."dim_uf"
where sigla_uf is not null
group by sigla_uf
having count(*) > 1



      
    ) dbt_internal_test