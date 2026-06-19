select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_empreendimento
from "energia"."public_staging"."stg_aneel_mmgd"
where id_empreendimento is null



      
    ) dbt_internal_test