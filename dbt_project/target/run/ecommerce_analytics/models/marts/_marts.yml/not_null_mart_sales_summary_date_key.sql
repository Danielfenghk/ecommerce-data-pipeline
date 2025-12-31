
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_key
from "ecommerce_warehouse"."public_marts"."mart_sales_summary"
where date_key is null



  
  
      
    ) dbt_internal_test