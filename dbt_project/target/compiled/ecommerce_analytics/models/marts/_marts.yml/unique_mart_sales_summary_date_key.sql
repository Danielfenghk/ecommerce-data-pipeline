
    
    

select
    date_key as unique_field,
    count(*) as n_records

from "ecommerce_warehouse"."public_marts"."mart_sales_summary"
where date_key is not null
group by date_key
having count(*) > 1


