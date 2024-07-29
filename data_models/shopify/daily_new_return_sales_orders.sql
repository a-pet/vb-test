-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +daily_new_return_sales_orders -t dbt_transform

{{ config(materialized='view')}}

with 
-- input
    source_daily_new_return_sales_orders_2024_shopify as ( select * from {{ source('sources_combined', 'source_daily_new_return_sales_orders_2024_shopify') }}),

    source_daily_new_return_sales_orders_2023_shopify as ( select * from {{ source('sources_combined', 'source_daily_new_return_sales_orders_2023_shopify') }}),

    combine_years as (
        select * from source_daily_new_return_sales_orders_2024_shopify
        union all
        select * from source_daily_new_return_sales_orders_2023_shopify
    ),

    map_to_country_codes as (
        select
            * except(country),
            {{ map_country_to_code('country') }}                                        as country
        from combine_years
    ),
    
    seperate_new_return as (
        select
            day,
            {{ map_country_to_region('country') }}                                      as region,
            sum(case when customer_type = 'First-time' then orders else 0 end)          as new_orders,
            sum(case when customer_type = 'First-time' then total_sales else 0 end)     as new_sales,
            sum(case when customer_type = 'Returning' then orders else 0 end)           as return_orders,
            sum(case when customer_type = 'Returning' then total_sales else 0 end)      as return_sales
        from
            map_to_country_codes
        group by
            day,
            region
    )
-- final
select * from seperate_new_return