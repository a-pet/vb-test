-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +shopify_monthly_orders_per_region_per_customer_type -t dbt_transform

{{ config(materialized='table')}}

with 
-- input
    source_shopify_monthly_sales_orders_new_return_per_order as ( select * from {{ source('sources_combined', 'source_shopify_monthly_sales_orders_new_return_per_order') }}),

    group_by_month_customer_type_region as (
        select
            parse_date('%Y-%m-%d', concat(month, '-01'))                            as month,
            {{ map_country_to_region('billing_country') }}                          as region,
            concat(customer_type, ' / ', purchase_option)                           as customer_type,
            count(distinct(order_id))                                               as orders
        FROM source_shopify_monthly_sales_orders_new_return_per_order 
        where 1=1
            and orders = 1
        group by
            month,
            region,
            customer_type
    ),

    union_quarters as (
        select
            cast(month as string) as period,
            region,
            customer_type,
            orders
        from group_by_month_customer_type_region

        union all

        select
            'Q1 2023' as period,
            region,
            customer_type,
            sum(orders) as orders
        from group_by_month_customer_type_region
        where 1=1
            and month between '2023-01-01' and '2023-03-01'
        group by
            period,
            region,
            customer_type

        union all

        select
            'Q2 2023' as period,
            region,
            customer_type,
            sum(orders) as orders
        from group_by_month_customer_type_region
        where 1=1
            and month between '2023-04-01' and '2023-06-01'
        group by
            period,
            region,
            customer_type

        union all

        select
            'Q1 2024' as period,
            region,
            customer_type,
            sum(orders) as orders
        from group_by_month_customer_type_region
        where 1=1
            and month between '2024-01-01' and '2024-03-01'
        group by
            period,
            region,
            customer_type

        union all

        select
            'Q2 2024' as period,
            region,
            customer_type,
            sum(orders) as orders
        from group_by_month_customer_type_region
        where 1=1
            and month between '2024-04-01' and '2024-06-01'
        group by
            period,
            region,
            customer_type
    )

--final
select * from union_quarters


