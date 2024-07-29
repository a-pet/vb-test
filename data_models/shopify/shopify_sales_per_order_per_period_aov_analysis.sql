-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +shopify_sales_per_order_per_period_aov_analysis -t dbt_transform

{{ config(materialized='table')}}

with 
-- input
    source_shopify_daily_sales_orders_users_by_country as ( select * from {{ source('sources_combined', 'source_shopify_daily_sales_orders_users_by_country') }}),

    source_shopify_monthly_sales_orders_new_return_per_order as ( select * from {{ source('sources_combined', 'source_shopify_monthly_sales_orders_new_return_per_order') }}),

    deduplicate_shopify_new_return_step_1 as (
        select
            *,
            row_number() over(partition by month, billing_country, order_id, customer_type order by total_sales desc) as rn
        from source_shopify_monthly_sales_orders_new_return_per_order
    ),

    deduplicate_shopify_new_return_step_2 as (
        select
            * except(rn)
        from deduplicate_shopify_new_return_step_1
        where rn = 1
    ),
    
    join_aov_data_with_new_return_data as (
        select
            parse_date('%Y-%m-%d', concat(aov.month, '-01'))                        as month,
            aov.order_id,
            concat(new_return.customer_type, ' / ', new_return.purchase_option)     as customer_type,
            variant_sku,
            product                                                                 as product_title,
            product_price,
            aov.ordered_items                                                       as ordered_item_quantity,
            gross_sales,
            discounts,
            net_sales,

             {{ map_country_to_region('aov.billing_country') }}                     as region

        from 
                        source_shopify_daily_sales_orders_users_by_country aov 
            inner join  deduplicate_shopify_new_return_step_2 new_return

            on
                    aov.month = new_return.month
                and aov.order_id = new_return.order_id
    ),

    group_by_periods as (
        select
            'Q1 2023' as period,
            variant_sku,
            product_title,
            customer_type,
            product_price,
            count(distinct(order_id))                               as orders,
            round(sum(ordered_item_quantity))                       as ordered_item_quantity,
            round(sum(gross_sales))                                 as gross_sales,
            round(sum(discounts))                                   as discounts,
            round(sum(net_sales))                                   as net_sales,
            region
        FROM join_aov_data_with_new_return_data 
        where 1=1
            and month >= '2023-01-01' and month <= '2023-03-01'
        group by 
            customer_type,
            variant_sku,
            product_title,
            product_price,
            region

        union all

        SELECT
            'Q2 2023' as period,
            variant_sku,
            product_title,
            customer_type,
            product_price,
            count(distinct(order_id))                               as orders,
            round(sum(ordered_item_quantity))                       as ordered_item_quantity,
            round(sum(gross_sales))                                 as gross_sales,
            round(sum(discounts))                                   as discounts,
            round(sum(net_sales))                                   as net_sales,
            region
        FROM join_aov_data_with_new_return_data 
        where 1=1
            and month >= '2023-04-01' and month <= '2023-06-01'
        group by 
            customer_type,
            variant_sku,
            product_title,
            product_price,
            region

        union all

        SELECT
            'Q1 2024' as period,
            variant_sku,
            product_title,
            customer_type,
            product_price,
            count(distinct(order_id))                               as orders,
            round(sum(ordered_item_quantity))                       as ordered_item_quantity,
            round(sum(gross_sales))                                 as gross_sales,
            round(sum(discounts))                                   as discounts,
            round(sum(net_sales))                                   as net_sales,
            region
        FROM join_aov_data_with_new_return_data 
        where 1=1
            and month >= '2024-01-01' and month <= '2024-03-01'
        group by 
            customer_type,
            variant_sku,
            product_title,
            product_price,
            region

        union all

        SELECT
            'Q2 2024' as period,
            variant_sku,
            product_title,
            customer_type,
            product_price,
            count(distinct(order_id))                               as orders,
            round(sum(ordered_item_quantity))                       as ordered_item_quantity,
            round(sum(gross_sales))                                 as gross_sales,
            round(sum(discounts))                                   as discounts,
            round(sum(net_sales))                                   as net_sales,
            region
        FROM join_aov_data_with_new_return_data 
        where 1=1
            and month >= '2024-04-01' and month <= '2024-06-01'
        group by 
            customer_type,
            variant_sku,
            product_title,
            product_price,
            region

        union all

        SELECT
            cast(month as string)                                   as period,
            variant_sku,
            product_title,
            customer_type,
            product_price,
            count(distinct(order_id))                               as orders,
            round(sum(ordered_item_quantity))                       as ordered_item_quantity,
            round(sum(gross_sales))                                 as gross_sales,
            round(sum(discounts))                                   as discounts,
            round(sum(net_sales))                                   as net_sales,
            region
        FROM join_aov_data_with_new_return_data 
        group by 
            period,
            customer_type,
            variant_sku,
            product_title,
            product_price,
            region
    )

--final
select 
    *,
    round(net_sales / orders, 2)                                    as aov
from group_by_periods
where 1=1
    and period is not null
    and variant_sku is not null
