-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +intermediate_daily_meta_ads_ad_name_updated -t dbt_transform

{{ config(materialized='view')}}

with
-- input
    ads_insights as (select * from {{ source('sources_combined', 'source_daily_meta_ads') }}),

--
    _update_ad_name as (
        select

            if(
                lower(ad_name) like '%free%' ,
                    'free shipping',
                    'no free shipping'
            )
                                                                                        as free_shipping_note,

            *

        from ads_insights
    )

-- final
        select * from _update_ad_name


