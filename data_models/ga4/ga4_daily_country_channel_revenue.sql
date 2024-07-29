-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +ga4_daily_country_channel_revenue -t dbt_transform

{{ config(materialized='view') }}

with
    -- input
    source_ga4_daily_country_marketing_channel as (select * from {{ source('sources_combined', 'source_ga4_daily_country_marketing_channel') }}),

    _columns_naming as (
        select 
            cast(dimension_date as DATE)                                                as date,

            dimension_country                                                           as country,
            {{ map_country_to_code('dimension_country') }}                              as country_code,

            metric_sessions                                                             as sessions,

            metric_purchase_revenue                                                     as revenue,
            metric_total_purchasers                                                     as purchasers,

            dimension_session_medium,
            dimension_session_source,

            case 
                when
                        lower(dimension_session_source) like '%google%'
                    and (
                            lower(dimension_session_medium) like '%paid%'
                        or  lower(dimension_session_medium) like '%cpc%'
                        or  lower(dimension_session_medium) like '%ads%'
                    )                                                           then 'google/paid'
                
                when
                            lower(dimension_session_source) like '%facebook%'
                        or  lower(dimension_session_source) like '%fb%'
                        or  lower(dimension_session_source) like '%meta%'
                        or  lower(dimension_session_source) like '%instagram%'
                    and (
                            lower(dimension_session_medium) like '%paid%'
                        or  lower(dimension_session_medium) like '%cpc%'
                        or  lower(dimension_session_medium) like '%igshopping%'
                    )                                                           then 'meta/paid'
                
                when
                        lower(dimension_session_source) like '%google%'
                    and lower(dimension_session_medium) like '%organic%'        then 'google/organic'

                when    lower(dimension_session_medium) like '%organic%'        then 'other/organic'
                
                when 
                        lower(dimension_session_source) like '%direct%'
                    and lower(dimension_session_medium) like '%none%'           then 'direct/none'

                when
                        lower(dimension_session_medium) like '%social%'
                    and lower(dimension_session_source) not like '%facebook%'
                    and lower(dimension_session_source) not like '%fb%'
                    and lower(dimension_session_source) not like '%meta%'
                    and lower(dimension_session_source) not like '%instagram%'  then 'organic/social'

                when    lower(dimension_session_medium) like '%referral%'       then 'referral'
                
                when
                        lower(dimension_session_source) like '%facebook%'
                    or  lower(dimension_session_source) like '%fb%'
                    or  lower(dimension_session_source) like '%meta%'
                    or  lower(dimension_session_source) like '%instagram%'      then 'meta'
                
                when    lower(dimension_session_medium) like '%email%'          then 'email'
                
                when    
                        lower(dimension_session_medium) not like '%referral%'
                    and lower(dimension_session_medium) not like '%not set%'    then 'affiliate'

                                                                                else 'other traffic'   -- (not set)
            
            end                                                                         as ga4_source_medium,

                
            dimension_session_default_channel_group                                     as session_default_channel_group

        from source_ga4_daily_country_marketing_channel
    ),

    _map_region as (
        select
            {{ map_country_to_region('country') }}                                      as region,

            *

        from _columns_naming
    ),

    _marketing_channels_mapping as (
        select 

            case
                when ga4_source_medium like '%google%'                          then 'Google Ads' 
                when ga4_source_medium like '%meta%'                            then 'Meta Ads'

            end                                                                         as ads_platform,

            *

        from _map_region

    )

-- final
        select * from _marketing_channels_mapping
