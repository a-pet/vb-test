-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +daily_meta_ads -t dbt_transform

{{ config(materialized='view')}}

with 
    -- input
    ads_insights as (select * from {{ ref('intermediate_daily_meta_ads_ad_name_updated') }}),

    _extract_metrics_from_nested_fields as (
        select 
            date_start                                                                  as ad_run_date,

            free_shipping_note,
        
            campaign_name,
            adset_name,
            ad_name,
            reach,
            spend,
            impressions,
            clicks,
            unique_clicks,

            array(
                select max(value)                                                           

                from 
                    UNNEST(json_extract_array(actions)) as action_json,
                    UNNEST([json_extract_scalar(action_json, '$.action_type')])             as action_type,

                    UNNEST(
                        [cast(
                            json_extract_scalar(action_json, '$.value') as INT64)
                        ]
                    )                                                                       as value

                where action_type = 'onsite_web_purchase'

            )[safe_offset(0)]                                                               as onsite_web_purchases, 

            array(
                select max(value)                                                          

                from 
                    UNNEST(json_extract_array(action_values)) as action_json,
                    UNNEST([json_extract_scalar(action_json, '$.action_type')])             as action_type,

                    UNNEST(
                        [cast(
                            json_extract_scalar(action_json, '$.value') as float64)
                        ]
                    )                                                                       as value

                where action_type = 'onsite_web_app_purchase'

            )[safe_offset(0)]                                                               as onsite_web_app_purchase_sum_usd, 


            array(
                select max(value)                                                           

                from 
                    UNNEST(json_extract_array(actions)) as action_json,
                    UNNEST([json_extract_scalar(action_json, '$.action_type')])             as action_type,

                    UNNEST(
                        [cast(
                            json_extract_scalar(action_json, '$.value') as INT64)
                        ]
                    )                                                                       as value

                where action_type = 'landing_page_view'

            )[safe_offset(0)]                                                               as landing_page_views, 


            array(
                select max(value)                                                           as onsite_web_purchase_value

                from 
                    UNNEST(json_extract_array(actions)) as action_json,
                    UNNEST([json_extract_scalar(action_json, '$.action_type')])             as action_type,

                    UNNEST(
                        [cast(
                            json_extract_scalar(action_json, '$.value') as INT64)
                        ]
                    )                                                                       as value

                where action_type = 'onsite_web_app_add_to_cart'

            )[safe_offset(0)]                                                               as onsite_web_app_add_to_cart_count, 

            quality_ranking,
            inline_post_engagement,
            unique_link_clicks_ctr,
            unique_inline_link_clicks,
            
            created_time,
            updated_time,   

            array(
                select
                    sum(
                        cast(
                            JSON_EXTRACT_SCALAR(
                                unnest_video_p75_watched_actions, 
                                '$.value'
                            )
                            as INT64
                        ) 
                    )                                                                       as video_p75_watched_actions_count

                from 
                    UNNEST(
                        JSON_EXTRACT_ARRAY(
                            video_p75_watched_actions
                        )
                    )                                                                       as unnest_video_p75_watched_actions

            )[safe_offset(0)]                                                               as video_p75_watched_actions_count, 

            array(
                select
                    sum(
                        cast(
                            JSON_EXTRACT_SCALAR(
                                unnest_video_15_sec_watched_actions, 
                                '$.value'
                            )
                            as INT64
                        ) 
                    )                                                                       as video_15_sec_watched_count

                from 
                    UNNEST(
                        JSON_EXTRACT_ARRAY(
                            video_15_sec_watched_actions
                        )
                    )                                                                       as unnest_video_15_sec_watched_actions

            )[safe_offset(0)]                                                               as video_15_sec_watched_count,

            campaign_id,
            adset_id,
            ad_id,

            frequency,
            cpc,
            cpm,
            cpp,
            ctr,
            -- unique_ctr,

            account_currency,
            optimization_goal

        from ads_insights
    ),

    _map_experiment_campaigns as (
        select
            *,

            case 
                when 
                    campaign_id in ('120210388163900589', '120210389141230589')
                    and ad_run_date between '2024-05-21' and '2024-06-04'
                                                                then true

                when 
                    lower(campaign_name) like ('%[exp]%')
                    and campaign_id not in ('120210388163900589', '120210389141230589', '120210387288060589')      
                                                                then true

                                                                else false

            end                                                                             as is_experiment
        
        from _extract_metrics_from_nested_fields
    ),

    _apply_minor_mapping_to_new_data as (
        select
            * except(
                free_shipping_note
            ),

            lower(campaign_name) like '%[rf]%'                                              as is_reach_and_frequency,

            lower(campaign_name) like '%retarget%'                                          as is_retargeting,

                not (lower(campaign_name) like '%[rf]%') 
            and 
                not (lower(campaign_name) like '%retarget%')
            and
                not is_experiment                                                           as is_prospecting,
            
            coalesce(
                free_shipping_note, 
                if(lower(ad_name) like '%free%' , 'free shipping', 'no free shipping')
            )                                                                               as free_shipping_note
        
        from _map_experiment_campaigns
    ),

    _extract_country_and_category as (
        select 
            *,
                        
            upper(
                case
                    when 
                            SUBSTR(adset_name,3,1) = '_'
                        and SUBSTR(adset_name,1,2) != 'GB'
                    then SUBSTR(adset_name,1,2)

                    when 
                            SUBSTR(adset_name,3,1) = '_'
                        and SUBSTR(adset_name,1,2) = 'GB'
                    then 'UK'
                
                    when REGEXP_EXTRACT(ad_name, r'\[([^\]]+)\]') != 'GB'
                    then REGEXP_EXTRACT(ad_name, r'\[([^\]]+)\]')

                    when 
                            SUBSTR(campaign_name,3,1) = '_'
                        and SUBSTR(campaign_name,1,2) != 'GB'
                    then SUBSTR(campaign_name,1,2)

                    when 
                            SUBSTR(campaign_name,3,1) = '_'
                        and SUBSTR(campaign_name,1,2) = 'GB'
                    then 'UK'

                    else null
                end
            )                                                                           as country,
            
            REGEXP_EXTRACT(ad_name, r'\[[^\]]+\]\s*\[([^\]]+)\]')                       as category
        
        from _apply_minor_mapping_to_new_data
    ),

    _mapping_regions as (
        select 
            * except (category),

            {{ map_country_to_region('country') }}                                      as region,

            case
                when 
                        category = 'FRAG'
                    or  lower(ad_name) like '%frag%'
                    or  lower(ad_name) like '%syd%'                                  then 'FRD'

                when category = 'MCE'                                                   then 'MSC'

                when category is null
                    then
                        case 
                            when 
                                    lower(ad_name) like '%satin%' 
                                or  lower(ad_name) like '%skl%'
                                or  lower(ad_name) like '%jewel_smoky%'
                                or  lower(ad_name) like '%sigsmokeout%'                 then 'SKL'
                            when 
                                    lower(ad_name) like '%lid%'  
                                or  lower(ad_name) like '%ibwp%'
                                or  (
                                        lower(ad_name) like '%free%'
                                    and lower(ad_name) like '%copy%'
                                )
                                or  lower(adset_name) like '%eye%'                      then 'EYE'
                            when 
                                    lower(ad_name) like '%augustinus%'  
                                or  lower(ad_name) like '%crpm%'                        then 'SKA'      -- Cell Rejuvenating Priming Moisturizer
                            when 
                                    lower(ad_name) like '%mascara%'
                                or  lower(adset_name) like '%vast lash%'                then 'MAS'
                            when 
                                    lower(ad_name) like '%lip%' 
                                or  lower(ad_name) like '%posh%' 
                                or  lower(ad_name) like '%blt%'                         then 'LIP'      -- bitten lip tint
                            when 
                                    lower(ad_name) like '%contour%' 
                                or  lower(ad_name) like '%highlighter%'
                                or  lower(ad_name) like '%grwvb%'
                                or  lower(ad_name) like '%majorglowset%'                then 'FCE'
                            when 
                                    lower(ad_name) like '%skin%' 
                                or  lower(ad_name) like '%serum%' 
                                or  lower(ad_name) like '%moisturizer%'
                                or  lower(campaign_name) like '%skin%'                  then 'SKN'

                                                                                        else 'MSC'
                        end
                                                                        else category
                end                                                                         as product_category

        from _extract_country_and_category
    ),

    _weight_outliers_by_aov_to_median as (
        select
            *,

                onsite_web_app_purchase_sum_usd 
            /   onsite_web_purchases                                               as aov,

            date_trunc(ad_run_date, month)                                              as month_start_date


        from _mapping_regions
    ),

    ordered_aov_values as (
        select
            region,

            month_start_date,

            aov,

            ROW_NUMBER() 
                over (
                    PARTITION BY 
                        region, 
                        month_start_date

                    ORDER BY aov
                )                                                                       as _row_number,

            COUNT(*) 
                over (
                    PARTITION BY 
                        region, 
                        month_start_date
                )                                                                       as rows_in_granula

        from _weight_outliers_by_aov_to_median
        where 1=1
            and onsite_web_app_purchase_sum_usd     is not null
            and onsite_web_purchases           is not null
            and onsite_web_purchases           != 0
    ),

    _calculate_median as (
        select
            region,
            month_start_date,

            AVG(aov)                                                                    as aov_reion_month_median

        from ordered_aov_values
        
        where
                _row_number 
                    = cast(     rows_in_granula         /   2 as INT64  ) + 1

            or  _row_number 
                    = cast((    rows_in_granula + 1 )   /   2 as INT64  )

        group by
            region,
            month_start_date
    ),

    _replace_too_big_number_to_median as (
        select 
            *,

            if(
                aov > 3 * aov_reion_month_median,

                aov_reion_month_median,
                
                aov
            )                                                                           as aov_corrected


        from _weight_outliers_by_aov_to_median
        left join _calculate_median

        using (
            region,
            month_start_date
        )
    ),

    _update_purchases_sum as (
        select 
            * except (onsite_web_app_purchase_sum_usd),                                  

            onsite_web_purchases * aov_corrected                                   as onsite_web_app_purchase_sum_usd 

        from _replace_too_big_number_to_median
    ),

    _add_ads_platform_and_channel as (
        select 
            *,

            'Meta Ads'                                                                  as ads_platform,
            'Paid Social'                                                               as channel

        from _update_purchases_sum
    ),

    _sort_by_date as (
        select * 
        from _add_ads_platform_and_channel
        order by ad_run_date desc    
    )

-- final
        select * from _sort_by_date


