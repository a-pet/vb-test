-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +intermediate_google_ads_group_with_product_categories -t dbt_transform

{{ config(materialized='view')}}

with
-- input
    source_daily_google_ads_ad_group as (
        select * from {{ source('sources_combined', 'source_daily_google_ads_ad_group') }}
    ),
    
    google_ads_keyword as (
        select * from {{ ref('google_ads_keyword') }}
    ),

    intermediate_google_ads_ad_with_last_state as (
        select * from {{ ref('intermediate_google_ads_ad_with_last_state') }}
    ),

    entity_ad_group_with_accomulitive_ad_texts_fields as (
        -- cte_type: acomulative subcategory strings
        select
            ad_group_id,

            string_agg(
                distinct(
                    regexp_extract(
                        to_json_string(final_urls),
                        '/(?:products|collections|pages)/([^/"]+)'
                    )
                ),
                ','
            )                                                                           as product,

            string_agg(distinct(ad_type))                                               as ad_type,
            string_agg(distinct(ad_name))                                               as ad_name,
            string_agg(distinct(to_json_string(responsive_search_ad_descriptions)))     as responsive_search_ad_descriptions_string,
            string_agg(distinct(to_json_string(video_responsive_ad_descriptions)))      as video_responsive_ad_descriptions_string,
            string_agg(distinct(to_json_string(expanded_text_ad_description)))          as expanded_text_ad_description_string,
            string_agg(distinct(to_json_string(responsive_display_ad_descriptions)))    as responsive_display_ad_descriptions_string

        from intermediate_google_ads_ad_with_last_state
        group by ad_group_id
    ),

    entity_ad_group_with_product_and_description as (
            select
                array_to_string(
                    regexp_extract_all(
                        case
                            when ad_type = 'RESPONSIVE_SEARCH_AD'       then responsive_search_ad_descriptions_string
                            when ad_type = 'VIDEO_RESPONSIVE_AD'        then video_responsive_ad_descriptions_string
                            when ad_type = 'EXPANDED_TEXT_AD'           then expanded_text_ad_description_string
                            when ad_type = 'RESPONSIVE_DISPLAY_AD'      then responsive_display_ad_descriptions_string
                            else NULL
                        end,

                        r'text: \\"([^\\"]+)\\"'
                    ), ' '
                )                                                                       as ad_descriptions,

                *

            from entity_ad_group_with_accomulitive_ad_texts_fields
        ),

        group_id_product_description_combined as (
            select
                ad_group_id,

                trim(
                    string_agg(
                        distinct(product),
                        ', ' order by product
                    )
                )                                                                       as products_by_comma,

                trim(
                    string_agg(
                        distinct(ad_descriptions),
                        '' order by ad_descriptions
                    )
                )                                                                       as description_combined

            from entity_ad_group_with_product_and_description
            group by ad_group_id
        ),

        product_descriptions_as_key as (
            select
                string_agg(
                    products_by_comma,
                    ', '
                )                                                                       as group_id_products_by_comma,

                description_combined

            from group_id_product_description_combined
            group by description_combined
        ),

        group_id_product_description_combined_adjusted as (
            select
                ad_group.ad_group_id,

                coalesce(
                    ad_group.products_by_comma,
                    product_descriptions_as_key.group_id_products_by_comma
                )                                                                       as products_by_comma

            from group_id_product_description_combined                                  as ad_group
            left join product_descriptions_as_key
            using(description_combined)
        ),

        entity_ad_groups_with_prodduct_categories_from_urls as (
            select
                case
                    when products_by_comma      like '%satin-kajal-liner%'              then 'SKL'

                    when products_by_comma      like '%fragrance-discovery%'
                        or products_by_comma    like '%suite-302%'
                        or products_by_comma    like '%san-ysidro-drive%'
                        or products_by_comma    like '%portofino-97%'                   then 'FRD'

                    when products_by_comma      like '%fragrance%'                      then 'FRG'

                    when products_by_comma      like '%skin-augustinus-bader%'
                        or products_by_comma    like '%power-serum%'                    then 'SKA'

                    when products_by_comma      like '%skin%'
                        or products_by_comma    like '%the-daily-cleansing-protocol%'   then 'SKN'

                    when products_by_comma      like '%mascara%'                        then 'MAS'

                    when products_by_comma      like '%lip%'
                        or products_by_comma    like '%gloss%'                          then 'LIP'

                    when products_by_comma      like '%face%'
                        or products_by_comma    like '%reflect-highlighter-stick%'
                        or products_by_comma    like '%contour-stylus%'
                        or products_by_comma    like '%babyblade%'                      then 'FCE'

                    when products_by_comma      like '%eye%'
                        or products_by_comma    like '%lid%'
                        or products_by_comma    like '%brightening-waterline-pencil%'   then 'EYE'

                    else Null
                end
                                                                                        as category_from_destination_url_or_description,

                * except (products_by_comma)

            from group_id_product_description_combined_adjusted
        ),

    _take_last_state_from_ad_group as (
        -- cte_type: last state
        select
            ad_group_id,

            max(ad_group_type)                                                          as ad_group_type,
            max(ad_group_name)                                                          as ad_group_name,


            round(sum(metrics_cost_micros) / 1000000)                                   as ad_group_total_spend,

            round(
                sum(
                    CASE
                        WHEN segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                        THEN metrics_cost_micros
                        ELSE 0
                    END

                )/1000000,

                2
            )                                                                           as ad_group_spend_in_last_7_days,

            round(
                sum(
                    CASE
                        WHEN segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                        THEN metrics_cost_micros
                        ELSE 0
                    END

                )/1000000,

                2
            )                                                                           as ad_group_spend_in_last_14_days

        from source_daily_google_ads_ad_group
        group by ad_group_id
    ),

    _join_info_from_ad_group_source as (
        -- ste_type: join columns
        select *
        from entity_ad_groups_with_prodduct_categories_from_urls
        left join _take_last_state_from_ad_group
        using (ad_group_id)
    ),

    _join_ad_entity_artefacts as (
        select *
        from _join_info_from_ad_group_source
        left join entity_ad_group_with_product_and_description
        using (ad_group_id)
    )

-- final
        select * from _join_ad_entity_artefacts