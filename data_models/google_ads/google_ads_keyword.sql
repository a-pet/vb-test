-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +google_ads_keyword -t dbt_transform

{{ config(materialized='view') }}

with
    source_daily_google_ads_keywords as (
        select * from {{ source('sources_combined','source_daily_google_ads_keywords') }}
    ),

    entity_keyword_last_state as (
        select
            campaign_id,
            ad_group_id,
            ad_group_criterion_keyword_text                                             as keyword,

            max_by(
                ad_group_criterion_type,
                segments_date
            )                                                                           as ad_group_criterion_type,

            max_by(
                campaign_bidding_strategy_type,
                segments_date
            )                                                                           as campaign_bidding_strategy_type,

            max_by(
                ad_group_criterion_keyword_match_type,
                segments_date
            )                                                                           as ad_group_criterion_keyword_match_type,

            max(segments_date)                                                          as keyword_last_run_date

        from source_daily_google_ads_keywords

        group by
            campaign_id,
            ad_group_id,
            keyword

    ),

    group_id_extracted_brand_raw as (
        select
            case
                when REGEXP_CONTAINS(keyword, r'(?i)\baerin\b')                                     then 'Aerin'
                when REGEXP_CONTAINS(keyword, r'(?i)\bthrive(?:\s+causemetics)?\b')                 then 'Thrive Causemetics'
                when REGEXP_CONTAINS(keyword, r'(?i)\bnishane\b')                                   then 'Nishane'
                when REGEXP_CONTAINS(keyword, r'(?i)\bbond\s*(?:no\s*)?9\b|\bperfume\s*bond\b')     then 'Bond No. 9 Tribeca'
                when REGEXP_CONTAINS(keyword, r'(?i)\bbvlgari\b')                                   then 'Bvlgari'
                when REGEXP_CONTAINS(keyword, r'(?i)\bbyredo\b')                                    then 'byredo'
                when REGEXP_CONTAINS(keyword, r'(?i)\bcartier\b')                                   then 'Cartier'
                when REGEXP_CONTAINS(keyword, r'(?i)\bcreed\b')                                     then 'Creed'
                when REGEXP_CONTAINS(keyword, r'(?i)\bdiptyque\b')                                  then 'Diptyque'
                when REGEXP_CONTAINS(keyword, r'(?i)\bdkny\b')                                      then 'DKNY'
                when REGEXP_CONTAINS(keyword, r'(?i)\bgivenchy\b')                                  then 'Givenchy'
                when REGEXP_CONTAINS(keyword, r'(?i)\bguerlain\b')                                  then 'Guerlain'
                when REGEXP_CONTAINS(keyword, r'(?i)\bjuliette\s+has\s+a\s+gun\b')                  then 'Juliette Has a Gun'
                when REGEXP_CONTAINS(keyword, r'(?i)\binitio\b')                                    then 'Initio Parfums Privés'
                when REGEXP_CONTAINS(keyword, r'(?i)\blanc(?:ô|o)me\b')                             then 'Lancôme'
                when REGEXP_CONTAINS(keyword, r'(?i)\blouis\s+vuitton\b')                           then 'Louis Vuitton'
                when REGEXP_CONTAINS(keyword, r'(?i)\bcedrat\s+boise(?:\s+intense)?\b')             then 'Mancera'
                when REGEXP_CONTAINS(keyword, r'(?i)\bmaison\s+margiela\b')                         then 'Maison Margiela'
                when REGEXP_CONTAINS(keyword, r'(?i)\bmontale\b')                                   then 'Montale'
                when REGEXP_CONTAINS(keyword, r'(?i)\bparfums\s+de\s+marly\b')                      then 'Parfums de Marly'
                when REGEXP_CONTAINS(keyword, r'(?i)\bvalentino\b')                                 then 'Valentino'
                when REGEXP_CONTAINS(keyword, r'(?i)\bescentric molecules\b')                       then 'Escentric Molecules'
                when REGEXP_CONTAINS(keyword, r'(?i)\blady gaga\b')                                 then 'Lady Gaga'
                when REGEXP_CONTAINS(keyword, r'(?i)\btiffany\b')                                   then 'Tiffany & Co.'
                when REGEXP_CONTAINS(keyword, r'(?i)\b(beckham|lauder)\b.*\b(lauder|beckham)\b')    then 'Estee Lauder x Victoria Beckham Beauty'
                when REGEXP_CONTAINS(keyword, r'(?i)\best(?:ée?|ee)\s+lauder\b')                    then 'Estee Lauder'

                when
                        REGEXP_CONTAINS(
                            keyword,
                            r'\bvictoria\s*beckham\b|\bv\s*beckham\b|\bvb\b|\bvic\s*beckham\b|\bvictoriabeckham\b|\bvictoria\s*beauty\b|\bbeckham\b')
                    or
                        REGEXP_CONTAINS(
                            keyword,
                            r'\bintimately\s*beckham\b|\bsatin\s*kajal\b|\bbaby\s*blade\b|\blid\s*lustre\b|\bcontour\s*stylus\b|\bsuite\s*302\b|\breflect\s*highlighter\s*stick\b|\bsatin\s*kajal\s*liner\b|\bportofino\s*97\b|\bjump\b|\bsway\b|\bpout\b|\btwist\b|\bpose\b|\bposh\b')

                    then 'Victoria Beckham Beauty'

                else Null
            end                                                                         as brand_name,

           *

        from entity_keyword_last_state
    ),

    group_id_with_brand_name_final as (
        select
            case
                when
                        brand_name like
                            '%Estee Lauder x Victoria Beckham Beauty%'

                    or  (
                                brand_name like '%Estee Lauder%'
                            and brand_name like '%Victoria Beckham Beauty%'
                        )

                then 'Estee Lauder x Victoria Beckham Beauty'

                when
                        brand_name like '%Estee Lauder%'
                    and brand_name like '%Aerin%' then 'Aerin'

                when brand_name is null then 'non-brand'
                else brand_name

            end                                                                         as brand_name,

            * except (brand_name)

        from group_id_extracted_brand_raw
    )

-- final
        select * from group_id_with_brand_name_final