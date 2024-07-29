-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +ga4_monthly_users_by_country -t dbt_transform

{{ config(materialized='table') }}

with
    -- input
    source_ga4_demographic_country_report as (select * from {{ source('sources_combined', 'source_ga4_demographic_country_report') }}),

    map_by_region as (
        select
            date_trunc(parse_date('%Y%m%d', date), month)       as month,
            
            {{ map_country_to_region('country') }}              as region,

            round(sum(totalUsers), 2)                           as totalUsers,
            round(sum(newUsers), 2)                             as newUsers,
            round(sum(totalUsers - newUsers), 2)                as returnUsers
        
        from source_ga4_demographic_country_report
        
        group by
            month,
            region
    )

-- final

select * from map_by_region