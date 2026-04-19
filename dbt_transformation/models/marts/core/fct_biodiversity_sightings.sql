{{ config(
    materialized='table',
    partition_by={
      "field": "occurrence_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['species', 'country_code']
) }}

with sightings as (
    select * from {{ ref('stg_gbif_occurrences') }}
)

select
    *,
    -- 1. Calculate Season based on the month
    case 
        when occurrence_month in (12, 1, 2) then 'Winter'
        when occurrence_month in (3, 4, 5) then 'Spring'
        when occurrence_month in (6, 7, 8) then 'Summer'
        when occurrence_month in (9, 10, 11) then 'Autumn'
        else 'Unknown'
    end as occurrence_season,

    -- 2. Define Conservation Severity for intuitive dashboarding
    case 
        when iucn_red_list_category in ('CR', 'EN') then 'High Priority'
        when iucn_red_list_category in ('VU', 'NT') then 'Medium Monitoring'
        when iucn_red_list_category = 'LC' then 'Low Concern'
        else 'Data Deficient'
    end as conservation_severity

from sightings
