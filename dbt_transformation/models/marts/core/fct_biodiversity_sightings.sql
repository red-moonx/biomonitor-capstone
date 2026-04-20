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
),

iucn_data as (
    select * from {{ ref('iucn_statuses') }}
)

select
    s.*,
    -- Join with IUCN status, default to 'DD' (Data Deficient) if not found
    coalesce(i.iucn_red_list_category, 'DD') as iucn_red_list_category,
    
    -- Define Conservation Severity for intuitive dashboarding
    case 
        when i.iucn_red_list_category in ('CR', 'EN') then 'High Priority'
        when i.iucn_red_list_category in ('VU', 'NT') then 'Medium Monitoring'
        when i.iucn_red_list_category = 'LC' then 'Low Concern'
        else 'Data Deficient'
    end as conservation_severity,

    -- Calculate Season based on the month
    case 
        when s.occurrence_month in (12, 1, 2) then 'Winter'
        when s.occurrence_month in (3, 4, 5) then 'Spring'
        when s.occurrence_month in (6, 7, 8) then 'Summer'
        when s.occurrence_month in (9, 10, 11) then 'Autumn'
        else 'Unknown'
    end as occurrence_season

from sightings s
left join iucn_data i on s.species = i.species
