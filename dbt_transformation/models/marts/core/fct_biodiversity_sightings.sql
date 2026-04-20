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
    -- Calculate Season based on the month
    case 
        when occurrence_month in (12, 1, 2) then 'Winter'
        when occurrence_month in (3, 4, 5) then 'Spring'
        when occurrence_month in (6, 7, 8) then 'Summer'
        when occurrence_month in (9, 10, 11) then 'Autumn'
        else 'Unknown'
    end as occurrence_season

from sightings
