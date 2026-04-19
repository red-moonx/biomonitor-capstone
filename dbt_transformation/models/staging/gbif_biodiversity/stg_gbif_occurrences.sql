{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('gbif_biodiversity', 'raw_mammals') }}
),

renamed as (
    select
        -- identifiers
        cast(gbif_id as string) as gbif_id,
        cast(occurrence_id as string) as occurrence_id,

        -- taxonomic info
        species,
        scientific_name,
        kingdom,
        phylum,
        class,
        "order" as species_order, -- order is a reserved word
        family,
        genus,
        taxon_rank,

        -- spatial info
        cast(decimal_latitude as float64) as latitude,
        cast(decimal_longitude as float64) as longitude,
        country,
        country_code,
        continent,

        -- temporal info
        safe_cast(split(event_date, '/')[offset(0)] as timestamp) as occurrence_timestamp,
        cast(year as int64) as occurrence_year,
        cast(month as int64) as occurrence_month,

        -- conservation info
        iucn_red_list_category,
        occurrence_status,
        basis_of_record

    from source
    where decimal_latitude is not null 
      and decimal_longitude is not null
      and species is not null -- Filter out records without species name
      and country_code is not null -- Filter out records without country code
)

, filtered_timestamps as (
    select * from renamed
    where occurrence_timestamp is not null
),

deduplicated as (
    select 
        *,
        row_number() over (
            partition by gbif_id 
            order by occurrence_timestamp desc
        ) as row_num
    from filtered_timestamps
)

select 
    * except(row_num)
from deduplicated
where row_num = 1
