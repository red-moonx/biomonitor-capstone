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
        cast(gbifid as string) as gbif_id,
        cast(occurrenceid as string) as occurrence_id,

        -- taxonomic info
        species,
        scientificname as scientific_name,
        kingdom,
        phylum,
        class,
        "order" as species_order, -- order is a reserved word
        family,
        genus,
        taxonrank as taxon_rank,

        -- spatial info
        cast(decimallatitude as float64) as latitude,
        cast(decimallongitude as float64) as longitude,
        countrycode as country_code,
        locality,

        -- temporal info
        safe_cast(eventdate as timestamp) as occurrence_timestamp,
        cast(year as int64) as occurrence_year,
        cast(month as int64) as occurrence_month,

        -- status info
        occurrencestatus as occurrence_status,
        basisofrecord as basis_of_record,
        recordedby as recorded_by

    from source
    where decimallatitude is not null 
      and decimallongitude is not null
      and species is not null -- Filter out records without species name
      and countrycode is not null -- Filter out records without country code
)

, deduplicated as (
    select 
        *,
        row_number() over (
            partition by gbif_id 
            order by occurrence_timestamp desc
        ) as row_num
    from renamed
    where occurrence_timestamp is not null
)

select 
    * except(row_num)
from deduplicated
where row_num = 1
