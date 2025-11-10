-- models/staging/raw/stg_raw__bing.sql

with source as (
    select *
    from {{ source('raw', 'bing') }}
),

renamed as (
    select
        date_date,
        paid_source,
        campaign_key,
        camPGN_name as campaign_name,          -- kolon adı değişti
        cast(ads_cost as float64) as ads_cost, -- tip değişimi
        impression,
        click
    from source
)

select *
from renamed
