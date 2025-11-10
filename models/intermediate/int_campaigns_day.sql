-- models/intermediate/int_campaigns_day.sql

with daily as (
    select *
    from {{ ref('int_campaigns') }}  -- int_campaigns modelini referans al
)

select *
from daily
order by date_date desc   -- Tarihe göre ters kronolojik sıralama
