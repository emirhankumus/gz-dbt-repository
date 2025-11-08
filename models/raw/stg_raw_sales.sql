SELECT
    *
FROM {{ source('raw', 'sales')}}