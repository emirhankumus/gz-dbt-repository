WITH orders_margin AS (
    SELECT *
    FROM {{ ref('int_orders_margin') }}
),
shipping AS (
    SELECT *
    FROM {{ ref('stg_raw__ship') }}
)

SELECT
    o.orders_id,
    o.date_date,
    o.margin,
    s.shipping_fee,
    s.logcost,
    s.ship_cost,
    -- Net marjı hesaplıyoruz
    o.margin - (s.shipping_fee + s.logcost + s.ship_cost) AS net_margin
FROM orders_margin o
LEFT JOIN shipping s
    ON o.orders_id = s.orders_id

