WITH sales AS (
    SELECT *
    FROM {{ ref('stg_raw__sales') }}
),
product AS (
    SELECT *
    FROM {{ ref('stg_raw__product') }}
)

SELECT
    s.orders_id,
    s.product_id,
    s.quantity,
    p.purchase_price,
    s.revenue,
    s.date_date,
    s.quantity * p.purchase_price AS purchase_cost,
     s.revenue - (s.quantity * p.purchase_price) AS margin
FROM sales s
JOIN product p
    ON s.product_id = p.products_id
