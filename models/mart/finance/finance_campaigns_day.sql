WITH campaigns_day AS (
    SELECT *
    FROM {{ ref('int_campaigns_day') }}
),

finance_base AS (
    SELECT *
    FROM {{ ref('finance_days') }}
),

joined AS (
    SELECT
        f.date_date,
        f.operational_margin - c.ads_cost AS ads_margin,
        f.avg_basket AS average_basket,          -- düzeltildi
        f.operational_margin,
        c.ads_cost,
        c.impression AS ads_impression,
        c.click AS ads_clicks,
        f.total_quantity_sold AS quantity,
        f.total_revenue AS revenue,
        f.total_purchase_cost AS purchase_cost,
        f.operational_margin - c.ads_cost AS margin,
        f.total_shipping_fees AS shipping_fee,
        f.total_log_costs AS log_cost,
        f.total_ship_costs AS ship_cost
    FROM finance_base f
    LEFT JOIN campaigns_day c
        ON f.date_date = c.date_date
)

SELECT *
FROM joined
ORDER BY date_date DESC







