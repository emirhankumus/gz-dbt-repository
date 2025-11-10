-- models/mart/finance/finance_campaigns_month.sql
/*
  Mart modeli: finance_campaigns_month
  Açıklama: Aylık bazda finansal ve reklam metrikleri.
  Kaynak: finance_campaigns_day modeli.
*/

WITH monthly_data AS (
    SELECT
        DATE_TRUNC(date_date, MONTH) AS datemonth,
        SUM(ads_margin) AS ads_margin,
        ROUND(AVG(average_basket), 2) AS average_basket,
        SUM(operational_margin) AS operational_margin,
        SUM(ads_cost) AS ads_cost,
        SUM(ads_impression) AS ads_impression,
        SUM(ads_clicks) AS ads_clicks,
        SUM(quantity) AS quantity,
        SUM(revenue) AS revenue,
        SUM(purchase_cost) AS purchase_cost,
        SUM(margin) AS margin,
        SUM(shipping_fee) AS shipping_fee,
        SUM(log_cost) AS log_cost,
        SUM(ship_cost) AS ship_cost
    FROM {{ ref('finance_campaigns_day') }}  -- artık doğrudan day tablosu
    GROUP BY datemonth
    ORDER BY datemonth DESC
)

SELECT * FROM monthly_data






