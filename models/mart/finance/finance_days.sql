-- models/mart/finance_days.sql
/*
  Mart modeli: finance_days
  Açıklama: Günlük düzeyde finansal metrikler (işlem sayısı, gelir, ort. sepet, operasyonel marj, maliyetler, toplam miktar)
  Kaynaklar: int_orders_margin, int_orders_operational
*/

with
orders as (
  select
    om.orders_id,
    om.date_date,
    cast(om.revenue as float64)     as revenue,
    cast(om.quantity as int64)      as quantity,
    cast(om.purchase_cost as float64) as purchase_cost,
    cast(om.margin as float64)      as margin
  from {{ ref('int_orders_margin') }} om
),

ops as (
  select
    io.orders_id,
    io.date_date,
    cast(io.shipping_fee as float64) as shipping_fee,
    cast(io.logcost as float64)      as logcost,
    cast(io.ship_cost as float64)    as ship_cost,
    cast(io.net_margin as float64)   as net_margin
  from {{ ref('int_orders_operational') }} io
)

select
  o.date_date                                         as date_date,
  count(distinct o.orders_id)                         as total_orders,
  ROUND(sum(o.revenue), 2)                            as total_revenue,
  ROUND(safe_divide(sum(o.revenue), nullif(count(distinct o.orders_id),0)), 2) as avg_basket,
  ROUND(sum(coalesce(ops.net_margin, 0.0)), 2)       as operational_margin,
  ROUND(sum(o.purchase_cost), 2)                      as total_purchase_cost,
  ROUND(sum(coalesce(ops.shipping_fee, 0.0)), 2)     as total_shipping_fees,
  ROUND(sum(coalesce(ops.logcost, 0.0)), 2)          as total_log_costs,
  ROUND(sum(coalesce(ops.ship_cost, 0.0)), 2)        as total_ship_costs,
  sum(o.quantity)                                     as total_quantity_sold

from orders o
left join ops
  on o.orders_id = ops.orders_id
  and o.date_date = ops.date_date

group by o.date_date
order by o.date_date
