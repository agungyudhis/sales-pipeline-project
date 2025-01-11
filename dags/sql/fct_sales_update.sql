INSERT INTO sales.fct_sales (
    line_item_id,
    order_id,
    sku_id,
    customer_id,
    location_id,
    order_date_id,
    order_time,
    quantity,
    revenue,
    gross_profit,
    created_at,
    updated_at
)
WITH new_data AS (
    SELECT DISTINCT
        so.line_item_id,
        so.order_id,
        ds.sku_id,
        dc.customer_id,
        dl.location_id,
        TO_CHAR(TO_TIMESTAMP(so.order_time), 'YYYYMMDD')::int4 AS order_date_id,
        TO_TIMESTAMP(so.order_time) AS order_time,
        so.quantity,
        so.price * so.quantity AS revenue,
        (so.price - so.cogs) * so.quantity AS gross_profit,
        TO_TIMESTAMP(so.updated_at) AS created_at,
        TO_TIMESTAMP(so.updated_at) AS updated_at
    FROM staging.stg_orders so
    LEFT JOIN public.dim_location dl
    ON
        so.place = dl.place
        AND so.country = dl.country
        AND so.latitude = dl.latitude
        AND so.longitude = dl.longitude
    LEFT JOIN sales.dim_customer dc
    ON
        so.customer_id = dc.customer_bk
        AND so."name" = dc.customer_name
        AND dl.location_id = dc.location_id
        AND dc.current_row_indicator = 'Active'
    LEFT JOIN sales.dim_sku ds
    ON
        so.sku_id = ds.sku_bk
        AND so.product_id = ds.product_id
        AND so.product_name = ds.product_name
        AND so."size" = ds."size"
        AND so.color = ds.color
        AND so.price = ds.price
        AND so.cogs = ds.cogs
        AND ds.current_row_indicator = 'Active'
)
SELECT * FROM new_data
ON CONFLICT (line_item_id) DO UPDATE
SET
    order_id = EXCLUDED.order_id,
    sku_id = EXCLUDED.sku_id,
    customer_id = EXCLUDED.customer_id,
    location_id = EXCLUDED.location_id,
    order_date_id = EXCLUDED.order_date_id,
    order_time = EXCLUDED.order_time,
    quantity = EXCLUDED.quantity,
    revenue = EXCLUDED.revenue,
    gross_profit = EXCLUDED.gross_profit,
    updated_at = EXCLUDED.updated_at
;