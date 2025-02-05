INSERT INTO public.fct_periodic_summary (
    date_id,
    unique_orders,
    unique_customers,
    quantity,
    revenue,
    gross_profit,
    page_views,
    num_sessions,
    session_duration,
    clicks,
    transactions
)
WITH new_data AS (
    SELECT 
        TO_CHAR(DATE(fs.order_time), 'YYYYMMDD')::int4 AS date_id,
        COUNT(DISTINCT fs.order_id) AS unique_orders,
        COUNT(DISTINCT fs.customer_id) AS unique_customers,
        SUM(fs.quantity) AS quantity,
        SUM(fs.revenue) AS revenue,
        SUM(fs.gross_profit) AS gross_profit,
        SUM(st.page_views) AS page_views,
        COUNT(DISTINCT session_id) AS num_sessions,
        SUM(st.session_duration) AS session_duration,
        SUM(st.clicks) AS clicks,
        SUM(st.transactions) AS transactions
    FROM sales.fct_sales fs
    INNER JOIN (
        SELECT 
            DISTINCT DATE(TO_TIMESTAMP(order_time)) AS order_date
        FROM staging.stg_orders
    ) AS so
    ON DATE(fs.order_time) = so.order_date
    LEFT JOIN (
        SELECT 
            DATE(TO_TIMESTAMP(visit_time)) AS visit_date,
            session_id,
            page_views,
            session_duration,
            clicks,
            transactions
        FROM staging.stg_traffic
    ) AS st
    ON DATE(fs.order_time) = st.visit_date
    GROUP BY TO_CHAR(DATE(fs.order_time), 'YYYYMMDD')::int4
)
SELECT * FROM new_data
ON CONFLICT (date_id) DO UPDATE
SET
    unique_orders = EXCLUDED.unique_orders,
    unique_customers = EXCLUDED.unique_customers,
    quantity = EXCLUDED.quantity,
    revenue = EXCLUDED.revenue,
    gross_profit = EXCLUDED.gross_profit,
    page_views = EXCLUDED.page_views,
    num_sessions = EXCLUDED.num_sessions,
    session_duration = EXCLUDED.session_duration,
    clicks = EXCLUDED.clicks,
    transactions = EXCLUDED.transactions
;
