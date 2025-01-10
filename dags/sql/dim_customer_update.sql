
INSERT INTO sales.dim_customer (
	customer_bk,
	customer_name,
	location_id,
	row_eff_time,
	current_row_indicator,
	updated_at
)
SELECT DISTINCT
	so.customer_id AS customer_bk,
	so.name AS customer_name,
	dl.location_id,
	TO_TIMESTAMP(so.updated_at) AS row_eff_time,
	'Active' AS current_row_indicator,
	TO_TIMESTAMP(so.updated_at) AS updated_at
FROM staging.stg_orders so
LEFT JOIN public.dim_location dl
ON so.place = dl.place
	AND so.country = dl.country
	AND so.latitude = dl.latitude
	AND so.longitude = dl.longitude
INNER JOIN sales.dim_customer dc 
ON so.customer_id = dc.customer_bk
	AND (
		dl.location_id <> dc.location_id
		OR so.name <> dc.customer_name
	)
;

UPDATE sales.dim_customer dc 
SET row_exp_time = new_data.new_exp_time,
	current_row_indicator = 'Expired'
FROM (
	WITH cte AS (
		SELECT 
			dc.customer_id,
			dc.customer_bk,
			dc.row_eff_time,
			dc.row_exp_time,
			LAG(dc.row_eff_time) OVER (PARTITION BY dc.customer_bk ORDER BY dc.row_eff_time DESC) AS next_eff_time
		FROM sales.dim_customer dc
		INNER JOIN staging.stg_orders so
		ON dc.customer_bk = so.customer_id
	)
	SELECT 
		customer_id,
		row_eff_time,
		row_exp_time,
		next_eff_time AS new_exp_time,
		COALESCE(row_exp_time, '9999-12-31'::timestamp)
	FROM cte
	WHERE COALESCE(row_exp_time, '9999-12-31'::timestamp) <> next_eff_time
	AND row_exp_time IS NULL
) AS new_data
WHERE dc.customer_id = new_data.customer_id
;