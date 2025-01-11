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
	MAX(TO_TIMESTAMP(so.updated_at)) AS row_eff_time,
	'Active' AS current_row_indicator,
	MAX(TO_TIMESTAMP(so.updated_at)) AS updated_at
FROM staging.stg_orders so
LEFT JOIN public.dim_location dl
ON so.place = dl.place
	AND so.country = dl.country
	AND so.latitude = dl.latitude
	AND so.longitude = dl.longitude
LEFT JOIN sales.dim_customer dc 
ON so.customer_id = dc.customer_bk
WHERE (
		dl.location_id is distinct from dc.location_id
		OR so.name is distinct from dc.customer_name
	)
GROUP BY so.customer_id, so.name, dl.location_id
;

UPDATE sales.dim_customer dc 
SET row_exp_time = new_data.new_exp_time,
	current_row_indicator = 'Expired'
FROM (
	WITH cte AS (
		SELECT
			dc.customer_id,
			dc.row_exp_time,
			LAG(dc.row_eff_time) OVER (PARTITION BY dc.customer_bk ORDER BY dc.row_eff_time DESC) AS next_eff_time
		FROM sales.dim_customer dc
		INNER JOIN (
			SELECT DISTINCT
				customer_id,
				name
			FROM staging.stg_orders
		) so
		ON dc.customer_bk = so.customer_id
	)
	SELECT 
		customer_id,
		next_eff_time AS new_exp_time
	FROM cte
	WHERE row_exp_time IS DISTINCT FROM next_eff_time
	AND row_exp_time IS NULL
) AS new_data
WHERE dc.customer_id = new_data.customer_id
;