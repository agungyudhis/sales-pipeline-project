INSERT INTO sales.dim_sku (
	sku_bk,
	product_id,
	product_name,
	"size",
	color,
	price,
	cogs,
	row_eff_time,
	current_row_indicator,
	updated_at
)
SELECT 
	so.sku_id AS sku_bk,
	so.product_id,
	so.product_name,
	so."size",
	so.color,
	so.price,
	so.cogs,
	MAX(TO_TIMESTAMP(so.updated_at)) AS row_eff_time,
	'Active' AS current_row_indicator,
	MAX(TO_TIMESTAMP(so.updated_at)) AS updated_at
FROM staging.stg_orders so
LEFT JOIN sales.dim_sku ds
ON so.sku_id = ds.sku_bk
WHERE (
	so.product_name IS DISTINCT FROM ds.product_name
	OR so."size" IS DISTINCT FROM ds."size"
	OR so.color IS DISTINCT FROM ds.color
	OR so.price IS DISTINCT FROM ds.price
	OR so.cogs IS DISTINCT FROM ds.cogs
)
GROUP BY so.sku_id, so.product_id, so.product_name, so."size", so.color, so.price, so.cogs
;

UPDATE sales.dim_sku ds
SET row_exp_time = new_data.new_exp_time,
	current_row_indicator = 'Expired'
FROM (
	WITH cte AS (
		SELECT 
			ds.sku_id,
			ds.row_exp_time,
			LAG(ds.row_eff_time) OVER (PARTITION BY ds.sku_bk ORDER BY ds.row_eff_time DESC) AS next_eff_time
		FROM sales.dim_sku ds
		INNER JOIN (
			SELECT DISTINCT
				sku_id,
				product_id,
				product_name,
				size,
				color,
				price,
				cogs
			FROM staging.stg_orders
		) so
		ON ds.sku_bk = so.sku_id
	)
	SELECT 
		sku_id,
		next_eff_time AS new_exp_time
	FROM cte
	WHERE row_exp_time IS DISTINCT FROM next_eff_time
		AND row_exp_time IS NULL
) AS new_data
WHERE ds.sku_id = new_data.sku_id
;