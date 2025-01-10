
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
	TO_TIMESTAMP(so.updated_at) AS row_eff_time,
	'Active' AS current_row_indicator,
	TO_TIMESTAMP(so.updated_at) AS updated_at
FROM staging.stg_orders so
INNER JOIN sales.dim_sku dp
ON so.sku_id = dp.sku_bk
AND (
	so.product_name <> dp.product_name
	OR so."size" <> dp."size"
	OR so.color <> dp.color
	OR so.price <> dp.price
	OR so.cogs <> dp.cogs
)
;

UPDATE sales.dim_sku ds
SET row_exp_time = new_data.new_exp_time,
	current_row_indicator = 'Expired'
FROM (
	WITH cte AS (
		SELECT 
			ds.sku_id,
			ds.sku_bk,
			ds.row_eff_time,
			ds.row_exp_time,
			LAG(ds.row_eff_time) OVER (PARTITION BY ds.sku_bk ORDER BY ds.row_eff_time DESC) AS next_eff_time
		FROM sales.dim_sku ds
		INNER JOIN staging.stg_orders so
		ON ds.sku_bk = so.sku_id
	)
	SELECT 
		sku_id,
		row_eff_time,
		row_exp_time,
		next_eff_time AS new_exp_time,
		COALESCE(row_exp_time, '9999-12-31'::timestamp)
	FROM cte
	WHERE COALESCE(row_exp_time, '9999-12-31'::timestamp) <> next_eff_time
	AND row_exp_time IS NULL
) AS new_data
WHERE ds.sku_id = new_data.sku_id
;
