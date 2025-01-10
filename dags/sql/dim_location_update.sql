INSERT INTO public.dim_location (
	place,
	country,
	latitude,
	longitude
)
SELECT DISTINCT
	place,
	country,
	latitude,
	longitude
FROM staging.stg_orders
ON CONFLICT (place, country, latitude, longitude) DO NOTHING
;