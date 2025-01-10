INSERT INTO website.dim_device (
	device_name,
	os,
	browser
)
SELECT DISTINCT
	device,
	os,
	browser
FROM staging.stg_traffic
ON CONFLICT (device_name, os, browser) DO NOTHING;