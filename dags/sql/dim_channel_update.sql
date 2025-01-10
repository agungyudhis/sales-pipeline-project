INSERT INTO website.dim_channel (
	traffic_source,
	referral_source
)
SELECT DISTINCT
	traffic_source,
	referral_source
FROM staging.stg_traffic
ON CONFLICT (traffic_source, referral_source) DO NOTHING;