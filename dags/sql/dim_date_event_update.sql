UPDATE public.dim_date
SET 
	is_holiday = stg.is_national_holiday,
	event_name = stg.event_name
FROM staging.stg_events stg
WHERE date_id = NULLIF(stg.event_date, '')::int4;

DROP TABLE staging.stg_events;
