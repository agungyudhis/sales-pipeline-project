INSERT INTO public.dim_date (
    date_id,
    date,
    full_date_description,
    year,
    quarter,
    month,
    day,
    day_of_week,
    calendar_day_of_week,
    calendar_month,
    is_weekend
)
SELECT * FROM staging.stg_dim_date;

INSERT INTO public.dim_date (
    date_id,
    date,
    full_date_description,
    year,
    quarter,
    month,
    day,
    day_of_week,
    calendar_day_of_week,
    calendar_month,
    is_weekend,
    event_name,
    is_holiday
)
VALUES 
    (-1, NULL, 'Not Entered', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (-2, NULL, 'Error', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

DROP TABLE staging.stg_dim_date;