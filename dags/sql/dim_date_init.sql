CREATE TABLE IF NOT EXISTS public.dim_date (
    date_id int4 primary key,
    date date,
    full_date_description text DEFAULT 'Not Entered',
    year int2,
    quarter int2,
    month int2,
    day int2,
    day_of_week int2,
    calendar_day_of_week text,
    calendar_month text,
    is_weekend text DEFAULT 'Weekday',
    event_name text DEFAULT '-',
    is_holiday text DEFAULT 'Non-Holiday'
);

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