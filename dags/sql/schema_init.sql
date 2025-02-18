CREATE SCHEMA staging;
CREATE SCHEMA sales;
CREATE SCHEMA website;

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

-- SCD Type 2: Add new row
CREATE TABLE sales.dim_sku (
	sku_id serial primary key,
	sku_bk char(8),
	product_id char(6),
	product_name text,
	size text,
	color text,
	price float,
	cogs float,
	row_eff_time timestamp,
	row_exp_time timestamp,
	current_row_indicator text,
	updated_at timestamp
);

-- SCD Type 0: Retain original
CREATE TABLE public.dim_location (
	location_id serial primary key,
	country text,
	place text,
	longitude numeric,
	latitude numeric
);

-- Create constraint unique index
CREATE UNIQUE INDEX idx_dim_location_unique
ON public.dim_location (country, place, longitude, latitude);

-- SCD Type 2: Add new row
CREATE TABLE sales.dim_customer (
	customer_id serial primary key,
	customer_bk char(11),
	customer_name text,
	row_eff_time timestamp,
	row_exp_time timestamp,
	current_row_indicator text,
	updated_at timestamp
);

-- SCD Type 0: Retain original
CREATE TABLE website.dim_device (
	device_id serial primary key,
	device_name text,
	os text,
	browser text
);

-- Create constraint unique index
CREATE UNIQUE INDEX idx_dim_device_unique
ON website.dim_device (device_name, os, browser);

-- SCD Type 0: Retain original
CREATE TABLE website.dim_channel (
	channel_id serial primary key,
	traffic_source text,
	referral_source text
);

-- Create constraint unique index
CREATE UNIQUE INDEX idx_dim_channel_unique
ON website.dim_channel (traffic_source, referral_source);

-- Sales fact table
CREATE TABLE sales.fct_sales (
	line_item_id char(26) primary key,
	order_id char(26),
	sku_id serial REFERENCES sales.dim_sku (sku_id),
	customer_id serial REFERENCES sales.dim_customer (customer_id),
	location_id serial REFERENCES public.dim_location (location_id),
	order_date_id int4 REFERENCES public.dim_date (date_id),
	order_time timestamp,
	quantity int4,
	revenue numeric,
	gross_profit numeric,
	created_at timestamp,
	updated_at timestamp
);

-- Website traffic fact table
CREATE TABLE website.fct_traffic (
	session_id char(26) primary key,
	channel_id serial REFERENCES website.dim_channel (channel_id),
	device_id serial REFERENCES website.dim_device (device_id),
	visit_date_id int4 REFERENCES public.dim_date (date_id),
	visit_time timestamp,
	page_views int4,
	session_duration numeric,
	clicks int4,
	transactions int4,
	created_at timestamp,
	updated_at timestamp
);

-- Periodic summary fact table
CREATE TABLE public.fct_periodic_summary (
	date_id int4 primary key REFERENCES public.dim_date (date_id),
	unique_orders int4,
	unique_customers int4,
	quantity int4,
	revenue int4,
	gross_profit int4,
	page_views int4,
	num_sessions int4,
	session_duration int4,
	clicks int4,
	transactions int4
);