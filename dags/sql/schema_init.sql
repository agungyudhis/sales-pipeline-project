CREATE SCHEMA synthetic;
CREATE SCHEMA staging;
CREATE SCHEMA sales;
CREATE SCHEMA website;

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
	location_id serial REFERENCES public.dim_location (location_id),
	customer_name text,
	row_eff_time timestamp,
	row_exp_time timestamp,
	current_row_indicator text,
	updated_at timestamp
);

CREATE TABLE sales.fct_sales (
	line_item_id char(26) primary key,
	order_id char(26),
	customer_id serial REFERENCES sales.dim_customer (customer_id),
	product_id serial REFERENCES sales.dim_sku (sku_id),
	location_id serial REFERENCES public.dim_location (location_id),
	order_date_id int4,
	order_time timestamp,
	quantity int4,
	revenue numeric,
	net_sales numeric,
	gross_profit numeric,
	created_at timestamp,
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
