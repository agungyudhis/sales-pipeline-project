CREATE SCHEMA synthetic;
CREATE SCHEMA staging;
CREATE SCHEMA sales;
CREATE SCHEMA website;

CREATE TABLE staging.stg_orders (
	order_id text NOT NULL,
	customer_id text NULL,
	order_time int4 NULL,
	"name" text NULL,
	latitude numeric NULL,
	longitude numeric NULL,
	place text NULL,
	country text NULL,
	line_item_id text NULL,
	sku_id text NULL,
	product_id text NULL,
	product_name text NULL,
	"size" text NULL,
	color text NULL,
	price numeric NULL,
	cogs numeric NULL,
	quantity int4 NULL
);

CREATE TABLE staging.stg_traffic (
	visit_time int4 NULL,
	page_views int4 NULL,
	session_duration numeric NULL,
	clicks int4 NULL,
	traffic_source text NULL,
	device text NULL,
	browser text NULL,
	transactions int4 NULL,
	session_id text NULL,
	referral_source text NULL,
	os text NULL
);

CREATE TABLE sales.dim_product (
	product_pk serial primary key,
	product_bk char(6),
	product_name varchar(32),
	size varchar(16),
	color varchar(32),
	price float,
	cogs float
);

CREATE TABLE sales.dim_customer (
	customer_pk serial primary key,
	customer_bk char(11),
	customer_name text,
	country text,
	place text,
	longitude numeric,
	latitude numeric
);

CREATE TABLE sales.fct_sales (
	line_item_id char(26) primary key,
	order_id char(26),
	customer_pk serial REFERENCES sales.dim_customer (customer_pk),
	product_pk serial REFERENCES sales.dim_product (product_pk),
	order_date char(12),
	order_time timestamp,
	quantity int4,
	revenue numeric,
	net_sales numeric,
	gross_profit numeric
);

