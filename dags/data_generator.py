import time
from datetime import datetime
from urllib.parse import quote_plus

import sqlalchemy
from airflow.models import Variable

def growth_factor(
    now: datetime,
    start_date: datetime,
    growth_rate: float
):
    unix_now = time.mktime(now.timetuple())
    unix_start_date = time.mktime(start_date.timetuple())
    unix_year_unit = 31556926
    return ((unix_now - unix_start_date) / unix_year_unit) * (1 + growth_rate)

def generate_orders(start: datetime, end: datetime):
    import json

    import numpy as np
    import polars as pl
    from ulid import ULID

    # Load postgres db credentials from airflow variables
    DB_CREDENTIALS = json.loads(Variable.get('db_credentials'))

    DB_HOST = DB_CREDENTIALS['host']
    DB_PORT = int(DB_CREDENTIALS['port'])
    DB_USER = DB_CREDENTIALS['user']
    DB_PASSWORD = DB_CREDENTIALS['password']

    # Const for data generator
    OPENING_DATE = datetime(2022, 1, 1)     # Store opening date (for growth rate calculation)
    INITIAL_CUST_MEAN = 2                   # Poisson lambda rate for number of customer at opening time
    ANNUAL_GROWTH_RATE = 0.1                # Simple annual growth rate
    PRODUCT_PROB = 0.8
    QUANTITY_PROB = 0.8
    
    # Convert datetime to float (unix timestamp)
    unix_start = time.mktime(start.timetuple())
    unix_end = time.mktime(end.timetuple())
    
    # Calculate growth multiplier for current or `start` time
    growth_multiplier = growth_factor(start, OPENING_DATE, ANNUAL_GROWTH_RATE)
    
    # Create sqlsalchemy engine to connect the postgres db
    engine = sqlalchemy.create_engine(f'postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/warehouse')
    
    # Randomize number of customers based on initial customer rate multiplied by growth multiplier
    num_of_cust = np.random.poisson(INITIAL_CUST_MEAN * growth_multiplier)
    
    # Connect to db
    with engine.connect() as connection:
        # Customer data sampling using RANDOM() and LIMIT
        customers = pl.read_database(
            query="SELECT * FROM synthetic.customers ORDER BY RANDOM() LIMIT :value;",
            execute_options={
                "value": int(num_of_cust)
            },
            connection=connection
        )
        
        # Fetch all products
        products_data = pl.read_database(
            query="SELECT * FROM synthetic.products;",
            connection=connection
        )

    # Add `order_id` and random items to customers data
    orders = customers.with_columns(
        pl.col('id').alias('customer_id'),
        pl.lit(np.random.uniform(unix_start, unix_end, customers.shape[0])).cast(pl.Int32).alias('order_time'),
        pl.lit(np.random.geometric(PRODUCT_PROB, customers.shape[0])).alias('product_count')
    ).with_columns(
        pl.col('order_time').map_elements(lambda x: str(ULID.from_timestamp(x)), return_dtype=pl.String).alias('order_id'),
        pl.col('product_count').map_elements(
            lambda x: 
                products_data.sample(x).to_struct(), 
                return_dtype=pl.List(pl.Struct(products_data.schema))
            ).alias('items')
    ).explode('items')

    # Randomize quantity and add `line_item_id`
    orders = orders.with_columns(
        pl.col('order_time').map_elements(lambda x: str(ULID.from_timestamp(x)), return_dtype=pl.String).alias('line_item_id'),
        pl.lit(np.random.geometric(QUANTITY_PROB, orders.shape[0])).alias('quantity')
    ).unnest('items').with_columns(
        pl.struct(pl.col(['line_item_id', 'sku_id', 'product_id', 'product_name', 'size', 'color', 'price', 'cogs', 'quantity'])).alias('item')
    ).select(
        pl.col(['order_id', 'customer_id', 'order_time', 'name', 'latitude', 'longitude', 'place', 'country', 'item'])
    ).group_by('order_id').agg(
        pl.col('customer_id').last(),
        pl.col('order_time').last(),
        pl.col('name').last(),
        pl.col('latitude').last(),
        pl.col('longitude').last(),
        pl.col('place').last(),
        pl.col('country').last(),
        pl.col('item').explode().alias('item_list'),
    ).sort('order_time')
    
    # Convert the data to python dictionary
    return {
        "order_count": orders.shape[0],
        "order_date": datetime.today().strftime(r"%Y-%m-%d"),
        "orders": orders.to_dicts()
    }


def generate_traffic_data(start: datetime, end: datetime):
    import polars as pl
    import numpy as np
    from ulid import ULID

    # Const for data generator
    OPENING_DATE = datetime(2019, 1, 1) # Web accessible date (for growth rate calculation)
    INITIAL_VISITOR_MEAN = 10           # Poisson lambda for number of visitors at opening time
    ANNUAL_GROWTH_RATE = 0.1            # Simple annual growth rate
    PAGE_VIEWS_MEAN = 3
    SESSION_DURATION_MEAN = 10
    CLICKS_MEAN = 10
    TRANSACTION_MEAN = 0.2
    TRAFFIC_SOURCE_LIST = ['Direct', 'Social', 'Paid', 'Referral', 'Organic']
    REFERRAL_SOURCE_LIST = ['m.facebook.com', 'youtube.com', 'instagram.com', 'x.com']
    DEVICE_LIST = ['Desktop', 'Mobile']
    OS_LIST = {
        'Desktop': ['Windows', 'iOS', 'Ubuntu', 'ChromeOS'], 
        'Mobile': ['Android', 'iOS']
    }
    BROWSER_LIST = ['Chrome', 'Edge', 'Brave', 'Safari', 'Firefox']
    
    # Convert datetime to float (unix timestamp)
    unix_start = time.mktime(start.timetuple())
    unix_end = time.mktime(end.timetuple())
    
    # Calculate growth multiplier
    growth_multiplier = growth_factor(start, OPENING_DATE, ANNUAL_GROWTH_RATE)
    sample_size = int(round(np.random.poisson(INITIAL_VISITOR_MEAN) * growth_multiplier, 0))
    
    web_traffic = {
        "visit_time": np.random.uniform(unix_start, unix_end, sample_size),
        "page_views": np.random.poisson(PAGE_VIEWS_MEAN, sample_size),
        "session_duration": np.random.exponential(SESSION_DURATION_MEAN, sample_size),
        "clicks": np.random.poisson(CLICKS_MEAN, sample_size),
        "traffic_source": np.random.choice(TRAFFIC_SOURCE_LIST, sample_size),
        "device": np.random.choice(DEVICE_LIST, sample_size),
        "browser": np.random.choice(BROWSER_LIST, sample_size),
        "transactions": np.random.poisson(TRANSACTION_MEAN, sample_size)
    }
    traffic_data = pl.DataFrame(web_traffic).with_columns(
        pl.col('visit_time').cast(pl.Int32),
        pl.col('visit_time').map_elements(lambda x: str(ULID.from_timestamp(x)), return_dtype=pl.String).alias('session_id'),
        pl.col('traffic_source').map_elements(
            lambda x: np.random.choice(REFERRAL_SOURCE_LIST) if x == "Referral" else None, 
            return_dtype=pl.String
        ).alias('referral_source'),
        pl.col('device').map_elements(
            lambda x: np.random.choice(OS_LIST[x]), return_dtype=pl.String
        ).alias('os')
    )
    return {
        "visitor_count": traffic_data.shape[0],
        "visit_date": datetime.today().strftime(r"%Y-%m-%d"),
        "visitors": traffic_data.to_dicts()
    }