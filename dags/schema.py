from typing import List

from pydantic import BaseModel

class Item(BaseModel):
    line_item_id: str
    sku_id: str
    product_id: str
    product_name: str
    size: str
    color: str
    price: float
    cogs: float
    quantity: int

class Orders(BaseModel):
    order_id: str
    customer_id: str
    order_time: int
    name: str = None
    latitude: float = None
    longitude: float = None
    place: str = None
    country: str = None
    item_list: List[Item]
    
class Visitors(BaseModel):
    session_id: str
    visit_time: int
    page_views: int
    session_duration: float
    clicks: int = 0
    traffic_source: str = "Unknown"
    device: str = "Unknown"
    browser: str = "Unknown"
    transaction: int = 0