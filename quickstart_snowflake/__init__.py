from dagster import Definitions, load_assets_from_modules

from .assets import init_sales
from .resources import snowflake

sales_assets = load_assets_from_modules([init_sales])

defs = Definitions(
    assets=sales_assets, 
    resources={"snowflake": snowflake}
)