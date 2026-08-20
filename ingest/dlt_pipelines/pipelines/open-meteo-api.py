import dlt
from dlt.sources.helpers import requests
from dlt.common.typing import TDataItems
@dlt.resource()
def open_meteo_api() -> TDataItems:
    url=
